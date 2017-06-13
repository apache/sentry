/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.core.common.transport;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pool of transport connections to Sentry servers.
 * The pool caches open connections to multiple Sentry servers,
 * specified in the configuration.
 *
 * When transport pooling is disabled in configuration,
 * creates transports directly and doesn't cache connections.
 */
@ThreadSafe
public final class SentryTransportPool implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryTransportPool.class);

  // Used for logging to identify pool instances. This is only useful for test debugging
  // so we do not preserve thread safety for this field.
  private static int poolId = 0;
  private final int id;

  // True if using Object pool
  private final boolean isPoolEnabled;

  // Load balance between servers if true
  private final boolean doLoadBalancing;

  // List of all known servers
  private final ArrayList<HostAndPort> endpoints;

  // Transport pool which keeps connected transports
  private final KeyedObjectPool<HostAndPort, TTransportWrapper> pool;
  // Source of connected transports
  private final TransportFactory transportFactory;

  // Set when we are closed
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Configure transport pool.
   * <p>
   * The pool accepts the following configuration:
   * <ul>
   *   <li>Maximum total number of objects in the pool</li>
   *   <li>Minimum number of idle objects</li>
   *   <li>Maximum number of idle objects</li>
   *   <li>Minimum time before the object is evicted</li>
   *   <li>Interval between evictions</li>
   * </ul>
   * @param conf Configuration
   * @param transportConfig Configuration interface
   * @param transportFactory Transport factory used to produce transports
   */
  public SentryTransportPool(Configuration conf,
                             SentryClientTransportConfigInterface transportConfig,
                             TransportFactory transportFactory) {

    // This isn't thread-safe, but we don't care - it is only used
    // for debugging when running tests - normal apps use a single pool
    poolId++;
    id = poolId;

    this.transportFactory = transportFactory;
    doLoadBalancing = transportConfig.isLoadBalancingEnabled(conf);
    isPoolEnabled = transportConfig.isTransportPoolEnabled(conf);

    // Get list of server addresses
    String hostsAndPortsStr = transportConfig.getSentryServerRpcAddress(conf);
    int serverPort = transportConfig.getServerRpcPort(conf);
    LOGGER.info("Creating pool for {} with default port {}",
            hostsAndPortsStr, serverPort);
    String[] hostsAndPortsStrArr = hostsAndPortsStr.split(",");
    Preconditions.checkArgument(hostsAndPortsStrArr.length > 0,
            "At least one server should be specified");

    endpoints = new ArrayList<>(hostsAndPortsStrArr.length);
    for(String addr: hostsAndPortsStrArr) {
      HostAndPort endpoint = ThriftUtil.parseAddress(addr, serverPort);
      LOGGER.info("Adding endpoint {}", endpoint);
      endpoints.add(endpoint);
    }

    if (!isPoolEnabled) {
      pool = null;
      LOGGER.info("Connection pooling is disabled");
      return;
    }

    LOGGER.info("Connection pooling is enabled");
    // Set pool configuration based on Configuration settings
    GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

    // Don't limit maximum number of objects in the pool
    poolConfig.setMaxTotal(-1);

    poolConfig.setMinIdlePerKey(transportConfig.getPoolMinIdle(conf));
    poolConfig.setMaxIdlePerKey(transportConfig.getPoolMaxIdle(conf));

    // Do not block when pool is exhausted, throw exception instead
    poolConfig.setBlockWhenExhausted(false);
    poolConfig.setTestOnReturn(true);

    // No limit for total objects in the pool
    poolConfig.setMaxTotalPerKey(transportConfig.getPoolMaxTotal(conf));
    poolConfig.setMinEvictableIdleTimeMillis(transportConfig.getMinEvictableTimeSec(conf));
    poolConfig.setTimeBetweenEvictionRunsMillis(transportConfig.getTimeBetweenEvictionRunsSec(conf));

    // Create object pool
    pool = new GenericKeyedObjectPool<>(new PoolFactory(this.transportFactory, id),
            poolConfig);
  }

  public TTransportWrapper getTransport() throws Exception {
    List<HostAndPort> servers;
    // If we are doing load balancing and there is more then one server,
    // shuffle them before obtaining connection
    if (doLoadBalancing && (endpoints.size() > 1)) {
      servers = new ArrayList<>(endpoints);
      Collections.shuffle(servers);
    } else {
      servers = endpoints;
    }

    // Try to get a connection from one of the pools
    Exception failure = null;
    for(HostAndPort addr: servers) {
      try {
        TTransportWrapper transport =
          isPoolEnabled ?
                pool.borrowObject(addr) :
                transportFactory.getTransport(addr);
        LOGGER.debug("[{}] obtained transport {}", id, transport);
        if (LOGGER.isDebugEnabled() && isPoolEnabled) {
          LOGGER.debug("Currently {} active connections, {} idle connections",
                  pool.getNumActive(), pool.getNumIdle());
        }
        return transport;
      } catch (IllegalStateException e) {
        // Should not happen
        LOGGER.error("Unexpected error from pool {}", id,  e);
        failure = e;
      } catch (Exception e) {
        LOGGER.error("Failed to obtain transport for {}: {}",
                addr, e.getMessage());
        failure = e;
      }
    }
    // Failed to borrow connect to any endpoint
    assert failure != null;
    throw failure;
  }

  /**
   * Return transport to the pool
   * @param transport Open transport
   */
  public void returnTransport(TTransportWrapper transport) {
    if (closed.get()) {
      LOGGER.debug("Returned {} to closed pool", transport);
      transport.close();
      return;
    }
    try {
      if (isPoolEnabled) {
        LOGGER.debug("[{}] returning {}", id, transport);
        pool.returnObject(transport.getAddress(), transport);
      } else {
        LOGGER.debug("Closing {}", transport);
        transport.close();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to return {}", transport, e);
    }
  }

  public void invalidateTransport(TTransportWrapper transport) {
    if (closed.get()) {
      LOGGER.debug("invalidated {} for closed pool", transport);
      transport.close();
      return;
    }
    try {
      LOGGER.debug("[{}] Invalidating address {}", id, transport);
      if (!isPoolEnabled) {
        transport.close();
      } else {
        pool.invalidateObject(transport.getAddress(), transport);
        // Invalidate the whole pool associated with the given address
        // It is a bit brutal since a single bad connection may
        // cause an invalidation, but otherwise we may have a lot of bad
        // connections in the pool and try to return them.
        pool.clear(transport.getAddress());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to invalidate {}", transport, e);
    }
  }

  @Override
  public void close() throws Exception {
    if (closed.get()) {
      // already closed
      return;
    }
    LOGGER.debug("[{}] closing", id);
    if (pool != null) {
      LOGGER.debug("Closing pool of {}/{} endpoints",
              pool.getNumIdle(), pool.getNumActive());
      pool.close();
    }
  }

  /**
   * Factory that creates and destroys pool objects
   */
  private static final class PoolFactory
          extends BaseKeyedPooledObjectFactory<HostAndPort, TTransportWrapper> {
    private final TransportFactory transportFactory;
    private final int id;

    /**
     * Create a pool factory associated with the given transport factory
     * @param transportFactory - factory producing transports
     * @param id pool id (for debugging)
     */
    private PoolFactory(TransportFactory transportFactory, int id) {
      this.transportFactory = transportFactory;
      this.id = id;
    }

    @Override
    public boolean validateObject(HostAndPort key, PooledObject<TTransportWrapper> p) {
      TTransportWrapper transport = p.getObject();
      if (transport == null) {
        LOGGER.error("No transport to validate");
        return false;
      }
      if (transport.getAddress() != key) {
        LOGGER.error("Invalid endpoint {}: does not match {}", transport, key);
        return false;
      }
      return true;
    }

    @Override
    public TTransportWrapper create(HostAndPort key) throws Exception {
      TTransportWrapper transportWrapper = transportFactory.getTransport(key);
      LOGGER.debug("[{}] created {}", id, transportWrapper);
      return transportWrapper;
    }

    @Override
    public void destroyObject(HostAndPort key, PooledObject<TTransportWrapper> p) throws Exception {
      TTransportWrapper transport = p.getObject();
      if (transport != null) {
        LOGGER.debug("[{}] Destroying endpoint {}", id, transport);
        try {
          transport.close();
        } catch (RuntimeException e) {
          LOGGER.error("fail to destroy endpoint {}", transport, e);
        }
      }
    }

    @Override
    public PooledObject<TTransportWrapper> wrap(TTransportWrapper value) {
      return new DefaultPooledObject<>(value);
    }
  }

}
