/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.service.thrift;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.common.net.HostAndPort;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.transport.SentryClientInvocationHandler;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PoolClientInvocationHandler is a proxy class for handling thrift
 * call. For every thrift call, get the instance of
 * SentryPolicyServiceBaseClient from the commons-pool, and return the instance
 * to the commons-pool after complete the call. For any exception with the call,
 * discard the instance and create a new one added to the commons-pool. Then,
 * get the instance and do the call again. For the thread safe, the commons-pool
 * will manage the connection pool, and every thread can get the connection by
 * borrowObject() and return the connection to the pool by returnObject().
 *
 * TODO: Current pool model does not manage the opening connections very well,
 * e.g. opening connections with failed servers should be closed promptly.
 */

public class PoolClientInvocationHandler extends SentryClientInvocationHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PoolClientInvocationHandler.class);

  private static final String POOL_EXCEPTION_MESSAGE = "Pool exception occurred ";

  private final Configuration conf;

  /**
   * The configuration to use for our object pools.
   * Null if we are not using object pools.
   */
  private final GenericObjectPoolConfig poolConfig;

  /**
   * The total number of connection retries to attempt per endpoint.
   */
  private final int connectionRetryTotal;

  /**
   * The configured sentry servers.
   */
  private final Endpoint[] endpoints;

  /**
   * The endpoint which we are currently using.  This can be read without any locks.
   * It must be written while holding the endpoints lock.
   */
  private volatile int freshestEndpointIdx = 0;

  private class Endpoint {
    /**
     * The server address or hostname.
     */
    private final String addr;

    /**
     * The server port.
     */
    private final int port;

    /**
     * The server's poolFactory used to create new clients.
     */
    private final PooledObjectFactory<SentryPolicyServiceClient> poolFactory;

    /**
     * The server's pool of cached clients.
     */
    private final GenericObjectPool<SentryPolicyServiceClient> pool;

    Endpoint(String addr, int port) {
      this.addr = addr;
      this.port = port;
      this.poolFactory = new SentryServiceClientPoolFactory(addr, port, conf);
      this.pool = new GenericObjectPool<SentryPolicyServiceClient>(
          this.poolFactory, poolConfig, new AbandonedConfig());
    }

    GenericObjectPool<SentryPolicyServiceClient> getPool() {
      return pool;
    }

    String getEndPointStr() {
      return new String("endpoint at [address " + addr + ", port " + port + "]");
    }
  }

  public PoolClientInvocationHandler(Configuration conf) throws Exception {
    this.conf = conf;

    this.poolConfig = new GenericObjectPoolConfig();
    // config the pool size for commons-pool
    this.poolConfig.setMaxTotal(conf.getInt(ClientConfig.SENTRY_POOL_MAX_TOTAL,
        ClientConfig.SENTRY_POOL_MAX_TOTAL_DEFAULT));
    this.poolConfig.setMinIdle(conf.getInt(ClientConfig.SENTRY_POOL_MIN_IDLE,
        ClientConfig.SENTRY_POOL_MIN_IDLE_DEFAULT));
    this.poolConfig.setMaxIdle(conf.getInt(ClientConfig.SENTRY_POOL_MAX_IDLE,
        ClientConfig.SENTRY_POOL_MAX_IDLE_DEFAULT));

    // get the retry number for reconnecting service
    this.connectionRetryTotal = conf.getInt(ClientConfig.SENTRY_POOL_RETRY_TOTAL,
        ClientConfig.SENTRY_POOL_RETRY_TOTAL_DEFAULT);

    String hostsAndPortsStr = conf.get(ClientConfig.SERVER_RPC_ADDRESS);
    if (hostsAndPortsStr == null) {
      throw new RuntimeException("Config key " +
          ClientConfig.SERVER_RPC_ADDRESS + " is required");
    }
    int defaultPort = conf.getInt(ClientConfig.SERVER_RPC_PORT,
        ClientConfig.SERVER_RPC_PORT_DEFAULT);
    String[] hostsAndPortsStrArr = hostsAndPortsStr.split(",");
    HostAndPort[] hostsAndPorts = ThriftUtil.parseHostPortStrings(hostsAndPortsStrArr, defaultPort);
    this.endpoints = new Endpoint[hostsAndPorts.length];
    for (int i = 0; i < this.endpoints.length; i++) {
      this.endpoints[i] = new Endpoint(hostsAndPorts[i].getHostText(),hostsAndPorts[i].getPort());
      LOGGER.info("Initiate sentry sever endpoint: hostname " +
          hostsAndPorts[i].getHostText() + ", port " + hostsAndPorts[i].getPort());
    }
  }

  @Override
  public Object invokeImpl(Object proxy, Method method, Object[] args)
      throws Exception {
    int retryCount = 0;
    /**
     * The maximum number of retries that we will do.  Each endpoint gets its
     * own set of retries.
     */
    int retryLimit = connectionRetryTotal * endpoints.length;

    /**
     * The index of the endpoint to use.
     */
    int endpointIdx = freshestEndpointIdx;

    /**
     * A list of exceptions from each endpoint.  This starts as null to avoid
     * memory allocation in the common case where there is no error.
     */
    Exception exc[] = null;

    Object ret = null;

    while (retryCount < retryLimit) {
      GenericObjectPool<SentryPolicyServiceClient> pool =
          endpoints[endpointIdx].getPool();
      try {
        if ((exc != null) &&
            (exc[endpointIdx] instanceof TTransportException)) {
          // If there was a TTransportException last time we tried to contact
          // this endpoint, attempt to create a new connection before we try
          // again.
          synchronized (endpoints) {
            // If there has room, create new instance and add it to the
            // commons-pool.  This instance will be returned first from the
            // commons-pool, because the configuration is LIFO.
            if (pool.getNumIdle() + pool.getNumActive() < pool.getMaxTotal()) {
              pool.addObject();
            }
          }
        }
        // Try to make the RPC.
        ret = invokeFromPool(method, args, pool);
        break;
      } catch (TTransportException e) {
        if (exc == null) {
          exc = new Exception[endpoints.length];
        }
        exc[endpointIdx] = e;
      }

      Exception lastExc = exc[endpointIdx];
      synchronized (endpoints) {
        int curFreshestEndpointIdx = freshestEndpointIdx;
        if (curFreshestEndpointIdx == endpointIdx) {
          curFreshestEndpointIdx =
              (curFreshestEndpointIdx  + 1) %  endpoints.length;
          freshestEndpointIdx = curFreshestEndpointIdx;
        }
        endpointIdx = curFreshestEndpointIdx;
      }
      // Increase the retry num, and throw the exception if can't retry again.
      retryCount++;
      if (retryCount == connectionRetryTotal) {
        for (int i = 0; i < exc.length; i++) {
          // Since freshestEndpointIdx is shared by multiple threads, it is possible that
          // the ith endpoint has been tried in another thread and skipped in the current
          // thread.
          if (exc[i] != null) {
            LOGGER.error("Sentry server " + endpoints[i].getEndPointStr()
                + " is in unreachable.");
          }
        }
        throw new SentryUserException("Sentry servers are unreachable. " +
            "Diagnostics is needed for unreachable servers.", lastExc);
      }
    }
    return ret;
  }

  private Object invokeFromPool(Method method, Object[] args,
      GenericObjectPool<SentryPolicyServiceClient> pool) throws Exception {
    Object result = null;
    SentryPolicyServiceClient client;
    try {
      // get the connection from the pool, don't know if the connection is broken.
      client = pool.borrowObject();
    } catch (Exception e) {
      LOGGER.debug(POOL_EXCEPTION_MESSAGE, e);
      // If the exception is caused by connection problem, throw the TTransportException
      // for reconnect.
      if (e instanceof IOException) {
        throw new TTransportException(e);
      }
      throw new SentryUserException(e.getMessage(), e);
    }
    try {
      // do the thrift call
      result = method.invoke(client, args);
    } catch (InvocationTargetException e) {
      // Get the target exception, check if SentryUserException or TTransportException is wrapped.
      // TTransportException or IOException means there has connection problem with the pool.
      Throwable targetException = e.getCause();
      if (targetException != null && targetException instanceof SentryUserException) {
        Throwable sentryTargetException = targetException.getCause();
        // If there has connection problem, eg, invalid connection if the service restarted,
        // sentryTargetException instanceof TTransportException or IOException = true.
        if (sentryTargetException != null && (sentryTargetException instanceof TTransportException
            || sentryTargetException instanceof IOException)) {
          // If the exception is caused by connection problem, destroy the instance and
          // remove it from the commons-pool. Throw the TTransportException for reconnect.
          pool.invalidateObject(client);
          throw new TTransportException(sentryTargetException);
        }
        // The exception is thrown by thrift call, eg, SentryAccessDeniedException.
        throw (SentryUserException) targetException;
      }
      throw e;
    } finally{
      try {
        // return the instance to commons-pool
        pool.returnObject(client);
      } catch (Exception e) {
        LOGGER.error(POOL_EXCEPTION_MESSAGE, e);
        throw e;
      }
    }
    return result;
  }

  @Override
  public void close() {
    for (int i = 0; i < endpoints.length; i++) {
      try {
        endpoints[i].getPool().close();
      } catch (Exception e) {
        LOGGER.debug(POOL_EXCEPTION_MESSAGE, e);
      }
    }
  }
}
