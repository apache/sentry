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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PoolClientInvocationHandler is a proxy class for handling thrift call. For every thrift call,
 * get the instance of SentryPolicyServiceBaseClient from the commons-pool, and return the instance
 * to the commons-pool after complete the call. For any exception with the call, discard the
 * instance and create a new one added to the commons-pool. Then, get the instance and do the call
 * again. For the thread safe, the commons-pool will manage the connection pool, and every thread
 * can get the connection by borrowObject() and return the connection to the pool by returnObject().
 */

public class PoolClientInvocationHandler extends SentryClientInvocationHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PoolClientInvocationHandler.class);

  private final Configuration conf;
  private PooledObjectFactory<SentryPolicyServiceClient> poolFactory;
  private GenericObjectPool<SentryPolicyServiceClient> pool;
  private GenericObjectPoolConfig poolConfig;
  private int connectionRetryTotal;

  private static final String POOL_EXCEPTION_MESSAGE = "Pool exception occured ";

  public PoolClientInvocationHandler(Configuration conf) throws Exception {
    this.conf = conf;
    readConfiguration();
    poolFactory = new SentryServiceClientPoolFactory(conf);
    pool = new GenericObjectPool<SentryPolicyServiceClient>(poolFactory, poolConfig, new AbandonedConfig());
  }

  @Override
  public Object invokeImpl(Object proxy, Method method, Object[] args) throws Exception {
    int retryCount = 0;
    Object result = null;
    while (retryCount < connectionRetryTotal) {
      try {
        // The wapper here is for the retry of thrift call, the default retry number is 3.
        result = invokeFromPool(proxy, method, args);
        break;
      } catch (TTransportException e) {
        // TTransportException means there has connection problem, create a new connection and try
        // again. Get the lock of pool and add new connection.
        synchronized (pool) {
          // If there has room, create new instance and add it to the commons-pool, this instance
          // will be back first from the commons-pool because the configuration is LIFO.
          if (pool.getNumIdle() + pool.getNumActive() < pool.getMaxTotal()) {
            pool.addObject();
          }
        }
        // Increase the retry num, and throw the exception if can't retry again.
        retryCount++;
        if (retryCount == connectionRetryTotal) {
          throw new SentryUserException(e.getMessage(), e);
        }
      }
    }
    return result;
  }

  private Object invokeFromPool(Object proxy, Method method, Object[] args) throws Exception {
    Object result = null;
    SentryPolicyServiceClient client;
    try {
      // get the connection from the pool, don't know if the connection is broken.
      client = pool.borrowObject();
    } catch (Exception e) {
      LOGGER.debug(POOL_EXCEPTION_MESSAGE, e);
      throw new SentryUserException(e.getMessage(), e);
    }
    try {
      // do the thrift call
      result = method.invoke(client, args);
    } catch (InvocationTargetException e) {
      // Get the target exception, check if SentryUserException or TTransportException is wrapped.
      // TTransportException means there has connection problem with the pool.
      Throwable targetException = e.getCause();
      if (targetException != null && targetException instanceof SentryUserException) {
        Throwable sentryTargetException = targetException.getCause();
        // If there has connection problem, eg, invalid connection if the service restarted,
        // sentryTargetException instanceof TTransportException = true.
        if (sentryTargetException != null && sentryTargetException instanceof TTransportException) {
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
    try {
      pool.close();
    } catch (Exception e) {
      LOGGER.debug(POOL_EXCEPTION_MESSAGE, e);
    }
  }

  private void readConfiguration() {
    poolConfig = new GenericObjectPoolConfig();
    // config the pool size for commons-pool
    poolConfig.setMaxTotal(conf.getInt(ClientConfig.SENTRY_POOL_MAX_TOTAL, ClientConfig.SENTRY_POOL_MAX_TOTAL_DEFAULT));
    poolConfig.setMinIdle(conf.getInt(ClientConfig.SENTRY_POOL_MIN_IDLE, ClientConfig.SENTRY_POOL_MIN_IDLE_DEFAULT));
    poolConfig.setMaxIdle(conf.getInt(ClientConfig.SENTRY_POOL_MAX_IDLE, ClientConfig.SENTRY_POOL_MAX_IDLE_DEFAULT));
    // get the retry number for reconnecting service
    connectionRetryTotal = conf.getInt(ClientConfig.SENTRY_POOL_RETRY_TOTAL,
        ClientConfig.SENTRY_POOL_RETRY_TOTAL_DEFAULT);
  }
}
