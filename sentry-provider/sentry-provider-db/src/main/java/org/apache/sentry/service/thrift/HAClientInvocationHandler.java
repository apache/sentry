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
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.HAContext;
import org.apache.sentry.provider.db.service.persistent.ServiceManager;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClientDefaultImpl;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HAClientInvocationHandler extends SentryClientInvocationHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HAClientInvocationHandler.class);

  private final Configuration conf;
  private ServiceManager manager;
  private ServiceInstance<Void> currentServiceInstance;
  private SentryPolicyServiceClient client = null;

  private static final String THRIFT_EXCEPTION_MESSAGE = "Thrift exception occured ";
  public static final String SENTRY_HA_ERROR_MESSAGE = "No Sentry server available. Please ensure that at least one Sentry server is online";

  public HAClientInvocationHandler(Configuration conf) throws Exception {
    this.conf = conf;
    checkClientConf();
  }

  @Override
  public Object invokeImpl(Object proxy, Method method, Object[] args) throws
      SentryUserException {
    Object result = null;
    while (true) {
      try {
        if (!method.isAccessible()) {
          method.setAccessible(true);
        }
        // The client is initialized in the first call instead of constructor.
        // This way we can propagate the connection exception to caller cleanly
        if (client == null) {
          renewSentryClient();
        }
        result = method.invoke(client, args);
      } catch (IllegalAccessException e) {
        throw new SentryUserException(e.getMessage(), e.getCause());
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof SentryUserException) {
          throw (SentryUserException)e.getTargetException();
        } else {
          LOGGER.warn(THRIFT_EXCEPTION_MESSAGE + ": Error in connect current" +
              " service, will retry other service.", e);
          if (client != null) {
            client.close();
            client = null;
          }
        }
      } catch (IOException e1) {
        throw new SentryUserException("Error connecting to sentry service "
            + e1.getMessage(), e1);
      }
      return result;
    }
  }

  // Retrieve the new connection endpoint from ZK and connect to new server
  private void renewSentryClient() throws IOException {
    try {
      manager = new ServiceManager(HAContext.getHAContext(conf));
    } catch (Exception e1) {
      throw new IOException("Failed to extract Sentry node info from zookeeper", e1);
    }

    try {
      while (true) {
        currentServiceInstance = manager.getServiceInstance();
        if (currentServiceInstance == null) {
          throw new IOException(SENTRY_HA_ERROR_MESSAGE);
        }
        InetSocketAddress serverAddress =
            ServiceManager.convertServiceInstance(currentServiceInstance);
        conf.set(ServiceConstants.ClientConfig.SERVER_RPC_ADDRESS, serverAddress.getHostName());
        conf.setInt(ServiceConstants.ClientConfig.SERVER_RPC_PORT, serverAddress.getPort());
        try {
          client = new SentryPolicyServiceClientDefaultImpl(conf);
          LOGGER.info("Sentry Client using server " + serverAddress.getHostName() +
              ":" + serverAddress.getPort());
          break;
        } catch (IOException e) {
          manager.reportError(currentServiceInstance);
          LOGGER.info("Transport exception while opening transport:", e, e.getMessage());
        }
      }
    } finally {
      manager.close();
    }
  }

  private void checkClientConf() {
    if (conf.getBoolean(ServerConfig.SENTRY_HA_ZOOKEEPER_SECURITY,
        ServerConfig.SENTRY_HA_ZOOKEEPER_SECURITY_DEFAULT)) {
      String serverPrincipal = Preconditions.checkNotNull(conf.get(ServerConfig.PRINCIPAL),
          ServerConfig.PRINCIPAL + " is required");
      Preconditions.checkArgument(serverPrincipal.contains(SecurityUtil.HOSTNAME_PATTERN),
          ServerConfig.PRINCIPAL + " : " + serverPrincipal + " should contain " + SecurityUtil.HOSTNAME_PATTERN);
    }
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }
}
