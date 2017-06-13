/**
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
package org.apache.sentry.provider.db.generic.service.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.transport.RetryClientInvocationHandler;
import org.apache.sentry.core.common.transport.SentryPolicyClientTransportConfig;
import org.apache.sentry.core.common.transport.SentryTransportFactory;
import org.apache.sentry.core.common.transport.SentryTransportPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Produces client connection for Sentry clients using Generic model.
 * Factory is [alost] a singleton. Tests can call {@link #factoryReset()} to destroy the
 * existing factory and create a new one. This may be needed because tests modify
 * configuration and start and stop servers.
 */
@ThreadSafe
public final class SentryGenericServiceClientFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryGenericServiceClientFactory.class);

  // Used to implement a singleton
  private static final AtomicReference<SentryGenericServiceClientFactory> clientFactory =
          new AtomicReference<>();

  private final SentryPolicyClientTransportConfig transportConfig =
          new SentryPolicyClientTransportConfig();
  private final SentryTransportPool transportPool;
  private final Configuration conf;

  /**
   * Obtain an Generic policy client instance.
   * @param conf Configuration that should be used. Configuration is only used for the
   *             initial creation and ignored afterwords.
   */
  public static SentryGenericServiceClient create(Configuration conf) throws Exception {
    SentryGenericServiceClientFactory factory = clientFactory.get();
    if (factory != null) {
      return factory.create();
    }
    factory = new SentryGenericServiceClientFactory(conf);
    boolean ok = clientFactory.compareAndSet(null, factory);
    if (ok) {
      return factory.create();
    }
    factory.close();
    return clientFactory.get().create();
  }

  /**
   * Create a new factory instance and atach it to a connection pool instance.
   * @param conf Configuration
   */
  private SentryGenericServiceClientFactory(Configuration conf) {
    if (transportConfig.isKerberosEnabled(conf) &&
            transportConfig.useUserGroupInformation(conf)) {
        LOGGER.info("Using UserGroupInformation authentication");
        UserGroupInformation.setConfiguration(conf);
    }

    this.conf = conf;

    transportPool = new SentryTransportPool(this.conf, transportConfig,
            new SentryTransportFactory(this.conf, transportConfig));
  }

  /**
   * Create a new client connection to the server for Generic model clients
   * @return client instance
   * @throws Exception if something goes wrong
   */
  @SuppressWarnings("squid:S00112")
  private SentryGenericServiceClient create() throws Exception {
    return (SentryGenericServiceClient) Proxy
      .newProxyInstance(SentryGenericServiceClientDefaultImpl.class.getClassLoader(),
        SentryGenericServiceClientDefaultImpl.class.getInterfaces(),
        new RetryClientInvocationHandler(conf,
          new SentryGenericServiceClientDefaultImpl(conf, transportPool), transportConfig));
  }

  // Should only be used by tests.
  // Resets the factory and destroys any pooled connections
  public static void factoryReset() {
    LOGGER.debug("factory reset");
    SentryGenericServiceClientFactory factory = clientFactory.getAndSet(null);
    if (factory != null) {
      try {
        factory.transportPool.close();
      } catch (Exception e) {
        LOGGER.error("failed to close transport pool", e);
      }
    }
  }

  private void close() {
    try {
      transportPool.close();
    } catch (Exception e) {
      LOGGER.error("failed to close transport pool", e);
    }
  }
}
