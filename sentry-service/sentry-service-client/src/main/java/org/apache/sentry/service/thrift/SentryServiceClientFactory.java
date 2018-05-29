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

package org.apache.sentry.service.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.transport.RetryClientInvocationHandler;
import org.apache.sentry.core.common.transport.SentryPolicyClientTransportConfig;
import org.apache.sentry.core.common.transport.SentryTransportFactory;
import org.apache.sentry.core.common.transport.SentryTransportPool;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClientDefaultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client factory for Hive clients. The factory uses connection pooling.
 */
@ThreadSafe
public final class SentryServiceClientFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceClientFactory.class);

  private static final SentryPolicyClientTransportConfig transportConfig =
          new SentryPolicyClientTransportConfig();
  private final Configuration conf;
  private final SentryTransportPool transportPool;

  /** Keep track of singleton instances */
  private static final AtomicReference<SentryServiceClientFactory> clientFactory =
          new AtomicReference<>();

  /**
   * Create a client instance. The supplied configuration is only used the first time and
   * ignored afterwords. Tests that want to supply different configurations
   * should call {@link #factoryReset(SentryServiceClientFactory)} to force new configuration
   * read.
   * @param conf Configuration
   * @return client instance
   * @throws Exception
   */
  public static SentryPolicyServiceClient create(Configuration conf) throws Exception {
    SentryServiceClientFactory factory = clientFactory.get();
    if (factory != null) {
      return factory.create();
    }
    factory = new SentryServiceClientFactory(conf);
    boolean ok = clientFactory.compareAndSet(null, factory);
    if (ok) {
      return factory.create();
    }
    // Close old factory
    factory.close();
    return clientFactory.get().create();
  }

  /**
   * Create a new instance of the factory which will hand hand off connections from
   * the pool.
   * @param conf Configuration object
   */
  private SentryServiceClientFactory(Configuration conf) {
    this.conf = conf;

    transportPool = new SentryTransportPool(conf, transportConfig,
            new SentryTransportFactory(conf, transportConfig));
  }

  private SentryPolicyServiceClient create() throws Exception {
    return (SentryPolicyServiceClient) Proxy
      .newProxyInstance(SentryPolicyServiceClientDefaultImpl.class.getClassLoader(),
        SentryPolicyServiceClientDefaultImpl.class.getInterfaces(),
        new RetryClientInvocationHandler(conf,
          new SentryPolicyServiceClientDefaultImpl(conf, transportPool), transportConfig));
  }

  /**
   * Reset existing factory and return the old one.
   * Only used by tests.
   */
  public static SentryServiceClientFactory factoryReset(SentryServiceClientFactory factory) {
    LOGGER.debug("factory reset");
    return clientFactory.getAndSet(factory);
  }

  public void close() {
    try {
      transportPool.close();
    } catch (Exception e) {
      LOGGER.error("failed to close transport pool", e);
    }
  }
}
