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
package org.apache.sentry.hdfs;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.transport.RetryClientInvocationHandler;
import org.apache.sentry.core.common.transport.SentryHDFSClientTransportConfig;
import org.apache.sentry.core.common.transport.SentryTransportFactory;
import org.apache.sentry.core.common.transport.SentryTransportPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client factory for creating HDFS service clients.
 * This is a singleton which uses a single factory.
 */
@ThreadSafe
public final class SentryHDFSServiceClientFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHDFSServiceClientFactory.class);

  private static final AtomicReference<SentryHDFSServiceClientFactory> clientFactory =
          new AtomicReference<>();

  private final SentryHDFSClientTransportConfig transportConfig =
          new SentryHDFSClientTransportConfig();
  private final Configuration conf;
  private final SentryTransportPool transportPool;

  /**
   * Return a client instance
   * @param conf
   * @return
   * @throws Exception
   */
  public static SentryHDFSServiceClient create(Configuration conf) throws Exception {
    SentryHDFSServiceClientFactory factory = clientFactory.get();
    if (factory != null) {
      return factory.create();
    }
    factory = new SentryHDFSServiceClientFactory(conf);
    boolean ok = clientFactory.compareAndSet(null, factory);
    if (ok) {
      return factory.create();
    }
    factory.close();
    return clientFactory.get().create();
  }

  private SentryHDFSServiceClientFactory(Configuration conf) {
    this.conf = conf;
    transportPool = new SentryTransportPool(conf, transportConfig,
            new SentryTransportFactory(conf, transportConfig));
  }

  /**
   * Create a new client connection to one of the Sentry servers.
   * @return client instance
   * @throws Exception if something goes wrong
   */
  @SuppressWarnings("squid:S00112")
  private SentryHDFSServiceClient create() throws Exception {
    return (SentryHDFSServiceClient) Proxy
      .newProxyInstance(SentryHDFSServiceClientDefaultImpl.class.getClassLoader(),
        SentryHDFSServiceClientDefaultImpl.class.getInterfaces(),
        new RetryClientInvocationHandler(conf,
          new SentryHDFSServiceClientDefaultImpl(conf, transportPool), transportConfig));
  }

  /**
   * Reset existing factory and return the old one.
   * Only used by tests.
   */
  public static void factoryReset() {
    LOGGER.debug("factory reset");
    SentryHDFSServiceClientFactory factory = clientFactory.getAndSet(null);
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
