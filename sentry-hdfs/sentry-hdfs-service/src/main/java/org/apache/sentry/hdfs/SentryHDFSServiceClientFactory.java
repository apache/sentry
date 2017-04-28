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

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.transport.RetryClientInvocationHandler;
import org.apache.sentry.core.common.transport.SentryHDFSClientTransportConfig;

/**
 * Client factory to create normal client or proxy with HA invocation handler
 */
public class SentryHDFSServiceClientFactory {
  private final static SentryHDFSClientTransportConfig transportConfig =
          new SentryHDFSClientTransportConfig();

  private SentryHDFSServiceClientFactory() {
    // Make constructor private to avoid instantiation
  }

  public static SentryHDFSServiceClient create(Configuration conf)
    throws Exception {
    return (SentryHDFSServiceClient) Proxy
      .newProxyInstance(SentryHDFSServiceClientDefaultImpl.class.getClassLoader(),
        SentryHDFSServiceClientDefaultImpl.class.getInterfaces(),
        new RetryClientInvocationHandler(conf,
          new SentryHDFSServiceClientDefaultImpl(conf, transportConfig), transportConfig));
  }
}
