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
import org.apache.sentry.core.common.transport.RetryClientInvocationHandler;
import org.apache.sentry.core.common.transport.SentryPolicyClientTransportConfig;

import java.lang.reflect.Proxy;

/**
 * SentryGenericServiceClientFactory is a public class for the components which using Generic Model to create sentry client.
 */

public final class SentryGenericServiceClientFactory {
  private static final SentryPolicyClientTransportConfig transportConfig =
          new SentryPolicyClientTransportConfig();
  private SentryGenericServiceClientFactory() {
  }

  public static SentryGenericServiceClient create(Configuration conf) throws Exception {
    return (SentryGenericServiceClient) Proxy
      .newProxyInstance(SentryGenericServiceClientDefaultImpl.class.getClassLoader(),
        SentryGenericServiceClientDefaultImpl.class.getInterfaces(),
        new RetryClientInvocationHandler(conf,
          new SentryGenericServiceClientDefaultImpl(conf, transportConfig), transportConfig));
  }

}
