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
package org.apache.sentry.provider.db.generic;


import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.junit.Test;

import static org.apache.sentry.service.thrift.ServiceConstants.ClientConfig.COMPONENT_TYPE;
import static org.apache.sentry.service.thrift.ServiceConstants.ClientConfig.SERVICE_NAME;
import static org.junit.Assert.assertEquals;

public class TestSentryGenericProviderBackend {
  @Test
  public void testScopeParamsGrabbedFromConf() throws Exception {
    Configuration conf = new Configuration();
    String sampleServiceName = "sampleServiceName123";
    String sampleComponentType = "sampleComponentType123";
    conf.set(SERVICE_NAME, sampleServiceName);
    conf.set(COMPONENT_TYPE, sampleComponentType);
    SentryGenericProviderBackend providerBackend = new SentryGenericProviderBackend(conf, "resource");
    assertEquals(sampleComponentType, providerBackend.getComponentType());
    assertEquals(sampleServiceName, providerBackend.getServiceName());
  }
  @Test(expected = NullPointerException.class)
  public void testScopeParamsValidated() throws Exception {
    Configuration conf = new Configuration();
    SentryGenericProviderBackend providerBackend = new SentryGenericProviderBackend(conf, "resource");
    providerBackend.initialize(new ProviderBackendContext());
  }
}
