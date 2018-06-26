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
package org.apache.sentry.api.service.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.common.ApiConstants.SentryPolicyServiceConstants;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;
import org.apache.sentry.service.thrift.ProcessorFactory;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;

public class SentryPolicyStoreProcessorFactory extends ProcessorFactory {
  public SentryPolicyStoreProcessorFactory(Configuration conf) {
    super(conf);
  }

  public boolean register(TMultiplexedProcessor multiplexedProcessor,
                          SentryStoreInterface sentryStore) throws Exception {
    SentryPolicyStoreProcessor sentryServiceHandler =
        new SentryPolicyStoreProcessor(SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
            conf, sentryStore);
    TProcessor processor =
      new SentryProcessorWrapper<SentryPolicyService.Iface>(sentryServiceHandler);
    multiplexedProcessor.registerProcessor(
      SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME, processor);
    return true;
  }
}
