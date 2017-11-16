/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.policy.solr;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.solr.SolrPrivilegeModel;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.policy.engine.common.CommonPolicyEngine;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;

import java.io.IOException;

public class SolrPolicyTestUtil {

  public static PolicyEngine createPolicyEngineForTest(String resource) throws IOException {

    ProviderBackend providerBackend = new SimpleFileProviderBackend(new Configuration(), resource);

    // create backendContext
    ProviderBackendContext context = new ProviderBackendContext();
    context.setAllowPerDatabase(false);
    context.setValidators(SolrPrivilegeModel.getInstance().getPrivilegeValidators());
    // initialize the backend with the context
    providerBackend.initialize(context);


    return new CommonPolicyEngine(providerBackend);
  }
}
