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
package org.apache.sentry.provider.db.generic.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.solr.SolrModelAuthorizables;
import org.apache.sentry.core.model.solr.SolrPrivilegeModel;
import org.apache.sentry.provider.common.AuthorizationComponent;

public class SentryShellSolr extends SentryShellGeneric {
  private static final String SOLR_SERVICE_NAME = "sentry.service.client.solr.service.name";

  @Override
  protected GenericPrivilegeConverter getPrivilegeConverter(String component, String service) {
    return new GenericPrivilegeConverter(
            component,
            service,
            SolrPrivilegeModel.getInstance().getPrivilegeValidators(),
            new SolrModelAuthorizables(),
            true
    );
  }

  @Override
  protected String getComponent(Configuration conf) {
    return AuthorizationComponent.Search;
  }

  @Override
  protected String getServiceName(Configuration conf) {
    return conf.get(SOLR_SERVICE_NAME, "service1");
  }

  public static void main(String[] args) throws Exception {
    new SentryShellSolr().doMain(args);
  }
}
