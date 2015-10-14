package org.apache.solr.handler.admin;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.sentry.core.model.search.Config;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.SecureRequestHandlerUtil;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.core.CoreContainer;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Secure (sentry-aware) version of ConfigSetsHandler
 */
public class SecureConfigSetsHandler extends ConfigSetsHandler {

  public SecureConfigSetsHandler(final CoreContainer coreContainer) {
    super(coreContainer);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Pick the action
    SolrParams params = req.getParams();
    ConfigSetAction action = null;
    String a = params.get(CoreAdminParams.ACTION);
    String config = null;
    boolean checkConfig = true;
    if (a != null) {
      action = ConfigSetAction.get(a);
    }
    if (action != null) {
      switch (action) {
        case CREATE:
        case DELETE:
        {
          config = req.getParams().required().get(NAME);
          break;
        }
        case LIST:
          checkConfig = false;
          break;
        default: {
          break;
        }
      }
    }
    SecureRequestHandlerUtil.checkSentryAdminConfig(req,
        (action != null ? "ConfigAction." + action.toString() : getClass().getName() + "/" + a), true, checkConfig, config);
    super.handleRequestBody(req, rsp);

    /**
     * FixMe:
     * Need to add service support, e.g. here in SecureCollectionsHandler we sync delete collection
     */
  }
}
