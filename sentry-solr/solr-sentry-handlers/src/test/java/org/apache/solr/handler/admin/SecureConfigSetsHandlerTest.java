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
package org.apache.solr.handler.admin;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.apache.solr.core.SolrCore;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecureConfigSetsHandlerTest extends SentryTestBase {

  private static SolrCore core;
  // Set of actions without an associated config
  private static Set<ConfigSetAction> actionsWithoutConfigs = EnumSet.of(ConfigSetAction.LIST);

  @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig-secureadmin.xml", "schema-minimal.xml");
    // ensure SentrySingletonTestInstance is initialized
    SentrySingletonTestInstance.getInstance();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    closeCore(core, null);
    core = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp(core);
  }

  private SolrQueryRequest getConfigSetsRequest(String configSet, String user,
      ConfigSetAction action) throws Exception {
    SolrQueryRequest req = getRequest();
    prepareUser(req, user, false);
    ModifiableSolrParams modParams = new ModifiableSolrParams(req.getParams());
    modParams.set(CoreAdminParams.ACTION, action.name());
    modParams.set("name", configSet);
    req.setParams(modParams);
    return req;
  }

  private void verifyUpdateAccess(ConfigSetAction action,
      ConfigSetsHandler handler,
      boolean hasAdminCollectionPrivs,
      boolean hasConfigSetPrivs) throws Exception {
    String user = hasAdminCollectionPrivs ? "updateOnlyAdmin" : "queryOnlyAdmin";
    String configSet = null;
    if (hasConfigSetPrivs) {
      configSet = hasAdminCollectionPrivs ? "adminConfig" : "nonAdminConfig";
    } else {
      configSet = hasAdminCollectionPrivs ? "nonAdminConfig" : "adminConfig";
    }

    if (hasAdminCollectionPrivs && (hasConfigSetPrivs || actionsWithoutConfigs.contains(action))) {
      verifyAuthorized(handler, getConfigSetsRequest(configSet, user, action));
    } else {
      verifyUnauthorized(handler, getConfigSetsRequest(configSet, user, action), configSet, user, !hasAdminCollectionPrivs);
    }
  }

  private void verifyUpdateAccess(ConfigSetAction action) throws Exception {
    ConfigSetsHandler handler = new SecureConfigSetsHandler(h.getCoreContainer());
    for (boolean hasAdminCollectionPrivs : Arrays.asList(true, false)) {
      for (boolean hasAdminConfigSetPrivs : Arrays.asList(true,false)) {
        verifyUpdateAccess(action, handler, hasAdminCollectionPrivs, hasAdminConfigSetPrivs);
      }
    }

    // bogusConfigSet
    if (!actionsWithoutConfigs.contains(action)) {
      verifyUnauthorized(handler, getConfigSetsRequest("bogusConfigSet", "junit", action), "bogusConfigSet", "junit");
    }
  }

  @Test
  public void testSecureConfigSetHandler() throws Exception {
    for (ConfigSetAction action : ConfigSetAction.values()) {
      verifyUpdateAccess(action);
    }
  }
}
