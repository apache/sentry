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

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecureCollectionsHandlerTest extends SentryTestBase {

  private static SolrCore core;
  private static CloudDescriptor cloudDescriptor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig-secureadmin.xml", "schema-minimal.xml");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    // ensure SentrySingletonTestInstance is initialized
    SentrySingletonTestInstance.getInstance();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    closeCore(core, cloudDescriptor);
    core = null;
    cloudDescriptor = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp(core);
  }

  private SolrQueryRequest getCollectionsRequest(String collection, String user,
      CollectionAction action) throws Exception {
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(core, req, collection, user, false);
    ModifiableSolrParams modParams = new ModifiableSolrParams(req.getParams());
    modParams.set(CoreAdminParams.ACTION, action.name());
    modParams.set("name", collection);
    modParams.set("collection", collection);
    req.setParams(modParams);
    return req;
  }

  private void verifyUpdateAccess(CollectionAction action) throws Exception {
    CollectionsHandler handler = new SecureCollectionsHandler(h.getCoreContainer());
    verifyAuthorized(handler, getCollectionsRequest("collection1", "junit", action));
    verifyAuthorized(handler, getCollectionsRequest("updateCollection", "junit", action));
    verifyUnauthorized(handler, getCollectionsRequest("queryCollection", "junit", action), "queryCollection", "junit");
    verifyUnauthorized(handler, getCollectionsRequest("bogusCollection", "junit", action), "bogusCollection", "junit");
  }

  private void verifyQueryAccess(CollectionAction action) throws Exception {
    CollectionsHandler handler = new SecureCollectionsHandler(h.getCoreContainer());
    verifyAuthorized(handler, getCollectionsRequest("collection1", "junit", action));
    verifyAuthorized(handler, getCollectionsRequest("queryCollection", "junit", action));
    verifyUnauthorized(handler, getCollectionsRequest("updateCollection", "junit", action), "updateCollection", "junit");
    verifyUnauthorized(handler, getCollectionsRequest("bogusCollection", "junit", action), "bogusCollection", "junit");
  }

  @Test
  public void testSecureCollectionsHandler() throws Exception {
    for (CollectionAction action : CollectionAction.values()) {
      if (SecureCollectionsHandler.QUERY_ACTIONS.contains(action)) {
        verifyQueryAccess(action);
      } else {
        verifyUpdateAccess(action);
      }
    }
  }
}
