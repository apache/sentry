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
package org.apache.solr.handler;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSecureAnalysisHandlers extends SentryTestBase {

  private static SolrCore core;
  private static CloudDescriptor cloudDescriptor;

   @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig-secureadmin.xml", "schema-minimal.xml");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    // ensure the SentrySingletonTestInstance is initialized
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

  private SolrQueryRequest getAnalysisRequest(String collection, String user)
      throws Exception {
    SolrQueryRequest request = getRequest();
    return prepareCollAndUser(core, request, collection, user);
  }

  private void verifyQueryAccess(SolrRequestHandler handler) throws Exception {
    verifyAuthorized(handler, getAnalysisRequest("collection1", "junit"));
    verifyAuthorized(handler, getAnalysisRequest("queryCollection", "junit"));
    verifyUnauthorized(handler, getAnalysisRequest("bogusCollection", "junit"),
      "bogusCollection", "junit");
    verifyUnauthorized(handler, getAnalysisRequest("updateCollection", "junit"),
      "updateCollection", "junit");
  }

  @Test
  public void testSecureFieldAnalysisRequestHandler() throws Exception {
    SolrRequestHandler handler = core.getRequestHandler("/analysis/field");
    verifyQueryAccess(handler);
  }

  @Test
  public void testDocumentAnalysisRequestHandler() throws Exception {
    SolrRequestHandler handler = core.getRequestHandler("/analysis/document");
    verifyQueryAccess(handler);
  }
}
