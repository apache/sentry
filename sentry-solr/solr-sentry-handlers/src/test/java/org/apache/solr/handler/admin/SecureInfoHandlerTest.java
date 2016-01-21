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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecureInfoHandlerTest extends SentryTestBase {

  private static SolrCore core;
  private static CloudDescriptor cloudDescriptor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig-secureadmin.xml", "schema-minimal.xml");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
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

  private SolrQueryRequest getInfoRequest(String collection, String user, String path)
      throws Exception {
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(core, req, collection, user, false);
    req.getContext().put("path", path);
    return req;
  }

  @Test
  public void testSecureInfoHandlers() throws Exception {
    verifyThreadDumpHandler();
    verifyPropertiesHandler();
    verifyLoggingHandler();
    verifySystemInfoHandler();
  }

  private void verifyQueryAccess(String path) throws Exception {
    InfoHandler handler = new SecureInfoHandler(h.getCoreContainer());
    verifyAuthorized(handler, getInfoRequest("collection1", "junit", path));
    verifyAuthorized(handler, getInfoRequest("queryCollection", "junit", path));
    verifyUnauthorized(handler, getInfoRequest("bogusCollection", "bogusUser", path), "bogusCollection", "bogusUser", true);
    verifyUnauthorized(handler, getInfoRequest("updateCollection", "updateOnlyAdmin", path), "updateCollection", "updateOnlyAdmin", true);
  }

  private void verifyQueryUpdateAccess(String path) throws Exception {
    InfoHandler handler = new SecureInfoHandler(h.getCoreContainer());
    verifyAuthorized(handler, getInfoRequest("collection1", "junit", path));
    verifyUnauthorized(handler, getInfoRequest("queryCollection", "queryOnlyAdmin", path), "queryCollection", "queryOnlyAdmin", true);
    verifyUnauthorized(handler, getInfoRequest("bogusCollection", "bogusUser", path), "bogusCollection", "bogusUser", true);
    verifyUnauthorized(handler, getInfoRequest("updateCollection", "updateOnlyAdmin", path), "updateCollection", "updateOnlyAdmin", true);
  }

  private void verifyThreadDumpHandler() throws Exception {
    verifyQueryAccess("info/threads");
  }

  private void verifyPropertiesHandler() throws Exception {
    verifyQueryAccess("info/properties");
  }

  private void verifyLoggingHandler() throws Exception {
    verifyQueryUpdateAccess("info/logging");
  }

  private void verifySystemInfoHandler() throws Exception {
    verifyQueryAccess("info/system");
  }
}
