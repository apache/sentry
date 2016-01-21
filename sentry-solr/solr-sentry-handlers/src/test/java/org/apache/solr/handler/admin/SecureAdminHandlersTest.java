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

import java.util.Map;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecureAdminHandlersTest extends SentryTestBase {

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

  @Test
  public void testAllAdminHandlersSecured() throws Exception {
    int numFound = 0;
    for (Map.Entry<String, SolrRequestHandler> entry : core.getRequestHandlers().entrySet() ) {
      // see note about ShowFileRequestHandler below
      if (entry.getKey().startsWith("/admin/") && !(entry.getValue() instanceof ShowFileRequestHandler)) {
         assertTrue(entry.getValue().getClass().getEnclosingClass().equals(SecureAdminHandlers.class));
         ++numFound;
      }
    }
    assertTrue(numFound > 0);
  }

  @Test
  public void testSecureAdminHandlers() throws Exception {
    verifyLuke();
    verifyMBeans();
    verifyPlugins();
    verifyThreads();
    verifyProperties();
    verifyLogging();
    verifyFile();
  }

  private void verifyAuthorized(RequestHandlerBase handler, String collection, String user) throws Exception {
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(core, req, collection, user, false);
    // just ensure we don't get an unauthorized exception
    try {
      handler.handleRequestBody(req, new SolrQueryResponse());
    } catch (SolrException ex) {
      assertFalse(ex.code() == SolrException.ErrorCode.UNAUTHORIZED.code);
    } catch (Throwable t) {
      // okay, we only want to verify we didn't get an Unauthorized exception,
      // going to treat each handler as a black box.
    }
  }

  private void verifyUnauthorized(RequestHandlerBase handler,
      String collection, String user, boolean shouldFailAdmin) throws Exception {
    String exMsgContains = "User " + user + " does not have privileges for " + (shouldFailAdmin?"admin":collection);
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(core, req, collection, user, false);
    try {
      handler.handleRequestBody(req, new SolrQueryResponse());
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(exMsgContains));
    }
  }

  private void verifyQueryAccess(RequestHandlerBase handler, boolean checkCollection) throws Exception {
    verifyAuthorized(handler, "collection1", "junit");
    verifyAuthorized(handler, "queryCollection", "junit");
    if (checkCollection) {
      verifyUnauthorized(handler, "bogusCollection", "junit", false);
      verifyUnauthorized(handler, "updateCollection", "junit", false);
    } else {
      verifyUnauthorized(handler, "collection1", "bogusUser", true);
    }
  }

  private void verifyQueryAccess(String path, boolean checkCollection) throws Exception {
    RequestHandlerBase handler =
      (RequestHandlerBase)core.getRequestHandlers().get(path);
    verifyQueryAccess(handler, checkCollection);
  }

  private void verifyQueryUpdateAccess(String path, boolean checkCollection) throws Exception {
    RequestHandlerBase handler =
      (RequestHandlerBase)core.getRequestHandlers().get(path);
    verifyAuthorized(handler, "collection1", "junit");
    verifyUnauthorized(handler, "collection1", "bogusUser", true);
    if (checkCollection) {
      verifyUnauthorized(handler, "queryCollection", "junit", false);
      verifyUnauthorized(handler, "bogusCollection", "junit", false);
      verifyUnauthorized(handler, "updateCollection", "junit", false);
    }
  }

  private void verifyLuke() throws Exception {
    verifyQueryAccess("/admin/luke", true);
  }

  private void verifyMBeans() throws Exception {
    verifyQueryAccess("/admin/mbeans", true);
  }

  private void verifyPlugins() throws Exception {
    verifyQueryAccess("/admin/plugins", true);
  }

  private void verifyThreads() throws Exception {
    verifyQueryAccess("/admin/threads", false);
  }

  private void verifyProperties() throws Exception {
    verifyQueryAccess("/admin/properties", false);
  }

  private void verifyLogging() throws Exception {
    verifyQueryUpdateAccess("/admin/logging", false);
  }

  private void verifyFile() throws Exception {
    // file handler is built-in for backwards compatibility reasons.  Thus,
    // handler will not be secure, so let's create one to test.
    String path = "/admin/file";
    RequestHandlerBase handler = (RequestHandlerBase)core.getRequestHandlers().get(path);
    assertFalse(handler instanceof SecureAdminHandlers.SecureShowFileRequestHandler);
    handler = new SecureAdminHandlers.SecureShowFileRequestHandler();
    verifyQueryAccess(handler, true);
  }
}
