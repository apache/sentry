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
package org.apache.sentry.tests.e2e.solr;

import static org.apache.sentry.tests.e2e.solr.TestSentryServer.ADMIN_USER;
import static org.apache.sentry.core.model.solr.AdminOperation.COLLECTIONS;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.sentry.core.model.solr.AdminOperation;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.ClusterStatus;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.junit.Test;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

@SuppressSSL
public class TestSolrAdminOperations extends SolrSentryServiceTestBase {

  @Test
  public void testQueryAdminOperation() throws Exception {
    // Success.
    adminQueryActionSuccess(ADMIN_USER);
    // Failure
    adminQueryActionFailure("user0");
    // Now grant admin privileges to user0 (i.e. role0) and verify the admin operations again.
    grantAdminPrivileges(ADMIN_USER, "role0", COLLECTIONS.getName(), SolrConstants.ALL);
    adminQueryActionSuccess("user0");
    // Now revoke admin update privileges from user0 (i.e. role0) and verify the admin operations again.
    revokeAdminPrivileges(ADMIN_USER, "role0", COLLECTIONS.getName(), SolrConstants.UPDATE);
    adminQueryActionSuccess("user0");
    // Now revoke admin query privileges from user0 (i.e. role0) and verify the admin operations again.
    revokeAdminPrivileges(ADMIN_USER, "role0", COLLECTIONS.getName(), SolrConstants.QUERY);
    adminQueryActionFailure("user0");
  }

  @Test
  public void testUpdateAdminOperation() throws Exception {
    String collectionName = "testUpdateAdminOperation";

    // Success.
    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, collectionName, SolrConstants.UPDATE);
    adminUpdateActionSuccess(ADMIN_USER, collectionName);

    // Failure
    adminUpdateActionFailure("user0", collectionName);

    // Now grant admin privileges role0 and verify the admin operations again.
    grantAdminPrivileges(ADMIN_USER, "role0", COLLECTIONS.getName(), SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.UPDATE);
    adminUpdateActionSuccess("user0", collectionName);

    // Now revoke admin query privileges from role0 and verify the admin operations again.
    revokeAdminPrivileges(ADMIN_USER, "role0", COLLECTIONS.getName(), SolrConstants.QUERY);
    adminUpdateActionSuccess(ADMIN_USER, collectionName);

    // Now revoke admin update privileges from role0 and verify the admin operations again.
    revokeAdminPrivileges(ADMIN_USER, "role0", COLLECTIONS.getName(), SolrConstants.UPDATE);
    adminUpdateActionFailure("user0", collectionName);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testMetricsQuerySuccess() throws Exception {
    grantAdminPrivileges(ADMIN_USER, "role0", AdminOperation.METRICS.getName(), SolrConstants.QUERY);

    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser("user0");

      String url = String.format("%s/admin/metrics?wt=json&group=jvm",
          cluster.getJettySolrRunner(0).getBaseUrl().toString());
      ClientResource resource = new ClientResource(url);
      Map result = readNestedElement(deserialize(resource.get(MediaType.APPLICATION_JSON)), "metrics");
      assertTrue(result.containsKey("solr.jvm"));

    } finally {
      revokeAdminPrivileges(ADMIN_USER, "role0", AdminOperation.METRICS.getName(), SolrConstants.QUERY);
      setAuthenticationUser(tmp);
    }
  }

  @Test
  public void testMetricsQueryFailure() throws Exception {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser("user1");

      String url = String.format("%s/admin/metrics?wt=json",
          cluster.getJettySolrRunner(0).getBaseUrl().toString());
      ClientResource resource = new ClientResource(url);
      resource.get(MediaType.APPLICATION_JSON);
      fail("This admin request should have failed with authorization error.");

    } catch (ResourceException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN , ex.getStatus().getCode());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void adminQueryActionSuccess(String userName) throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      ClusterStatus clusterStatus = new ClusterStatus();
      assertEquals(0, clusterStatus.process(cluster.getSolrClient()).getStatus());

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void adminQueryActionFailure(String userName) throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      ClusterStatus clusterStatus = new ClusterStatus();
      clusterStatus.process(cluster.getSolrClient());
      fail("This admin request should have failed with authorization error.");

    } catch (RemoteSolrException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN , ex.code());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void adminUpdateActionSuccess(String userName, String collectionName)
      throws SolrServerException, IOException {
    // Success.
    String tmp = getAuthenticatedUser();
    try {
      // Create collection.
      setAuthenticationUser(userName);
      CollectionAdminRequest.Create createCmd =
          CollectionAdminRequest.createCollection(collectionName, "cloud-minimal", 1, NUM_SERVERS);
      assertEquals(0, createCmd.process(cluster.getSolrClient()).getStatus());

      // Delete collection.
      CollectionAdminRequest.Delete delCmd = CollectionAdminRequest.deleteCollection(collectionName);
      assertEquals(0, delCmd.process(cluster.getSolrClient()).getStatus());

    } finally {
      setAuthenticationUser(tmp);
    }
  }

}
