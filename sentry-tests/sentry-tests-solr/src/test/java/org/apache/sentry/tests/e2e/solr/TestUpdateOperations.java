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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

public class TestUpdateOperations extends AbstractSolrSentryTestBase {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestUpdateOperations.class);
  private static final String COLLECTION_NAME = "sentryCollection";
  private static final List<Boolean> BOOLEAN_VALUES = Arrays.asList(new Boolean[]{true, false});
  private static final String DEFAULT_COLLECTION = "collection1";

  @Test
  public void testUpdateOperations() throws Exception {
    // Upload configs to ZK
    uploadConfigDirToZk(RESOURCES_DIR + File.separator + DEFAULT_COLLECTION
        + File.separator + "conf");
    setupCollection(COLLECTION_NAME);
    ArrayList<String> testFailures = new ArrayList<String>();

    for (boolean query : BOOLEAN_VALUES) {
      for (boolean update : BOOLEAN_VALUES) {
        for (boolean all : BOOLEAN_VALUES) {
          String test_user = getUsernameForPermissions(COLLECTION_NAME, query, update, all);
          LOG.info("TEST_USER: " + test_user);

          try {
            if (all || update) {
              cleanSolrCollection(COLLECTION_NAME);
              SolrInputDocument solrInputDoc = createSolrTestDoc();
              verifyUpdatePass(test_user, COLLECTION_NAME, solrInputDoc);

              cleanSolrCollection(COLLECTION_NAME);
              uploadSolrDoc(COLLECTION_NAME, null);
              verifyDeletedocsPass(test_user, COLLECTION_NAME, false);
            } else {
              cleanSolrCollection(COLLECTION_NAME);
              SolrInputDocument solrInputDoc = createSolrTestDoc();
              verifyUpdateFail(test_user, COLLECTION_NAME, solrInputDoc);

              cleanSolrCollection(COLLECTION_NAME);
              uploadSolrDoc(COLLECTION_NAME, null);
              verifyDeletedocsFail(test_user, COLLECTION_NAME, false);
            }
          } catch (Throwable testException) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            testException.printStackTrace(printWriter);
            testFailures.add("\n\nTestFailure: User -> " + test_user + "\n"
                + stringWriter.toString());
          }
        }
      }
    }

    assertEquals("Total test failures: " + testFailures.size() + " \n\n"
        + testFailures.toString() + "\n\n\n", 0, testFailures.size());
  }

  @Test
  public void testInvariantProcessor() throws Exception {
    String collectionName = "testInvariantCollection";
    // Upload configs to ZK
    uploadConfigDirToZk(RESOURCES_DIR + File.separator + DEFAULT_COLLECTION
        + File.separator + "conf");
    setupCollection(collectionName);

    // Send a update request and try to set the update.chain to skip the
    // index-authorization checks
    setAuthenticationUser("junit");
    CloudSolrServer server = getCloudSolrServer(collectionName);
    try {
      String path = "/" + collectionName + "/update?update.chain=skipUpdateIndexAuthorization&commit=true";
      String body = "<add><doc><field name=\"id\">testInvariantDoc</field></doc></add>";
      String ret = makeHttpRequest(server, "POST", path, body.getBytes("UTF-8"), "text/xml");
      assertTrue("Expected sentry exception", ret.contains("SentrySolrAuthorizationException: User junit"
        + " does not have privileges for testInvariantCollection"));
    } finally {
      server.shutdown();
    }
  }

  private void checkUpdateDistribPhase(CloudSolrServer server, String collectionName,
      String userName, DistribPhase distribPhase) throws Exception {
    String path = "/" + collectionName + "/update?commit=true";
    String updateDistribParam="";
    if (distribPhase != null) {
      updateDistribParam = distribPhase.toString();
      path += "&update.distrib="+updateDistribParam;
    }
    String docId = "testUpdateDistribDoc"+updateDistribParam;
    String body = "<add><doc><field name=\"id\">"+docId+"</field></doc></add>";

    String node = null;
    ClusterState clusterState = server.getZkStateReader().getClusterState();
    for (Slice slice : clusterState.getActiveSlices(collectionName)) {
      if(slice.getRange().includes(docId.hashCode())) {
        node = slice.getLeader().getNodeName().replace("_solr", "/solr");
      }
    }
    assertNotNull("Expected to find leader node for document", node);

    String ret = makeHttpRequest(server, node, "POST", path, body.getBytes("UTF-8"), "text/xml");
    assertTrue("Expected sentry exception",
      ret.contains("SentrySolrAuthorizationException: " +
        "User " + userName + " does not have privileges for " + collectionName));
  }

  @Test
  public void testUpdateDistribPhase() throws Exception {
    final String collectionName = "testUpdateDistribPhase";
    final String userName = "junit";
    // Upload configs to ZK
    uploadConfigDirToZk(RESOURCES_DIR + File.separator + DEFAULT_COLLECTION
        + File.separator + "conf");
    setupCollection(collectionName);

    setAuthenticationUser(userName);
    CloudSolrServer server = getCloudSolrServer(collectionName);
    try {
      // ensure user can't update collection
      checkUpdateDistribPhase(server, collectionName, userName, null);

      // now, try to update collection, setting update.distrib to possible values
      for ( DistribPhase phase : DistribPhase.values() ) {
        checkUpdateDistribPhase(server, collectionName, userName, phase);
      }
    } finally {
      server.shutdown();
    }
  }
}
