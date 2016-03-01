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
package org.apache.sentry.tests.e2e.solr.db.integration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;

import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Test;

public class TestSolrDocLevelOperations extends AbstractSolrSentryTestWithDbProvider {
  private static final String TEST_COLLECTION_NAME1 = "collection1";
  private static final String AUTH_FIELD = "sentry_auth";
  private static final int NUM_DOCS = 100;

  private void setupCollectionWithDocSecurity(String name) throws Exception {
    String configDir = RESOURCES_DIR + File.separator + "collection1"
        + File.separator + "conf";
    uploadConfigDirToZk(configDir);
    // replace solrconfig.xml with solrconfig-doc-level.xml
    uploadConfigFileToZk(configDir + File.separator + "solrconfig-doclevel.xml",
        "solrconfig.xml");
    setupCollection(name);
  }

  @Test
  public void testDocLevelOperations() throws Exception {
    setupCollectionWithDocSecurity(TEST_COLLECTION_NAME1);

    createDocument(TEST_COLLECTION_NAME1);

    CloudSolrServer server = getCloudSolrServer(TEST_COLLECTION_NAME1);
    try {
      // queries
      SolrQuery query = new SolrQuery();
      query.setQuery("*:*");

      // as admin
      setAuthenticationUser(ADMIN_USER);
      QueryResponse  rsp = server.query(query);
      SolrDocumentList docList = rsp.getResults();
      assertEquals(NUM_DOCS, docList.getNumFound());

      // as user0
      setAuthenticationUser("user0");
      grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.QUERY);
      rsp = server.query(query);
      docList = rsp.getResults();
      assertEquals(NUM_DOCS/4, rsp.getResults().getNumFound());

      //as user1
      setAuthenticationUser("user1");
      grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role1", SearchConstants.QUERY);
      rsp = server.query(query);
      docList = rsp.getResults();
      assertEquals(NUM_DOCS/4, rsp.getResults().getNumFound());  docList = rsp.getResults();
      assertEquals(NUM_DOCS/4, rsp.getResults().getNumFound());

      //as user2
      setAuthenticationUser("user2");
      grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role2", SearchConstants.QUERY);
      rsp = server.query(query);
      docList = rsp.getResults();
      assertEquals(NUM_DOCS/4, rsp.getResults().getNumFound());

      //as user3
      setAuthenticationUser("user3");
      grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role3", SearchConstants.QUERY);
      rsp = server.query(query);
      docList = rsp.getResults();
      assertEquals(NUM_DOCS/4, rsp.getResults().getNumFound());
    } finally {
      server.shutdown();
    }

    deleteCollection(TEST_COLLECTION_NAME1);
    dropCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER);
  }

  @Test
  public void updateDocsTest() throws Exception {
    setupCollectionWithDocSecurity(TEST_COLLECTION_NAME1);

    createDocument(TEST_COLLECTION_NAME1);

    CloudSolrServer server = getCloudSolrServer(TEST_COLLECTION_NAME1);
    try {
      setAuthenticationUser("user0");
      grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.QUERY);
      String docIdStr = Long.toString(1);

      // verify we can't view one of the odd documents
      SolrQuery query = new SolrQuery();
      query.setQuery("id:"+docIdStr);
      QueryResponse rsp = server.query(query);
      assertEquals(0, rsp.getResults().getNumFound());

      // overwrite the document that we can't see
      setAuthenticationUser(ADMIN_USER);
      ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docIdStr);
      doc.addField("description", "description" + docIdStr);
      doc.addField(AUTH_FIELD, "role0");
      docs.add(doc);
      server.add(docs);
      server.commit();

      // verify we can now view the document
      setAuthenticationUser("user0");
      rsp = server.query(query);
      assertEquals(1, rsp.getResults().getNumFound());
    } finally {
      server.shutdown();
    }

    deleteCollection(TEST_COLLECTION_NAME1);
    dropCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER);
  }

  /**
   * Test to validate doc level security on collections without perm for Index level auth.
   * @throws Exception
   */
  @Test
  public void indexDocAuthTests() throws Exception {
    setupCollectionWithDocSecurity(TEST_COLLECTION_NAME1);
    try {
      createDocument(TEST_COLLECTION_NAME1);
      // test query for "*:*" fails as user0 (user0 doesn't have index level permissions but has doc level permissions set)
      verifyQueryFail("user0", TEST_COLLECTION_NAME1, ALL_DOCS);
      verifyQueryFail("user1", TEST_COLLECTION_NAME1, ALL_DOCS);
      verifyQueryFail("user2", TEST_COLLECTION_NAME1, ALL_DOCS);
      verifyQueryFail("user3", TEST_COLLECTION_NAME1, ALL_DOCS);

    } finally {
      deleteCollection(TEST_COLLECTION_NAME1);
      dropCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER);
    }
  }

  /**
   * Creates docs as follows and verifies queries work as expected:
   * - creates NUM_DOCS documents, where the document id equals the order
   *   it was created in, starting at 0
   * - when id % 4 == 0, documents get "role0" auth token
   * - when id % 4 == 1, documents get "role1" auth token
   * - when id % 4 == 2, documents get "role2" auth token
   * - when id % 4 == 3, documents get "role3" auth token
   * - all documents get a admin role
   */
  private void createDocument(String collectionName) throws Exception {
    // ensure no current documents
    verifyDeletedocsPass(ADMIN_USER, collectionName, true);

    // create documents
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "description" + iStr);

      if (i % 4 == 0) {
        doc.addField(AUTH_FIELD, "role0");
      } else if (i % 4 ==1) {
        doc.addField(AUTH_FIELD, "role1");
      } else if (i % 4 ==2) {
        doc.addField(AUTH_FIELD, "role2");
      } else {
        doc.addField(AUTH_FIELD, "role3");
      }
      doc.addField(AUTH_FIELD, ADMIN_ROLE);
      docs.add(doc);
    }

    setAuthenticationUser(ADMIN_USER);
    CloudSolrServer server = getCloudSolrServer(collectionName);
    try {
      server.add(docs);
      server.commit(true, true);
    } finally {
      server.shutdown();
    }
  }
}