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

import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.File;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.junit.Test;

/**
 * Test the document-level security features
 */
public class TestDocLevelOperations extends AbstractSolrSentryTestBase {
  private static final Logger LOG = LoggerFactory
    .getLogger(TestDocLevelOperations.class);
  private static final String DEFAULT_COLLECTION = "collection1";
  private static final String AUTH_FIELD = "sentry_auth";
  private static final int NUM_DOCS = 100;
  private static final int EXTRA_AUTH_FIELDS = 2;
  private String userName = null;

  @Before
  public void beforeTest() throws Exception {
    userName = getAuthenticatedUser();
  }

  @After
  public void afterTest() throws Exception {
    setAuthenticationUser(userName);
  }

  private void setupCollectionWithDocSecurity(String name) throws Exception {
    String configDir = RESOURCES_DIR + File.separator + DEFAULT_COLLECTION
      + File.separator + "conf";
    uploadConfigDirToZk(configDir);
    // replace solrconfig.xml with solrconfig-doc-level.xml
    uploadConfigFileToZk(configDir + File.separator + "solrconfig-doclevel.xml",
      "solrconfig.xml");
    setupCollection(name);
  }

  /**
   * Test that queries from different users only return the documents they have access to.
   */
  @Test
  public void testDocLevelOperations() throws Exception {
    String collectionName = "docLevelCollection";
    setupCollectionWithDocSecurity(collectionName);

    // create documents
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "description" + iStr);

      // put some bogus tokens in
      for (int k = 0; k < EXTRA_AUTH_FIELDS; ++k) {
        doc.addField(AUTH_FIELD, AUTH_FIELD + Long.toString(k));
      }
      // 50% of docs get "junit", 50% get "admin" as token
      if (i % 2 == 0) {
        doc.addField(AUTH_FIELD, "junit_role");
      } else {
        doc.addField(AUTH_FIELD, "admin_role");
      }
      // add a token to all docs so we can check that we can get all
      // documents returned
      doc.addField(AUTH_FIELD, "docLevel_role");

      docs.add(doc);
    }
    CloudSolrServer server = getCloudSolrServer(collectionName);
    try {
      server.add(docs);
      server.commit(true, true);

      // queries
      SolrQuery query = new SolrQuery();
      query.setQuery("*:*");

      // as junit -- should get half the documents
      setAuthenticationUser("junit");
      QueryResponse rsp = server.query(query);
      SolrDocumentList docList = rsp.getResults();
      assertEquals(NUM_DOCS / 2, docList.getNumFound());
      for (SolrDocument doc : docList) {
        String id = doc.getFieldValue("id").toString();
        assertEquals(0, Long.valueOf(id) % 2);
      }

      // as admin  -- should get the other half
      setAuthenticationUser("admin");
      rsp = server.query(query);
      docList = rsp.getResults();
      assertEquals(NUM_DOCS / 2, docList.getNumFound());
      for (SolrDocument doc : docList) {
        String id = doc.getFieldValue("id").toString();
        assertEquals(1, Long.valueOf(id) % 2);
      }

      // as docLevel -- should get all
      setAuthenticationUser("docLevel");
      rsp = server.query(query);
      assertEquals(NUM_DOCS, rsp.getResults().getNumFound());

      // test filter queries work as AND -- i.e. user can't avoid doc-level
      // checks by prefixing their own filterQuery
      setAuthenticationUser("junit");
      String fq = URLEncoder.encode(" {!raw f=" + AUTH_FIELD + " v=docLevel_role}");
      String path = "/" + collectionName + "/select?q=*:*&fq="+fq;
      String retValue = makeHttpRequest(server, "GET", path, null, null);
      assertTrue(retValue.contains("numFound=\"" + NUM_DOCS / 2 + "\" "));

      // test that user can't inject an "OR" into the query
      final String syntaxErrorMsg = "org.apache.solr.search.SyntaxError: Cannot parse";
      fq = URLEncoder.encode(" {!raw f=" + AUTH_FIELD + " v=docLevel_role} OR ");
      path = "/" + collectionName + "/select?q=*:*&fq="+fq;
      retValue = makeHttpRequest(server, "GET", path, null, null);
      assertTrue(retValue.contains(syntaxErrorMsg));

      // same test, prefix OR this time
      fq = URLEncoder.encode(" OR {!raw f=" + AUTH_FIELD + " v=docLevel_role}");
      path = "/" + collectionName + "/select?q=*:*&fq="+fq;
      retValue = makeHttpRequest(server, "GET", path, null, null);
      assertTrue(retValue.contains(syntaxErrorMsg));
    } finally {
      server.shutdown();
    }
  }

  /**
   * Test the allRolesToken.  Make it a keyword in the query language ("OR")
   * to make sure it is treated literally rather than interpreted.
   */
  @Test
  public void testAllRolesToken() throws Exception {
    String allRolesToken = "OR";
    String collectionName = "allRolesCollection";
    setupCollectionWithDocSecurity(collectionName);

    int junitFactor = 2;
    int allRolesFactor  = 5;

    int totalJunitAdded = 0; // total docs added with junit token
    int totalAllRolesAdded = 0; // total number of docs with the allRolesToken
    int totalOnlyAllRolesAdded = 0; // total number of docs with _only_ the allRolesToken

    // create documents
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      boolean addedViaJunit = false;
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "description" + iStr);

      if (i % junitFactor == 0) {
        doc.addField(AUTH_FIELD, "junit_role");
        addedViaJunit = true;
        ++totalJunitAdded;
      } if (i % allRolesFactor == 0) {
        doc.addField(AUTH_FIELD, allRolesToken);
        ++totalAllRolesAdded;
        if (!addedViaJunit) ++totalOnlyAllRolesAdded;
      }
      docs.add(doc);
    }
    // make sure our factors give us interesting results --
    // that some docs only have all roles and some only have junit
    assert(totalOnlyAllRolesAdded > 0);
    assert(totalJunitAdded > totalAllRolesAdded);

    CloudSolrServer server = getCloudSolrServer(collectionName);
    try {
      server.add(docs);
      server.commit(true, true);

      // queries
      SolrQuery query = new SolrQuery();
      query.setQuery("*:*");

      // as admin  -- should only get all roles token documents
      setAuthenticationUser("admin");
      QueryResponse rsp = server.query(query);
      SolrDocumentList docList = rsp.getResults();
      assertEquals(totalAllRolesAdded, docList.getNumFound());
      for (SolrDocument doc : docList) {
        String id = doc.getFieldValue("id").toString();
        assertEquals(0, Long.valueOf(id) % allRolesFactor);
      }

      // as junit -- should get junit added + onlyAllRolesAdded
      setAuthenticationUser("junit");
      rsp = server.query(query);
      docList = rsp.getResults();
      assertEquals(totalJunitAdded + totalOnlyAllRolesAdded, docList.getNumFound());
      for (SolrDocument doc : docList) {
        String id = doc.getFieldValue("id").toString();
        boolean addedJunit = (Long.valueOf(id) % junitFactor) == 0;
        boolean onlyAllRoles = !addedJunit && (Long.valueOf(id) % allRolesFactor) == 0;
        assertEquals(true, addedJunit || onlyAllRoles);
      }
    } finally {
      server.shutdown();
    }
  }
}
