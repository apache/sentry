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

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the document-level security features
 */
public class TestDocLevelOperations extends SolrSentryServiceTestBase {
  private static final String AUTH_FIELD = "sentry_auth";
  private static final int NUM_DOCS = 100;
  private static final int EXTRA_AUTH_FIELDS = 2;

  @BeforeClass
  public static void setupPermissions() throws SentryUserException {
    sentryClient.createRole(ADMIN_USER, "junit_role", COMPONENT_SOLR);
    sentryClient.createRole(ADMIN_USER, "doclevel_role", COMPONENT_SOLR);
    sentryClient.grantRoleToGroups(ADMIN_USER, "junit_role", COMPONENT_SOLR,
        Collections.singleton("junit"));
    sentryClient.grantRoleToGroups(ADMIN_USER, "doclevel_role", COMPONENT_SOLR,
        Collections.singleton("doclevel"));

    // junit user
    grantAdminPrivileges(ADMIN_USER, "junit_role", SolrConstants.ALL, SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "junit_role", "docLevelCollection", SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "junit_role", "allRolesCollection", SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "junit_role", "testUpdateDeleteOperations", SolrConstants.ALL);

    // docLevel user
    grantCollectionPrivileges(ADMIN_USER, "doclevel_role", "docLevelCollection", SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "doclevel_role", "testUpdateDeleteOperations", SolrConstants.ALL);

    // admin user
    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, SolrConstants.ALL, SolrConstants.ALL);
  }

  @Before
  public void resetAuthenticatedUser() {
    setAuthenticationUser("admin");
  }

  private QueryRequest getRealTimeGetRequest() {
    // real time get request
    StringBuilder idsBuilder = new StringBuilder("0");
    for (int i = 1; i < NUM_DOCS; ++i) {
      idsBuilder.append("," + i);
    }
    return getRealTimeGetRequest(idsBuilder.toString());
  }

  @SuppressWarnings("serial")
  private QueryRequest getRealTimeGetRequest(String ids) {
    final ModifiableSolrParams idsParams = new ModifiableSolrParams();
    idsParams.add("ids", ids);
    return new QueryRequest() {
      @Override
      public String getPath() {
        return "/get";
      }

      @Override
      public SolrParams getParams() {
        return idsParams;
      }
    };
  }

  /**
   * Creates docs as follows and verifies queries work as expected:
   * - creates NUM_DOCS documents, where the document id equals the order
   *   it was created in, starting at 0
   * - even-numbered documents get "junit_role" auth token
   * - odd-numbered documents get "admin_role" auth token
   * - all documents get some bogus auth tokens
   * - all documents get a docLevel_role auth token
   */
  private void createDocsAndQuerySimple(String collectionName, boolean checkNonAdminUsers) throws Exception {

    // ensure no current documents
    verifyDeletedocsPass(ADMIN_USER, collectionName, true);

    DocLevelGenerator generator = new DocLevelGenerator(collectionName, AUTH_FIELD);
    generator.generateDocs(cluster.getSolrClient(), NUM_DOCS, "junit_role", "admin_role", EXTRA_AUTH_FIELDS);

    querySimple(collectionName, new QueryRequest(new SolrQuery("*:*")), cluster.getSolrClient(), checkNonAdminUsers);
    querySimple(collectionName, getRealTimeGetRequest(), cluster.getSolrClient(), checkNonAdminUsers);
  }

  private void querySimple(String collectionName, QueryRequest request, CloudSolrClient client,
      boolean checkNonAdminUsers) throws Exception {
    // as admin  -- should get the other half
    setAuthenticationUser("admin");
    QueryResponse  rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();
    assertEquals(NUM_DOCS / 2, docList.getNumFound());
    for (SolrDocument doc : docList) {
      String id = doc.getFieldValue("id").toString();
      assertEquals(1, Long.valueOf(id) % 2);
    }

    if (checkNonAdminUsers) {
      // as junit -- should get half the documents
      setAuthenticationUser("junit");
      rsp = request.process(client, collectionName);
      docList = rsp.getResults();
      assertEquals(NUM_DOCS / 2, docList.getNumFound());
      for (SolrDocument doc : docList) {
        String id = doc.getFieldValue("id").toString();
        assertEquals(0, Long.valueOf(id) % 2);
      }

      // as docLevel -- should get all
      setAuthenticationUser("doclevel");
      rsp = request.process(client, collectionName);
      assertEquals(NUM_DOCS, rsp.getResults().getNumFound());
    }
  }

  /**
   * Test that queries from different users only return the documents they have access to.
   */
  @Test
  public void testDocLevelOperations() throws Exception {
    String collectionName = "docLevelCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_doc_level_security", NUM_SERVERS, 1);

    CloudSolrClient client = cluster.getSolrClient();
    createDocsAndQuerySimple(collectionName, true);

    // test filter queries work as AND -- i.e. user can't avoid doc-level
    // checks by prefixing their own filterQuery
    setAuthenticationUser("junit");
    String fq = URLEncoder.encode(" {!raw f=" + AUTH_FIELD + " v=doclevel_role}");
    String path = "/" + collectionName + "/select?q=*:*&fq="+fq;
    String retValue = makeHttpRequest(client, "GET", path, null, null, HttpServletResponse.SC_OK);
    assertTrue("Result : " + retValue, retValue.contains("\"numFound\":"  + NUM_DOCS / 2));

    // test that user can't inject an "OR" into the query
    final String syntaxErrorMsg = "org.apache.solr.search.SyntaxError: Cannot parse";
    fq = URLEncoder.encode(" {!raw f=" + AUTH_FIELD + " v=docLevel_role} OR ");
    path = "/" + collectionName + "/select?q=*:*&fq="+fq;
    retValue = makeHttpRequest(client, "GET", path, null, null, HttpServletResponse.SC_BAD_REQUEST);
    assertTrue(retValue.contains(syntaxErrorMsg));

    // same test, prefix OR this time
    fq = URLEncoder.encode(" OR {!raw f=" + AUTH_FIELD + " v=docLevel_role}");
    path = "/" + collectionName + "/select?q=*:*&fq="+fq;
    retValue = makeHttpRequest(client, "GET", path, null, null, HttpServletResponse.SC_BAD_REQUEST);
    assertTrue(retValue.contains(syntaxErrorMsg));
  }

  /**
   * Test the allRolesToken.  Make it a keyword in the query language ("OR")
   * to make sure it is treated literally rather than interpreted.
   */
  @Test
  public void testAllRolesToken() throws Exception {
    String collectionName = "allRolesCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_doc_level_security", NUM_SERVERS, 1);

    String allRolesToken = "OR";
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
        if (!addedViaJunit) {
          ++totalOnlyAllRolesAdded;
        }
      }
      docs.add(doc);
    }
    // make sure our factors give us interesting results --
    // that some docs only have all roles and some only have junit
    assert(totalOnlyAllRolesAdded > 0);
    assert(totalJunitAdded > totalAllRolesAdded);

    cluster.getSolrClient().add(collectionName, docs);
    cluster.getSolrClient().commit(collectionName, true, true);

    checkAllRolesToken(collectionName, new QueryRequest(new SolrQuery("*:*")), cluster.getSolrClient(),
        totalAllRolesAdded, totalOnlyAllRolesAdded, allRolesFactor, totalJunitAdded, junitFactor);
    checkAllRolesToken(collectionName, getRealTimeGetRequest(), cluster.getSolrClient(),
         totalAllRolesAdded, totalOnlyAllRolesAdded, allRolesFactor, totalJunitAdded, junitFactor);
  }

  private void checkAllRolesToken(String collectionName, QueryRequest request, CloudSolrClient client,
      int totalAllRolesAdded, int totalOnlyAllRolesAdded, int allRolesFactor, int totalJunitAdded, int junitFactor) throws Exception {
    // as admin  -- should only get all roles token documents
    setAuthenticationUser("admin");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();
    assertEquals(totalAllRolesAdded, docList.getNumFound());
    for (SolrDocument doc : docList) {
      String id = doc.getFieldValue("id").toString();
      assertEquals(0, Long.valueOf(id) % allRolesFactor);
    }

    // as junit -- should get junit added + onlyAllRolesAdded
    setAuthenticationUser("junit");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(totalJunitAdded + totalOnlyAllRolesAdded, docList.getNumFound());
    for (SolrDocument doc : docList) {
      String id = doc.getFieldValue("id").toString();
      boolean addedJunit = (Long.valueOf(id) % junitFactor) == 0;
      boolean onlyAllRoles = !addedJunit && (Long.valueOf(id) % allRolesFactor) == 0;
      assertEquals(true, addedJunit || onlyAllRoles);
    }
  }

  /**
   * delete the docs as "deleteUser" using deleteByQuery "deleteQueryStr".
   * Verify that number of docs returned for "queryUser" equals
   * "expectedQueryDocs" after deletion.
   */
  private void deleteByQueryTest(String collectionName, String deleteUser,
      String deleteByQueryStr, String queryUser, int expectedQueryDocs) throws Exception {
    createDocsAndQuerySimple(collectionName, true);
    setAuthenticationUser(deleteUser);
    cluster.getSolrClient().deleteByQuery(collectionName, deleteByQueryStr);
    cluster.getSolrClient().commit(collectionName);

    checkDeleteByQuery(collectionName, new QueryRequest(new SolrQuery("*:*")), cluster.getSolrClient(),
        queryUser, expectedQueryDocs);
    checkDeleteByQuery(collectionName, getRealTimeGetRequest(), cluster.getSolrClient(),
        queryUser, expectedQueryDocs);
  }

  private void checkDeleteByQuery(String collectionName, QueryRequest query, CloudSolrClient server,
      String queryUser, int expectedQueryDocs) throws Exception {
    QueryResponse rsp =  query.process(server, collectionName);
    long junitResults = rsp.getResults().getNumFound();
    assertEquals(0, junitResults);

    setAuthenticationUser(queryUser);
    rsp =  query.process(server, collectionName);
    long docLevelResults = rsp.getResults().getNumFound();
    assertEquals(expectedQueryDocs, docLevelResults);
  }

  private void deleteByIdTest(String collectionName) throws Exception {
    createDocsAndQuerySimple(collectionName, true);
    setAuthenticationUser("junit");
    List<String> allIds = new ArrayList<String>(NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; ++i) {
      allIds.add(Long.toString(i));
    }
    cluster.getSolrClient().deleteById(collectionName, allIds);
    cluster.getSolrClient().commit(collectionName);

    checkDeleteById(collectionName, new QueryRequest(new SolrQuery("*:*")), cluster.getSolrClient());
    checkDeleteById(collectionName, getRealTimeGetRequest(), cluster.getSolrClient());

  }

  private void checkDeleteById(String collectionName, QueryRequest request, CloudSolrClient server)
      throws Exception {
    QueryResponse rsp = request.process(server, collectionName);
    long junitResults = rsp.getResults().getNumFound();
    assertEquals(0, junitResults);

    setAuthenticationUser("doclevel");
    rsp =  request.process(server, collectionName);
    long docLevelResults = rsp.getResults().getNumFound();
    assertEquals(0, docLevelResults);
  }

  private void updateDocsTest(String collectionName) throws Exception {
    createDocsAndQuerySimple(collectionName, true);
    setAuthenticationUser("junit");
    String docIdStr = Long.toString(1);

    // verify we can't view one of the odd documents
    QueryRequest query = new QueryRequest(new SolrQuery("id:"+docIdStr));
    QueryRequest rtgQuery = getRealTimeGetRequest(docIdStr);
    checkUpdateDocsQuery(collectionName, query, cluster.getSolrClient(), 0);
    checkUpdateDocsQuery(collectionName, rtgQuery, cluster.getSolrClient(), 0);

    // overwrite the document that we can't see
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", docIdStr);
    doc.addField("description", "description" + docIdStr);
    doc.addField(AUTH_FIELD, "junit_role");
    docs.add(doc);
    cluster.getSolrClient().add(collectionName, docs);
    cluster.getSolrClient().commit(collectionName);

    // verify we can now view the document
    checkUpdateDocsQuery(collectionName, query, cluster.getSolrClient(), 1);
    checkUpdateDocsQuery(collectionName, rtgQuery, cluster.getSolrClient(), 1);

  }

  private void checkUpdateDocsQuery(String collectionName, QueryRequest request, CloudSolrClient server, int expectedDocs)
      throws Exception {
    QueryResponse rsp = request.process(server, collectionName);
    assertEquals(expectedDocs, rsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateDeleteOperations() throws Exception {
    String collectionName = "testUpdateDeleteOperations";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_doc_level_security", NUM_SERVERS, 1);

    createDocsAndQuerySimple(collectionName, true);

    // test deleteByQuery "*:*" - we expect this to delete all docs, and it does.
    deleteByQueryTest(collectionName, "junit", "*:*", "doclevel", 0);

    // test deleteByQuery non-*:* - this proves that the junit can delete documents (sentry_auth:doclevel_role) that he can't see.
    //                              We verify this with querying as doclevel (who can see all, and he now sees zero of these docs)
    deleteByQueryTest(collectionName, "junit", "sentry_auth:doclevel_role", "doclevel", 0);

    // test deleting all documents by Id - in this case the junit user can delete all docs
    deleteByIdTest(collectionName);

    // This is testing the expected behaviour that if we have been granted the ALL role then we can
    // update docs that we can't see, even to the point that we make them visible.
    updateDocsTest(collectionName);

  }

  /**
   * Test to validate doc level security on collections without perm for Index level auth.
   * @throws Exception
   */
  @Test
  public void indexDocAuthTests() throws Exception {
    String collectionName = "testIndexlevelDoclevelOperations";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_doc_level_security", NUM_SERVERS, 1);

    createDocsAndQuerySimple(collectionName, false);

    // test query for "*:*" fails as junit user (junit user doesn't have index level permissions but has doc level permissions set)
    verifyQueryFail("junit", collectionName, ALL_DOCS);

    // test query for "*:*" fails as docLevel user (docLevel user has neither index level permissions nor doc level permissions set)
    verifyQueryFail("doclevel", collectionName, ALL_DOCS);
  }
}
