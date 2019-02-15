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

import com.google.common.collect.Sets;
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

import javax.servlet.http.HttpServletResponse;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.sentry.tests.e2e.solr.TestSentryServer.ADMIN_USER;

/**
 * Test the document-level security features
 */
public class TestSubsetQueryOperations extends SolrSentryServiceTestBase {
  private static final String AUTH_FIELD = "sentry_auth";
  private static final String COUNTER_FIELD = "sentry_auth_count";
  private static final int NUM_DOCS = 16;
  private static final int NUM_AUTH_TOKENS = 4;
  private static final String AUTH_TOKEN_PREFIX = "subset_role";
  private static final String AUTH_GROUP_PREFIX = "subset_group";

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
    grantCollectionPrivileges(ADMIN_USER, "junit_role", "allRolesCollection1", SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "junit_role", "allRolesCollection2", SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "junit_role", "testUpdateDeleteOperations", SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "junit_role", "subsetCollection", SolrConstants.QUERY);

    // docLevel user
    grantCollectionPrivileges(ADMIN_USER, "doclevel_role", "docLevelCollection", SolrConstants.ALL);
    grantCollectionPrivileges(ADMIN_USER, "doclevel_role", "testUpdateDeleteOperations", SolrConstants.ALL);

    // admin user
    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, SolrConstants.ALL, SolrConstants.ALL);

    for (int i=0; i<NUM_AUTH_TOKENS; i++) {
      String roleName = AUTH_TOKEN_PREFIX + i;
      sentryClient.createRole(ADMIN_USER, roleName, COMPONENT_SOLR);
      sentryClient.grantRoleToGroups(ADMIN_USER, roleName, COMPONENT_SOLR, Collections.singleton(AUTH_GROUP_PREFIX + i));
      grantCollectionPrivileges(ADMIN_USER, roleName, "subsetCollection", SolrConstants.QUERY);
      grantCollectionPrivileges(ADMIN_USER, roleName, "allRolesCollection1", SolrConstants.QUERY);
      grantCollectionPrivileges(ADMIN_USER, roleName, "allRolesCollection2", SolrConstants.QUERY);
      grantCollectionPrivileges(ADMIN_USER, roleName, "testUpdateDeleteOperations", SolrConstants.QUERY);
      grantCollectionPrivileges(ADMIN_USER, roleName, "testIndexlevelDoclevelOperations", SolrConstants.QUERY);
    }

    sentryClient.createRole(ADMIN_USER, "subset_norole", COMPONENT_SOLR);
    sentryClient.createRole(ADMIN_USER, "subset_delete", COMPONENT_SOLR);
    sentryClient.grantRoleToGroups(ADMIN_USER, "subset_norole", COMPONENT_SOLR, Collections.singleton("subset_nogroup"));
    sentryClient.grantRoleToGroups(ADMIN_USER, "subset_delete", COMPONENT_SOLR, Collections.singleton("subset_delete"));
    grantCollectionPrivileges(ADMIN_USER, "subset_norole", "subsetCollection", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "subset_norole", "allRolesCollection1", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "subset_norole", "allRolesCollection2", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "subset_norole", "testUpdateDeleteOperations", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "subset_norole", "testIndexlevelDoclevelOperations", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "subset_delete", "testUpdateDeleteOperations", SolrConstants.ALL);

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
  private void createDocsAndQuerySimple(String collectionName) throws Exception {

    // ensure no current documents
    verifyDeletedocsPass(ADMIN_USER, collectionName, true);

    DocLevelGenerator generator = new DocLevelGenerator(collectionName, AUTH_FIELD);
    generator.generateDocsForSubsetQueries(cluster.getSolrClient(), NUM_DOCS, NUM_AUTH_TOKENS, AUTH_TOKEN_PREFIX);

    querySimple(collectionName, new QueryRequest(new SolrQuery("*:*")), cluster.getSolrClient());
    querySimple(collectionName, getRealTimeGetRequest(), cluster.getSolrClient());
  }

  private void querySimple(String collectionName, QueryRequest request, CloudSolrClient client) throws Exception {
    /*
    subset_user_012  => subset_role0, subset_role1, subset_role2
    subset_user_013  => subset_role0, subset_role1, subset_role3
    subset_user_023  => subset_role0, subset_role2, subset_role3
    subset_user_123  => subset_role1, subset_role2, subset_role3
    subset_user_0    => subset_role0
    subset_user_2    => subset_role2
    subset_user_01   => subset_role0, subset_role1
    subset_user_23   => subset_role2, subset_role3
    subset_user_0123 => subset_role0, subset_role1, subset_role2, subset_role3
    Note: All users have an extra role for good measure. This should not impact the results
     */

    // as junit  -- should only get docs with no labels as allowMissing=true


    int expectedResultMultiplier = (int) (NUM_DOCS / (Math.pow(2, NUM_AUTH_TOKENS)));

    Set<String> roleSet = Sets.newHashSet();
    checkSimpleQueryResults(collectionName, request, client, "subset_user_no", expectedResultMultiplier, roleSet);


    setAuthenticationUser("subset_user_no");
    QueryResponse  rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();
    assertEquals(expectedResultMultiplier, docList.getNumFound());
    for (SolrDocument doc : docList) {
      String id = doc.getFieldValue("id").toString();
      assertEquals(0, Long.valueOf(id) % 16);
      assertTrue(doc.getFieldValues(AUTH_FIELD) == null || doc.getFieldValues(AUTH_FIELD).isEmpty());
    }

    // as subset_user_1234 - should see all docs
    roleSet = Sets.newHashSet("subset_role0", "subset_role1", "subset_role2", "subset_role3");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_0123", NUM_DOCS, roleSet);

    roleSet = Sets.newHashSet("subset_role0", "subset_role1", "subset_role2");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_012", 8 * expectedResultMultiplier, roleSet);

    roleSet = Sets.newHashSet("subset_role0", "subset_role1", "subset_role3");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_013", 8 * expectedResultMultiplier, roleSet);

    roleSet = Sets.newHashSet("subset_role0", "subset_role2", "subset_role3");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_023", 8 * expectedResultMultiplier, roleSet);

    roleSet = Sets.newHashSet("subset_role1", "subset_role2", "subset_role3");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_123", 8 * expectedResultMultiplier, roleSet);


    roleSet = Sets.newHashSet("subset_role0");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_0", 2 * expectedResultMultiplier, roleSet);

    roleSet = Sets.newHashSet("subset_role2");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_2", 2 * expectedResultMultiplier, roleSet);


    roleSet = Sets.newHashSet("subset_role0", "subset_role1");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_01", 4 * expectedResultMultiplier, roleSet);

    roleSet = Sets.newHashSet("subset_role2", "subset_role3");
    checkSimpleQueryResults(collectionName, request, client, "subset_user_23", 4 * expectedResultMultiplier, roleSet);


  }

  private void checkSimpleQueryResults(String collectionName, QueryRequest request, CloudSolrClient client, String username, int expectedResults, Set<String> roles) throws Exception {
    setAuthenticationUser(username);
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();

    assertEquals(expectedResults, docList.getNumFound());
    for (SolrDocument doc : docList) {
      Collection<Object> fieldValues = doc.getFieldValues(AUTH_FIELD);
      if (fieldValues != null && !fieldValues.isEmpty()) {
        assertTrue(roles.containsAll(fieldValues));
      }
    }
  }

  /**
   * Test that queries from different users only return the documents they have access to.
   */
  @Test
  public void testDocLevelOperations() throws Exception {
    String collectionName = "subsetCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_subset_match", NUM_SERVERS, 1);

    /*
       Going to test using subset_user_01 - he should only be able to access 4 / 16th of the docs available
       We're going try and break out and access docs with subset_role2.
    */

    String targetRole = "subset_role2";

    createDocsAndQuerySimple(collectionName);
    CloudSolrClient client = cluster.getSolrClient();

    // test filter queries work as AND -- i.e. user can't avoid doc-level
    // checks by prefixing their own filterQuery
    setAuthenticationUser("subset_user_01");
    String fq = URLEncoder.encode(" {!raw f=" + AUTH_FIELD + " v=" + targetRole + "}");
    String path = "/" + collectionName + "/select?q=*:*&fq="+fq;
    String retValue = makeHttpRequest(client, "GET", path, null, null, HttpServletResponse.SC_OK);
    assertTrue(retValue.contains("numFound\":" + 0 + ",\""));

    // test that user can't use a simple q
    path = "/" + collectionName + "/select?q=" + AUTH_FIELD + ":" + targetRole + "&fq="+fq;
    retValue = makeHttpRequest(client, "GET", path, null, null, HttpServletResponse.SC_OK);
    assertTrue(retValue.contains("numFound\":" + 0 + ",\""));


    // test that user can't inject an "OR" into the query
    final String syntaxErrorMsg = "org.apache.solr.search.SyntaxError: Cannot parse";
    fq = URLEncoder.encode(" {!raw f=" + AUTH_FIELD + " v=" + targetRole + "} OR ");
    path = "/" + collectionName + "/select?q=*:*&fq="+fq;
    retValue = makeHttpRequest(client, "GET", path, null, null, HttpServletResponse.SC_BAD_REQUEST);
    assertTrue(retValue.contains(syntaxErrorMsg));

    // same test, prefix OR this time
    fq = URLEncoder.encode(" OR {!raw f=" + AUTH_FIELD + " v=" + targetRole + "}");
    path = "/" + collectionName + "/select?q=*:*&fq="+fq;
    retValue = makeHttpRequest(client, "GET", path, null, null, HttpServletResponse.SC_BAD_REQUEST);
    assertTrue(retValue.contains(syntaxErrorMsg));

  }

  /**
   * Test the allRolesToken.  Make it a keyword in the query language ("OR")
   * to make sure it is treated literally rather than interpreted.
   * Note: In the {@link org.apache.solr.handler.component.QueryDocAuthorizationComponent}, allRoles is a role that is automatically given to everyone.
   * It is then added to the list of things that is ANDed together.
   * So if a doc has ROLE1, ROLE2, ROLE3 and ROLE1 is the allRoles token, the user must have ROLE2 AND ROLE3
   * Note: This test performs differently if the allow_missing_val is set (allow_missing_val works in a similar way to allRoles)
   * i.e. If turned on, everyone can see a doc that has no values for the auth token.
   */
  @Test
  public void testAllRolesToken() throws Exception {
    testAllRolesAndMissingValues(true);
  }

  @Test
  public void testAllRolesTokenWithMissingFalse() throws Exception {
    testAllRolesAndMissingValues(false);
  }



  private void testAllRolesAndMissingValues(boolean allowMissingValues) throws Exception {
    String collectionName;

    if (allowMissingValues) {
      collectionName = "allRolesCollection1";
      createCollection(ADMIN_USER, collectionName, "cloud-minimal_subset_match", NUM_SERVERS, 1);
    } else {
      collectionName = "allRolesCollection2";
      createCollection(ADMIN_USER, collectionName, "cloud-minimal_subset_match_missing_false", NUM_SERVERS, 1);
    }

    String allRolesToken = "OR";
    /* Going to create:
       4 with no auth tokens
       5 with the junit role, 3 of which have all roles token
       13 with the junit2 role, 7 of which have all roles token
       17 with junit2 AND junit1, 2 of which have no roles
       19 with just all roles token

       Expected results:
       junit user can see 19 + 5 + 4 = 28 docs when allow_missing_val is true, and 24 when not
       admin user can see 19 + 4 = 23 roles when allow_missing_val is true, and 19 when not
     */

    String junitRole = "junit_role";
    String junit2Role = "junit_role2";

    int junit = 5;
    int junitAllRoles = 3;
    int junit2 = 13;
    int junit2AllRoles = 7;
    int allRolesOnly = 19;
    int noRoles = 4;
    int junit1Andjunit2 = 17;
    int junit1Andjunit2AllRoles = 2;
    int counter=0;

    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

    for (int i=0; i<junit; i++) {
      Set<String> roles = Sets.newHashSet(junitRole);
      if (i<junitAllRoles) {
        roles.add(allRolesToken);
      }
      docs.add(createAllDocsTestDocument(counter++, roles));
    }

    for (int i=0; i<junit2; i++) {
      Set<String> roles = Sets.newHashSet(junit2Role);
      if (i<junit2AllRoles) {
        roles.add(allRolesToken);
      }
      docs.add(createAllDocsTestDocument(counter++, roles));
    }

    for (int i=0; i<allRolesOnly; i++) {
      Set<String> roles = Sets.newHashSet(allRolesToken);
      docs.add(createAllDocsTestDocument(counter++, roles));
    }

    for (int i=0; i<noRoles; i++) {
      Set<String> roles = Sets.newHashSet();
      docs.add(createAllDocsTestDocument(counter++, roles));
    }

    for (int i=0; i<junit1Andjunit2; i++) {
      Set<String> roles = Sets.newHashSet(junitRole, junit2Role);
      if (i<junit1Andjunit2AllRoles) {
        roles.add(allRolesToken);
      }
      docs.add(createAllDocsTestDocument(counter++, roles));
    }

    CloudSolrClient client = cluster.getSolrClient();

    client.add(collectionName, docs);
    client.commit(collectionName, true, true);

    testAllRolesTokenQueries(collectionName, allowMissingValues, junit, allRolesOnly, noRoles, client);

    //TODO SecureRealTimeGetRequest
    //checkAllRolesToken(getRealTimeGetRequest(), server,
    //     totalAllRolesAdded, totalOnlyAllRolesAdded, allRolesFactor, totalJunitAdded, junitFactor);



  }

  private void testAllRolesTokenQueries(String collectionName, boolean allowMissingValues, int junit, int allRolesOnly, int noRoles, CloudSolrClient client) throws Exception {
    QueryRequest request = new QueryRequest(new SolrQuery("*:*"));

    setAuthenticationUser("admin");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();

    int expectedResults = allRolesOnly;
    if (allowMissingValues) {
      expectedResults += noRoles;
    }

    assertEquals(expectedResults, docList.getNumFound());

    // as junit -- should get junit added + onlyAllRolesAdded
    setAuthenticationUser("junit");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();

    expectedResults = junit + allRolesOnly;
    if (allowMissingValues) {
      expectedResults += noRoles;
    }

    assertEquals("junit user, with allowMissingValues: " + allowMissingValues, expectedResults, docList.getNumFound());
  }

  private SolrInputDocument createAllDocsTestDocument(int id, Set<String> roles) {
    SolrInputDocument doc = new SolrInputDocument();
    String iStr = Long.toString(id);
    doc.addField("id", iStr);
    doc.addField("description", "description" + iStr);

    for (String role : roles) {
      doc.addField(AUTH_FIELD, role);
    }
    doc.addField(COUNTER_FIELD, roles.size());
    return doc;
  }



  /**
   * delete the docs as "deleteUser" using deleteByQuery "deleteQueryStr".
   * Verify that number of docs returned for "queryUser" equals
   * "expectedQueryDocs" after deletion.
   */
  private void deleteByQueryTest(String collectionName, String deleteUser,
      String deleteByQueryStr, String queryUser, int expectedQueryDocs) throws Exception {
    setAuthenticationUser(ADMIN_USER);
    createDocsAndQuerySimple(collectionName);

    // First check that the end result isn't yet true
    setAuthenticationUser(queryUser);
    assertFalse(expectedQueryDocs == checkDeleteByQuery(collectionName, new QueryRequest(new SolrQuery("*:*")), cluster.getSolrClient(), queryUser));

    // Now delete the docs
    setAuthenticationUser(deleteUser);
    cluster.getSolrClient().deleteByQuery(collectionName, deleteByQueryStr);
    cluster.getSolrClient().commit(collectionName);

    // Now check that the end result is now true
    assertEquals(expectedQueryDocs, checkDeleteByQuery(collectionName, new QueryRequest(new SolrQuery("*:*")), cluster.getSolrClient(), queryUser));
    assertEquals(expectedQueryDocs, checkDeleteByQuery(collectionName, getRealTimeGetRequest(), cluster.getSolrClient(), queryUser));
  }

  private long checkDeleteByQuery(String collectionName, QueryRequest query, CloudSolrClient server,
      String queryUser) throws Exception {

    setAuthenticationUser(queryUser);
    QueryResponse rsp =  query.process(server, collectionName);
    long docLevelResults = rsp.getResults().getNumFound();
    return docLevelResults;
  }

  private void deleteByIdTest(String collectionName) throws Exception {
    createDocsAndQuerySimple(collectionName);
    setAuthenticationUser("subset_user_01");
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

    setAuthenticationUser("subset_user_0123");
    rsp =  request.process(server, collectionName);
    long docLevelResults = rsp.getResults().getNumFound();
    assertEquals(0, docLevelResults);
  }

  private void updateDocsTest(String collectionName) throws Exception {
    createDocsAndQuerySimple(collectionName);
    setAuthenticationUser("subset_user_01");
    String docIdStr = Long.toString(4);

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
    doc.addField(AUTH_FIELD, "subset_role1");
    doc.addField(COUNTER_FIELD, 1);
    docs.add(doc);
    cluster.getSolrClient().add(collectionName, docs);
    cluster.getSolrClient().commit(collectionName);

    // verify we can now view the document
    checkUpdateDocsQuery(collectionName, query, cluster.getSolrClient(), 1);
    //checkUpdateDocsQuery(collectionName, rtgQuery, cluster.getSolrClient(), 1);

  }

  private void checkUpdateDocsQuery(String collectionName, QueryRequest request, CloudSolrClient server, int expectedDocs)
      throws Exception {
    QueryResponse rsp = request.process(server, collectionName);
    assertEquals(expectedDocs, rsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateDeleteOperations() throws Exception {
    String collectionName = "testUpdateDeleteOperations";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_subset_match", NUM_SERVERS, 1);

    createDocsAndQuerySimple(collectionName);

    // test deleteByQuery "*:*"
    deleteByQueryTest(collectionName, "subset_user_01", "*:*", "subset_user_0123", 0);

    // test deleteByQuery non-*:* - There should have been 8 of these documents in the collection before, and zero after, therefore leaving 8 docs remaining
    deleteByQueryTest(collectionName, "subset_user_01", AUTH_FIELD +":subset_role2", "subset_user_0123", 8);

    // test deleting all documents by Id
    deleteByIdTest(collectionName);

    // Test that, by design, users who have been granted ALL on index-level perms can update documents, even if they can't see them
    updateDocsTest(collectionName);

  }

  /**
   * Test to validate doc level security on collections without perm for Index level auth.
   * @throws Exception
   */
  @Test
  public void indexDocAuthTests() throws Exception {
    String collectionName = "testIndexlevelDoclevelOperations";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_subset_match", NUM_SERVERS, 1);

    createDocsAndQuerySimple(collectionName);

    // test query for "*:*" fails as junit user (junit user doesn't have index level permissions but has doc level permissions set)
    verifyQueryFail("junit", collectionName, ALL_DOCS);

    // test query for "*:*" fails as docLevel user (docLevel user has neither index level permissions nor doc level permissions set)
    verifyQueryFail("doclevel", collectionName, ALL_DOCS);
  }
}
