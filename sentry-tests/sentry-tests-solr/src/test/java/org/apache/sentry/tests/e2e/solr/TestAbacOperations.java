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

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.zapodot.junit.ldap.EmbeddedLdapRule;
import org.zapodot.junit.ldap.EmbeddedLdapRuleBuilder;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.sentry.tests.e2e.solr.TestSentryServer.ADMIN_USER;

public class TestAbacOperations extends SolrSentryServiceTestBase {
  private int numDocs = 16;

  private static final String gteFieldName = "grade2";
  private static final String lteFieldName = "grade1";
  private static final String orGroupsFieldName = "orGroups";
  private static final String andGroupsFieldName = "andGroups";
  private static final String andGroupsCountFieldName = "andGroupsCount";

  public static final String DOMAIN_DSN = "dc=example,dc=com";
  @Rule
  public EmbeddedLdapRule embeddedLdapRule = EmbeddedLdapRuleBuilder.newInstance().bindingToPort(10389)
      .bindingToAddress("localhost").usingDomainDsn(DOMAIN_DSN).withoutDefaultSchema().withSchema("ldap/ldap.schema")
      .importingLdifs("ldap/ldap.ldiff").build();

  @BeforeClass
  public static void setupPermissions() throws SentryUserException {
    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, SolrConstants.ALL, SolrConstants.ALL);

    sentryClient.createRole(ADMIN_USER, "abac_role", COMPONENT_SOLR);
    sentryClient.grantRoleToGroups(ADMIN_USER, "abac_role", COMPONENT_SOLR, Collections.singleton("abac_group"));
    grantCollectionPrivileges(ADMIN_USER, "abac_role", "simpleCollection", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "abac_role", "lteCollection", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "abac_role", "gteCollection", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "abac_role", "orGroupsCollection", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "abac_role", "andGroupsCollection", SolrConstants.QUERY);
    grantCollectionPrivileges(ADMIN_USER, "abac_role", "nestedOrGroupsCollection", SolrConstants.QUERY);
  }

  @Before
  public void resetAuthenticatedUser() {
    setAuthenticationUser("admin");
  }

  @Test
  public void simpleTest() throws Exception {
    String collectionName = "simpleCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_abac", NUM_SERVERS, 1);

    CloudSolrClient client = cluster.getSolrClient();

    /*abacuser1 has the following:
        orGroupsAttr: group1
        orGroupsAttr: group2
        andGroupsAttr: group11
        andGroupsAttr: group12
        lteAttr: THREE
        gteAttr: THIRTY
    */

    // Add a set of docs that we know will match
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "description" + iStr);
      doc.addField(lteFieldName, "THREE");
      doc.addField(gteFieldName, "THIRTY");
      doc.addField(orGroupsFieldName, "group1");
      doc.addField(andGroupsFieldName, "group11");
      doc.addField(andGroupsCountFieldName, 1);
      docs.add(doc);
    }

    //Add one further doc that won't match
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", numDocs);
    doc.addField("description", "description" + numDocs);
    doc.addField(lteFieldName, "ONE");
    doc.addField(gteFieldName, "FIFTY");
    doc.addField(orGroupsFieldName, "group9");
    doc.addField(andGroupsFieldName, "group99");
    doc.addField(andGroupsCountFieldName, 1);
    docs.add(doc);

    client.add(collectionName, docs);
    client.commit(collectionName, true, true);

    QueryRequest request = new QueryRequest(new SolrQuery("*:*"));

    setAuthenticationUser("abacuser1");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();
    assertEquals(numDocs, docList.getNumFound());

  }

  @Test
  public void lteFilterTest() throws Exception {
     /*
       We're going to test with three users, each with different values for lteAttr, ONE, THREE and FIVE
       All other attributes for those users will be fixed.
     */
    String collectionName = "lteCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_abac", NUM_SERVERS, 1);

    CloudSolrClient client = cluster.getSolrClient();

    String[] lteValues = {"ONE", "TWO", "THREE", "FOUR", "FIVE"};

    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs * lteValues.length; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "lte Test Doc: " + iStr);

      doc.addField(lteFieldName, lteValues[i % lteValues.length]);
      doc.addField(gteFieldName, "THIRTY");
      doc.addField(orGroupsFieldName, "group1");
      doc.addField(andGroupsFieldName, "group11");
      doc.addField(andGroupsCountFieldName, 1);
      docs.add(doc);
    }

    client.add(collectionName, docs);
    client.commit(collectionName, true, true);

    QueryRequest request = new QueryRequest(new SolrQuery("*:*"));

    /*
      lteuser1 has lteAttr of ONE -> Should see numDocs docs
      lteuser2 has lteAttr of THREE -> Should see numDocs * 3
      lteuser3 has lteAttr of FIVE -> Should see numDocs * 5 docs
     */

    setAuthenticationUser("lteuser1");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();

    assertEquals(numDocs, docList.getNumFound());

    setAuthenticationUser("lteuser2");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(numDocs * 3, docList.getNumFound());

    setAuthenticationUser("lteuser3");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(numDocs * 5, docList.getNumFound());

  }

  @Test
  public void gteFilterTest() throws Exception {
     /*
       We're going to test with three users, each with different values for gteAttr, TEN, THIRTY and FIFTY
       All other attributes for those users will be fixed.
     */
    String collectionName = "gteCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_abac", NUM_SERVERS, 1);

    CloudSolrClient client = cluster.getSolrClient();

    String[] gteValues = {"TEN", "TWENTY", "THIRTY", "FORTY", "FIFTY"};

    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs * gteValues.length; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "gte Test Doc: " + iStr);

      doc.addField(gteFieldName, gteValues[i % gteValues.length]);
      doc.addField(lteFieldName, "THREE");
      doc.addField(orGroupsFieldName, "group1");
      doc.addField(andGroupsFieldName, "group11");
      doc.addField(andGroupsCountFieldName, 1);
      docs.add(doc);
    }

    client.add(collectionName, docs);
    client.commit(collectionName, true, true);

    QueryRequest request = new QueryRequest(new SolrQuery("*:*"));

    /*
      gteuser1 has gteAttr of TEN -> Should see numDocs * 5 docs
      gteuser2 has gteAttr of THIRTY -> Should see numDocs * 3
      gteuser3 has gteAttr of FIFTY -> Should see numDocs docs
     */

    setAuthenticationUser("gteuser1");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();

    assertEquals(numDocs * 5, docList.getNumFound());

    setAuthenticationUser("gteuser2");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(numDocs * 3, docList.getNumFound());

    setAuthenticationUser("gteuser3");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(numDocs, docList.getNumFound());

  }

  @Test
  public void orGroupsFilterTest() throws Exception {
     /*
       We're going to test with three users, each with different values for orGroups
       All other attributes for those users will be fixed.
     */
    String collectionName = "orGroupsCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_abac", NUM_SERVERS, 1);

    CloudSolrClient client = cluster.getSolrClient();

    int[] groupDocCount = new int[3];

    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs * 4; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "Or Groups Test Doc: " + iStr);
      doc.addField(gteFieldName, "THIRTY");
      doc.addField(lteFieldName, "THREE");
      if (i % 2 == 0) {
        groupDocCount[0]++;
        doc.addField(orGroupsFieldName, "group1");
      }
      if (i % 3 == 0) {
        doc.addField(orGroupsFieldName, "group2");
      }
      if (i % 4 == 0) {
        doc.addField(orGroupsFieldName, "group3");
      }
      if (i % 2 == 0 || i % 3 == 0) {
        groupDocCount[1]++;
      }
      if (i % 2 == 0 || i % 3 == 0 || i % 4 == 0) {
        groupDocCount[2]++;
      }
      doc.addField(andGroupsFieldName, "group11");
      doc.addField(andGroupsCountFieldName, 1);

      docs.add(doc);
    }

    client.add(collectionName, docs);
    client.commit(collectionName, true, true);

    QueryRequest request = new QueryRequest(new SolrQuery("*:*"));

    /*
      oruser1 has group1
      oruser2 has group1, group2
      oruser3 has group1, group2, group3
     */

    setAuthenticationUser("oruser1");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();

    assertEquals(groupDocCount[0], docList.getNumFound());

    setAuthenticationUser("oruser2");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(groupDocCount[1], docList.getNumFound());

    setAuthenticationUser("oruser3");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(groupDocCount[2], docList.getNumFound());

  }

  @Test
  public void andGroupsFilterTest() throws Exception {
      /*
       We're going to test with three users, each with different values for andGroups
       All other attributes for those users will be fixed.
     */
    String collectionName = "andGroupsCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_abac", NUM_SERVERS, 1);

    CloudSolrClient client = cluster.getSolrClient();

    int[] groupDocCount = new int[3];

    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs * 4; ++i) {
      int andGroups = 0;
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "Or Groups Test Doc: " + iStr);
      doc.addField(gteFieldName, "THIRTY");
      doc.addField(lteFieldName, "THREE");
      if (i % 2 == 0) {
        doc.addField(andGroupsFieldName, "group11");
        andGroups++;
      }
      if (i % 3 == 0) {
        doc.addField(andGroupsFieldName, "group12");
        andGroups++;
      }
      if (i % 4 == 0) {
        doc.addField(andGroupsFieldName, "group13");
        andGroups++;
      }

      if (i % 2 == 0 && (i % 3 != 0 && i % 4 != 0)) {
        groupDocCount[0]++;
      }
      if ((i % 2 == 0 || i % 3 == 0) && i % 4 != 0) {
        groupDocCount[1]++;
      }
      if (i % 2 == 0 || i % 3 == 0 || i % 4 == 0) {
        groupDocCount[2]++;
      }
      doc.addField(andGroupsCountFieldName, andGroups);
      doc.addField(orGroupsFieldName, "group1");

      docs.add(doc);
    }

    client.add(collectionName, docs);
    client.commit(collectionName, true, true);

    QueryRequest request = new QueryRequest(new SolrQuery("*:*"));

    /*
      anduser1 has group11
      anduser2 has group11, group12
      anduser3 has group11, group12, group13
     */

    setAuthenticationUser("anduser1");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();

    assertEquals(groupDocCount[0], docList.getNumFound());

    setAuthenticationUser("anduser2");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(groupDocCount[1], docList.getNumFound());

    setAuthenticationUser("anduser3");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();
    assertEquals(groupDocCount[2], docList.getNumFound());

  }

  @Test
  public void nestedOrGroupsFilterTest() throws Exception {
     /*
       We're going to test with three users, each with different values for orGroups
       All other attributes for those users will be fixed.
     */
    String collectionName = "nestedOrGroupsCollection";
    createCollection(ADMIN_USER, collectionName, "cloud-minimal_abac", NUM_SERVERS, 1);

    CloudSolrClient client = cluster.getSolrClient();

    int[] groupDocCount = new int[3];

    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs * 4; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "Nested Or Groups Test Doc: " + iStr);
      doc.addField(gteFieldName, "THIRTY");
      doc.addField(lteFieldName, "THREE");
      if (i % 2 == 0) {
        doc.addField(orGroupsFieldName, "nestedgroup1");
        doc.addField(orGroupsFieldName, "nestedgroup4");
      }
      if (i % 3 == 0) {
        doc.addField(orGroupsFieldName, "nestedgroup2");
        doc.addField(orGroupsFieldName, "nestedgroup5");
      }
      if (i % 4 == 0) {
        doc.addField(orGroupsFieldName, "nestedgroup3");
        doc.addField(orGroupsFieldName, "nestedgroup6");
      }

      if (i % 2 == 0 || i % 3 == 0 || i % 4 == 0) {
        groupDocCount[0]++;
      }
      doc.addField(andGroupsFieldName, "group11");
      doc.addField(andGroupsCountFieldName, 1);
      docs.add(doc);
    }

    client.add(collectionName, docs);
    client.commit(collectionName, true, true);

    QueryRequest request = new QueryRequest(new SolrQuery("*:*"));

    /*
      nesteduser1 has nestedgroup1, nestedgroup2 and nestedgroup3
      nesteduser2 has nestedgroup4, nestedgroup5 and nestedgroup6
      oruser1 does not have any of the nested groups so should return zero
     */

    setAuthenticationUser("oruser1");
    QueryResponse rsp = request.process(client, collectionName);
    SolrDocumentList docList = rsp.getResults();

    assertEquals(0, docList.getNumFound());

    setAuthenticationUser("nesteduser1");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();

    assertEquals(groupDocCount[0], docList.getNumFound());

    setAuthenticationUser("nesteduser2");
    rsp = request.process(client, collectionName);
    docList = rsp.getResults();

    assertEquals(groupDocCount[0], docList.getNumFound());

  }
}
