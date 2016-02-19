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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class TestRealTimeGet extends AbstractSolrSentryTestBase {
  private static final Logger LOG = LoggerFactory
    .getLogger(TestRealTimeGet.class);
  private static final String AUTH_FIELD = "sentry_auth";
  private static final Random rand = new Random();
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
    setupCollectionWithDocSecurity(name, 2);
  }

  private void setupCollectionWithDocSecurity(String name, int shards) throws Exception {
    String configDir = RESOURCES_DIR + File.separator + DEFAULT_COLLECTION
      + File.separator + "conf";
    uploadConfigDirToZk(configDir, name);
    // replace solrconfig.xml with solrconfig-doc-level.xml
    uploadConfigFileToZk(configDir + File.separator + "solrconfig-doclevel.xml",
        "solrconfig.xml", name);
    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set("numShards", shards);
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < shards; ++i) {
      if (i != 0) builder.append(",");
      builder.append("shard").append(i+1);
    }
    modParams.set("shards", builder.toString());
    verifyCollectionAdminOpPass(ADMIN_USER, CollectionAction.CREATE, name, modParams);
  }

  private void setupCollectionWithoutDocSecurity(String name) throws Exception {
    String configDir = RESOURCES_DIR + File.separator + DEFAULT_COLLECTION
      + File.separator + "conf";
    uploadConfigDirToZk(configDir, name);
    setupCollection(name);
  }

  private QueryRequest getRealTimeGetRequest(final SolrParams params) {
    return new QueryRequest() {
      @Override
      public String getPath() {
        return "/get";
      }

      @Override
      public SolrParams getParams() {
        return params;
      }
    };
  }

  private void assertExpected(ExpectedResult expectedResult, QueryResponse rsp,
        ExpectedResult controlExpectedResult, QueryResponse controlRsp) throws Exception {
    SolrDocumentList docList = rsp.getResults();
    SolrDocumentList controlDocList = controlRsp.getResults();
    SolrDocument doc = (SolrDocument)rsp.getResponse().get("doc");
    SolrDocument controlDoc = (SolrDocument)controlRsp.getResponse().get("doc");

    if (expectedResult.expectedDocs == 0) {
      // could be null rather than 0 size, check against control that format is identical
      assertNull("Should be no doc present: " + doc, doc);
      assertNull("Should be no doc present: " + controlDoc, controlDoc);
      assertTrue((docList == null && controlDocList == null) ||
          (controlDocList.getNumFound() == 0 && controlDocList.getNumFound() == 0));
    } else {
      if (docList == null) {
        assertNull(controlDocList);
        assertNotNull(doc);
        assertNotNull(controlDoc);
      } else {
        assertNotNull(controlDocList);
        assertNull(doc);
        assertNull(controlDoc);
        assertEquals(expectedResult.expectedDocs, docList.getNumFound());
        assertEquals(docList.getNumFound(), controlDocList.getNumFound());
      }
    }
  }

  private QueryResponse getIdResponse(ExpectedResult expectedResult) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (int i = 0; i < expectedResult.ids.length; ++i) {
      params.add("id", expectedResult.ids[ i ]);
    }
    if (expectedResult.fl != null) {
      params.add("fl", expectedResult.fl);
    }
    QueryRequest request = getRealTimeGetRequest(params);
    return request.process(expectedResult.server);
  }

  private QueryResponse getIdsResponse(ExpectedResult expectedResult) throws Exception {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < expectedResult.ids.length; ++i) {
      if (i != 0) builder.append(",");
      builder.append(expectedResult.ids[ i ]);
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("ids", builder.toString());
    if (expectedResult.fl != null) {
      params.add("fl", expectedResult.fl);
    }
    QueryRequest request = getRealTimeGetRequest(params);
    return request.process(expectedResult.server);
  }

  private void assertIdVsIds(ExpectedResult expectedResult, ExpectedResult controlExpectedResult)
      throws Exception {
    // test specifying with "id"
    QueryResponse idRsp = getIdResponse(expectedResult);
    QueryResponse idControlRsp = getIdResponse(controlExpectedResult);
    assertExpected(expectedResult, idRsp, controlExpectedResult, idControlRsp);

    // test specifying with "ids"
    QueryResponse idsRsp = getIdsResponse(expectedResult);
    QueryResponse idsControlRsp = getIdsResponse(controlExpectedResult);
    assertExpected(expectedResult, idsRsp, controlExpectedResult, idsControlRsp);
  }

  @Test
  public void testIdvsIds() throws Exception {
    final String collection = "testIdvsIds";
    final String collectionControl = collection + "Control";
    setupCollectionWithDocSecurity(collection);
    setupCollectionWithoutDocSecurity(collectionControl);
    CloudSolrServer server = getCloudSolrServer(collection);
    CloudSolrServer serverControl = getCloudSolrServer(collectionControl);

    try {
      for (CloudSolrServer s : new CloudSolrServer [] {server, serverControl}) {
        DocLevelGenerator generator = new DocLevelGenerator(s.getDefaultCollection(), AUTH_FIELD);
        generator.generateDocs(s, 100, "junit_role", "admin_role", 2);
      }

      // check that control collection does not filter
      assertIdVsIds(new ExpectedResult(serverControl, new String[] {"2"}, 1),
          new ExpectedResult(serverControl, new String[] {"2"}, 1));

      // single id
      assertIdVsIds(new ExpectedResult(server, new String[] {"1"}, 1),
          new ExpectedResult(serverControl, new String[] {"1"}, 1));

      // single id (invalid)
      assertIdVsIds(new ExpectedResult(server, new String[] {"bogusId"}, 0),
          new ExpectedResult(serverControl, new String[] {"bogusId"}, 0));

      // single id (no permission)
      assertIdVsIds(new ExpectedResult(server, new String[] {"2"}, 0),
          new ExpectedResult(serverControl, new String[] {"2fake"}, 0));

      // multiple ids (some invalid, some valid, some no permission)
      assertIdVsIds(new ExpectedResult(server, new String[] {"bogus1", "1", "2"}, 1),
          new ExpectedResult(serverControl, new String[] {"bogus1", "1", "bogus2"}, 1));
      assertIdVsIds(new ExpectedResult(server, new String[] {"bogus1", "1", "2", "3"}, 2),
          new ExpectedResult(serverControl, new String[] {"bogus1", "1", "bogus2", "3"}, 2));

      // multiple ids (all invalid)
      assertIdVsIds(new ExpectedResult(server, new String[] {"bogus1", "bogus2", "bogus3"}, 0),
          new ExpectedResult(serverControl, new String[] {"bogus1", "bogus2", "bogus3"}, 0));

      // multiple ids (all no permission)
      assertIdVsIds(new ExpectedResult(server, new String[] {"2", "4", "6"}, 0),
          new ExpectedResult(serverControl, new String[] {"bogus2", "bogus4", "bogus6"}, 0));

    } finally {
      server.shutdown();
      serverControl.shutdown();
    }
  }

  private void assertFlOnDocList(SolrDocumentList list, Set<String> expectedIds,
      List<String> expectedFields) {
    assertEquals("Doc list size should be: " + expectedIds.size(), expectedIds.size(), list.getNumFound());
    for (SolrDocument doc : list) {
      expectedIds.contains(doc.get("id"));
      for (String field : expectedFields) {
        assertNotNull("Field: " + field + " should not be null in doc: " + doc, doc.get(field));
      }
      assertEquals("doc should have: " + expectedFields.size() + " fields.  Doc: " + doc,
          expectedFields.size(), doc.getFieldNames().size());
    }
  }

  private void assertFl(CloudSolrServer server, String [] ids, Set<String> expectedIds,
        String fl, List<String> expectedFields) throws Exception {
    {
      QueryResponse idRsp = getIdResponse(new ExpectedResult(server, ids, expectedIds.size(), fl));
      SolrDocumentList idList = idRsp.getResults();
      assertFlOnDocList(idList, expectedIds, expectedFields);
    }
    {
      QueryResponse idsRsp = getIdsResponse(new ExpectedResult(server, ids, expectedIds.size(), fl));
      SolrDocumentList idsList = idsRsp.getResults();
      assertFlOnDocList(idsList, expectedIds, expectedFields);
    }
  }

  @Test
  public void testFl() throws Exception {
    final String collection = "testFl";
    // FixMe: have to use one shard, because of a Solr bug where "fl" is not applied to
    // multi-shard get requests
    setupCollectionWithDocSecurity(collection, 1);
    CloudSolrServer server = getCloudSolrServer(collection);

    try {
      DocLevelGenerator generator = new DocLevelGenerator(collection, AUTH_FIELD);
      generator.generateDocs(server, 100, "junit_role", "admin_role", 2);
      String [] ids = new String[] {"1", "3", "5"};

      assertFl(server, ids, new HashSet<String>(Arrays.asList(ids)), "id", Arrays.asList("id"));
      assertFl(server, ids, new HashSet<String>(Arrays.asList(ids)), null, Arrays.asList("id", "description", "_version_"));
      // test transformer
      assertFl(server, ids, new HashSet<String>(Arrays.asList(ids)), "id,mydescription:description", Arrays.asList("id", "mydescription"));
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testNonCommitted() throws Exception {
    final String collection = "testNonCommitted";
    setupCollectionWithDocSecurity(collection, 1);
    CloudSolrServer server = getCloudSolrServer(collection);

    try {
      DocLevelGenerator generator = new DocLevelGenerator(collection, AUTH_FIELD);
      generator.generateDocs(server, 100, "junit_role", "admin_role", 2);

      // make some uncommitted modifications and ensure they are reflected
      server.deleteById("1");

      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField("id", "2");
      doc2.addField("description", "description2");
      doc2.addField(AUTH_FIELD, "admin_role");
 
      SolrInputDocument doc3 = new SolrInputDocument();
      doc3.addField("id", "3");
      doc3.addField("description", "description3");
      doc3.addField(AUTH_FIELD, "junit_role");

      SolrInputDocument doc200 = new SolrInputDocument();
      doc200.addField("id", "200");
      doc200.addField("description", "description200");
      doc200.addField(AUTH_FIELD, "admin_role");
      server.add(Arrays.asList(new SolrInputDocument [] {doc2, doc3, doc200}));

      assertFl(server, new String[] {"1", "2", "3", "4", "5", "200"},
          new HashSet<String>(Arrays.asList("2", "5", "200")), "id", Arrays.asList("id"));
    } finally {
      server.shutdown();
    }
  }

  private void assertConcurrentOnDocList(SolrDocumentList list, String authField, String expectedAuthFieldValue) {
    for (SolrDocument doc : list) {
      Collection<Object> authFieldValues = doc.getFieldValues(authField);
      assertNotNull(authField + " should not be null.  Doc: " + doc, authFieldValues);

      boolean foundAuthFieldValue = false;
      for (Object obj : authFieldValues) {
        if (obj.toString().equals(expectedAuthFieldValue)) {
          foundAuthFieldValue = true;
          break;
        }
      }
      assertTrue("Did not find: " + expectedAuthFieldValue + " in doc: " + doc, foundAuthFieldValue);
    }
  }

  private void assertConcurrent(CloudSolrServer server, String [] ids, String authField, String expectedAuthFieldValue)
      throws Exception {
    {
      QueryResponse idRsp = getIdResponse(new ExpectedResult(server, ids, -1, null));
      SolrDocumentList idList = idRsp.getResults();
      assertConcurrentOnDocList(idList, authField, expectedAuthFieldValue);
    }
    {
      QueryResponse idsRsp = getIdsResponse(new ExpectedResult(server, ids, -1, null));
      SolrDocumentList idsList = idsRsp.getResults();
      assertConcurrentOnDocList(idsList, authField, expectedAuthFieldValue);
    }
  }

  @Test
  public void testConcurrentChanges() throws Exception {
    final String collection = "testConcurrentChanges";
    // Ensure the auth field is stored so we can check a consistent doc is returned
    final String authField = "sentry_auth_stored";
    System.setProperty("sentry.auth.field", authField);
    setupCollectionWithDocSecurity(collection, 1);
    CloudSolrServer server = getCloudSolrServer(collection);
    int numQueries = 5;

    try {
      DocLevelGenerator generator = new DocLevelGenerator(collection, authField);
      generator.generateDocs(server, 100, "junit_role", "admin_role", 2);

      List<AuthFieldModifyThread> threads = new LinkedList<AuthFieldModifyThread>();
      int docsToModify = 10;
      for (int i = 0; i < docsToModify; ++i) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", Integer.toString(i));
        doc.addField("description", "description" + Integer.toString(i));
        doc.addField(authField, "junit_role");
        server.add(doc);

        threads.add(new AuthFieldModifyThread(server, doc,
            authField, "junit_role", "admin_role"));
      }
      server.commit();

      for (AuthFieldModifyThread thread : threads) {
        thread.start();
      }

      // query
      String [] ids = new String[docsToModify];
      for (int j = 0; j < ids.length; ++j) {
        ids[ j ] = Integer.toString(j);
      }
      for (int k = 0; k < numQueries; ++k) {
        assertConcurrent(server, ids, authField, "admin_role");
      }

      for (AuthFieldModifyThread thread : threads) {
        thread.setFinished();
        thread.join();
      }
    } finally {
      System.clearProperty("sentry.auth.field");
      server.shutdown();
    }
  }

  @Test
  public void testSuperUser() throws Exception {
    final String collection = "testSuperUser";
    setupCollectionWithDocSecurity(collection, 1);
    CloudSolrServer server = getCloudSolrServer(collection);
    int docCount = 100;

    try {
      DocLevelGenerator generator = new DocLevelGenerator(collection, AUTH_FIELD);
      generator.generateDocs(server, docCount, "junit_role", "admin_role", 2);

      setAuthenticationUser("solr");
      String [] ids = new String[docCount];
      for (int i = 0; i < docCount; ++i) {
        ids[ i ] = Integer.toString(i);
      }
      QueryResponse response = getIdResponse(new ExpectedResult(server, ids, docCount));
      assertEquals("Wrong number of documents", docCount, response.getResults().getNumFound());
    } finally {
      server.shutdown();
    }
  }

  private class AuthFieldModifyThread extends Thread {
    private CloudSolrServer server;
    private SolrInputDocument doc;
    private String authField;
    private String authFieldValue0;
    private String authFieldValue1;
    private volatile boolean finished = false;

    private AuthFieldModifyThread(CloudSolrServer server,
        SolrInputDocument doc, String authField,
        String authFieldValue0, String authFieldValue1) {
      this.server = server;
      this.doc = doc;
      this.authField = authField;
      this.authFieldValue0 = authFieldValue0;
      this.authFieldValue1 = authFieldValue1;
    }

    @Override
    public void run() {
      while (!finished) {
        if (rand.nextBoolean()) {
          doc.setField(authField, authFieldValue0);
        } else {
          doc.setField(authField, authFieldValue1);
        }
        try {
          server.add(doc);
        } catch (SolrServerException sse) {
          throw new RuntimeException(sse);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }

    public void setFinished() {
      finished = true;
    }
  }

  private static class ExpectedResult {
    public final CloudSolrServer server;
    public final String [] ids;
    public final int expectedDocs;
    public final String fl;

    public ExpectedResult(CloudSolrServer server, String [] ids, int expectedDocs) {
      this(server, ids, expectedDocs, null);
    }

    public ExpectedResult(CloudSolrServer server, String [] ids, int expectedDocs, String fl) {
      this.server = server;
      this.ids = ids;
      this.expectedDocs = expectedDocs;
      this.fl = fl;
    }
  }
}
