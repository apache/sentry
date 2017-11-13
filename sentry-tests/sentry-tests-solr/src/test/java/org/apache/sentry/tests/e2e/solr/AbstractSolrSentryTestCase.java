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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Locale;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;

import org.apache.sentry.binding.solr.authz.SentrySolrPluginImpl;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.sentry.core.model.solr.SolrModelAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.Utils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.restlet.representation.Representation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@SolrTestCaseJ4.SuppressSSL
public abstract class AbstractSolrSentryTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(AbstractSolrSentryTestCase.class);
  protected static final int NUM_SERVERS = 3;
  protected static final String COMPONENT_SOLR = "solr";
  protected static final String SERVICE_NAME = "service1";
  protected static final String ADMIN_ROLE  = "admin_role";
  private static final String SENTRY_SITE_LOC_SYSPROP = "solr."+SentrySolrPluginImpl.SNTRY_SITE_LOCATION_PROPERTY;
  protected static final String ALL_DOCS = "*:*";

  protected static TestSentryServer sentrySvc;
  protected static SentryGenericServiceClient sentryClient;
  protected static UncaughtExceptionHandler orgExceptionHandler;

  @BeforeClass
  public static void setupClass() throws Exception {
    skipBrokenLocales();

    Path testDataPath = createTempDir("solr-integration-db-");

    try {
      sentrySvc = new TestSentryServer(testDataPath, getUserGroupMappings());
      sentrySvc.startSentryService();
      sentryClient = sentrySvc.connectToSentryService();
      log.info("Successfully started Sentry service");
    } catch (Exception ex) {
      log.error ("Unexpected exception while starting Sentry service", ex);
      throw ex;
    }

    for (int i = 0; i < 4; i++) {
      sentryClient.createRole(TestSentryServer.ADMIN_USER, "role"+i, COMPONENT_SOLR);
      sentryClient.grantRoleToGroups(TestSentryServer.ADMIN_USER, "role"+i, COMPONENT_SOLR, Collections.singleton("group"+i));
    }

    log.info("Successfully created roles in Sentry service");

    sentryClient.createRole(TestSentryServer.ADMIN_USER, ADMIN_ROLE, COMPONENT_SOLR);
    sentryClient.grantRoleToGroups(TestSentryServer.ADMIN_USER, ADMIN_ROLE, COMPONENT_SOLR,
        Collections.singleton(TestSentryServer.ADMIN_GROUP));
    grantAdminPrivileges(TestSentryServer.ADMIN_USER, ADMIN_ROLE, SolrConstants.ALL, SolrConstants.ALL);

    log.info("Successfully granted admin privileges to " + ADMIN_ROLE);


    System.setProperty(SENTRY_SITE_LOC_SYSPROP, sentrySvc.getSentrySitePath().toString());

    // set the solr for the loginUser and belongs to solr group
    // Note - Solr/Sentry unit tests don't use Hadoop authentication framework. Hence the
    // UserGroupInformation is not available when the request is being processed by the Solr server.
    // The document level security search component requires this UserGroupInformation while querying
    // the roles associated with the user. Please refer to implementation of
    // SentryGenericProviderBackend#getRoles(...) method. Hence this is a workaround to satisfy this requirement.
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("solr", new String[]{"solr"}));

    try {
      configureCluster(NUM_SERVERS)
        .withSecurityJson(TEST_PATH().resolve("security").resolve("security.json"))
        .addConfig("cloud-minimal", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .addConfig("cloud-managed", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .addConfig("cloud-minimal_doc_level_security", TEST_PATH().resolve("configsets")
                      .resolve("cloud-minimal_doc_level_security").resolve("conf"))
        .configure();
      log.info("Successfully started Solr service");

    } catch (Exception ex) {
      log.error ("Unexpected exception while starting SolrCloud", ex);
      throw ex;
    }

    log.info("Successfully setup Solr with Sentry service");
  }

  /**
   * A sample failed run was with following parameters
   * -Dtests.seed=C92C593AE70E7466 -Dtests.locale=und -Dtests.timezone=Europe/Sarajevo -Dtests.asserts=true -Dtests.file.encoding=UTF-8
   */
  private static void skipBrokenLocales() {
    assumeFalse("This test fails on UNIX whenever Locale has a language, country, or variant that does not satisfy" +
                " the IETF BCP 47 language tag syntax requirements",
                "und".equals(Locale.getDefault().toLanguageTag()));
    assumeFalse("This test fails on UNIX with error - Supplied locale description 'sr__#Latn' is invalid, expecting ln[_CO[_variant]]" +
                "ln=lower-case two-letter ISO-639 language code, CO=upper-case two-letter ISO-3166 country code",
                "sr-Latn".equals(Locale.getDefault().toLanguageTag()));
  }

  @AfterClass
  public static void cleanUpResources() throws Exception {
    if (sentryClient != null) {
      sentryClient.close();
      sentryClient = null;
    }
    if (sentrySvc != null) {
      sentrySvc.close();
      sentrySvc = null;
    }
    System.clearProperty(SENTRY_SITE_LOC_SYSPROP);
  }

  public static void grantAdminPrivileges(String requestor, String roleName, String entityName, String action)
      throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Admin.name(), entityName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeAdminPrivileges(String requestor, String roleName, String entityName, String action)
      throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Admin.name(), entityName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void grantCollectionPrivileges(String requestor, String roleName,
      String collectionName, String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Collection.name(), collectionName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeCollectionPrivileges(String requestor, String roleName,
      String collectionName, String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Collection.name(), collectionName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void grantConfigPrivileges(String requestor, String roleName, String configName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Config.name(), configName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeConfigPrivileges(String requestor, String roleName, String configName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Config.name(), configName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void grantSchemaPrivileges(String requestor, String roleName, String schemaName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Schema.name(), schemaName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeSchemaPrivileges(String requestor, String roleName, String schemaName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Schema.name(), schemaName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  protected static Map<String, Set<String>> getUserGroupMappings() {
    Map<String, Set<String>> result = new HashMap<>();
    result.put("user0", Collections.singleton("group0"));
    result.put("user1", Collections.singleton("group1"));
    result.put("user2", Collections.singleton("group2"));
    result.put("user3", Collections.singleton("group3"));
    result.put(TestSentryServer.ADMIN_USER, Collections.singleton(TestSentryServer.ADMIN_GROUP));
    result.put("solr", Collections.singleton("solr"));
    result.put("junit", Collections.singleton("junit"));
    result.put("doclevel", Collections.singleton("doclevel"));
    return Collections.unmodifiableMap(result);
  }

  /**
   * Get the user defined in the Solr authentication plugin
   *
   * @return - the username as String
   * @throws Exception in case of errors
   */
  protected String getAuthenticatedUser() {
    return DummyAuthPluginImpl.getUserName();
  }

  /**
   * Set the proper user in the Solr authentication plugin
   * @param solrUser
   */
  protected void setAuthenticationUser(String solrUser) {
    DummyAuthPluginImpl.setUserName(solrUser);
  }

  protected void createCollection (String userName, String collectionName,
      String configName, int numShards, int numReplicas) throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      // Create collection.
      CollectionAdminRequest.Create createCmd =
          CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas);
      assertEquals(0, createCmd.process(cluster.getSolrClient()).getStatus());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  /**
   * Function to clean Solr collections
   * @param userName Name of the user performing this operation
   * @param collectionName - Name of the collection
   * @throws Exception In case of error
   */
  protected void cleanSolrCollection(String userName, String collectionName)
                                     throws Exception {
    verifyDeletedocsPass(userName, collectionName, true);
  }

  /**
   * Method to validate Solr deletedocs passes
   * (This function doesn't check if there is at least one Solr document present in Solr)
   * @param solrUserName - User authenticated into Solr
   * @param collectionName - Name of the collection to which the data has to be updated
   * @param allowZeroDocs - Boolean for running this method only if there is atleast one Solr doc present.
   * @throws MalformedURLException, SolrServerException, IOException
   */
  protected void verifyDeletedocsPass(String solrUserName,
                                      String collectionName,
                                      boolean allowZeroDocs) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      SolrDocumentList orginalSolrDocs = getSolrDocs(solrUserName, collectionName, ALL_DOCS);
      if (!allowZeroDocs) {
        assertTrue("Solr should contain atleast one solr doc to run this test.", orginalSolrDocs.size() > 0);
      }

      setAuthenticationUser(solrUserName);
      cluster.getSolrClient().deleteByQuery(collectionName, ALL_DOCS);
      cluster.getSolrClient().commit(collectionName);

      // Validate Solr doc count is zero
      SolrDocumentList solrRespDocs = getSolrDocs(solrUserName, collectionName, ALL_DOCS);
      validateSolrDocCountAndContent(new SolrDocumentList(), solrRespDocs);
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Function to query the collection and fetch the Solr docs
   * @param userName The name of the user performing this operation.
   * @param collectionName -  Name of the collection
   * @param solrQueryStr - Query string to be searched in Solr
   * @return an instance of SolrDocumentList
   * @throws Exception
   */
  protected SolrDocumentList getSolrDocs(String userName,
                                         String collectionName,
                                         String solrQueryStr) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);

      assertNotNull("Solr query shouldn't be null.", solrQueryStr);
      QueryResponse response = cluster.getSolrClient().query(collectionName, new SolrQuery(solrQueryStr));
      assertEquals(0, response.getStatus());
      return response.getResults();
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Function to validate the count and content of two SolrDocumentList's.
   * @param solrOriginalDocs - Instance of initial set of solr docs before processing
   * @param solrResponseDocs - Instance of response solr docs after processing
   */
  protected void validateSolrDocCountAndContent(SolrDocumentList solrOriginalDocs,
                                                SolrDocumentList solrResponseDocs) {
    assertEquals("Expected number of Solr docs: " + solrOriginalDocs.size() + "; But found:" + solrResponseDocs.size(),
        solrOriginalDocs.size(), solrResponseDocs.size());
    for (SolrDocument solrDoc : solrOriginalDocs) {
      validateSolrDocContent(solrDoc, solrResponseDocs);
    }
  }

  /**
   * Function to validate the content of Solr response with that of input document.
   * @param solrInputDoc - Solr doc inserted into Solr
   * @param solrRespDocs - List of Solr doc obtained as response
   * (NOTE: This function ignores "_version_" field in validating Solr doc content)
   */
  public void validateSolrDocContent(SolrDocument solrInputDoc,
                                     SolrDocumentList solrRespDocs) {
    for (SolrDocument solrRespDoc : solrRespDocs) {
      String expFieldValue = (String) solrInputDoc.getFieldValue("id");
      String resFieldValue = (String) solrRespDoc.getFieldValue("id");
      if (expFieldValue.equals(resFieldValue)) {
        int expectedRespFieldCount = solrRespDoc.size();
        if (solrRespDoc.containsKey("_version_")) {
          expectedRespFieldCount = expectedRespFieldCount - 1;
        }
        int expectedOrigFieldCount = solrInputDoc.size();
        if (solrInputDoc.containsKey("_version_")) {
          expectedOrigFieldCount = expectedOrigFieldCount - 1;
        }
        assertEquals("Expected " + expectedOrigFieldCount + " fields. But, found "
              + expectedRespFieldCount + " fields", expectedOrigFieldCount , expectedRespFieldCount);
        for (String field : solrInputDoc.getFieldNames()) {
          if (field.equals("_version_") == true) {
            continue;
          }

          expFieldValue = (String) solrInputDoc.getFieldValue(field);
          resFieldValue = (String) solrRespDoc.getFieldValue(field);
          assertEquals("Expected value for field: " + field + " is " + expFieldValue
              + "; But, found " + resFieldValue, expFieldValue, resFieldValue);
        }

        return;
      }
    }

    fail("Solr doc not found in Solr collection");
  }

  /**
   * Function to create a test Solrdoc with a random number as the ID
   * @throws Exception in case of error
   */
  protected SolrInputDocument createSolrTestDoc() throws Exception {
    String solrDocId = String.valueOf(random().nextInt());

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", String.valueOf(random().nextInt()));
    doc.setField("name", "testdoc" + solrDocId);
    return doc;
  }

  /**
   * Method to validate Solr update passes
   * @param solrUserName - User authenticated into Solr
   * @param collectionName - Name of the collection to which the data has to be updated
   * @param solrInputDoc - Instance of SolrInputDocument
   * @throws Exception
   */
  protected void verifyUpdatePass(String solrUserName,
                                  String collectionName,
                                  SolrInputDocument solrInputDoc) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      SolrDocumentList orginalSolrDocs = getSolrDocs(solrUserName, collectionName, ALL_DOCS);
      setAuthenticationUser(solrUserName);

      cluster.getSolrClient().add(collectionName, solrInputDoc);
      cluster.getSolrClient().commit(collectionName);

      orginalSolrDocs.add(toSolrDocument(solrInputDoc));
      SolrDocumentList solrRespDocs = getSolrDocs(solrUserName, collectionName, ALL_DOCS);
      // Validate Solr content to check whether the update command went through.
      validateSolrDocCountAndContent(orginalSolrDocs, solrRespDocs);
    }
    finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Method to validate Solr query fails
   * @param solrUserName - User authenticated into Solr
   * @param collectionName - Name of the collection to be queried
   * @param solrQueryStr - Query string to be searched in Solr
   * @throws Exception
   */
  protected void verifyQueryFail(String solrUserName,
                                 String collectionName,
                                 String solrQueryStr) throws Exception {
    try {
      getSolrDocs(solrUserName, collectionName, solrQueryStr);
      fail("The specified user: " + solrUserName + " shouldn't get query access!");
    } catch (SolrServerException exception) {
      assertTrue(exception.getCause() instanceof RemoteSolrException);
      assertEquals(HttpServletResponse.SC_FORBIDDEN, ((RemoteSolrException)exception.getCause()).code());
    }
  }


  /**
   * Load Solr collection with the SolrDocument passed.
   * @param collectionName - Name of the Solr collection
   * @param solrInputDoc - Solr document to be uploaded
   * (If solrInputDoc is null, then a test Solr doc will be uploaded)
   * @throws Exception
   */
  protected void uploadSolrDoc(String userName,
                               String collectionName,
                               SolrInputDocument solrInputDoc) throws Exception {
    Objects.requireNonNull(solrInputDoc);
    verifyUpdatePass(userName, collectionName, solrInputDoc);
  }


  private static SolrDocument toSolrDocument(SolrInputDocument doc) {
    SolrDocument result = new SolrDocument();
    result.setField("id", doc.getFieldValue("id"));
    result.setField("name", doc.getFieldValue("name"));
    return result;
  }

  @SuppressWarnings("unchecked")
  protected <T> T deserialize (Representation r) throws IOException {
    ByteArrayOutputStream str = new ByteArrayOutputStream();
    r.write(str);
    return (T)Utils.fromJSON(str.toByteArray());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected <T> T readNestedElement(Map object, String... fields) {
    Map t = object;
    int i = 0;

    while (i < fields.length - 1) {
      String field = fields[i];
      t = (Map)Objects.requireNonNull(t.get(field));
      i++;
    }

    return (T)Objects.requireNonNull(t.get(fields[fields.length - 1]));
  }

  /**
   * Make a raw http request to specific cluster node.  Node is of the format
   * host:port/context, i.e. "localhost:8983/solr"
   */
  protected String makeHttpRequest(CloudSolrClient client, String node, String httpMethod,
      String path, byte [] content, String contentType, int expectedStatusCode) throws Exception {
    HttpClient httpClient = client.getLbClient().getHttpClient();
    URI uri = new URI("http://" + node + path);
    HttpRequestBase method = null;
    if ("GET".equals(httpMethod)) {
      method = new HttpGet(uri);
    } else if ("HEAD".equals(httpMethod)) {
      method = new HttpHead(uri);
    } else if ("POST".equals(httpMethod)) {
      method = new HttpPost(uri);
    } else if ("PUT".equals(httpMethod)) {
      method = new HttpPut(uri);
    } else {
      throw new IOException("Unsupported method: " + method);
    }

    if (method instanceof HttpEntityEnclosingRequestBase) {
      HttpEntityEnclosingRequestBase entityEnclosing =
        (HttpEntityEnclosingRequestBase)method;
      ByteArrayEntity entityRequest = new ByteArrayEntity(content);
      entityRequest.setContentType(contentType);
      entityEnclosing.setEntity(entityRequest);
    }

    HttpEntity httpEntity = null;
    boolean success = false;
    String retValue = "";
    try {
      final HttpResponse response = httpClient.execute(method);
      httpEntity = response.getEntity();

      assertEquals (expectedStatusCode, response.getStatusLine().getStatusCode());

      if (httpEntity != null) {
        InputStream is = httpEntity.getContent();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
          IOUtils.copyLarge(is, os);
          os.flush();
        } finally {
          IOUtils.closeQuietly(os);
          IOUtils.closeQuietly(is);
        }
        retValue = os.toString();
      }
      success = true;
    } finally {
      if (!success) {
        EntityUtils.consumeQuietly(httpEntity);
         method.abort();
      }
    }
    return retValue;
  }

  /**
   * Make a raw http request (not specifying cluster node)
   */
  protected String makeHttpRequest(CloudSolrClient client, String httpMethod,
      String path, byte [] content, String contentType, int expectedStatusCode) throws Exception {
    Set<String> liveNodes =
      client.getZkStateReader().getClusterState().getLiveNodes();
    assertTrue("Expected at least one live node", !liveNodes.isEmpty());
    String firstServer = liveNodes.toArray(new String[0])[0].replace("_solr", "/solr");
    return makeHttpRequest(client, firstServer, httpMethod, path, content, contentType, expectedStatusCode);
  }


}
