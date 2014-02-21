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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.servlet.SolrDispatchFilter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSolrSentryTestBase extends AbstractFullDistribZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSolrSentryTestBase.class);
  protected static final String SENTRY_ERROR_MSG = "401, message:Unauthorized";
  private static MiniDFSCluster dfsCluster;
  private static SortedMap<Class, String> extraRequestFilters;
  protected static final String ADMIN_USER = "admin";
  protected static final String ALL_DOCS = "*:*";
  protected static final Random RANDOM = new Random();

  private static void addPropertyToSentry(StringBuilder builder, String name, String value) {
    builder.append("<property>\n");
    builder.append("<name>").append(name).append("</name>\n");
    builder.append("<value>").append(value).append("</value>\n");
    builder.append("</property>\n");
  }

  public static File setupSentry() throws Exception {
    File sentrySite = File.createTempFile("sentry-site", "xml");
    sentrySite.deleteOnExit();
    File authProviderDir = new File(SolrTestCaseJ4.TEST_HOME(), "sentry");
    String authProviderName = "test-authz-provider.ini";
    FileSystem clusterFs = dfsCluster.getFileSystem();
    clusterFs.copyFromLocalFile(false,
      new Path(authProviderDir.toString(), authProviderName),
      new Path(authProviderName));

    // need to write sentry-site at execution time because we don't know
    // the location of sentry.solr.provider.resource beforehand
    StringBuilder sentrySiteData = new StringBuilder();
    sentrySiteData.append("<configuration>\n");
    addPropertyToSentry(sentrySiteData, "sentry.provider",
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    addPropertyToSentry(sentrySiteData, "sentry.solr.provider.resource",
       clusterFs.getWorkingDirectory() + File.separator + authProviderName);
    sentrySiteData.append("</configuration>\n");
    FileUtils.writeStringToFile(sentrySite,sentrySiteData.toString());
    return sentrySite;
  }

  @BeforeClass
  public static void beforeTestSimpleSolrEndToEnd() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(new File(TEMP_DIR,
      AbstractSolrSentryTestBase.class.getName() + "_"
        + System.currentTimeMillis()).getAbsolutePath());
    File sentrySite = setupSentry();
    System.setProperty("solr.authorization.sentry.site", sentrySite.toURI().toURL().toString().substring("file:".length()));
    System.setProperty("solr.hdfs.home", dfsCluster.getURI().toString() + "/solr");
    extraRequestFilters = new TreeMap<Class, String>(new Comparator<Class>() {
      // There's only one class, make this as simple as possible
      public int compare(Class o1, Class o2) {
        return 0;
      }

      public boolean equals(Object obj) {
        return true;
      }
    });
    extraRequestFilters.put(ModifiableUserAuthenticationFilter.class, "*");
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    System.clearProperty("solr.authorization.sentry.site");
    dfsCluster = null;
    extraRequestFilters = null;
  }

  @Before
  public void setupBeforeTest() throws Exception {
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("numShards");
    System.clearProperty("solr.xml.persist");
  }

  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }

  @Override
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig.xml";
  }

  @Override
  public SortedMap<Class,String> getExtraRequestFilters() {
    return extraRequestFilters;
  }

  /**
   * Set the proper user in the Solr authentication filter
   * @param solrUser
   */
  protected void setAuthenticationUser(String solrUser) throws Exception {
    ModifiableUserAuthenticationFilter.setUser(solrUser);
  }

  /**
   * Get the user defined in the Solr authentication filter
   * @return - the username as String
   * @throws Exception
   */
  private String getAuthenticatedUser() throws Exception {
    return ModifiableUserAuthenticationFilter.getUser();
  }

  /**
   * Function to return the user name based on the permissions provided.
   * @param collectionName - Name of the solr collection.
   * @param isQuery - Boolean that specifies query permission.
   * @param isUpdate - Boolean that specifies update permission.
   * @param isAll - Boolean that specifies all permission.
   * @return - String which represents the Solr username.
   */
  protected String getUsernameForPermissions(String collectionName,
                                             boolean isQuery,
                                             boolean isUpdate,
                                             boolean isAll) {
    StringBuilder finalStr = new StringBuilder();
    finalStr.append(collectionName);
    finalStr.append("_");
    StringBuilder permissions = new StringBuilder();
    if (isQuery) {
      permissions.append("q");
    }

    if (isUpdate) {
      permissions.append("u");
    }

    if (isAll) {
      permissions.append("a");
    }

    finalStr.append(permissions.toString());
    return finalStr.toString();
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
      SolrDocumentList orginalSolrDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      setAuthenticationUser(solrUserName);
      CloudSolrServer cloudSolrServer = getCloudSolrServer(collectionName);
      try {
        cloudSolrServer.add(solrInputDoc);
        cloudSolrServer.commit();
      } finally {
        cloudSolrServer.shutdown();
      }

      orginalSolrDocs.add(ClientUtils.toSolrDocument(solrInputDoc));
      SolrDocumentList solrRespDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      // Validate Solr content to check whether the update command went through.
      validateSolrDocCountAndContent(orginalSolrDocs, solrRespDocs);
    }
    finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Method to validate Solr update fails
   * @param solrUserName - User authenticated into Solr
   * @param collectionName - Name of the collection to which the data has to be updated
   * @param solrInputDoc - Instance of SolrInputDocument
   * @throws Exception
   */
  protected void verifyUpdateFail(String solrUserName,
                                  String collectionName,
                                  SolrInputDocument solrInputDoc) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      SolrDocumentList orginalSolrDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      setAuthenticationUser(solrUserName);
      CloudSolrServer cloudSolrServer = getCloudSolrServer(collectionName);
      try {
        cloudSolrServer.add(solrInputDoc);
        cloudSolrServer.commit();
        fail("The specified user: " + solrUserName + " shouldn't get update access!");
      } catch (Exception exception) {
        assertTrue("Expected " + SENTRY_ERROR_MSG + " in " + exception.toString(),
            exception.toString().contains(SENTRY_ERROR_MSG));
      } finally {
        cloudSolrServer.shutdown();
      }

      SolrDocumentList solrRespDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      // Validate Solr content to check whether the update command didn't go through.
      validateSolrDocCountAndContent(orginalSolrDocs, solrRespDocs);
    } finally {
      setAuthenticationUser(originalUser);
    }
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
      SolrDocumentList orginalSolrDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      if (allowZeroDocs == false) {
        assertTrue("Solr should contain atleast one solr doc to run this test.", orginalSolrDocs.size() > 0);
      }

      setAuthenticationUser(solrUserName);
      CloudSolrServer cloudSolrServer = getCloudSolrServer(collectionName);
      try {
        cloudSolrServer.deleteByQuery(ALL_DOCS);
        cloudSolrServer.commit();
      } finally {
        cloudSolrServer.shutdown();
      }

      // Validate Solr doc count is zero
      SolrDocumentList solrRespDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      validateSolrDocCountAndContent(new SolrDocumentList(), solrRespDocs);
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Method to validate Solr deletedocs fails
   * (This function doesn't check if there is at least one Solr document present in Solr)
   * @param solrUserName - User authenticated into Solr
   * @param collectionName - Name of the collection to which the data has to be updated
   * @param allowZeroDocs - Boolean for running this method only if there is atleast one Solr doc present.
   * @throws Exception
   */
  protected void verifyDeletedocsFail(String solrUserName,
                                      String collectionName,
                                      boolean allowZeroDocs) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      SolrDocumentList orginalSolrDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      if (allowZeroDocs == false) {
        assertTrue("Solr should contain atleast one solr doc to run this test.", orginalSolrDocs.size() > 0);
      }

      setAuthenticationUser(solrUserName);
      CloudSolrServer cloudSolrServer = getCloudSolrServer(collectionName);
      try {
        cloudSolrServer.deleteByQuery(ALL_DOCS);
        cloudSolrServer.commit();
        fail("The specified user: " + solrUserName + " shouldn't get deletedocs access!");
      } catch (Exception exception) {
        assertTrue("Expected " + SENTRY_ERROR_MSG + " in " + exception.toString(),
            exception.toString().contains(SENTRY_ERROR_MSG));
      } finally {
        cloudSolrServer.shutdown();
      }

      // Validate Solr doc count and content is same as original set.
      SolrDocumentList solrRespDocs = getSolrDocs(collectionName, ALL_DOCS, true);
      validateSolrDocCountAndContent(orginalSolrDocs, solrRespDocs);
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Method to validate Solr query passes
   * @param solrUserName - User authenticated into Solr
   * @param collectionName - Name of the collection to be queried
   * @param solrQueryStr - Query string to be searched in Solr
   * @throws Exception
   */
  protected void verifyQueryPass(String solrUserName,
                                 String collectionName,
                                 String solrQueryStr) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      SolrDocumentList orginalSolrDocs = getSolrDocs(collectionName, solrQueryStr, true);
      setAuthenticationUser(solrUserName);
      SolrDocumentList solrRespDocs = null;
      solrRespDocs = getSolrDocs(collectionName, solrQueryStr, false);

      // Validate Solr content to check whether the query command went through.
      validateSolrDocCountAndContent(orginalSolrDocs, solrRespDocs);
    } finally {
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
    String originalUser = getAuthenticatedUser();
    try {
      setAuthenticationUser(solrUserName);
      try {
        getSolrDocs(collectionName, solrQueryStr, false);
        fail("The specified user: " + solrUserName + " shouldn't get query access!");
      } catch (Exception exception) {
        assertTrue("Expected " + SENTRY_ERROR_MSG + " in " + exception.toString(),
            exception.toString().contains(SENTRY_ERROR_MSG));
      }
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Method to validate collection Admin operation pass
   * @param solrUserName - User authenticated into Solr
   * @param adminOp - Admin operation to be performed
   * @param collectionName - Name of the collection to be queried
   * @param ignoreError - boolean to specify whether to ignore the error if any occurred.
   *                      (We may need this attribute for running DELETE command on a collection which doesn't exist)
   * @throws Exception
   */
  protected void verifyCollectionAdminOpPass(String solrUserName,
                                             CollectionAction adminOp,
                                             String collectionName) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      setAuthenticationUser(solrUserName);
      QueryRequest request = populateCollectionAdminParams(adminOp, collectionName);
      SolrServer solrServer = createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0)));
      try {
        NamedList<Object> result = solrServer.request(request);
        if (adminOp.compareTo(CollectionAction.CREATE) == 0) {
          // Wait for collection creation to complete.
          waitForRecoveriesToFinish(collectionName, false);
        }
      } finally {
        solrServer.shutdown();
      }
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Method to validate collection Admin operation fail
   * @param solrUserName - User authenticated into Solr
   * @param adminOp - Admin operation to be performed
   * @param collectionName - Name of the collection to be queried
   * @throws Exception
   */
  protected void verifyCollectionAdminOpFail(String solrUserName,
                                             CollectionAction adminOp,
                                             String collectionName) throws Exception {

    String originalUser = getAuthenticatedUser();
    try {
      setAuthenticationUser(solrUserName);
      try {
        QueryRequest request = populateCollectionAdminParams(adminOp, collectionName);
        SolrServer solrServer = createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0)));
        try {
          NamedList<Object> result = solrServer.request(request);
          if (adminOp.compareTo(CollectionAction.CREATE) == 0) {
            // Wait for collection creation to complete.
            waitForRecoveriesToFinish(collectionName, false);
          }
        } finally {
          solrServer.shutdown();
        }

        fail("The specified user: " + solrUserName + " shouldn't get admin access for " + adminOp);
      } catch (Exception exception) {
        assertTrue("Expected " + SENTRY_ERROR_MSG + " in " + exception.toString(),
            exception.toString().contains(SENTRY_ERROR_MSG));
      }
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Method to populate the Solr params based on the collection admin being performed.
   * @param adminOp - Collection admin operation
   * @param collectionName - Name of the collection
   * @return - instance of QueryRequest.
   */
  public QueryRequest populateCollectionAdminParams(CollectionAction adminOp,
                                                            String collectionName) {
    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION, adminOp.name());
    switch (adminOp) {
      case CREATE:
        modParams.set("name", collectionName);
        modParams.set("numShards", 2);
        modParams.set("shards", "shard1,shard2");
        modParams.set("replicationFactor", 1);
        break;
      case DELETE:
        modParams.set("name", collectionName);
        break;
      case RELOAD:
        modParams.set("name", collectionName);
        break;
      case SPLITSHARD:
        modParams.set("collection", collectionName);
        modParams.set("shard", "shard1");
        break;
      case DELETESHARD:
        modParams.set("collection", collectionName);
        modParams.set("shard", "shard1");
        break;
      case CREATEALIAS:
        modParams.set("name", collectionName);
        modParams.set("collections", collectionName + "_underlying1"
            + "," + collectionName + "_underlying2");
        break;
      case DELETEALIAS:
        modParams.set("name", collectionName);
        break;
      default:
        throw new IllegalArgumentException("Admin operation: " + adminOp + " is not supported!");
    }

    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    return request;
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
      SolrInputDocument solrInputDoc = ClientUtils.toSolrInputDocument(solrDoc);
      validateSolrDocContent(solrInputDoc, solrResponseDocs);
    }
  }

  /**
   * Function to query the collection and fetch the Solr docs
   * @param collectionName -  Name of the collection
   * @param solrQueryStr - Query string to be searched in Solr
   * @param runAsAdmin - Boolean to specify whether to execute the Solr query as admin user
   * @return -  Instance of SolrDocumentList
   * @throws Exception
   */
  protected SolrDocumentList getSolrDocs(String collectionName,
                                         String solrQueryStr,
                                         boolean runAsAdmin) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      if (runAsAdmin == true) {
        // Authenticate as user "admin"
        setAuthenticationUser(ADMIN_USER);
      }

      CloudSolrServer cloudSolrServer = getCloudSolrServer(collectionName);
      assertNotNull("Solr query shouldn't be null.", solrQueryStr);
      SolrDocumentList solrDocs = null;
      try {
        SolrQuery query = new SolrQuery(solrQueryStr);
        QueryResponse response = cloudSolrServer.query(query);
        solrDocs = response.getResults();
        return solrDocs;
      } finally {
        cloudSolrServer.shutdown();
      }
    } finally {
      setAuthenticationUser(originalUser);
    }
  }

  /**
   * Function to validate the content of Solr response with that of input document.
   * @param solrInputDoc - Solr doc inserted into Solr
   * @param solrRespDocs - List of Solr doc obtained as response
   * (NOTE: This function ignores "_version_" field in validating Solr doc content)
   */
  public void validateSolrDocContent(SolrInputDocument solrInputDoc,
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
   * Function to return the instance of CloudSolrServer for the collectionName specified
   * @param collectionName - Name of the collection
   * @return instance of CloudSolrServer
   * @throws MalformedURLException
   */
  protected CloudSolrServer getCloudSolrServer(String collectionName) throws MalformedURLException {
    CloudSolrServer cloudSolrServer = new CloudSolrServer(zkServer.getZkAddress(),
        random().nextBoolean());
    cloudSolrServer.setDefaultCollection(collectionName);
    cloudSolrServer.connect();
    return cloudSolrServer;
  }

  /**
   * Function to create a solr collection with the name passed as parameter
   * (Runs commands as ADMIN user)
   * @param collectionName - Name of the collection
   * @throws Exception
   */
  protected void setupCollection(String collectionName) throws Exception {
    verifyCollectionAdminOpPass(ADMIN_USER,
                                CollectionAction.CREATE,
                                collectionName);
  }

  /**
   * Function to delete a solr collection with the name passed as parameter
   * (Runs commands as ADMIN user)
   * @param collectionName - Name of the collection
   * This function will simply ignore the errors raised in deleting the collections.
   * e.g: As part of the clean up job, the tests can issue a DELETE command on the collection which doesn't exist.
   */
  protected void deleteCollection(String collectionName) {
    try {
      verifyCollectionAdminOpPass(ADMIN_USER,
                                  CollectionAction.DELETE,
                                  collectionName);
    } catch (Exception e) {
      LOG.warn("Ignoring errors raised while deleting the collection : " + e.toString());
    }
  }

  /**
   * Function to clean Solr collections
   * @param collectionName - Name of the collection
   * @throws Exception
   */
  protected void cleanSolrCollection(String collectionName)
                                     throws Exception {
    verifyDeletedocsPass(ADMIN_USER, collectionName, true);
  }

  /**
   * Function to create a test Solrdoc with a random number as the ID
   * @throws Exception
   */
  protected SolrInputDocument createSolrTestDoc() throws Exception {
    SolrInputDocument solrInputDoc = new SolrInputDocument();
    String solrDocId = String.valueOf(RANDOM.nextInt());
    solrInputDoc.addField("id", solrDocId);
    solrInputDoc.addField("name", "testdoc" + solrDocId);
    return solrInputDoc;
  }

  /**
   * Load Solr collection with the SolrDocument passed.
   * @param collectionName - Name of the Solr collection
   * @param solrInputDoc - Solr document to be uploaded
   * (If solrInputDoc is null, then a test Solr doc will be uploaded)
   * @throws Exception
   */
  protected void uploadSolrDoc(String collectionName,
                               SolrInputDocument solrInputDoc) throws Exception {
    if (solrInputDoc == null) {
      solrInputDoc = createSolrTestDoc();
    }

    verifyUpdatePass(ADMIN_USER, collectionName, solrInputDoc);
  }

  /**
   * Subclasses can override this to change a test's solr home
   * (default is in test-files)
   */
  public String getSolrHome() {
    return SolrTestCaseJ4.TEST_HOME();
  }

  protected void uploadConfigDirToZk(String collectionConfigDir) throws Exception {
    SolrDispatchFilter dispatchFilter =
      (SolrDispatchFilter) jettys.get(0).getDispatchFilter().getFilter();
    ZkController zkController = dispatchFilter.getCores().getZkController();
    // conf1 is the config used by AbstractFullDistribZkTestBase
    zkController.uploadConfigDir(new File(collectionConfigDir), "conf1");
  }
}
