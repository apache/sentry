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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.sentry.binding.solr.HdfsTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
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

import com.google.common.io.Files;

public class AbstractSolrSentryTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSolrSentryTestBase.class);
  protected static final String SENTRY_ERROR_MSG = "SentrySolrAuthorizationException";
  protected static MiniDFSCluster dfsCluster;
  protected static MiniSolrCloudCluster miniSolrCloudCluster;
  protected static SortedMap<Class, String> extraRequestFilters;
  protected static final String ADMIN_USER = "admin";
  protected static final String ALL_DOCS = "*:*";
  protected static final Random RANDOM = new Random();
  protected static final String RESOURCES_DIR = "target" + File.separator + "test-classes" + File.separator + "solr";
  protected static final String CONF_DIR_IN_ZK = "conf1";
  protected static final String DEFAULT_COLLECTION = "collection1";
  protected static final int NUM_SERVERS = 4;

  private static void addPropertyToSentry(StringBuilder builder, String name, String value) {
    builder.append("<property>\n");
    builder.append("<name>").append(name).append("</name>\n");
    builder.append("<value>").append(value).append("</value>\n");
    builder.append("</property>\n");
  }

  public static File setupSentry() throws Exception {
    File sentrySite = File.createTempFile("sentry-site", "xml");
    sentrySite.deleteOnExit();
    File authProviderDir = new File(RESOURCES_DIR, "sentry");
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
    dfsCluster = HdfsTestUtil.setupClass(new File(Files.createTempDir(),
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
    File solrXml = new File(RESOURCES_DIR, "solr-no-core.xml");
    miniSolrCloudCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, solrXml,
      null, extraRequestFilters);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    System.clearProperty("solr.authorization.sentry.site");
    dfsCluster = null;
    extraRequestFilters = null;
    miniSolrCloudCluster.shutdown();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    System.setProperty("solr.xml.persist", "true");
    // Disable the block cache because we can run out of memory
    // on a MiniCluster.
    System.setProperty("solr.hdfs.blockcache.enabled", "false");
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty("solr.hdfs.blockcache.enabled");
    System.clearProperty("solr.xml.persist");
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
  protected String getAuthenticatedUser() throws Exception {
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
   * @throws Exception
   */
  protected void verifyCollectionAdminOpPass(String solrUserName,
                                             CollectionAction adminOp,
                                             String collectionName) throws Exception {
    verifyCollectionAdminOpPass(solrUserName, adminOp, collectionName, null);
  }

  /**
   * Method to validate collection Admin operation pass
   * @param solrUserName - User authenticated into Solr
   * @param adminOp - Admin operation to be performed
   * @param collectionName - Name of the collection to be queried
   * @param params - SolrParams to use
   * @throws Exception
   */
  protected void verifyCollectionAdminOpPass(String solrUserName,
                                             CollectionAction adminOp,
                                             String collectionName,
                                             SolrParams params) throws Exception {
    String originalUser = getAuthenticatedUser();
    try {
      setAuthenticationUser(solrUserName);
      QueryRequest request = populateCollectionAdminParams(adminOp, collectionName, params);
      CloudSolrServer solrServer = createNewCloudSolrServer();
      try {
        NamedList<Object> result = solrServer.request(request);
        if (adminOp.compareTo(CollectionAction.CREATE) == 0) {
          // Wait for collection creation to complete.
          waitForRecoveriesToFinish(collectionName, solrServer, false);
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
    verifyCollectionAdminOpFail(solrUserName, adminOp, collectionName, null);
  }

  /**
   * Method to validate collection Admin operation fail
   * @param solrUserName - User authenticated into Solr
   * @param adminOp - Admin operation to be performed
   * @param collectionName - Name of the collection to be queried
   * @param params - SolrParams to use
   * @throws Exception
   */
  protected void verifyCollectionAdminOpFail(String solrUserName,
                                             CollectionAction adminOp,
                                             String collectionName,
                                             SolrParams params) throws Exception {

    String originalUser = getAuthenticatedUser();
    try {
      setAuthenticationUser(solrUserName);
      try {
        QueryRequest request = populateCollectionAdminParams(adminOp, collectionName, params);
        CloudSolrServer solrServer = createNewCloudSolrServer();
        try {
          NamedList<Object> result = solrServer.request(request);
          if (adminOp.compareTo(CollectionAction.CREATE) == 0) {
            // Wait for collection creation to complete.
            waitForRecoveriesToFinish(collectionName, solrServer, false);
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
    return populateCollectionAdminParams(adminOp, collectionName, null);
  }

  /**
   * Method to populate the Solr params based on the collection admin being performed.
   * @param adminOp - Collection admin operation
   * @param collectionName - Name of the collection
   * @param params - SolrParams to use
   * @return - instance of QueryRequest.
   */
  public QueryRequest populateCollectionAdminParams(CollectionAction adminOp,
                                                    String collectionName,
                                                    SolrParams params) {
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

    if (params != null) {
      Iterator<String> it = params.getParameterNamesIterator();
      while (it.hasNext()) {
        String param = it.next();
        String [] value = params.getParams(param);
        modParams.set(param, value);
      }
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
    CloudSolrServer cloudSolrServer = new CloudSolrServer(miniSolrCloudCluster.getZkServer().getZkAddress(),
        RANDOM.nextBoolean());
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

  private ZkController getZkController() {
    SolrDispatchFilter dispatchFilter =
      (SolrDispatchFilter) miniSolrCloudCluster.getJettySolrRunners().get(0).getDispatchFilter().getFilter();
    return dispatchFilter.getCores().getZkController();
  }

  protected void uploadConfigDirToZk(String collectionConfigDir) throws Exception {
    uploadConfigDirToZk(collectionConfigDir, CONF_DIR_IN_ZK);
  }

  protected void uploadConfigDirToZk(String collectionConfigDir, String confDirInZk) throws Exception {
    ZkController zkController = getZkController();
    zkController.uploadConfigDir(new File(collectionConfigDir), confDirInZk);
  }

  protected void uploadConfigFileToZk(String file, String nameInZk) throws Exception {
    uploadConfigFileToZk(file, nameInZk, CONF_DIR_IN_ZK);
  }

  protected void uploadConfigFileToZk(String file, String nameInZk, String confDirInZk) throws Exception {
    ZkController zkController = getZkController();
    zkController.getZkClient().makePath(ZkController.CONFIGS_ZKNODE + "/"
      + confDirInZk + "/" + nameInZk, new File(file), false, true);
  }

  protected CloudSolrServer createNewCloudSolrServer() throws Exception {
    CloudSolrServer css = new CloudSolrServer(miniSolrCloudCluster.getZkServer().getZkAddress());
    css.connect();
    return css;
  }

  /**
   * Make a raw http request to specific cluster node.  Node is of the format
   * host:port/context, i.e. "localhost:8983/solr"
   */
  protected String makeHttpRequest(CloudSolrServer server, String node, String httpMethod,
      String path, byte [] content, String contentType) throws Exception {
    HttpClient httpClient = server.getLbServer().getHttpClient();
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
      int httpStatus = response.getStatusLine().getStatusCode();
      httpEntity = response.getEntity();

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
  protected String makeHttpRequest(CloudSolrServer server, String httpMethod,
      String path, byte [] content, String contentType) throws Exception {
    Set<String> liveNodes =
      server.getZkStateReader().getClusterState().getLiveNodes();
    assertTrue("Expected at least one live node", !liveNodes.isEmpty());
    String firstServer = liveNodes.toArray(new String[0])[0].replace("_solr", "/solr");
    return makeHttpRequest(server, firstServer, httpMethod, path, content, contentType);
  }

  protected static void waitForRecoveriesToFinish(String collection,
                                                  CloudSolrServer solrServer,
                                                  boolean verbose) throws Exception {
    waitForRecoveriesToFinish(collection, solrServer, verbose, true, 60);
  }

  protected static void waitForRecoveriesToFinish(String collection,
                                                  CloudSolrServer solrServer,
                                                  boolean verbose,
                                                  boolean failOnTimeout,
                                                  int timeoutSeconds) throws Exception {
    LOG.info("Entering solr wait with timeout " + timeoutSeconds);
    ZkStateReader zkStateReader = solrServer.getZkStateReader();
    try {
      boolean cont = true;
      int cnt = 0;

      while (cont) {
        if (verbose) LOG.debug("-");
        boolean sawLiveRecovering = false;
        zkStateReader.updateClusterState(true);
        ClusterState clusterState = zkStateReader.getClusterState();
        Map<String, Slice> slices = clusterState.getSlicesMap(collection);
        assertNotNull("Could not find collection:" + collection, slices);
        for (Map.Entry<String, Slice> entry : slices.entrySet()) {
          Map<String, Replica> shards = entry.getValue().getReplicasMap();
          for (Map.Entry<String, Replica> shard : shards.entrySet()) {
            if (verbose) LOG.debug("rstate:"
                + shard.getValue().getStr(ZkStateReader.STATE_PROP) + " live:"
                + clusterState.liveNodesContain(shard.getValue().getNodeName()));
            String state = shard.getValue().getStr(ZkStateReader.STATE_PROP);
            if ((state.equals(ZkStateReader.RECOVERING)
                || state.equals(ZkStateReader.SYNC) || state
                .equals(ZkStateReader.DOWN))
                && clusterState.liveNodesContain(shard.getValue().getStr(
                ZkStateReader.NODE_NAME_PROP))) {
              sawLiveRecovering = true;
            }
          }
        }
        if (!sawLiveRecovering || cnt == timeoutSeconds) {
          if (!sawLiveRecovering) {
            if (verbose) LOG.debug("no one is recovering");
          } else {
            if (verbose) LOG.debug("Gave up waiting for recovery to finish..");
            if (failOnTimeout) {
              fail("There are still nodes recovering - waited for "
                  + timeoutSeconds + " seconds");
              // won't get here
              return;
            }
          }
          cont = false;
        } else {
          Thread.sleep(1000);
        }
        cnt++;
      }
    } finally {
      LOG.info("Exiting solr wait");
    }
  }
}
