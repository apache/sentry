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


import static org.apache.sentry.core.model.search.SearchModelAuthorizable.AuthorizableType.Collection;

import java.io.File;
import java.io.FileOutputStream;
import java.net.MalformedURLException;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.binding.solr.HdfsTestUtil;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.solr.AbstractSolrSentryTestBase;
import org.apache.sentry.tests.e2e.solr.ModifiableUserAuthenticationFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This class used to test the Solr integration with DB store.
 * It will set up a miniSolrCloud, miniHDFS and Sentry service in a JVM process.
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class, // hdfs currently leaks thread(s)
    SentryServiceThreadLeakFilter.class
})
@SolrTestCaseJ4.SuppressSSL
public class AbstractSolrSentryTestWithDbProvider extends AbstractSolrSentryTestBase{
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AbstractSolrSentryTestWithDbProvider.class);

  protected static final String SERVER_HOST = NetUtils
      .createSocketAddr("localhost:80").getAddress().getCanonicalHostName();
  protected static final int PORT = 8038;
  protected static final String ADMIN_GROUP = "admin_group";
  protected static final String ADMIN_ROLE  = "admin_role";
  protected static final String ADMIN_COLLECTION_NAME = "admin";
  protected static final String COMPONENT_SOLR = "solr";
  protected static final String SERVICE_NAME = SearchConstants.SENTRY_SEARCH_SERVICE_DEFAULT;

  protected static final Configuration conf = new Configuration(false);

  protected static SortedMap<Class, String> extraRequestFilters;
  protected static MiniDFSCluster dfsCluster;
  protected static MiniSolrCloudCluster miniSolrCloudCluster;
  protected static SentryService server;
  protected static SentryGenericServiceClient client;

  protected static File baseDir;
  protected static File hdfsDir;
  protected static File dbDir;
  protected static File policyFilePath;
  protected static File sentrySitePath;

  protected static PolicyFile policyFile;

  /**
   * Overwrite the method from super class AbstractSolrSentryTestBase
   * take over the management of miniSolrCloudCluster and dfsCluster
   */
  @BeforeClass
  public static void beforeTestSimpleSolrEndToEnd() throws Exception {
    setupConf();
    startHDFS();
    startSolrWithDbProvider();
    startSentryService();
    connectToSentryService();
    setGroupsAndRoles();
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    stopAllService();
    FileUtils.deleteDirectory(baseDir);
    unsetSystemProperties();
  }

  public static void setupConf() throws Exception {
    baseDir = createTempDir();
    hdfsDir = new File(baseDir, "hdfs");
    dbDir = new File(baseDir, "sentry_policy_db");
    policyFilePath = new File(baseDir, "local_policy_file.ini");
    sentrySitePath = new File(baseDir, "sentry-site.xml");
    policyFile = new PolicyFile();

    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP + ",solr");
    conf.set(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    conf.set(ServerConfig.RPC_PORT, String.valueOf(PORT));
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    server = new SentryServiceFactory().create(conf);

    conf.set(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress().getHostName());
    conf.set(ClientConfig.SERVER_RPC_PORT, String.valueOf(server.getAddress().getPort()));
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    conf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        LocalGroupResourceAuthorizationProvider.class.getName());
    conf.set(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(),
        SentryGenericProviderBackend.class.getName());
    conf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), policyFilePath.getPath());
  }

  public static File createTempDir() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    String baseName = "solr-integration-db-";
    File tempDir = new File(baseDir, baseName + UUID.randomUUID().toString());
    if (tempDir.mkdir()) {
        return tempDir;
    }
    throw new IllegalStateException("Failed to create temp directory");
  }

  public static void configureWithSolr() throws Exception {
    conf.set(ServerConfig.SECURITY_USE_UGI_TRANSPORT, "true");
    //save configuration to sentry-site.xml
    conf.writeXml(new FileOutputStream(sentrySitePath));
    setSystemProperties();
    extraRequestFilters = new TreeMap<Class, String>(new Comparator<Class>() {
      // There's only one class, make this as simple as possible
      @Override
      public int compare(Class o1, Class o2) {
        return 0;
      }
      @Override
      public boolean equals(Object obj) {
        return true;
      }
    });
    extraRequestFilters.put(ModifiableUserAuthenticationFilter.class, "*");

    //set the solr for the loginUser and belongs to solr group
    addGroupsToUser("solr", "solr");
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("solr", new String[]{"solr"}));
  }

  public static void startHDFS() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(hdfsDir.getPath());
    conf.set(
        CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        dfsCluster.getFileSystem().getConf()
        .get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
  }

  public static void startSolrWithDbProvider() throws Exception {
    LOGGER.info("starting Solr authorization via Sentry Service");
    configureWithSolr();
    miniSolrCloudCluster = new MiniSolrCloudCluster(NUM_SERVERS, null,
        new File(RESOURCES_DIR, "solr-no-core.xml"), null, extraRequestFilters);
  }

  public static void startSentryService() throws Exception {
    server.start();
    final long start = System.currentTimeMillis();
    while(!server.isRunning()) {
      Thread.sleep(1000);
      if(System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  public static void connectToSentryService() throws Exception {
    client = SentryGenericServiceClientFactory.create(conf);
  }

  public static void stopAllService() throws Exception {
    if (client != null) {
      client.close();
      client = null;
    }
    SentryGenericServiceClientFactory.factoryReset();
    if (server != null) {
      server.stop();
      server = null;
    }
    if (miniSolrCloudCluster != null) {
      miniSolrCloudCluster.shutdown();
      miniSolrCloudCluster = null;
    }
    if (dfsCluster != null) {
      HdfsTestUtil.teardownClass(dfsCluster);
      dfsCluster = null;
    }
  }

  public static void addGroupsToUser(String user, String... groupNames) {
    policyFile.addGroupsToUser(user, groupNames);
  }

  public static void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
    FileSystem clusterFs = dfsCluster.getFileSystem();
    clusterFs.copyFromLocalFile(false,
        new Path(policyFilePath.getPath()),
        new Path(policyFilePath.getPath()));
  }

  public static void setSystemProperties() throws Exception {
    System.setProperty("solr.xml.persist", "true");
    // Disable the block cache because we can run out of memory
    // on a MiniCluster.
    System.setProperty("solr.hdfs.blockcache.enabled", "false");
    System.setProperty("solr.hdfs.home", dfsCluster.getURI().toString() + "/solr");
    System.setProperty("solr.authorization.sentry.site", sentrySitePath.toURI().toURL().toString().substring("file:".length()));
  }

  public static void unsetSystemProperties() {
    System.clearProperty("solr.xml.persist");
    System.clearProperty("solr.hdfs.blockcache.enabled");
    System.clearProperty("solr.hdfs.home");
    System.clearProperty("solr.authorization.sentry.site");
  }

  public static void setGroupsAndRoles() throws Exception {
    /**set local group mapping
     * user0->group0->role0
     * user1->group1->role1
     * user2->group2->role2
     * user3->group3->role3
     */
    String[] users = {"user0","user1","user2","user3"};
    String[] groups = {"group0","group1","group2","group3"};
    String[] roles = {"role0","role1","role2","role3"};

    for (int i = 0; i < users.length; i++) {
      addGroupsToUser(users[i], groups[i]);
    }
    addGroupsToUser(ADMIN_USER, ADMIN_GROUP);
    writePolicyFile();

    for (int i = 0; i < roles.length; i++) {
      client.createRole(ADMIN_USER, roles[i], COMPONENT_SOLR);
      client.addRoleToGroups(ADMIN_USER, roles[i], COMPONENT_SOLR, Sets.newHashSet(groups[i]));
    }

    /**
     * user[admin]->group[admin]->role[admin]
     * grant ALL privilege on collection ALL to role admin
     */
    client.createRole(ADMIN_USER, ADMIN_ROLE, COMPONENT_SOLR);
    client.addRoleToGroups(ADMIN_USER, ADMIN_ROLE, COMPONENT_SOLR, Sets.newHashSet(ADMIN_GROUP));
    grantCollectionPrivilege(SearchConstants.ALL, ADMIN_USER, ADMIN_ROLE, SearchConstants.ALL);
  }

  protected static void grantCollectionPrivilege(String collection, String requestor,
      String roleName, String action) throws SentryUserException {
    TSentryPrivilege tPrivilege = toTSentryPrivilege(collection, action);
    client.grantPrivilege(requestor, roleName, COMPONENT_SOLR, tPrivilege);
  }

  protected static void revokeCollectionPrivilege(String collection, String requestor,
      String roleName, String action) throws SentryUserException {
    TSentryPrivilege tPrivilege = toTSentryPrivilege(collection, action);
    client.revokePrivilege(requestor, roleName, COMPONENT_SOLR, tPrivilege);
  }

  protected static void dropCollectionPrivilege(String collection, String requestor)
      throws SentryUserException {
    final TSentryPrivilege tPrivilege = toTSentryPrivilege(collection, Action.ALL);
    client.dropPrivilege(requestor, COMPONENT_SOLR, tPrivilege);
  }

  private static TSentryPrivilege toTSentryPrivilege(String collection, String action) {
    TSentryPrivilege tPrivilege = new TSentryPrivilege();
    tPrivilege.setComponent(COMPONENT_SOLR);
    tPrivilege.setServiceName(SERVICE_NAME);
    tPrivilege.setAction(action);
    tPrivilege.setGrantOption(TSentryGrantOption.FALSE);

    List<TAuthorizable> authorizables = Lists.newArrayList(new TAuthorizable(Collection.name(),
        collection));
    tPrivilege.setAuthorizables(authorizables);
    return tPrivilege;
  }

  protected CloudSolrServer getCloudSolrServer(String collectionName) throws MalformedURLException {
    CloudSolrServer cloudSolrServer = new CloudSolrServer(miniSolrCloudCluster.getZkServer().getZkAddress(),
        RANDOM.nextBoolean());
    cloudSolrServer.setDefaultCollection(collectionName);
    cloudSolrServer.connect();
    return cloudSolrServer;
  }

  protected CloudSolrServer createNewCloudSolrServer() throws Exception {
    CloudSolrServer css = new CloudSolrServer(miniSolrCloudCluster.getZkServer().getZkAddress());
    css.connect();
    return css;
  }

  protected ZkController getZkController() {
    SolrDispatchFilter dispatchFilter =
      (SolrDispatchFilter) miniSolrCloudCluster.getJettySolrRunners().get(0).getDispatchFilter().getFilter();
    return dispatchFilter.getCores().getZkController();
  }
}