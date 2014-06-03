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
package org.apache.sentry.tests.e2e.hive;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.dbprovider.PolicyProviderForTest;
import org.apache.sentry.tests.e2e.hive.fs.DFS;
import org.apache.sentry.tests.e2e.hive.fs.DFSFactory;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServer;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

public abstract class AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AbstractTestWithStaticConfiguration.class);
  protected static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  protected static final String ALL_DB1 = "server=server1->db=db_1",
      ALL_DB2 = "server=server1->db=db_2",
      SELECT_DB1_TBL1 = "server=server1->db=db_1->table=tb_1->action=select",
      SELECT_DB1_TBL2 = "server=server1->db=db_1->table=tb_2->action=select",
      SELECT_DB1_NONTABLE = "server=server1->db=db_1->table=this table does not exist->action=select",
      INSERT_DB1_TBL1 = "server=server1->db=db_1->table=tb_1->action=insert",
      INSERT_DB1_TBL2 = "server=server1->db=db_1->table=tb_2->action=insert",
      SELECT_DB2_TBL2 = "server=server1->db=db_2->table=tb_2->action=select",
      INSERT_DB2_TBL1 = "server=server1->db=db_2->table=tb_1->action=insert",
      SELECT_DB1_VIEW1 = "server=server1->db=db_1->table=view_1->action=select",
      ADMIN1 = StaticUserGroup.ADMIN1,
      ADMINGROUP = StaticUserGroup.ADMINGROUP,
      USER1_1 = StaticUserGroup.USER1_1,
      USER1_2 = StaticUserGroup.USER1_2,
      USER2_1 = StaticUserGroup.USER2_1,
      USER3_1 = StaticUserGroup.USER3_1,
      USER4_1 = StaticUserGroup.USER4_1,
      USERGROUP1 = StaticUserGroup.USERGROUP1,
      USERGROUP2 = StaticUserGroup.USERGROUP2,
      USERGROUP3 = StaticUserGroup.USERGROUP3,
      USERGROUP4 = StaticUserGroup.USERGROUP4,
      GROUP1_ROLE = "group1_role",
      DB1 = "db_1",
      DB2 = "db_2",
      DB3 = "db_3",
      TBL1 = "tb_1",
      TBL2 = "tb_2",
      TBL3 = "tb_3",
      VIEW1 = "view_1",
      VIEW2 = "view_2",
      VIEW3 = "view_3",
      INDEX1 = "index_1",
      INDEX2 = "index_2";
  protected static final String SERVER_HOST = "localhost";

  protected static boolean policy_on_hdfs = false;
  protected static boolean useSentryService = false;

  protected static File baseDir;
  protected static File logDir;
  protected static File confDir;
  protected static File dataDir;
  protected static File policyFileLocation;
  protected static HiveServer hiveServer;
  protected static FileSystem fileSystem;
  protected static DFS dfs;
  protected static Map<String, String> properties;
  protected static SentryService sentryServer;
  protected static Configuration sentryConf;
  protected static SentryPolicyServiceClient sentryClient;
  protected Context context;

  public Context createContext() throws Exception {
    context = new Context(hiveServer, fileSystem,
        baseDir, confDir, dataDir, policyFileLocation);
    return context;
  }
  protected void dropDb(String user, String...dbs) throws Exception {
    Connection connection = context.createConnection(user);
    Statement statement = connection.createStatement();
    for(String db : dbs) {
      statement.execute("DROP DATABASE IF EXISTS " + db + " CASCADE");
    }
    statement.close();
    connection.close();
  }
  protected void createDb(String user, String...dbs) throws Exception {
    Connection connection = context.createConnection(user);
    Statement statement = connection.createStatement();
    for(String db : dbs) {
      statement.execute("CREATE DATABASE " + db);
    }
    statement.close();
    connection.close();
  }
  protected void createTable(String user, String db, File dataFile, String...tables)
      throws Exception {
    Connection connection = context.createConnection(user);
    Statement statement = connection.createStatement();
    statement.execute("USE " + db);
    for(String table : tables) {
      statement.execute("DROP TABLE IF EXISTS " + table);
      statement.execute("create table " + table
          + " (under_col int comment 'the under column', value string)");
      statement.execute("load data local inpath '" + dataFile.getPath()
          + "' into table " + table);
      ResultSet res = statement.executeQuery("select * from " + table);
      Assert.assertTrue("Table should have data after load", res.next());
      res.close();
    }
    statement.close();
    connection.close();
  }

  protected static File assertCreateDir(File dir) {
    if(!dir.isDirectory()) {
      Assert.assertTrue("Failed creating " + dir, dir.mkdirs());
    }
    return dir;
  }

  @BeforeClass
  public static void setupTestStaticConfiguration()
      throws Exception {
    if(!policy_on_hdfs) {
      policy_on_hdfs = new Boolean(System.getProperty("sentry.e2etest.policyonhdfs", "false"));
    }
    properties = Maps.newHashMap();
    baseDir = Files.createTempDir();
    LOGGER.info("BaseDir = " + baseDir);
    logDir = assertCreateDir(new File(baseDir, "log"));
    confDir = assertCreateDir(new File(baseDir, "etc"));
    dataDir = assertCreateDir(new File(baseDir, "data"));
    policyFileLocation = new File(confDir, HiveServerFactory.AUTHZ_PROVIDER_FILENAME);

    String dfsType = System.getProperty(DFSFactory.FS_TYPE);
    dfs = DFSFactory.create(dfsType, baseDir);
    fileSystem = dfs.getFileSystem();

    String policyURI;
    if (policy_on_hdfs) {
      String dfsUri = fileSystem.getDefaultUri(fileSystem.getConf()).toString();
      LOGGER.error("dfsUri " + dfsUri);
      policyURI = dfsUri + System.getProperty("sentry.e2etest.hive.policy.location", "/user/hive/sentry");
      policyURI += "/" + HiveServerFactory.AUTHZ_PROVIDER_FILENAME;
    } else {
      policyURI = policyFileLocation.getPath();
    }

    if (useSentryService) {
      setupSentryService();
    }

    hiveServer = HiveServerFactory.create(properties, baseDir, confDir, logDir, policyURI, fileSystem);
    hiveServer.start();
  }
  protected void writePolicyFile(PolicyFile policyFile) throws Exception{

    policyFile.write(context.getPolicyFile());
    if(policy_on_hdfs) {
      dfs.writePolicyFile(context.getPolicyFile());
    }
  }

  private static void setupSentryService() throws Exception {
    properties = Maps.newHashMap();
    sentryConf = new Configuration(false);
    PolicyFile policyFile = new PolicyFile();

    properties.put(HiveServerFactory.AUTHZ_PROVIDER_BACKEND,
        SimpleDBProviderBackend.class.getName());
    properties.put(ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
        SentryHiveAuthorizationTaskFactoryImpl.class.getName());
    properties
        .put(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS.varname, "2");
    properties.put(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    properties.put(ServerConfig.ADMIN_GROUPS, ADMINGROUP);
    properties.put(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    properties.put(ServerConfig.RPC_PORT, String.valueOf(0));
    properties.put(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");

    properties.put(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + baseDir.getPath()
            + "/sentrystore_db;create=true");
    properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING, ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFileLocation.getPath());
    properties.put(ServerConfig.RPC_MIN_THREADS, "3");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      sentryConf.set(entry.getKey(), entry.getValue());
    }
    sentryServer = new SentryServiceFactory().create(sentryConf);
    properties.put(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.getAddress()
        .getHostString());
    sentryConf.set(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.getAddress()
        .getHostString());
    properties.put(ClientConfig.SERVER_RPC_PORT,
        String.valueOf(sentryServer.getAddress().getPort()));
    sentryConf.set(ClientConfig.SERVER_RPC_PORT,
        String.valueOf(sentryServer.getAddress().getPort()));
    startSentryService();
  }

  private static void startSentryService() throws Exception {
    sentryServer.start();
    final long start = System.currentTimeMillis();
    while (!sentryServer.isRunning()) {
      Thread.sleep(1000);
      if (System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  @Before
  public void setup() throws Exception{
    dfs.createBaseDir();
  }

  @AfterClass
  public static void tearDownTestStaticConfiguration() throws Exception {
    if(hiveServer != null) {
      hiveServer.shutdown();
      hiveServer = null;
    }

    if (sentryServer != null) {
      if (sentryClient != null) {
        sentryClient.close();
      }
      sentryClient = null;
      sentryServer.stop();
      sentryServer = null;
      PolicyProviderForTest.clearSentryClient();
    }

    if(baseDir != null) {
      if(System.getProperty(HiveServerFactory.KEEP_BASEDIR) == null) {
        FileUtils.deleteQuietly(baseDir);
      }
      baseDir = null;
    }
    if(dfs != null) {
      try {
        dfs.tearDown();
      } catch (Exception e) {
        LOGGER.info("Exception shutting down dfs", e);
      }
    }
  }

  public static SentryPolicyServiceClient getSentryClient() throws Exception {
    if (sentryServer == null) {
      throw new IllegalAccessException("Sentry service not initialized");
    }
    SentryServiceClientFactory factory = new SentryServiceClientFactory();
    return factory.create(sentryServer.getConf());
  }
}
