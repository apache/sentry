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

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.provider.common.ProviderConstants.PRIVILEGE_PREFIX;
import static org.apache.sentry.provider.common.ProviderConstants.ROLE_SPLITTER;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashSet;

import com.google.common.collect.Sets;
import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.binding.metastore.SentryMetastorePostEventListener;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.policy.db.DBModelAuthorizables;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.KerberosConfiguration;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.hive.fs.DFS;
import org.apache.sentry.tests.e2e.hive.fs.DFSFactory;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServer;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.apache.sentry.tests.e2e.minisentry.SentrySrvFactory;
import org.apache.sentry.tests.e2e.minisentry.SentrySrvFactory.SentrySrvType;
import org.apache.sentry.tests.e2e.minisentry.SentrySrv;
import org.apache.tools.ant.util.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

public abstract class AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AbstractTestWithStaticConfiguration.class);
  protected static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  protected static final String ALL_DB1 = "server=server1->db=db_1",
      ALL_DB2 = "server=server1->db=db_2",
      SELECT_DB1_TBL1 = "server=server1->db=db_1->table=tb_1->action=select",
      SELECT_DB1_TBL2 = "server=server1->db=db_1->table=tb_2->action=select",
      SELECT_DB1_NONTABLE = "server=server1->db=db_1->table=blahblah->action=select",
      INSERT_DB1_TBL1 = "server=server1->db=db_1->table=tb_1->action=insert",
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
      INDEX1 = "index_1";

  protected static final String SERVER_HOST = "localhost";
  private static final String EXTERNAL_SENTRY_SERVICE = "sentry.e2etest.external.sentry";
  protected static final String EXTERNAL_HIVE_LIB = "sentry.e2etest.hive.lib";
  private static final String ENABLE_SENTRY_HA = "sentry.e2etest.enable.service.ha";

  protected static boolean policyOnHdfs = false;
  protected static boolean useSentryService = false;
  protected static boolean setMetastoreListener = true;
  protected static String testServerType = null;
  protected static boolean enableHiveConcurrency = false;
  // indicate if the database need to be clear for every test case in one test class
  protected static boolean clearDbPerTest = true;

  protected static File baseDir;
  protected static File logDir;
  protected static File confDir;
  protected static File dataDir;
  protected static File policyFileLocation;
  protected static HiveServer hiveServer;
  protected static FileSystem fileSystem;
  protected static HiveServerFactory.HiveServer2Type hiveServer2Type;
  protected static DFS dfs;
  protected static Map<String, String> properties;
  protected static SentrySrv sentryServer;
  protected static Configuration sentryConf;
  protected static boolean enableSentryHA = false;
  protected static Context context;
  protected final String semanticException = "SemanticException No valid privileges";

  protected static boolean clientKerberos = false;
  protected static String REALM = System.getProperty("sentry.service.realm", "EXAMPLE.COM");
  protected static final String SERVER_KERBEROS_NAME = "sentry/" + SERVER_HOST + "@" + REALM;
  protected static final String SERVER_KEY_TAB = System.getProperty("sentry.service.server.keytab");

  private static LoginContext clientLoginContext;
  protected static SentryPolicyServiceClient client;
  private static boolean startSentry = new Boolean(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));

  /**
   * Get sentry client with authenticated Subject
   * (its security-related attributes(for example, kerberos principal and key)
   * @param clientShortName
   * @param clientKeyTabDir
   * @return client's Subject
   */
  public static Subject getClientSubject(String clientShortName, String clientKeyTabDir) {
    String clientKerberosPrincipal = clientShortName + "@" + REALM;
    File clientKeyTabFile = new File(clientKeyTabDir);
    Subject clientSubject = new Subject(false, Sets.newHashSet(
            new KerberosPrincipal(clientKerberosPrincipal)), new HashSet<Object>(),
            new HashSet<Object>());
    try {
      clientLoginContext = new LoginContext("", clientSubject, null,
              KerberosConfiguration.createClientConfig(clientKerberosPrincipal, clientKeyTabFile));
      clientLoginContext.login();
    } catch (Exception ex) {
      LOGGER.error("Exception: " + ex);
    }
    clientSubject = clientLoginContext.getSubject();
    return clientSubject;
  }

  public static void createContext() throws Exception {
    context = new Context(hiveServer, fileSystem,
        baseDir, confDir, dataDir, policyFileLocation);
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
    ArrayList<String> allowedDBs = new ArrayList<String>(Arrays.asList(DB1, DB2, DB3));
    for(String db : dbs) {
      assertTrue(db + " is not part of known test dbs which will be cleaned up after the test", allowedDBs.contains(db));
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
      if(dataFile != null) {
        statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + table);
        ResultSet res = statement.executeQuery("select * from " + table);
        Assert.assertTrue("Table should have data after load", res.next());
        res.close();
      }
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
  public static void setupTestStaticConfiguration() throws Exception {
    LOGGER.info("AbstractTestWithStaticConfiguration setupTestStaticConfiguration");
    properties = Maps.newHashMap();
    if(!policyOnHdfs) {
      policyOnHdfs = new Boolean(System.getProperty("sentry.e2etest.policyonhdfs", "false"));
    }
    if (testServerType != null) {
      properties.put("sentry.e2etest.hiveServer2Type", testServerType);
    }
    baseDir = Files.createTempDir();
    LOGGER.info("BaseDir = " + baseDir);
    logDir = assertCreateDir(new File(baseDir, "log"));
    confDir = assertCreateDir(new File(baseDir, "etc"));
    dataDir = assertCreateDir(new File(baseDir, "data"));
    policyFileLocation = new File(confDir, HiveServerFactory.AUTHZ_PROVIDER_FILENAME);

    String dfsType = System.getProperty(DFSFactory.FS_TYPE);
    dfs = DFSFactory.create(dfsType, baseDir, testServerType);
    fileSystem = dfs.getFileSystem();

    PolicyFile policyFile = PolicyFile.setAdminOnServer1(ADMIN1)
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(policyFileLocation);

    String policyURI;
    if (policyOnHdfs) {
      String dfsUri = FileSystem.getDefaultUri(fileSystem.getConf()).toString();
      LOGGER.error("dfsUri " + dfsUri);
      policyURI = dfsUri + System.getProperty("sentry.e2etest.hive.policy.location",
          "/user/hive/sentry");
      policyURI += "/" + HiveServerFactory.AUTHZ_PROVIDER_FILENAME;
    } else {
      policyURI = policyFileLocation.getPath();
    }

    if ("true".equalsIgnoreCase(System.getProperty(ENABLE_SENTRY_HA, "false"))) {
      enableSentryHA = true;
    }
    if (useSentryService && (!startSentry)) {
      setupSentryService();
    }

    if (enableHiveConcurrency) {
      properties.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "true");
      properties.put(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
          "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
      properties.put(HiveConf.ConfVars.HIVE_LOCK_MANAGER.varname,
          "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    }

    properties.put(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname, "0");

    hiveServer = create(properties, baseDir, confDir, logDir, policyURI, fileSystem);
    hiveServer.start();
    createContext();

    // Create tmp as scratch dir if it doesn't exist
    Path tmpPath = new Path("/tmp");
    if (!fileSystem.exists(tmpPath)) {
      fileSystem.mkdirs(tmpPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  public static HiveServer create(Map<String, String> properties,
      File baseDir, File confDir, File logDir, String policyFile,
      FileSystem fileSystem) throws Exception {
    String type = properties.get(HiveServerFactory.HIVESERVER2_TYPE);
    if(type == null) {
      type = System.getProperty(HiveServerFactory.HIVESERVER2_TYPE);
    }
    if(type == null) {
      type = HiveServerFactory.HiveServer2Type.InternalHiveServer2.name();
    }
    hiveServer2Type = HiveServerFactory.HiveServer2Type.valueOf(type.trim());
    return HiveServerFactory.create(hiveServer2Type, properties,
        baseDir, confDir, logDir, policyFile, fileSystem);
  }

  protected static void writePolicyFile(PolicyFile policyFile) throws Exception {
    policyFile.write(context.getPolicyFile());
    if(policyOnHdfs) {
      LOGGER.info("use policy file on HDFS");
      dfs.writePolicyFile(context.getPolicyFile());
    } else if(useSentryService) {
      LOGGER.info("use sentry service, granting permissions");
      grantPermissions(policyFile);
    }
  }

  private static void grantPermissions(PolicyFile policyFile) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    // remove existing metadata
    ResultSet resultSet = statement.executeQuery("SHOW ROLES");
    while( resultSet.next()) {
      Statement statement1 = context.createStatement(connection);
      String roleName = resultSet.getString(1).trim();
      if(!roleName.equalsIgnoreCase("admin_role")) {
        LOGGER.info("Dropping role :" + roleName);
        statement1.execute("DROP ROLE " + roleName);
      }
    }

    // create roles and add privileges
    for (Map.Entry<String, Collection<String>> roleEntry : policyFile.getRolesToPermissions()
        .asMap().entrySet()) {
      String roleName = roleEntry.getKey();
      if(!roleEntry.getKey().equalsIgnoreCase("admin_role")){
        LOGGER.info("Creating role : " + roleName);
        statement.execute("CREATE ROLE " + roleName);
        for (String privilege : roleEntry.getValue()) {
          addPrivilege(roleEntry.getKey(), privilege, statement);
        }
      }
    }
    // grant roles to groups
    for (Map.Entry<String, Collection<String>> groupEntry : policyFile.getGroupsToRoles().asMap()
        .entrySet()) {
      for (String roleNames : groupEntry.getValue()) {
        for (String roleName : roleNames.split(",")) {
          String sql = "GRANT ROLE " + roleName + " TO GROUP " + groupEntry.getKey();
          LOGGER.info("Granting role to group: " + sql);
          statement.execute(sql);
        }
      }
    }
  }

  private static void addPrivilege(String roleName, String privileges, Statement statement)
      throws IOException, SQLException{
    String serverName = null, dbName = null, tableName = null, uriPath = null, columnName = null;
    String action = "ALL";//AccessConstants.ALL;
    for (String privilege : ROLE_SPLITTER.split(privileges)) {
      for(String section : AUTHORIZABLE_SPLITTER.split(privilege)) {
        // action is not an authorizeable
        if(!section.toLowerCase().startsWith(PRIVILEGE_PREFIX)) {
          DBModelAuthorizable dbAuthorizable = DBModelAuthorizables.from(section);
          if(dbAuthorizable == null) {
            throw new IOException("Unknown Auth type " + section);
          }

          if (DBModelAuthorizable.AuthorizableType.Server.equals(dbAuthorizable.getAuthzType())) {
            serverName = dbAuthorizable.getName();
          } else if (DBModelAuthorizable.AuthorizableType.Db.equals(dbAuthorizable.getAuthzType())) {
            dbName = dbAuthorizable.getName();
          } else if (DBModelAuthorizable.AuthorizableType.Table.equals(dbAuthorizable.getAuthzType())) {
            tableName = dbAuthorizable.getName();
          } else if (DBModelAuthorizable.AuthorizableType.Column.equals(dbAuthorizable.getAuthzType())) {
            columnName = dbAuthorizable.getName();
          } else if (DBModelAuthorizable.AuthorizableType.URI.equals(dbAuthorizable.getAuthzType())) {
            uriPath = dbAuthorizable.getName();
          } else {
            throw new IOException("Unsupported auth type " + dbAuthorizable.getName()
                + " : " + dbAuthorizable.getTypeName());
          }
        } else {
          action = DBModelAction.valueOf(
              StringUtils.removePrefix(section, PRIVILEGE_PREFIX).toUpperCase())
              .toString();
        }
      }

      LOGGER.info("addPrivilege");
      if (columnName != null) {
        statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
        statement.execute("USE " + dbName);
        String sql = "GRANT " + action + " ( " + columnName + " ) ON TABLE " + tableName + " TO ROLE " + roleName;
        LOGGER.info("Granting column level privilege: database = " + dbName + ", sql = " + sql);
        statement.execute(sql);
      } else if (tableName != null) {
        statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
        statement.execute("USE " + dbName);
        String sql = "GRANT " + action + " ON TABLE " + tableName + " TO ROLE " + roleName;
        LOGGER.info("Granting table level privilege:  database = " + dbName + ", sql = " + sql);
        statement.execute(sql);
      } else if (dbName != null) {
        String sql = "GRANT " + action + " ON DATABASE " + dbName + " TO ROLE " + roleName;
        LOGGER.info("Granting db level privilege: " + sql);
        statement.execute(sql);
      } else if (uriPath != null) {
        String sql = "GRANT " + action + " ON URI '" + uriPath + "' TO ROLE " + roleName;
        LOGGER.info("Granting uri level privilege: " + sql);
        statement.execute(sql);//ALL?
      } else if (serverName != null) {
        String sql = "GRANT ALL ON SERVER " + serverName + " TO ROLE " + roleName;
        LOGGER.info("Granting server level privilege: " + sql);
        statement.execute(sql);
      }
    }
  }

  private static void setupSentryService() throws Exception {

    sentryConf = new Configuration(false);

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
    properties.put(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING, ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFileLocation.getPath());
    properties.put(ServerConfig.RPC_MIN_THREADS, "3");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      sentryConf.set(entry.getKey(), entry.getValue());
    }
    sentryServer = SentrySrvFactory.create(
        SentrySrvType.INTERNAL_SERVER, sentryConf, enableSentryHA ? 2 : 1);
    properties.put(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.get(0)
        .getAddress()
        .getHostName());
    sentryConf.set(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.get(0)
        .getAddress()
        .getHostName());
    properties.put(ClientConfig.SERVER_RPC_PORT,
        String.valueOf(sentryServer.get(0).getAddress().getPort()));
    sentryConf.set(ClientConfig.SERVER_RPC_PORT,
        String.valueOf(sentryServer.get(0).getAddress().getPort()));
    if (enableSentryHA) {
      properties.put(ClientConfig.SERVER_HA_ENABLED, "true");
      properties.put(ClientConfig.SENTRY_HA_ZOOKEEPER_QUORUM,
          sentryServer.getZKQuorum());
    }
    startSentryService();
    if (setMetastoreListener) {
      LOGGER.info("setMetastoreListener is enabled");
      properties.put(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname,
          SentryMetastorePostEventListener.class.getName());
    }

  }

  private static void startSentryService() throws Exception {
    sentryServer.startAll();
  }

  public static SentryPolicyServiceClient getSentryClient() throws Exception {
    if (sentryServer == null) {
      throw new IllegalAccessException("Sentry service not initialized");
    }
    return SentryServiceClientFactory.create(sentryServer.get(0).getConf());
  }

  /**
   * Get Sentry authorized client to communicate with sentry server,
   * the client can be for a minicluster, real distributed cluster,
   * sentry server can use policy file or it's a service.
   * @param clientShortName: principal prefix string
   * @param clientKeyTabDir: authorization key path
   * @return sentry client to talk to sentry server
   * @throws Exception
   */
  public static SentryPolicyServiceClient getSentryClient(String clientShortName,
                                                          String clientKeyTabDir) throws Exception {
    if (!startSentry) {
      LOGGER.info("Running on a minicluser env.");
      return getSentryClient();
    }

    if (clientKerberos) {
      if (sentryConf == null ) {
        sentryConf = new Configuration(false);
      }
      final String SENTRY_HOST = System.getProperty("sentry.host", SERVER_HOST);
      final String SERVER_KERBEROS_PRINCIPAL = "sentry/" + SENTRY_HOST + "@" + REALM;
      sentryConf.set(ServerConfig.PRINCIPAL, SERVER_KERBEROS_PRINCIPAL);
      sentryConf.set(ServerConfig.KEY_TAB, SERVER_KEY_TAB);
      sentryConf.set(ServerConfig.ALLOW_CONNECT, "hive");
      sentryConf.set(ServerConfig.SECURITY_USE_UGI_TRANSPORT, "false");
      sentryConf.set(ClientConfig.SERVER_RPC_ADDRESS,
              System.getProperty("sentry.service.server.rpc.address"));
      sentryConf.set(ClientConfig.SERVER_RPC_PORT,
              System.getProperty("sentry.service.server.rpc.port", "8038"));
      sentryConf.set(ClientConfig.SERVER_RPC_CONN_TIMEOUT, "720000"); //millis
      Subject clientSubject = getClientSubject(clientShortName, clientKeyTabDir);
      client = Subject.doAs(clientSubject,
              new PrivilegedExceptionAction<SentryPolicyServiceClient>() {
                @Override
                public SentryPolicyServiceClient run() throws Exception {
                  return SentryServiceClientFactory.create(sentryConf);
                }
              });
    } else {
      client = getSentryClient();
    }
    return client;
  }

  @Before
  public void setup() throws Exception{
    LOGGER.info("AbstractTestStaticConfiguration setup");
    dfs.createBaseDir();
    if (clearDbPerTest) {
      LOGGER.info("Before per test run clean up");
      clearAll(true);
    }
  }

  @After
  public void clearAfterPerTest() throws Exception {
    LOGGER.info("AbstractTestStaticConfiguration clearAfterPerTest");
    if (clearDbPerTest) {
      LOGGER.info("After per test run clean up");
      clearAll(true);
     }
  }

  protected static void clearAll(boolean clearDb) throws Exception {
    LOGGER.info("About to run clearAll");
    ResultSet resultSet;
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    if (clearDb) {
      LOGGER.info("About to clear all databases and default database tables");
      resultSet = execQuery(statement, "SHOW DATABASES");
      while (resultSet.next()) {
        String db = resultSet.getString(1);
        if (!db.equalsIgnoreCase("default")) {
          try (Statement statement1 = context.createStatement(connection)) {
            exec(statement1, "DROP DATABASE IF EXISTS " + db + " CASCADE");
          } catch (Exception ex) {
            // For database and tables managed by other processes than Sentry
            // drop them might run into exception
            LOGGER.error("Exception: " + ex);
          }
        }
      }
      if (resultSet != null) {  resultSet.close(); }
      exec(statement, "USE default");
      resultSet = execQuery(statement, "SHOW TABLES");
      while (resultSet.next()) {
        try (Statement statement1 = context.createStatement(connection)) {
          exec(statement1, "DROP TABLE IF EXISTS " + resultSet.getString(1));
        } catch (Exception ex) {
          // For table managed by other processes than Sentry
          // drop it might run into exception
          LOGGER.error("Exception: " + ex);
        }
      }
      if (resultSet != null) {  resultSet.close(); }
    }

    if(useSentryService) {
      LOGGER.info("About to clear all roles");
      resultSet = execQuery(statement, "SHOW ROLES");
      while (resultSet.next()) {
        try (Statement statement1 = context.createStatement(connection)) {
          String role = resultSet.getString(1);
          if (!role.toLowerCase().contains("admin")) {
            exec(statement1, "DROP ROLE " + role);
          }
        }
      }
      if (resultSet != null) { resultSet.close(); }
    }

    if (statement != null) { statement.close(); }
    if (connection != null) { connection.close(); }
  }

  protected static void setupAdmin() throws Exception {
    if(useSentryService) {
      LOGGER.info("setupAdmin to create admin_role");
      Connection connection = context.createConnection(ADMIN1);
      Statement statement = connection.createStatement();
      try {
        statement.execute("CREATE ROLE admin_role");
      } catch ( Exception e) {
        //It is ok if admin_role already exists
      }
      statement.execute("GRANT ALL ON SERVER "
          + HiveServerFactory.DEFAULT_AUTHZ_SERVER_NAME + " TO ROLE admin_role");
      statement.execute("GRANT ROLE admin_role TO GROUP " + ADMINGROUP);
      statement.close();
      connection.close();
    }
  }

  protected PolicyFile setupPolicy() throws Exception {
    LOGGER.info("Pre create policy file with admin group mapping");
    PolicyFile policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
    return policyFile;
  }

  @AfterClass
  public static void tearDownTestStaticConfiguration() throws Exception {
    if(hiveServer != null) {
      hiveServer.shutdown();
      hiveServer = null;
    }

    if (sentryServer != null) {
      sentryServer.close();
      sentryServer = null;
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
    if (context != null) {
      context.close();
    }
  }

  public static SentrySrv getSentrySrv() {
    return sentryServer;
  }

  /**
   * A convenience method to validate:
   * if expected is equivalent to returned;
   * Firstly check if each expected item is in the returned list;
   * Secondly check if each returned item in in the expected list.
   */
  protected void validateReturnedResult(List<String> expected, List<String> returned) {
    for (String obj : expected) {
      assertTrue("expected " + obj + " not found in the returned list: " + returned.toString(),
              returned.contains(obj));
    }
    for (String obj : returned) {
      assertTrue("returned " + obj + " not found in the expected list: " + expected.toString(),
              expected.contains(obj));
    }
  }

  /**
   * A convenient function to run a sequence of sql commands
   * @param user
   * @param sqls
   * @throws Exception
   */
  protected static void execBatch(String user, List<String> sqls) throws Exception {
    Connection conn = context.createConnection(user);
    Statement stmt = context.createStatement(conn);
    for (String sql : sqls) {
      exec(stmt, sql);
    }
    if (stmt != null) {
      stmt.close();
    }
    if (conn != null) {
      conn.close();
    }
  }

  /**
   * A convenient funciton to run one sql with log
   * @param stmt
   * @param sql
   * @throws Exception
   */
  protected static void exec(Statement stmt, String sql) throws Exception {
    if (stmt == null) {
      LOGGER.error("Statement is null");
      return;
    }
    LOGGER.info("Running [" + sql + "]");
    stmt.execute(sql);
  }

  /**
   * A convenient funciton to execute query with log then return ResultSet
   * @param stmt
   * @param sql
   * @return ResetSet
   * @throws Exception
   */
  protected static ResultSet execQuery(Statement stmt, String sql) throws Exception {
    LOGGER.info("Running [" + sql + "]");
    return stmt.executeQuery(sql);
  }
}
