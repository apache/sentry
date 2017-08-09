/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME;

import com.google.common.io.Files;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.UserProvider;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.HCatEventMessage.EventType;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory;

import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.HiveSimpleConnectionFactory;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.HMSFollower;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test integration of HMSFollower with SentryStore
 */
public class TestHMSFollowerSentryStoreIntegration {

  // SentryStore related member
  private static File dataDir;
  private static SentryStore sentryStore;
  private static String[] adminGroups = { "adminGroup1" };
  private static PolicyFile policyFile;
  private static File policyFilePath;
  private static Configuration conf = null;
  private static char[] passwd = new char[] { '1', '2', '3'};
  private static String dbName1 = "db1";
  private static String tableName1 = "table1";
  private static String serverName1 = "server1";

  // HMSFollower related member
  SentryJSONMessageFactory messageFactory = new SentryJSONMessageFactory();
  final static String hiveInstance = serverName1;


  // SentryStore related
  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration(false);
    final String ourUrl = UserProvider.SCHEME_NAME + ":///";
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    // THis should be a UserGroupInformation provider
    CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);

    // The user credentials are stored as a static variable by UserGrouoInformation provider.
    // We need to only set the password the first time, an attempt to set it for the second
    // time fails with an exception.
    if(provider.getCredentialEntry(ServerConfig.SENTRY_STORE_JDBC_PASS) == null) {
      provider.createCredentialEntry(ServerConfig.SENTRY_STORE_JDBC_PASS, passwd);
      provider.flush();
    }

    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.setStrings(ServerConfig.ADMIN_GROUPS, adminGroups);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    policyFilePath = new File(dataDir, "local_policy_file.ini");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    conf.setInt(ServerConfig.SENTRY_STORE_TRANSACTION_RETRY, 10);

  }

  @Before
  public void before() throws Exception {
    sentryStore = new SentryStore(conf);
    sentryStore.setPersistUpdateDeltas(true);
    policyFile = new PolicyFile();
    String adminUser = "g1";
    addGroupsToUser(adminUser, adminGroups);
    writePolicyFile();
  }

  @After
  public void after() {
    if (sentryStore != null) {
      sentryStore.clearAllTables();
      sentryStore.stop();
    }
  }

  @AfterClass
  public static void teardown() {

    if (dataDir != null) {
      FileUtils.deleteQuietly(dataDir);
    }
  }

  protected static void addGroupsToUser(String user, String... groupNames) {
    policyFile.addGroupsToUser(user, groupNames);
  }

  protected static void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }

  /**
   * Test that SentryStore droppes the permission associated with the table when HMSFollower
   * processes the drop table event
   * @throws Exception
   */
  @Test
  public void testDropTableDropRelatedPermission() throws Exception {
    String serverName = "server1";

    // create HMSFollower
    Configuration configuration = new Configuration();
    configuration.set(AUTHZ_SERVER_NAME.getVar(), serverName);
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
      new HiveSimpleConnectionFactory(conf, new HiveConf()), null);

    // configure permission of the table
    String roleName1 = "list-privs-r1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName(serverName);
    privilege_tbl1.setDbName(dbName1);
    privilege_tbl1.setTableName(tableName1);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege1 = new TSentryPrivilege(privilege_tbl1);
    privilege1.setAction("SELECT");

    TSentryPrivilege privilege1_2 = new TSentryPrivilege(privilege_tbl1);
    privilege1_2.setAction("INSERT");
    TSentryPrivilege privilege1_3 = new TSentryPrivilege(privilege_tbl1);
    privilege1_3.setAction("*");

    TSentryPrivilege privilege_server = new TSentryPrivilege();
    privilege_server.setPrivilegeScope("SERVER");
    privilege_server.setServerName(serverName1);
    privilege_server.setCreateTime(System.currentTimeMillis());

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1_2);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_server);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1_3);

    // Create notification events to drop the table
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.DROP_TABLE.toString(),
        messageFactory.buildDropTableMessage(new Table(tableName1, dbName1, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    hmsFollower.processNotifications(events);

    Assert.assertEquals(1, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1)
        .size());
  }

  /**
   * Test that SentryStore droppes the permission associated with the database when HMSFollower
   * processes the drop database event
   * @throws Exception
   */
  @Test
  public void testDropDatabaseDropRelatedPermission() throws Exception {
    String serverName = "server1";

    // create HMSFollower
    Configuration configuration = new Configuration();
    configuration.set(AUTHZ_SERVER_NAME.getVar(), serverName);
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        new HiveSimpleConnectionFactory(conf, new HiveConf()), null);

    // configure permission of the database
    String roleName1 = "list-privs-r1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("DATABASE");
    privilege_tbl1.setServerName(serverName);
    privilege_tbl1.setDbName(dbName1);
    privilege_tbl1.setTableName(tableName1);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege1 = new TSentryPrivilege(privilege_tbl1);
    privilege1.setAction("SELECT");

    TSentryPrivilege privilege1_2 = new TSentryPrivilege(privilege_tbl1);
    privilege1_2.setAction("INSERT");
    TSentryPrivilege privilege1_3 = new TSentryPrivilege(privilege_tbl1);
    privilege1_3.setAction("*");

    TSentryPrivilege privilege_server = new TSentryPrivilege();
    privilege_server.setPrivilegeScope("SERVER");
    privilege_server.setServerName(serverName1);
    privilege_server.setCreateTime(System.currentTimeMillis());

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1_2);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_server);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1_3);

    // Create notification events to drop the database
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, EventType.DROP_DATABASE.toString(),
        messageFactory.buildDropDatabaseMessage(new Database(dbName1, null, "hdfs:///" + dbName1, null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    hmsFollower.processNotifications(events);

    Assert.assertEquals(1, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1)
        .size());
  }
}
