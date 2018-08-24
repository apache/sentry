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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions
 * and
 * limitations under the License.
*/

package org.apache.sentry.tests.e2e.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.hdfs.SentryAuthorizationConstants;
import org.apache.sentry.service.common.ServiceConstants;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

/**
 * this test class includes tests to verify the behaviour of sentry server
 * when the HDFS sync feature is toggled on/off
 */
public class TestHDFSIntegrationTogglingConf extends TestHDFSIntegrationBase {

  private static long getSleepTimeAfterFollowerRestart(Configuration conf) {
    long followerInitDelay = conf.getLong(ServiceConstants.ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS,
            ServiceConstants.ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS_DEFAULT);
    long followerInterval = conf.getLong(ServiceConstants.ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS,
            ServiceConstants.ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT);
    long refreshIntervalMillisec = conf.getInt(
            SentryAuthorizationConstants.CACHE_REFRESH_INTERVAL_KEY,
            SentryAuthorizationConstants.CACHE_REFRESH_INTERVAL_DEFAULT);

    return (followerInitDelay + followerInterval + refreshIntervalMillisec) * 2;
  }

  private static void enableHdfsSync(int serverIndex) throws Exception {
    Configuration newConfig = new Configuration(sentryConf);
    newConfig.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES,
            "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    newConfig.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS,
            "org.apache.sentry.hdfs.SentryPlugin");
    newConfig.set(ServiceConstants.ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS,
            "1000");
    sentryServer.restartHMSFollower(newConfig, serverIndex,
            getSleepTimeAfterFollowerRestart(newConfig));
  }

  private static void disableHdfsSync(int serverIndex) throws Exception {
    Configuration newConfig = new Configuration(sentryConf);
    newConfig.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "");
    newConfig.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "");
    newConfig.set(ServiceConstants.ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS,
            "1000");

    sentryServer.restartHMSFollower(newConfig, serverIndex,
            getSleepTimeAfterFollowerRestart(newConfig));
  }

  @BeforeClass
  public static void setup() throws Exception {
    hdfsSyncEnabled = true;
    TestHDFSIntegrationBase.setup();
  }

  /**
   * Test makes sure that the namenode is not synced with the new change to HMS when
   * processor and sentry_plugin for HDFS sync are not configured.
   *
   * @throws Throwable
   */
  @Test
  public void testDisablingHDFSSync() throws Throwable {
    disableHdfsSync(0);
    dbNames = new String[]{"db1"};
    roles = new String[]{"admin_role", "tab_role"};
    admin = "hive";

    Connection conn;
    Statement stmt;
    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant role admin_role to group hive");
    stmt.execute("grant all on server server1 to role admin_role");

    // db privileges
    stmt.execute("create database db1");
    stmt.execute("create role tab_role");
    stmt.execute("grant role tab_role to group flume");
    stmt.execute("create table db1.p2(id int)");

    stmt.execute("use db1");
    stmt.execute("grant all on table p2 to role tab_role");
    stmt.execute("use default");
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db", FsAction.ALL, "hbase", false);
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db/p2", FsAction.ALL, "flume", false);
    verifyGroupPermOnPath("/user/hive/warehouse/db1.db", FsAction.ALL, "flume", false);

    //Enabling HDFS sync back in sentry server
    enableHdfsSync(0);
  }

  /**
   * Test makes sure that HDFS sync configurations in sentryserver are toggled multiple times.
   * <ul>
   * <li>When processor and sentry_plugin for HDFS sync are configured,
   * Namenode should have all the HMS path and permission updates.</li>
   * <li>When processor and sentry_plugin for HDFS sync are configured,
   * Namenode should not have the HMS path updates.</li>
   * <li>When processor and sentry_plugin for HDFS sync are configured again,
   * Namenode should not have the HMS path updates by getting HMS full snapshot
   * from sentry server.</li>
   * </ul>
   *
   * @throws Throwable
   */
  @Test
  public void testEnablingDisablingHDFSSync() throws Throwable {
    dbNames = new String[]{"db1", "db6"};
    roles = new String[]{"admin_role", "db_role", "tab_role", "p1_admin"};
    admin = "hive";

    Connection conn;
    Statement stmt;
    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant role admin_role to group hive");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("create table p1 (s string) partitioned by (month int, day " +
            "int)");
    stmt.execute("alter table p1 add partition (month=1, day=1)");

    // db privileges
    stmt.execute("create database db1");
    stmt.execute("create role db_role");
    stmt.execute("create role tab_role");
    stmt.execute("grant role db_role to group hbase");
    stmt.execute("grant role tab_role to group flume");
    stmt.execute("create table db1.p2(id int)");

    stmt.execute("create role p1_admin");
    stmt.execute("grant role p1_admin to group hbase");

    // Verify default db is inaccessible initially
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse", null, "hbase", false);

    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    stmt.execute("grant all on database db1 to role db_role");
    stmt.execute("use db1");
    stmt.execute("grant all on table p2 to role tab_role");
    stmt.execute("use default");
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db", FsAction.ALL, "hbase", true);
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db/p2", FsAction.ALL, "hbase", true);
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db/p2", FsAction.ALL, "flume", true);
    verifyGroupPermOnPath("/user/hive/warehouse/db1.db", FsAction.ALL, "flume", false);

    loadData(stmt);

    verifyHDFSandMR(stmt);

    //Disabling HDFS sync in sentry server
    disableHdfsSync(0);

    stmt.execute("revoke all on database db1 from role db_role");
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db", FsAction.ALL, "hbase", false);

    // create a table and grant all to db_role
    stmt.execute("create database db6");
    stmt.execute("grant all on database db6 to role db_role");

    // verify that db_role does not have required ACL's as HDFS sync is disabled in sentry server.
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db6.db", FsAction.ALL, "hbase", false);

    //Create table in db6 and grant all privileges to tab role
    stmt.execute("use db6");
    stmt.execute("create table db6.p1(id int)");
    stmt.execute("grant all on table db6.p1 to role tab_role");

    // verify that tab_role does not have required permissions
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db6.db/p1", FsAction.ALL, "flume", false);

    //Enabling HDFS sync in sentry server
    enableHdfsSync(0);

    // As HDFS sync is re-enabled, sentry should take full snapshot and send it NN.
    // db_role and tab_role should have required privileges.
    // Checks below will make sure that sentry/NN have the updates that happened
    // to HMS objects when HDFS was disabled.
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db6.db", FsAction.ALL, "hbase", true);
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db6.db/p1", FsAction.ALL, "flume", true);

    stmt.close();
    conn.close();
  }
}