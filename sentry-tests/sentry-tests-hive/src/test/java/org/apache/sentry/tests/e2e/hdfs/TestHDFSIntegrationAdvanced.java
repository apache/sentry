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
package org.apache.sentry.tests.e2e.hdfs;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Advanced tests for HDFS Sync integration
 */
public class TestHDFSIntegrationAdvanced extends TestHDFSIntegrationBase {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestHDFSIntegrationAdvanced.class);

  @BeforeClass
  public static void setup() throws Exception{
    hdfsSyncEnabled = true;
    TestHDFSIntegrationBase.setup();
  }
  @Test
  public void testNoPartitionInsert() throws Throwable {
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

    //Create table and grant select to user flume
    stmt.execute("create database db1");
    stmt.execute("use db1");
    stmt.execute("create table t1 (s string)");
    stmt.execute("create role tab_role");
    stmt.execute("grant select on table t1 to role tab_role");
    stmt.execute("grant role tab_role to group flume");

    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db/t1", FsAction.READ_EXECUTE, "flume", true);
    stmt.execute("INSERT INTO TABLE t1 VALUES (1)");
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/db1.db/t1", FsAction.READ_EXECUTE, "flume", true);

  }

  /**
   * Make sure non HDFS paths are not added to the object - location map.
   * @throws Throwable
   */
  @Test
  public void testNonHDFSLocations() throws Throwable {
    String dbName = "db2";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "user_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant all on uri 'file:///tmp/external' to role admin_role");
    stmt.execute("grant all on uri 'hdfs:///tmp/external' to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(admin, admin);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role user_role");
    stmt.execute("grant all on database " + dbName + " to role user_role");
    stmt.execute("grant role user_role to group " + StaticUserGroup.USERGROUP1);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(admin, admin);
    stmt = conn.createStatement();

    //External table on local file system
    miniDFS.getFileSystem().mkdirs(new Path("/tmp/external/tab1_loc"));
    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1(a int) location 'file:///tmp/external/tab1_loc'");
    verifyGroupPermOnAllSubDirs("/tmp/external/tab1_loc", null, StaticUserGroup.USERGROUP1, false);

    //External partitioned table on local file system
    miniDFS.getFileSystem().mkdirs(new Path("/tmp/external/tab2_loc/i=1"));
    stmt.execute("create external table tab2 (s string) partitioned by (i int) location 'file:///tmp/external/tab2_loc'");
    verifyGroupPermOnAllSubDirs("/tmp/external/tab2_loc", null, StaticUserGroup.USERGROUP1, false);
    //Partition on local file system
    stmt.execute("alter table tab2 add partition (i=1)");
    stmt.execute("alter table tab2 partition (i=1) set location 'file:///tmp/external/tab2_loc/i=1'");

    verifyGroupPermOnAllSubDirs("/tmp/external/tab2_loc/i=1", null, StaticUserGroup.USERGROUP1, false);

    //HDFS to local file system, also make sure does not specifying scheme still works
    stmt.execute("create external table tab3(a int) location '/tmp/external/tab3_loc'");
    // SENTRY-546
    // SENTRY-1471 - fixing the validation logic revealed that FsAction.ALL is the right value.
    verifyGroupPermOnAllSubDirs("/tmp/external/tab3_loc", FsAction.ALL, StaticUserGroup.USERGROUP1, true);
    // verifyGroupPermOnAllSubDirs("/tmp/external/tab3_loc", null, StaticUserGroup.USERGROUP1, true);
    stmt.execute("alter table tab3 set location 'file:///tmp/external/tab3_loc'");
    verifyGroupPermOnAllSubDirs("/tmp/external/tab3_loc", null, StaticUserGroup.USERGROUP1, false);

    //Local file system to HDFS
    stmt.execute("create table tab4(a int) location 'file:///tmp/external/tab4_loc'");
    stmt.execute("alter table tab4 set location 'hdfs:///tmp/external/tab4_loc'");
    miniDFS.getFileSystem().mkdirs(new Path("/tmp/external/tab4_loc"));
    // SENTRY-546
    // SENTRY-1471 - fixing the validation logic revealed that FsAction.ALL is the right value.
    verifyGroupPermOnAllSubDirs("/tmp/external/tab4_loc", FsAction.ALL, StaticUserGroup.USERGROUP1, true);
    // verifyGroupPermOnAllSubDirs("/tmp/external/tab4_loc", null, StaticUserGroup.USERGROUP1, true);
    stmt.close();
    conn.close();
  }

  /**
   * Make sure when events such as table creation fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testTableCreationFailure() throws Throwable {
    String dbName = "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant all on uri 'hdfs:///tmp/external' to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.execute("grant role admin_role to group " + StaticUserGroup.HIVE);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);

    miniDFS.getFileSystem().setOwner(tmpHDFSDir, "hdfs", "hdfs");
    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));

    // Expect table creation to fail because hive:hive does not have
    // permission to write at parent directory.
    try {
      stmt.execute("create external table tab1(a int) location '" + tmpHDFSPartitionStr + "'");
      Assert.fail("Expect table creation to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when creating table: " + ex.getMessage());
    }

    // When the table creation failed, the path will not be managed by sentry. And the
    // permission of the path will not be hive:hive.
    verifyGroupPermOnAllSubDirs("/tmp/external/p1", null, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  /**
   * Make sure when events such as add partition fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testAddPartitionFailure() throws Throwable {
    String dbName = "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("create external table tab2 (s string) partitioned by (month int)");

    // Expect adding partition to fail because hive:hive does not have
    // permission to write at parent directory.
    miniDFS.getFileSystem().setOwner(tmpHDFSDir, "hdfs", "hdfs");
    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));

    try {
      stmt.execute("alter table tab2 add partition (month = 1) location '" + tmpHDFSPartitionStr + "'");
      Assert.fail("Expect adding partition to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when adding partition: " + ex.getMessage());
    }

    // When the table creation failed, the path will not be managed by sentry. And the
    // permission of the path will not be hive:hive.
    verifyGroupPermOnAllSubDirs("/tmp/external/p1", null, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  /**
   * Make sure when events such as drop table fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testDropTableFailure() throws Throwable {
    String dbName = "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    LOGGER.info("create external table in " + tmpHDFSPartitionStr);
    stmt.execute("create external table tab1(a int) partitioned by (date1 string) location 'hdfs://" + tmpHDFSPartitionStr + "'");

    miniDFS.getFileSystem().setOwner(tmpHDFSDir, "hdfs", "hdfs");
    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));

    // Expect dropping table to fail because hive:hive does not have
    // permission to write at parent directory when
    // hive.metastore.authorization.storage.checks property is true.
    try {
      stmt.execute("set hive.metastore.authorization.storage.checks=true");
      stmt.execute("drop table tab1");
      Assert.fail("Expect dropping table to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when creating table: " + ex.getMessage());
    }

    // When the table dropping failed, the path will still be managed by sentry. And the
    // permission of the path still should be hive:hive.
    verifyGroupPermOnAllSubDirs(tmpHDFSPartitionStr, FsAction.ALL, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  /**
   * Make sure when events such as drop table fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testDropPartitionFailure() throws Throwable {
    String dbName = "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("create table tab3 (s string) partitioned by (month int)");
    stmt.execute("alter table tab3 add partition (month = 1) location '" + tmpHDFSPartitionStr + "'");

    miniDFS.getFileSystem().setOwner(tmpHDFSDir, "hdfs", "hdfs");
    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));

    // Expect dropping partition to fail because because hive:hive does not have
    // permission to write at parent directory.
    try {
      stmt.execute("ALTER TABLE tab3 DROP PARTITION (month = 1)");
      Assert.fail("Expect dropping partition to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when dropping partition: " + ex.getMessage());
    }

    // When the partition dropping failed, the path for the partition will still
    // be managed by sentry. And the permission of the path still should be hive:hive.
    verifyGroupPermOnAllSubDirs(tmpHDFSPartitionStr, FsAction.ALL, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  @Test
  public void testURIsWithoutSchemeandAuthority() throws Throwable {
    // In the local test environment, EXTERNAL_SENTRY_SERVICE is false,
    // set the default URI scheme to be hdfs.
    boolean testConfOff = Boolean.valueOf(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));
    if (!testConfOff) {
      PathUtils.getConfiguration().set("fs.defaultFS", fsURI);
    }

    String dbName= "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "db_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();

    stmt.execute("create database " + dbName);
    stmt.execute("create role db_role");
    stmt.execute("grant all on database " + dbName +" to role db_role");
    stmt.execute("grant all on URI '/tmp/external' to role db_role");
    stmt.execute("grant role db_role to group " + StaticUserGroup.USERGROUP1);

    conn = hiveServer2.createConnection(StaticUserGroup.USER1_1, StaticUserGroup.USER1_1);
    stmt = conn.createStatement();

    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) location '/tmp/external'");

    stmt.close();
    conn.close();
  }

  /**
   * Test combination of "grant all on URI" where URI has scheme,
   * followed by "create external table" where location URI has no scheme.
   * Neither URI has authority.
   */
  @Test
  public void testURIsWithAndWithoutSchemeNoAuthority() throws Throwable {
    // In the local test environment, EXTERNAL_SENTRY_SERVICE is false,
    // set the default URI scheme to be hdfs.
    boolean testConfOff = Boolean.valueOf(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));
    if (!testConfOff) {
      PathUtils.getConfiguration().set("fs.defaultFS", fsURI);
    }

    String dbName= "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "db_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();

    stmt.execute("create database " + dbName);
    stmt.execute("create role db_role");
    stmt.execute("grant all on database " + dbName +" to role db_role");
    stmt.execute("grant all on URI 'hdfs:///tmp/external' to role db_role");
    stmt.execute("grant role db_role to group " + StaticUserGroup.USERGROUP1);

    conn = hiveServer2.createConnection(StaticUserGroup.USER1_1, StaticUserGroup.USER1_1);
    stmt = conn.createStatement();

    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) location '/tmp/external'");

    stmt.close();
    conn.close();
  }

  /**
   * Test combination of "grant all on URI" where URI has no scheme,
   * followed by "create external table" where location URI has scheme.
   * Neither URI has authority.
   */
  @Test
  public void testURIsWithoutAndWithSchemeNoAuthority() throws Throwable {
    // In the local test environment, EXTERNAL_SENTRY_SERVICE is false,
    // set the default URI scheme to be hdfs.
    boolean testConfOff = Boolean.valueOf(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));
    if (!testConfOff) {
      PathUtils.getConfiguration().set("fs.defaultFS", fsURI);
    }

    String dbName= "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "db_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();

    stmt.execute("create database " + dbName);
    stmt.execute("create role db_role");
    stmt.execute("grant all on database " + dbName +" to role db_role");
    stmt.execute("grant all on URI '/tmp/external' to role db_role");
    stmt.execute("grant role db_role to group " + StaticUserGroup.USERGROUP1);

    conn = hiveServer2.createConnection(StaticUserGroup.USER1_1, StaticUserGroup.USER1_1);
    stmt = conn.createStatement();

    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) location 'hdfs:///tmp/external'");

    stmt.close();
    conn.close();
  }

  /**
   * Test combination of "grant all on URI" where URI has scheme and authority,
   * followed by "create external table" where location URI has neither scheme nor authority.
   */
  @Test
  public void testURIsWithAndWithoutSchemeAndAuthority() throws Throwable {
    // In the local test environment, EXTERNAL_SENTRY_SERVICE is false,
    // set the default URI scheme to be hdfs.
    boolean testConfOff = Boolean.valueOf(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));
    if (!testConfOff) {
      PathUtils.getConfiguration().set("fs.defaultFS", fsURI);
    }

    String dbName= "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "db_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();

    stmt.execute("create database " + dbName);
    stmt.execute("create role db_role");
    stmt.execute("grant all on database " + dbName +" to role db_role");
    stmt.execute("grant all on URI 'hdfs://" + new URI(fsURI).getAuthority() + "/tmp/external' to role db_role");
    stmt.execute("grant role db_role to group " + StaticUserGroup.USERGROUP1);

    conn = hiveServer2.createConnection(StaticUserGroup.USER1_1, StaticUserGroup.USER1_1);
    stmt = conn.createStatement();

    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) location '/tmp/external'");

    stmt.close();
    conn.close();
  }

  //SENTRY-884
  @Test
  public void testAccessToTableDirectory() throws Throwable {
    String dbName= "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "table_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create table tb1(a string)");

    stmt.execute("create role table_role");
    stmt.execute("grant all on table tb1 to role table_role");
    stmt.execute("grant role table_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode
    //Verify user1 is able to access table directory
    verifyAccessToPath(StaticUserGroup.USER1_1, StaticUserGroup.USERGROUP1, "/user/hive/warehouse/db1.db/tb1", true);

    stmt.close();
    conn.close();
  }

  /* SENTRY-953 */
  /* SENTRY-1471 - fixing the validation logic revealed that this test is broken.
   * Disabling this test for now; to be fixed in a separate JIRA.
   */
  @Test
  public void testAuthzObjOnPartitionMultipleTables() throws Throwable {
    String dbName = "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab1_role", "tab2_role", "tab3_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    // Create external table tab1 on location '/tmp/external/p1'.
    // Create tab1_role, and grant it with insert permission on table tab1 to user_group1.
    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) partitioned by (month int) location '/tmp/external/p1'");
    stmt.execute("create role tab1_role");
    stmt.execute("grant insert on table tab1 to role tab1_role");
    stmt.execute("grant role tab1_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode

    // Verify that user_group1 has insert(write_execute) permission on '/tmp/external/p1'.
    verifyGroupPermOnAllSubDirs("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    // Create external table tab2 and partition on location '/tmp/external'.
    // Create tab2_role, and grant it with select permission on table tab2 to user_group2.
    stmt.execute("create external table tab2 (s string) partitioned by (month int)");
    stmt.execute("alter table tab2 add partition (month = 1) location '" + tmpHDFSDirStr + "'");
    stmt.execute("create role tab2_role");
    stmt.execute("grant select on table tab2 to role tab2_role");
    stmt.execute("grant role tab2_role to group " + StaticUserGroup.USERGROUP2);
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode

    // Verify that user_group2 have select(read_execute) permission on both paths.
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab2", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);
    verifyGroupPermOnPath(tmpHDFSDirStr, FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);

    // Create table tab3 and partition on the same location '/tmp/external' as tab2.
    // Create tab3_role, and grant it with insert permission on table tab3 to user_group3.
    stmt.execute("create table tab3 (s string) partitioned by (month int)");
    stmt.execute("alter table tab3 add partition (month = 1) location '" + tmpHDFSDirStr + "'");
    stmt.execute("create role tab3_role");
    stmt.execute("grant insert on table tab3 to role tab3_role");
    stmt.execute("grant role tab3_role to group " + StaticUserGroup.USERGROUP3);
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode

    // When two partitions of different tables pointing to the same location with different grants,
    // ACLs should have union (no duplicates) of both rules.
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab3", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);
    verifyGroupPermOnPath(tmpHDFSDirStr, FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);
    verifyGroupPermOnPath(tmpHDFSDirStr, FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    // When alter the table name (tab2 to be tabx), ACLs should remain the same.
    stmt.execute("alter table tab2 rename to tabx");
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode
    verifyGroupPermOnPath(tmpHDFSDirStr, FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);
    verifyGroupPermOnPath(tmpHDFSDirStr, FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    // When drop a partition that shares the same location with other partition belonging to
    // other table, should still have the other table permissions.
    stmt.execute("ALTER TABLE tabx DROP PARTITION (month = 1)");
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab3", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);
    verifyGroupPermOnPath(tmpHDFSDirStr, FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    // When drop a table that has a partition shares the same location with other partition
    // belonging to other table, should still have the other table permissions.
    stmt.execute("DROP TABLE IF EXISTS tabx");
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode
    verifyGroupPermOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab3", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);
    verifyGroupPermOnPath(tmpHDFSDirStr, FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    stmt.close();
    conn.close();

    miniDFS.getFileSystem().delete(partitionDir, true);
  }

  /* SENTRY-953 */
  @Test
  public void testAuthzObjOnPartitionSameTable() throws Throwable {
    String dbName = "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab1_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    // Create table tab1 and partition on the same location '/tmp/external/p1'.
    // Create tab1_role, and grant it with insert permission on table tab1 to user_group1.
    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create table tab1 (s string) partitioned by (month int)");
    stmt.execute("alter table tab1 add partition (month = 1) location '/tmp/external/p1'");
    stmt.execute("create role tab1_role");
    stmt.execute("grant insert on table tab1 to role tab1_role");
    stmt.execute("grant role tab1_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode

    // Verify that user_group1 has insert(write_execute) permission on '/tmp/external/p1'.
    verifyGroupPermOnAllSubDirs("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    // When two partitions of the same table pointing to the same location,
    // ACLS should not be repeated. Exception will be thrown if there are duplicates.
    stmt.execute("alter table tab1 add partition (month = 2) location '/tmp/external/p1'");
    verifyGroupPermOnPath("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    stmt.close();
    conn.close();
  }

  /* SENTRY-953 */
  @Test
  public void testAuthzObjOnMultipleTables() throws Throwable {
    String dbName = "db1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab1_role", "tab2_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    // Create external table tab1 on location '/tmp/external/p1'.
    // Create tab1_role, and grant it with insert permission on table tab1 to user_group1.
    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) partitioned by (month int) location '/tmp/external/p1'");
    stmt.execute("create role tab1_role");
    stmt.execute("grant insert on table tab1 to role tab1_role");
    stmt.execute("grant role tab1_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode

    // Verify that user_group1 has insert(write_execute) permission on '/tmp/external/p1'.
    verifyGroupPermOnAllSubDirs("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    // Create table tab2 on the same location '/tmp/external/p1' as table tab1.
    // Create tab2_role, and grant it with select permission on table tab2 to user_group1.
    stmt.execute("create table tab2 (s string) partitioned by (month int) location '/tmp/external/p1'");
    stmt.execute("create role tab2_role");
    stmt.execute("grant select on table tab2 to role tab2_role");
    stmt.execute("grant role tab2_role to group " + StaticUserGroup.USERGROUP1);

    // When two tables pointing to the same location, ACLS should have union (no duplicates)
    // of both rules.
    verifyGroupPermOnPath("/tmp/external/p1", FsAction.ALL, StaticUserGroup.USERGROUP1, true);

    // When drop table tab1, ACLs of tab2 still remain.
    stmt.execute("DROP TABLE IF EXISTS tab1");
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode
    verifyGroupPermOnPath("/tmp/external/p1", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP1, true);

    stmt.close();
    conn.close();
  }

  /**
   * SENTRY-1002:
   * Ensure the paths with no scheme will not cause NPE during paths update.
   */
   @Test
   public void testMissingScheme() throws Throwable {
     // In the local test environment, EXTERNAL_SENTRY_SERVICE is false,
     // set the default URI scheme to be hdfs.
     boolean testConfOff = Boolean.valueOf(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));
     if (!testConfOff) {
       PathsUpdate.setDefaultScheme("hdfs");
     }
     String dbName = "db1";
     String tblName = "tab1";
     dbNames = new String[]{dbName};
     roles = new String[]{"admin_role"};
     admin = StaticUserGroup.ADMIN1;

     Connection conn;
     Statement stmt;

     conn = hiveServer2.createConnection("hive", "hive");
     stmt = conn.createStatement();
     stmt.execute("create role admin_role");
     stmt.execute("grant all on server server1 to role admin_role");
     stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
     stmt.close();
     conn.close();

     conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
     stmt = conn.createStatement();
     stmt.execute("create database " + dbName);
     stmt.execute("create external table " + dbName + "." + tblName + "(s string) location '/tmp/external/p1'");

     // Deep copy of table tab1
     Table tbCopy = hmsClient.getTable(dbName, tblName);

     // Change the location of the table to strip the scheme.
     StorageDescriptor sd = hmsClient.getTable(dbName, tblName).getSd();
     sd.setLocation("/tmp/external");
     tbCopy.setSd(sd);

     // Alter table tab1 to be tbCopy which is at scheme-less location.
     // And the corresponding path will be updated to sentry server.
     hmsClient.alter_table(dbName, "tab1", tbCopy);

     // Remove the checking for the location of the table. The HMS will never return scheme-less
     // URI locations anymore. However, if any NPE being triggered in future because of any changes,
     // the test case will cover it and capture it.
     // i.e. hdfs://<localhost>/tmp/external (location with scheme)
     //      /tmp/external                   (location without scheme)
     // Assert.assertEquals("/tmp/external", hmsClient.getTable(dbName, tblName).getSd().getLocation());

     verifyGroupPermOnPath("/tmp/external", FsAction.ALL, StaticUserGroup.HIVE, true);

     stmt.close();
     conn.close();
   }

  @Test
  public void testRenameHivePartitions() throws Throwable {
    final String dbName = "db1";
    final String tblName = "tab1";
    final String newTblName = "tab2";
    final String patName = "pat1";
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    try (Connection conn = hiveServer2.createConnection("hive", "hive");
      Statement stmt = conn.createStatement()) {

      stmt.execute("create role admin_role");
      stmt.execute("grant all on server server1 to role admin_role");
      stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    }

    try (Connection conn = hiveServer2.createConnection(
        StaticUserGroup.ADMIN1, StaticUserGroup.ADMINGROUP);
         Statement stmt = conn.createStatement()) {
      stmt.execute("create database " + dbName);
      stmt.execute("use " + dbName);
      stmt.execute("create table " + tblName + " (s string) partitioned by (month int) ");
      String tblPath = Paths.get("/user/hive/warehouse", dbName + ".db", tblName).toString();
      String patPath = Paths.get(tblPath, patName).toString();
      stmt.execute("alter table " + tblName + " add partition (month = 1) location '" +
          patPath + "'");

      stmt.execute("grant all on TABLE " + tblName + " to role admin_role");
      stmt.execute("create role user_role");
      stmt.execute("grant insert on table " + tblName + " to role user_role");
      stmt.execute("grant role user_role to group " + StaticUserGroup.USERGROUP1);

      // Rename the hive table
      stmt.execute("alter table " + tblName + " rename to " + newTblName);

      // Verify that the permissions are preserved.
      String newTblPath = Paths.get("/user/hive/warehouse", dbName + ".db", newTblName).toString();
      verifyGroupPermOnAllSubDirs(newTblPath, FsAction.ALL, StaticUserGroup.HIVE, true);
      verifyGroupPermOnAllSubDirs(newTblPath, FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);
      String newPatPath = new File(newTblPath, patName).toString();
      verifyGroupPermOnPath(newPatPath, FsAction.ALL, StaticUserGroup.ADMINGROUP, true);
      verifyGroupPermOnPath(newPatPath, FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);
    }
  }
}
