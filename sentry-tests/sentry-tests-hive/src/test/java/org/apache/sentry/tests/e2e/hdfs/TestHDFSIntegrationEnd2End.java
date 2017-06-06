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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * This test class includes all HDFS Sync smoke tests
 */
public class TestHDFSIntegrationEnd2End extends TestHDFSIntegrationBase {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestHDFSIntegrationEnd2End.class);

  private static String adminRole = "admin_role";

  @Test
  public void testEnd2End() throws Throwable {
    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{"db1"};
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
    stmt.execute("alter table p1 add partition (month=1, day=2)");
    stmt.execute("alter table p1 add partition (month=2, day=1)");
    stmt.execute("alter table p1 add partition (month=2, day=2)");

    // db privileges
    stmt.execute("create database db5");
    stmt.execute("create role db_role");
    stmt.execute("create role tab_role");
    stmt.execute("grant role db_role to group hbase");
    stmt.execute("grant role tab_role to group flume");
    stmt.execute("create table db5.p2(id int)");

    stmt.execute("create role p1_admin");
    stmt.execute("grant role p1_admin to group hbase");

    // Verify default db is inaccessible initially
    verifyOnAllSubDirs("/user/hive/warehouse", null, "hbase", false);

    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    stmt.execute("grant all on database db5 to role db_role");
    stmt.execute("use db5");
    stmt.execute("grant all on table p2 to role tab_role");
    stmt.execute("use default");
    verifyOnAllSubDirs("/user/hive/warehouse/db5.db", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db5.db/p2", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db5.db/p2", FsAction.ALL, "flume", true);
    verifyOnPath("/user/hive/warehouse/db5.db", FsAction.ALL, "flume", false);

    loadData(stmt);

    verifyHDFSandMR(stmt);

    // Verify default db is STILL inaccessible after grants but tables are fine
    verifyOnPath("/user/hive/warehouse", null, "hbase", false);
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.READ_EXECUTE,
        "hbase", true);

    adminUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        // Simulate hdfs dfs -setfacl -m <aclantry> <path>
        AclStatus existing =
            miniDFS.getFileSystem()
                .getAclStatus(new Path("/user/hive/warehouse/p1"));
        ArrayList<AclEntry> newEntries =
            new ArrayList<AclEntry>(existing.getEntries());
        newEntries.add(AclEntry.parseAclEntry("user::---", true));
        newEntries.add(AclEntry.parseAclEntry("group:bla:rwx", true));
        newEntries.add(AclEntry.parseAclEntry("other::---", true));
        miniDFS.getFileSystem().setAcl(new Path("/user/hive/warehouse/p1"),
            newEntries);
        return null;
      }
    });

    stmt.execute("revoke select on table p1 from role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    // Verify default db grants work
    stmt.execute("grant select on database default to role p1_admin");
    verifyOnPath("/user/hive/warehouse", FsAction.READ_EXECUTE, "hbase", true);

    // Verify default db grants are propagated to the tables
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.READ_EXECUTE,
        "hbase", true);

    // Verify default db revokes work
    stmt.execute("revoke select on database default from role p1_admin");
    verifyOnPath("/user/hive/warehouse", null, "hbase", false);
    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    stmt.execute("grant all on table p1 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.ALL, "hbase", true);

    stmt.execute("revoke select on table p1 from role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.WRITE_EXECUTE, "hbase", true);


    // Verify table rename works when locations are also changed
    stmt.execute("alter table p1 rename to p3");
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);
    //This is true as parent hive object's (p3) ACLS are used.
    verifyOnAllSubDirs("/user/hive/warehouse/p3/month=1/day=1", FsAction.WRITE_EXECUTE, "hbase", true);

    // Verify when oldName == newName and oldPath != newPath
    stmt.execute("alter table p3 partition (month=1, day=1) rename to partition (month=1, day=3)");
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/p3/month=1/day=3", FsAction.WRITE_EXECUTE, "hbase", true);

    // Test DB case insensitivity
    stmt.execute("create database extdb");
    stmt.execute("grant all on database ExtDb to role p1_admin");
    writeToPath("/tmp/external/ext100", 5, "foo", "bar");
    writeToPath("/tmp/external/ext101", 5, "foo", "bar");
    stmt.execute("use extdb");
    stmt.execute(
        "create table ext100 (s string) location \'/tmp/external/ext100\'");
    verifyQuery(stmt, "ext100", 5);
    verifyOnAllSubDirs("/tmp/external/ext100", FsAction.ALL, "hbase", true);
    stmt.execute("use default");

    stmt.execute("use EXTDB");
    stmt.execute(
        "create table ext101 (s string) location \'/tmp/external/ext101\'");
    verifyQuery(stmt, "ext101", 5);
    verifyOnAllSubDirs("/tmp/external/ext101", FsAction.ALL, "hbase", true);

    // Test table case insensitivity
    stmt.execute("grant all on table exT100 to role tab_role");
    verifyOnAllSubDirs("/tmp/external/ext100", FsAction.ALL, "flume", true);

    stmt.execute("use default");

    //TODO: SENTRY-795: HDFS permissions do not sync when Sentry restarts in HA mode.
    if(!testSentryHA) {
      long beforeStop = System.currentTimeMillis();
      sentryServer.stopAll();
      long timeTakenForStopMs = System.currentTimeMillis() - beforeStop;
      LOGGER.info("Time taken for Sentry server stop: " + timeTakenForStopMs);

      // Verify that Sentry permission are still enforced for the "stale" period only if stop did not take too long
      if(timeTakenForStopMs < STALE_THRESHOLD) {
        verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);
        Thread.sleep((STALE_THRESHOLD - timeTakenForStopMs));
      } else {
        LOGGER.warn("Sentry server stop took too long");
      }

      // Verify that Sentry permission are NOT enforced AFTER "stale" period
      verifyOnAllSubDirs("/user/hive/warehouse/p3", null, "hbase", false);

      sentryServer.startAll();
    }

    // Verify that After Sentry restart permissions are re-enforced
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);

    // Create new table and verify everything is fine after restart...
    stmt.execute("create table p2 (s string) partitioned by (month int, day int)");
    stmt.execute("alter table p2 add partition (month=1, day=1)");
    stmt.execute("alter table p2 add partition (month=1, day=2)");
    stmt.execute("alter table p2 add partition (month=2, day=1)");
    stmt.execute("alter table p2 add partition (month=2, day=2)");

    verifyOnAllSubDirs("/user/hive/warehouse/p2", null, "hbase", false);

    stmt.execute("grant select on table p2 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.execute("grant select on table p2 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    // Create external table
    writeToPath("/tmp/external/ext1", 5, "foo", "bar");

    stmt.execute("create table ext1 (s string) location \'/tmp/external/ext1\'");
    verifyQuery(stmt, "ext1", 5);

    // Ensure existing group permissions are never returned..
    verifyOnAllSubDirs("/tmp/external/ext1", null, "bar", false);
    verifyOnAllSubDirs("/tmp/external/ext1", null, "hbase", false);

    stmt.execute("grant all on table ext1 to role p1_admin");
    verifyOnAllSubDirs("/tmp/external/ext1", FsAction.ALL, "hbase", true);

    stmt.execute("revoke select on table ext1 from role p1_admin");
    verifyOnAllSubDirs("/tmp/external/ext1", FsAction.WRITE_EXECUTE, "hbase", true);

    // Verify database operations works correctly
    stmt.execute("create database db1");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db", null, "hbase", false);

    stmt.execute("create table db1.tbl1 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", null, "hbase", false);
    stmt.execute("create table db1.tbl2 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", null, "hbase", false);

    // Verify default db grants do not affect other dbs
    stmt.execute("grant all on database default to role p1_admin");
    verifyOnPath("/user/hive/warehouse", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db", null, "hbase", false);

    // Verify table rename works
    stmt.execute("create table q1 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/q1", FsAction.ALL, "hbase", true);
    stmt.execute("alter table q1 rename to q2");
    verifyOnAllSubDirs("/user/hive/warehouse/q2", FsAction.ALL, "hbase", true);

    // Verify table GRANTS do not trump db GRANTS
    stmt.execute("grant select on table q2 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/q2", FsAction.ALL, "hbase", true);

    stmt.execute("create table q3 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/q3", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/q2", FsAction.ALL, "hbase", true);

    // Verify db privileges are propagated to tables
    stmt.execute("grant select on database db1 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.READ_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    // Verify default db revokes do not affect other dbs
    stmt.execute("revoke all on database default from role p1_admin");
    verifyOnPath("/user/hive/warehouse", null, "hbase", false);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.READ_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.execute("use db1");
    stmt.execute("grant all on table tbl1 to role p1_admin");

    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    // Verify recursive revoke
    stmt.execute("revoke select on database db1 from role p1_admin");

    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.WRITE_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", null, "hbase", false);

    // Verify cleanup..
    stmt.execute("drop table tbl1");
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db/tbl1")));

    stmt.execute("drop table tbl2");
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db/tbl2")));

    stmt.execute("use default");
    stmt.execute("drop database db1");
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db")));

    // START : Verify external table set location..
    writeToPath("/tmp/external/tables/ext2_before/i=1", 5, "foo", "bar");
    writeToPath("/tmp/external/tables/ext2_before/i=2", 5, "foo", "bar");

    stmt.execute("create external table ext2 (s string) partitioned by (i int) location \'/tmp/external/tables/ext2_before\'");
    stmt.execute("alter table ext2 add partition (i=1)");
    stmt.execute("alter table ext2 add partition (i=2)");
    verifyQuery(stmt, "ext2", 10);
    verifyOnAllSubDirs("/tmp/external/tables/ext2_before", null, "hbase", false);
    stmt.execute("grant all on table ext2 to role p1_admin");
    verifyOnPath("/tmp/external/tables/ext2_before", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2/stuff.txt", FsAction.ALL, "hbase", true);

    writeToPath("/tmp/external/tables/ext2_after/i=1", 6, "foo", "bar");
    writeToPath("/tmp/external/tables/ext2_after/i=2", 6, "foo", "bar");

    stmt.execute("alter table ext2 set location \'hdfs:///tmp/external/tables/ext2_after\'");
    // Even though table location is altered, partition location is still old (still 10 rows)
    verifyQuery(stmt, "ext2", 10);
    // You have to explicitly alter partition location..
    verifyOnPath("/tmp/external/tables/ext2_before", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2/stuff.txt", FsAction.ALL, "hbase", true);

    stmt.execute("alter table ext2 partition (i=1) set location \'hdfs:///tmp/external/tables/ext2_after/i=1\'");
    stmt.execute("alter table ext2 partition (i=2) set location \'hdfs:///tmp/external/tables/ext2_after/i=2\'");
    // Now that partition location is altered, it picks up new data (12 rows instead of 10)
    verifyQuery(stmt, "ext2", 12);

    verifyOnPath("/tmp/external/tables/ext2_before", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1/stuff.txt", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2/stuff.txt", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_after", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2/stuff.txt", FsAction.ALL, "hbase", true);
    // END : Verify external table set location..

    //Create a new table partition on the existing partition
    stmt.execute("create table tmp (s string) partitioned by (i int)");
    stmt.execute("alter table tmp add partition (i=1)");
    stmt.execute("alter table tmp partition (i=1) set location \'hdfs:///tmp/external/tables/ext2_after/i=1\'");
    stmt.execute("grant all on table tmp to role tab_role");
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "flume", true);

    //Alter table rename of external table => oldName != newName, oldPath == newPath
    stmt.execute("alter table ext2 rename to ext3");
    //Verify all original paths still have the privileges
    verifyOnPath("/tmp/external/tables/ext2_after", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "flume", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2/stuff.txt", FsAction.ALL, "hbase", true);


    // Restart HDFS to verify if things are fine after re-start..

    // TODO : this is currently commented out since miniDFS.restartNameNode() does
    //        not work corectly on the version of hadoop sentry depends on
    //        This has been verified to work on a real cluster.
    //        Once miniDFS is fixed, this should be uncommented..
    // miniDFS.shutdown();
    // miniDFS.restartNameNode(true);
    // miniDFS.waitActive();
    // verifyOnPath("/tmp/external/tables/ext2_after", FsAction.ALL, "hbase", true);
    // verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.close();
    conn.close();
  }

  //SENTRY-780
  @Test
  public void testViews() throws Throwable {
    LOGGER.info("testViews starts");
    String dbName= "db1";

    tmpHDFSDir = new Path("/tmp/external");
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

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    try {
      stmt.execute("create database " + dbName);
      stmt.execute("create table test(a string)");
      stmt.execute("create view testView as select * from test");
      stmt.execute("create or replace view testView as select * from test");
      stmt.execute("drop view testView");
    } catch(Exception s) {
      throw s;
    }

    stmt.close();
    conn.close();
    LOGGER.info("testViews ends");
  }

  /*
TODO:SENTRY-819
*/
  @Test
  public void testAllColumn() throws Throwable {
    LOGGER.info("testAllColumn starts");
    String dbName = "db2";
    String userRole = "col1_role";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", userRole};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role with grant option");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create table p1 (c1 string, c2 string) partitioned by (month int, day int)");
    stmt.execute("alter table p1 add partition (month=1, day=1)");
    loadDataTwoCols(stmt);

    stmt.execute("create role " + userRole);
    stmt.execute("grant select(c1,c2) on p1 to role " + userRole);
    stmt.execute("grant role " + userRole + " to group "+ StaticUserGroup.USERGROUP1);
    Thread.sleep(100);

    //User with privileges on all columns of the data cannot still read the HDFS files
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", null, StaticUserGroup.USERGROUP1, false);

    stmt.close();
    conn.close();
    LOGGER.info("testAllColumn ends");
  }

  @Test
  public void testColumnPrivileges() throws Throwable {
    LOGGER.info("testColumnPrivileges starts");
    String dbName = "db2";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
      dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab_role", "db_role", "col_role"};
    roles = new String[]{adminRole, "tab_role", "db_role", "col_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role with grant option");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use "+ dbName);
    stmt.execute("create table p1 (s string) partitioned by (month int, day int)");
    stmt.execute("alter table p1 add partition (month=1, day=1)");
    stmt.execute("alter table p1 add partition (month=1, day=2)");
    stmt.execute("alter table p1 add partition (month=2, day=1)");
    stmt.execute("alter table p1 add partition (month=2, day=2)");
    loadData(stmt);

    stmt.execute("create role db_role");
    stmt.execute("grant select on database " + dbName + " to role db_role");
    stmt.execute("create role tab_role");
    stmt.execute("grant select on p1 to role tab_role");
    stmt.execute("create role col_role");
    stmt.execute("grant select(s) on p1 to role col_role");

    stmt.execute("grant role col_role to group "+ StaticUserGroup.USERGROUP1);

    stmt.execute("grant role tab_role to group "+ StaticUserGroup.USERGROUP2);
    stmt.execute("grant role col_role to group "+ StaticUserGroup.USERGROUP2);

    stmt.execute("grant role db_role to group "+ StaticUserGroup.USERGROUP3);
    stmt.execute("grant role col_role to group "+ StaticUserGroup.USERGROUP3);

    stmt.execute("grant role col_role to group " + StaticUserGroup.ADMINGROUP);

    Thread.sleep(WAIT_BEFORE_TESTVERIFY);//Wait till sentry cache is updated in Namenode

    //User with just column level privileges cannot read HDFS
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", null, StaticUserGroup.USERGROUP1, false);

    //User with permissions on table and column can read HDFS file
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);

    //User with permissions on db and column can read HDFS file
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP3, true);

    //User with permissions on server and column cannot read HDFS file
    //TODO:SENTRY-751
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", null, StaticUserGroup.ADMINGROUP, false);

    stmt.close();
    conn.close();
    LOGGER.info("testColumnPrivileges ends");
  }


  @Ignore("SENTRY-546")
  @Test
  public void testExternalTable() throws Throwable {
    String dbName = "db2";

    tmpHDFSDir = new Path("/tmp/external");
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

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("create external table tab1(a int) location '/tmp/external/tab1_loc'");
    verifyOnAllSubDirs("/tmp/external/tab1_loc", FsAction.ALL, StaticUserGroup.ADMINGROUP, true);

    stmt.close();
    conn.close();
  }


}
