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

package org.apache.access.tests.e2e;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSandboxOps {
  private EndToEndTestContext context;

  @Before
  public void setup() throws Exception {
    context = new EndToEndTestContext(new HashMap<String, String>());
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  @AfterClass
  public static void shutDown() {
    EndToEndTestContext.shutdown();
  }

  /**
   * 2.8 admin user create two database, DB_1, DB_2 admin grant all to USER_1,
   * USER_2 on DB_1, admin grant all to USER_1's group, USER_2's group on DB_2
   * positive test case: USER_1, USER_2 has ALL privilege on both DB_1 and DB_2
   * negative test case: USER_1, USER_2 don't have ALL privilege on SERVER
   */
  @Test
  public void testDbPrivileges() throws Exception {
    // edit policy file
    String testPolicies[] = { "[groups]", "admin_group = admin_role",
        "user_group  = db1_all,db2_all", "[roles]",
        "db1_all = server=server1:db=db1:*",
        "db2_all = server=server1:db=db2:*", "admin_role = server=server1:*",
        "[users]", "user1 = user_group", "user2 = user_group",
        "admin = admin_group" };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    for (String dbName : new String[] { "db1", "db2" }) {
      adminStmt.execute("use default");
      adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
      adminStmt.execute("CREATE DATABASE " + dbName);
    }
    context.close();

    for (String user : new String[] { "user1", "user2" }) {
      for (String dbName : new String[] { "db1", "db2" }) {
        Connection userConn = context.createConnection(user, "foo");
        String tabName = user + "_tab1";
        Statement userStmt = context.createStatement(userConn);
        // Positive case: test user1 and user2 has permissions to access db1 and
        // db2
        userStmt.execute("use " + dbName);
        userStmt.execute("create table " + tabName + " (id int)");
        userStmt.execute("select * from " + tabName);
        context.close();
      }
    }

    // cleaup
    // create dbs
    adminCon = context.createConnection("admin", "foo");
    adminStmt = context.createStatement(adminCon);
    for (String dbName : new String[] { "db1", "db2" }) {
      adminStmt.execute("use default");
      adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    }
    context.close();

  }

  /**
   * Test Case 2.10 negative test case: non-admin user try to create a database
   * should fail
   */
  @Test
  public void testNegativeDbPrivileges() throws Exception {
    // edit policy file
    String testPolicies[] = { "[groups]", "admin_group = admin_role",
        "user_group  = db1_all,db2_all", "[roles]",
        "db1_all = server=server1:db=db1:*",
        "db2_all = server=server1:db=db2:*", "admin_role = server=server1:*",
        "[users]", "user1 = user_group", "user2 = user_group",
        "admin = admin_group" };
    context.makeNewPolicy(testPolicies);

    for (String user : new String[] { "user1", "user2" }) {
      for (String dbName : new String[] { "db1", "db2" }) {
        Connection userConn = context.createConnection(user, "foo");
        Statement userStmt = context.createStatement(userConn);
        // negative case, user1 and user2 don't have server level access
        try {
          userStmt.execute("create database badDb");
          assertTrue("Create db shouldn't pass for user " + user, false);
        } catch (SQLException e) {
           context.verifyAuthzException(e);
        }
        context.close();
      }
    }
  }

  /**
   * Test Case 2.11 admin user create a new database DB_1 and grant ALL to
   * himself on DB_1 should work
   */
  @Test
  public void testAdminDbPrivileges() throws Exception {
    // edit policy file
    String testPolicies[] = { "[groups]", "admin_group = admin_role",
        "[roles]", "admin_role = server=server1:*", "[users]",
        "admin = admin_group" };
    context.makeNewPolicy(testPolicies);

    // Admin should be able to create new databases
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    String dbName = "db1";
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);

    // access the new databases
    adminStmt.execute("use " + dbName);
    String tabName = "admin_tab1";
    adminStmt.execute("create table " + tabName + "(c1 string)");
    adminStmt.execute("load data local inpath '/etc/passwd' into table "
        + tabName);
    adminStmt.execute("select * from " + tabName);

    // cleanup
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE " + dbName + " CASCADE");

    context.close();
  }

  /**
   * Test Case 2.14 admin user create a new database DB_1 create TABLE_1 in DB_1
   * admin user grant INSERT to USER_1's group on TABLE_1 negative test case:
   * USER_1 try to do following on TABLE_1 will fail: --explain --analyze
   * --describe --describe function --show columns --show table status --show
   * table properties --show create table --show partitions --show indexes
   * --select * from TABLE_1.
   */
  @Test
  public void testNegativeUserPrivileges() throws Exception {
    // edit policy file
    String testPolicies[] = { "[groups]", "admin_group = admin_role",
        "user_group  = db1_tab1_insert, db1_tab2_all", "[roles]",
        "db1_tab2_all = server=server1:db=db1:table=table_2:*",
        "db1_tab1_insert = server=server1:db=db1:table=table_1:insert",
        "admin_role = server=server1:*", "[users]", "user3 = user_group",
        "admin = admin_group" };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    String dbName = "db1";
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);
    adminStmt.execute("create table table_1 (id int)");
    context.close();

    Connection userConn = context.createConnection("user3", "foo");
    Statement userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);

    // since user3 only has insert privilege, all other statements should fail
    try {
      userStmt.execute("select * from table_1");
      assertTrue("select shouldn't pass for user user3 ", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("alter table table_1 add columns (name string)");
      assertTrue("alter table shouldn't pass for user user3 ", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // TODO: Metadata operations are not handled yet ...

    context.close();
  }

  /**
   * Test Case 2.16 admin user create a new database DB_1 create TABLE_1 and
   * TABLE_2 (same schema) in DB_1 admin user grant SELECT, INSERT to USER_1's
   * group on TABLE_2 negative test case: USER_1 try to do following on TABLE_1
   * will fail: --insert overwrite TABLE_2 select * from TABLE_1
   */
  @Test
  public void testNegativeUserDMLPrivileges() throws Exception {
    String testPolicies[] = { "[groups]", "admin_group = admin_role",
        "user_group  = db1_tab2_all", "[roles]",
        "db1_tab2_all = server=server1:db=db1:table=table_2:*",
        "admin_role = server=server1:*", "[users]", "user3 = user_group",
        "admin = admin_group" };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    String dbName = "db1";
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);
    adminStmt.execute("create table table_1 (id int)");
    adminStmt.execute("create table table_2 (id int)");
    context.close();

    Connection userConn = context.createConnection("user3", "foo");
    Statement userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);

    // user3 doesn't have select priviledge on table_1, so insert/select should fail
    try {
      userStmt.execute("insert overwrite table table_2 select * from table_1");
      assertTrue("select shouldn't pass for user user3 ", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("insert overwrite directory '/tmp' select * from table_1");
      assertTrue("select shouldn't pass for user user3 ", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
  }

  /**
   * Test Case 2.17 Execution steps a) Admin user creates a new database DB_1,
   * b) Admin user grants ALL on DB_1 to group GROUP_1 c) User from GROUP_1
   * creates table TAB_1, TAB_2 in DB_1 d) Admin user grants SELECT on TAB_1 to
   * group GROUP_2
   * 
   * 1) verify users from GROUP_2 have only SELECT privileges on TAB_1. They
   * shouldn't be able to perform any operation other than those listed as
   * requiring SELECT in the privilege model.
   * 
   * 2) verify users from GROUP_2 can't perform queries involving join between
   * TAB_1 and TAB_2.
   * 
   * 3) verify users from GROUP_1 can't perform operations requiring ALL @
   * SERVER scope. Refer to list
   */
  @Test
  public void testNegUserPrivilegesAll() throws Exception {
    String testPolicies[] = { 
        "[groups]", 
        "admin_group = admin_role",
        "user_group1 = db1_all",
        "user_group2 = db1_tab1_select",
        "[roles]",
        "db1_all = server=server1:db=db1:*",
        "db1_tab1_select = server=server1:db=db1:table=table_1:select",
        "admin_role = server=server1:*",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        "admin = admin_group" 
        };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    String dbName = "db1";
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);

    adminStmt.execute("create table table_1 (name string)");
    adminStmt
        .execute("load data local inpath '/etc/passwd' into table table_1");
    adminStmt.execute("create table table_2 (name string)");
    adminStmt
        .execute("load data local inpath '/etc/passwd' into table table_2");
    adminStmt.execute("create view v1 AS select * from table_1");
    adminStmt.execute("create table table_part_1 (name string) PARTITIONED BY (year INT)");
    context.close();

    Connection userConn = context.createConnection("user2", "foo");
    Statement userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);

    try {
      userStmt.execute("alter table table_2 add columns (id int)");
      assertTrue("alter shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("drop database " + dbName);
      assertTrue("drop db shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("create view v2 as select * from table_2");
      assertTrue("create view shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt
          .execute("CREATE INDEX x ON TABLE table_1(name) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'");
      assertTrue("create index shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("CREATE TEMPORARY FUNCTION strip AS '"
          + LocalGroupResourceAuthorizationProvider.class.getName() + "'");
      assertTrue("create function shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("create tabl c_tab_2 as select * from table_2");
      assertTrue("CTAS shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("ALTER DATABASE " + dbName
          + " SET DBPROPERTIES ('foo' = 'bar')");
      assertTrue("ALTER DATABASE shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("ALTER VIEW v1 SET TBLPROPERTIES ('foo' = 'bar')");
      assertTrue("ALTER DATABASE shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      adminStmt.execute("drop table table_1 (name string)");
      assertTrue("drop table shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      adminStmt.execute("ALTER TABLE table_part_1 PARTITION (year = 2011) SET LOCATION '/etc'");
      assertTrue("ALTER TABLE set location shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // bad ones ..
    try {
      userStmt.execute("DROP VIEW IF EXISTS v1");
      assertTrue("DROP VIEW shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      adminStmt.execute("create table table_5 (name string)");
      assertTrue("create table shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      adminStmt.execute("ALTER TABLE table_1  RENAME TO table_99");
      assertTrue("ALTER TABLE rename shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("insert overwrite table table_2 select * from table_1");
      assertTrue("insert shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      adminStmt.execute("ALTER TABLE table_part_1 ADD IF NOT EXISTS PARTITION (year = 2011)");
      assertTrue("ALTER TABLE add partition shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    
    context.close();
  }

}
