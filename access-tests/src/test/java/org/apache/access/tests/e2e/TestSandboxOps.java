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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestSandboxOps {
  private EndToEndTestContext context;
  private final String dataFileDir = "src/test/resources";
  private Path dataFilePath = new Path(dataFileDir, "kv1.dat");
  private String EXTERNAL_HDFS_DIR = "hdfs://namenode:9000/tmp/externalDir";
  private String NONE_EXISTS_DIR = "hdfs://namenode:9000/tmp/nonExists";

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
    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group  = db1_all,db2_all",
        "[roles]",
        "db1_all = server=server1:db=db1:*",
        "db2_all = server=server1:db=db2:*",
        "admin_role = server=server1:*",
        "[users]",
        "user1 = user_group",
        "user2 = user_group",
        "admin = admin_group"
        };
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
    adminStmt.execute("ALTER TABLE table_part_1 ADD PARTITION (year = 2012)");

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
      userStmt.execute("create table c_tab_2 as select * from table_2");
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
      userStmt.execute("drop table table_1");
      assertTrue("drop table shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("DROP VIEW IF EXISTS v1");
      assertTrue("DROP VIEW shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("create table table_5 (name string)");
      assertTrue("create table shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("ALTER TABLE table_1  RENAME TO table_99");
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
      userStmt.execute("ALTER TABLE table_part_1 ADD IF NOT EXISTS PARTITION (year = 2012)");
      assertTrue("ALTER TABLE add partition shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("ALTER TABLE table_part_1 PARTITION (year = 2012) SET LOCATION '/etc'");
      assertTrue("ALTER TABLE set location shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }


    context.close();
  }

  /**
   * Steps:
   * 1. admin user create databases, DB_1 and DB_2, no table or other
   * object in database
   * 2. admin grant all to USER_1's group on DB_1 and DB_2
   *   positive test case:
   *     a)USER_1 has the privilege to create table, load data,
   *     drop table, create view, insert more data on both databases
   *     b) USER_1 can switch between DB_1 and DB_2 without
   *     exception negative test case:
   *     c) USER_1 cannot drop database
   * 3. admin remove all to group1 on DB_2
   *   positive test case:
   *     d) USER_1 has the privilege to create view on tables in DB_1
   *   negative test case:
   *     e) USER_1 cannot create view on tables in DB_1 that select
   *     from tables in DB_2
   * 4. admin grant select to group1 on DB_2.ta_2
   *   positive test case:
   *     f) USER_1 has the privilege to create view to select from
   *     DB_1.tb_1 and DB_2.tb_2
   *   negative test case:
   *     g) USER_1 cannot create view to select from DB_1.tb_1
   *     and DB_2.tb_3
   * @throws Exception
   */
  @Test
  public void testSandboxOpt9() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("group1 = all_db2", "groups");
    editor.addPolicy("admin = server=server1:*", "roles");
    editor.addPolicy("all_db1 = server=server1:db=db_1:*", "roles");
    editor.addPolicy("all_db2 = server=server1:db=db_2:*", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    String dbName1 = "db_1";
    String dbName2 = "db_2";
    String tableName1 = "tb_1";
    String tableName2 = "tb_2";
    String tableName3 = "tb_3";
    String viewName1 = "view_1";
    String viewName2 = "view_2";
    String viewName3 = "view_3";

    // admin create two databases
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1);
    statement.execute("DROP DATABASE IF EXISTS " + dbName2);
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("CREATE DATABASE " + dbName2);
    statement.close();
    connection.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);

    // a
    assertEquals("user1 should be able to switch to " + dbName1,
        statement.execute("USE " + dbName1));
    assertEquals("user1 should be able to drop table " + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + tableName1));
    assertEquals(
        "user1 should be able to create table " + tableName1,
        statement.execute("create table " + tableName1
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to load data into table " + tableName1,
        statement.execute("load data local inpath '" + dataFilePath.toString()
            + "' into table " + tableName1));
    assertEquals("user1 should be able to drop view " + viewName1,
        statement.execute("DROP VIEW IF EXISTS " + viewName1));
    assertEquals(
        "user1 should be able to create view " + viewName1,
        statement.execute("CREATE VIEW " + viewName1
            + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));

    // b
    assertEquals("user1 should be able to switch between database",
        statement.execute("USE " + dbName2));
    assertEquals("user1 should be able to drop table " + tableName2,
        statement.execute("DROP TABLE IF EXISTS " + tableName2));
    assertEquals(
        "user1 should be able to create table " + tableName2,
        statement.execute("create table " + tableName2
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to load data into table " + tableName2,
        statement.execute("load data local inpath '" + dataFilePath.toString()
            + "' into table " + tableName2));
    assertEquals("user1 should be able to drop table " + tableName3,
        statement.execute("DROP TABLE IF EXISTS " + tableName3));
    assertEquals(
        "user1 should be able to create table " + tableName3,
        statement.execute("create table " + tableName3
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to load data into table " + tableName3,
        statement.execute("load data local inpath '" + dataFilePath.toString()
            + "' into table " + tableName3));

    // c
    assertFalse("user1 shouldn't be able to drop database",
        statement.execute("DROP DATABASE IF EXISTS " + dbName1));
    assertFalse("user1 shouldn't be able to drop database",
        statement.execute("DROP DATABASE IF EXISTS " + dbName2));

    // d
    assertEquals("user1 should be able to switch between database",
        statement.execute("USE " + dbName1));
    editor.removePolicy("group1 = all_db2");
    editor.removePolicy("all_db2 = server=server1:db=db_2:*");
    assertEquals("user1 should be able to drop table " + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + tableName1));
    assertEquals(
        "user1 should be able to create table " + tableName1,
        statement.execute("create table " + tableName1
            + " (under_col int comment 'the under column', value string)"));

    // e
    assertEquals("user1 should be able to drop view " + viewName2,
        statement.execute("DROP VIEW IF EXISTS " + viewName2));
    assertFalse(
        "user1 should not be able to create based on table " + tableName2,
        statement.execute("CREATE VIEW " + viewName2
            + " (value) AS SELECT value from " + tableName2 + " LIMIT 10"));

    // f
    editor.addPolicy("group1 = select_tb2", "groups");
    editor.addPolicy("select_tb2 = server=server1:db=db_2:tb=tb_2:select",
        "roles");
    assertEquals("user1 should be able to drop view " + viewName2,
        statement.execute("DROP VIEW IF EXISTS " + viewName2));
    assertEquals(
        "user1 should be able to create based on table " + tableName2,
        statement.execute("CREATE VIEW " + viewName2
            + " (value) AS SELECT value from " + tableName2 + " LIMIT 10"));

    // g
    assertEquals("user1 should be able to drop view " + viewName3,
        statement.execute("DROP VIEW IF EXISTS " + viewName3));
    assertFalse(
        "user1 should not be able to create based on table " + tableName3,
        statement.execute("CREATE VIEW " + viewName3
            + " (value) AS SELECT value from " + tableName3 + " LIMIT 10"));

    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin user create a new database DB_1
   * 2. admin create TABLE_1 in DB_1
   * 3. admin create INDEX_1 for COLUMN_1 in TABLE_1 in DB_1
   * 4. admin user grant INSERT and SELECT to USER_1's group on TABLE_1
   * 5. admin user doesn't grant SELECT to USER_1's group on INDEX_1
   *   negative test case:
   *     a) USER_1 try to SELECT * FROM TABLE_1 WHERE COLUMN_1 == ...
   *     should NOT work
   *     b) USER_1 should not be able to check the list of view or
   *     index in DB_1
   * @throws Exception
   */
  @Test
  public void testSandboxOpt13() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1:*", "roles");
    editor.addPolicy("admin1 = admin", "users");

    // verify by SQL
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String indexName1 = "index_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1);
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + tableName1);
    statement.execute("create table " + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName1);
    statement.execute("DROP INDEX IF EXISTS " + indexName1 + " ON "
        + tableName1);
    statement.execute("CREATE INDEX " + indexName1 + " ON TABLE " + tableName1
        + " (under_col) as 'COMPACT' WITH DEFERRED REBUILD");

    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("insert_tb1 = server=server1:db=db_1:insert", "roles");
    editor.addPolicy("select_tb1 = server=server1:db=db_1:select", "roles");
    editor.addPolicy("user1 = group1", "users");

    // a
    assertFalse(
        "user1 should not be able to issue this query",
        statement.execute("SELECT * FROM " + tableName1
            + " WHERE under_col > 5"));
    // b
    assertFalse("user1 should not be able to see all indexes",
        statement.execute("SHOW INDEXES"));
  }

  /**
   * Steps:
   * 1. Admin user creates a new database DB_1
   * 2. Admin user grants ALL on DB_1 to group GROUP_1
   * 3. User from GROUP_1 creates table TAB_1, TAB_2 in DB_1
   * 4. Admin user grants SELECT/INSERT on TAB_1 to group GROUP_2
   *     a) verify users from GROUP_2 have only SELECT/INSERT
   *     privileges on TAB_1. They shouldn't be able to perform
   *     any operation other than those listed as
   *     requiring SELECT in the privilege model.
   *   b) verify users from GROUP_2 can't perform queries
   *   involving join between TAB_1 and TAB_2.
   *   c) verify users from GROUP_1 can't perform operations
   *   requiring ALL @SERVER scope:
   *     *) create database
   *     *) drop database
   *     *) show databases
   *     *) show locks
   *     *) execute ALTER TABLE .. SET LOCATION on a table in DB_1
   *     *) execute ALTER PARTITION ... SET LOCATION on a table in DB_1
   *     *) execute CREATE EXTERNAL TABLE ... in DB_1
   *     *) execute ADD JAR
   *     *) execute a query with TRANSOFORM
   * @throws Exception
   */
  @Test
  public void testSandboxOpt17() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("group2 = select_tb1", "groups");
    editor.addPolicy("select_tb1 = server=server1:db=db_1:select", "roles");
    editor.addPolicy("all_db1 = server=server1:db=db_1:*", "roles");
    editor.addPolicy("admin = server=server1:*", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group2", "users");

    // verify by SQL
    String dbName1 = "db_1";
    String dbName2 = "db_2";
    String tableName1 = "tb_1";
    String tableName2 = "tb_2";
    String tableName3 = "tb_external";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1);
    statement.execute("CREATE DATABASE " + dbName1);
    statement.close();
    connection.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertEquals("user1 shouldn't have privilege to switch to " + dbName1,
        statement.execute("USE " + dbName1));
    assertEquals("user1 shouldn't have privilege to drop table " + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + tableName1));
    assertEquals("user1 shouldn't have privilege to drop table " + tableName2,
        statement.execute("DROP TABLE IF EXISTS " + tableName2));
    assertEquals(
        "user1 shouldn't have privilege to create table " + tableName1,
        statement.execute("create table " + tableName1
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to create table " + tableName2,
        statement.execute("create table " + tableName2
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to load data into table " + tableName1,
        statement.execute("load data local inpath '" + dataFilePath.toString()
            + "' into table " + tableName1));
    assertEquals(
        "user1 should be able to load data into table " + tableName2,
        statement.execute("load data local inpath '" + dataFilePath.toString()
            + "' into table " + tableName2));
    // c
    assertFalse("user1 shouldn't have privilege to create any database",
        statement.execute("CREATE DATABASE " + dbName2));
    assertFalse("user1 shouldn't have privilege to drop any database",
        statement.execute("DROP DATABASE IF EXISTS " + dbName1));
    assertFalse("user1 shouldn't have privilege to list all databases",
        statement.execute("SHOW DATABASES"));
    assertFalse("user1 shouldn't have privilege to list lock for any table",
        statement.execute("SHOW LOCKS " + tableName1));
    assertFalse(
        "user1 shouldn't have privilege to alter table",
        statement.execute("ALTER TABLE " + tableName1
            + " ADD PARTITION (value = 10) LOCATION 'part1'"));
    assertFalse(
        "uesr1 shouldn't have privilege to alter partition",
        statement.execute("ALTER TABLE " + tableName1
            + " SET PARTITION (value = 10) LOCATION 'part2'"));
    assertFalse(
        "user1 shouldn't have privilege to create external table",
        statement.execute("CREATE EXTERNAL TABLE " + tableName3
            + " (under_col int, value string) LOCATION 'external'"));
    assertFalse("user1 shouldn't have privilege to execute 'ADD JAR'",
        statement.execute("ADD JAR /usr/lib/hive/lib/hbase.jar"));

    statement.close();
    connection.close();

    connection = context.createConnection("user2", "foo");
    statement = context.createStatement(connection);
    // a
    assertEquals("user2 shouldn't have privilege to select data from table "
        + tableName1,
        statement.execute("SELECT * FROM TABLE " + tableName1 + " LIMIT 10"));
    assertEquals(
        "user2 shouldn't have privilege to explain a query",
        statement.execute("EXPLAIN SELECT * FROM TABLE " + tableName1
            + " WHERE under_col > 5 LIMIT 10"));
    assertEquals("user2 shouldn't have privilege to describe a table",
        statement.execute("DESCRIBE " + tableName1));

    assertFalse(
        "user2 shouldn't have privilege to insert data into table "
            + tableName1,
        statement.execute("LOAD DATA LOCAL INPATH '" + dataFilePath.toString()
            + "' INTO TABLE " + tableName1));
    assertFalse(
        "user2 shouldn't have privilege to analyze table " + tableName1,
        statement.execute("analyze table " + tableName1
            + " compute statistics for columns under_col, value"));
    // b
    assertFalse(
        "user2 shouldn't have privilege to join " + tableName1 + " and "
            + tableName2,
        statement.execute("SELECT " + tableName1 + ".* FROM " + tableName1
            + " JOIN " + tableName2 + " ON (" + tableName1 + ".value = "
            + tableName2 + ".value)"));
    statement.close();
    connection.close();
  }

  /**
   * this case depends on the design of ACCESS-19
   * Steps:
   * 1. Admin user creates a new database DB_1
   * 2. Admin user grants ALL on DB_1 to group GROUP_1
   *   positive test case: users from GROUP_1 should be able to execute
   *     a) LOAD from those locations on which they have been granted
   *     privilege to read from
   *     b) IMPORT from those locations on which they have been granted
   *     privilege to read from
   *     c) EXPORT to those locations on which they have been granted
   *     privilege to write to
   *   negative test case:
   *     d)invert test cases above to execute LOAD, IMPORT, EXPORT
   *     from locations users don't have privileges on
   * @throws Exception
   */
  @Test
  public void testSandboxOpt18() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1:db=db_1:*", "roles");
    editor.addPolicy("admin = server=server1:*", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");
    // location specified policy
    editor.addPolicy("group1 = path1", "groups");
    editor.addPolicy("path1 = " + EXTERNAL_HDFS_DIR + ":*", "locations");// *
                                                                         // means
                                                                         // all

    // verify by SQL
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String tableName2 = "tb_2";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1);
    statement.execute("CREATE DATABASE " + dbName1);
    statement.close();
    connection.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertEquals("user1 should be able to switch to " + dbName1,
        statement.execute("USE " + dbName1));
    assertEquals("user1 should be able to drop table " + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + tableName1));
    assertEquals(
        "user1 should be able to create table " + tableName1,
        statement.execute("create table " + tableName1
            + " (under_col int comment 'the under column', value string)"));
    assertEquals("user1 should be able to drop table " + tableName2,
        statement.execute("DROP TABLE IF EXISTS " + tableName2));
    assertEquals(
        "user1 should be able to create table " + tableName2,
        statement.execute("create table " + tableName2
            + " (under_col int comment 'the under column', value string)"));
    // a
    assertEquals(
        "user1 has privilege to load data from " + EXTERNAL_HDFS_DIR,
        statement.execute("LOAD DATA INPATH '" + EXTERNAL_HDFS_DIR
            + "' INTO TABLE " + tableName1));
    // b
    assertEquals(
        "user1 should be able to import data from " + tableName1 + " to "
            + tableName2,
        statement.execute("INSERT OVERWRITE " + tableName2 + "SELECT * FROM "
            + tableName1));
    // c
    assertEquals(
        "user1 should be able to export data from " + tableName2 + " to "
            + EXTERNAL_HDFS_DIR,
        statement.execute("INSERT OVERWRITE LOCAL DIRECTORY '"
            + EXTERNAL_HDFS_DIR + "' SELECT * FROM " + tableName2));
    // d
    assertEquals("user1 should be able to switch to " + dbName1,
        statement.execute("USE " + dbName1));
    assertEquals("user1 should be able to drop table " + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + tableName1));
    assertEquals(
        "user1 should be able to create table " + tableName1,
        statement.execute("create table " + tableName1
            + " (under_col int comment 'the under column', value string)"));
    assertEquals("user1 should be able to drop table " + tableName2,
        statement.execute("DROP TABLE IF EXISTS " + tableName2));
    assertEquals(
        "user1 should be able to create table " + tableName2,
        statement.execute("create table " + tableName2
            + " (under_col int comment 'the under column', value string)"));
    assertFalse(
        "",
        statement.execute("LOAD DATA INPATH '" + NONE_EXISTS_DIR
            + "' INTO TABLE " + tableName1));
    assertFalse(
        "",
        statement.execute("INSERT OVERWRITE " + tableName2 + "SELECT * FROM "
            + tableName1));
    assertFalse(
        "",
        statement.execute("INSERT OVERWRITE LOCAL DIRECTORY '"
            + NONE_EXISTS_DIR + "' SELECT * FROM " + tableName2));
    statement.close();
    connection.close();
  }
}
