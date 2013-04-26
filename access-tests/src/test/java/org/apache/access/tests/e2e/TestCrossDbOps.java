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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at table scope with cross database access */

public class TestCrossDbOps extends AbstractTestWithStaticHiveServer {
  private Context context;
  private final String dataFileDir = "src/test/resources";
  // private Path dataFilePath = new Path(dataFileDir, "kv1.dat");
  private String EXTERNAL_HDFS_DIR = "hdfs://namenode:9000/tmp/externalDir";
  private String NONE_EXISTS_DIR = "hdfs://namenode:9000/tmp/nonExists";
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private String dataFilePath = this.getClass().getResource("/" +
       SINGLE_TYPE_DATA_FILE_NAME).getFile();
  private File dataFile;

  @Before
  public void setup() throws Exception {
    context = createContext();
    File dataDir = context.getDataDir();
    //copy data file to test dir
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
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
        "user_group  = db1_all,db2_all, load_data",
        "[roles]",
        "db1_all = server=server1->db=db1",
        "db2_all = server=server1->db=db2",
        "load_data = server=server1->URI=file:" + dataFilePath,
        "admin_role = server=server1",
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
        userStmt.execute("create table " + dbName + "." + tabName + " (id int)");
        userStmt.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE " + dbName + "." + tabName);
        userStmt.execute("select * from " + dbName + "." + tabName);
        context.close();
      }
    }

    // cleaup
    // drop dbs
    adminCon = context.createConnection("admin", "foo");
    adminStmt = context.createStatement(adminCon);
    for (String dbName : new String[] { "db1", "db2" }) {
      adminStmt.execute("use default");
      adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    }
    context.close();

  }

  /**
   * Test Case 2.11 admin user create a new database DB_1 and grant ALL to
   * himself on DB_1 should work
   */
  @Test
  public void testAdminDbPrivileges() throws Exception {
    // edit policy file
    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "[roles]",
        "admin_role = server=server1->",
        "[users]",
        "admin = admin_group"
    };
    context.makeNewPolicy(testPolicies);

    // Admin should be able to create new databases
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    String dbName = "db1";
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);

    // access the new databases
//    adminStmt.execute("use " + dbName);
    String tabName = dbName + "." + "admin_tab1";
    adminStmt.execute("create table " + tabName + "(c1 string)");
    adminStmt.execute("load data local inpath '/etc/passwd' into table "
        + tabName);
    adminStmt.execute("select * from " + tabName);

    // cleanup
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
        "db1_tab2_all = server=server1->db=db1->table=table_2",
        "db1_tab1_insert = server=server1->db=db1->table=table_1->insert",
        "admin_role = server=server1", "[users]", "user3 = user_group",
        "admin = admin_group" };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    String dbName = "db1";
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("create table " + dbName + ".table_1 (id int)");
    context.close();

    Connection userConn = context.createConnection("user3", "foo");
    Statement userStmt = context.createStatement(userConn);

    // since user3 only has insert privilege, all other statements should fail
    try {
      userStmt.execute("select * from " + dbName + ".table_1");
      assertTrue("select shouldn't pass for user user3 ", false);
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
        "db1_tab2_all = server=server1->db=db1->table=table_2",
        "admin_role = server=server1", "[users]", "user3 = user_group",
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

    // user3 doesn't have select priviledge on table_1, so insert/select should fail
    try {
      userStmt.execute("insert overwrite table  " + dbName + ".table_2 select * from " + dbName + ".table_1");
      assertTrue("select shouldn't pass for user user3 ", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("insert overwrite directory '/tmp' select * from  " + dbName + ".table_1");
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
        "db1_all = server=server1->db=db1",
        "db1_tab1_select = server=server1->db=db1->table=table_1->action=select",
        "admin_role = server=server1",
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
    adminStmt.execute("create table table_def (name string)");
    adminStmt
    .execute("load data local inpath '/etc/passwd' into table table_def");

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

    try {
      userStmt.execute("drop database " + dbName);
      assertTrue("drop db shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // Hive currently doesn't support cross db index DDL

    try {
      userStmt.execute("CREATE TEMPORARY FUNCTION strip AS '"
          + LocalGroupResourceAuthorizationProvider.class.getName() + "'");
      assertTrue("create function shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("create table  " + dbName + ".c_tab_2 as select * from  " + dbName + ".table_2");
      assertTrue("CTAS shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("select * from  " + dbName + ".table_2");
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
      userStmt.execute("drop table " + dbName + ".table_1");
      assertTrue("drop table shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("DROP VIEW IF EXISTS " + dbName + ".v1");
      assertTrue("DROP VIEW shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("create table " + dbName + ".table_5 (name string)");
      assertTrue("create table shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("ALTER TABLE " + dbName + ".table_1  RENAME TO " + dbName + ".table_99");
      assertTrue("ALTER TABLE rename shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("insert overwrite table " + dbName + ".table_2 select * from " + dbName + ".table_1");
      assertTrue("insert shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("insert overwrite table " + dbName + ".table_2 select * from " + "table_def");
      assertTrue("insert shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("ALTER TABLE " + dbName + ".table_part_1 ADD IF NOT EXISTS PARTITION (year = 2012)");
      assertTrue("ALTER TABLE add partition shouldn't pass for user user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      userStmt.execute("ALTER TABLE " + dbName + ".table_part_1 PARTITION (year = 2012) SET LOCATION '/etc'");
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
    editor.addPolicy("group1 = load_data", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    editor.addPolicy("all_db2 = server=server1->db=db_2", "roles");
    editor.addPolicy("load_data = server=server1->URI=file:" + dataFilePath, "roles");
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
    statement.execute("CREATE TABLE " + dbName1 + "." + tableName1 + "(id int)");
    statement.execute("CREATE TABLE " + dbName2 + "." + tableName1 + "(id int)");
    statement.close();
    connection.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);

    // a
    assertEquals("user1 should be able to drop table " + dbName1 + "." + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1));
    assertEquals(
        "user1 should be able to create table " + dbName1 + "." +tableName1,
        statement.execute("create table " + tableName1
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to load data into table " + dbName1 + "." +tableName1,
        statement.execute("load data local inpath '" + dataFilePath
            + "' into table " + dbName1 + "." + tableName1));
    assertEquals("user1 should be able to drop view " + dbName1 + "." +viewName1,
        statement.execute("DROP VIEW IF EXISTS " + dbName1 + "." +viewName1));
    assertEquals(
        "user1 should be able to create view " + dbName1 + "." +viewName1,
        statement.execute("CREATE VIEW " + dbName1 + "." +viewName1
            + " (value) AS SELECT value from " + dbName1 + "." + tableName1 + " LIMIT 10"));

    // b
    assertEquals("user1 should be able to drop table " + dbName2 + "." + tableName2,
        statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName2));
    assertEquals(
        "user1 should be able to create table " + dbName2 + "." + tableName2,
        statement.execute("create table " + dbName2 + "." + tableName2
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to load data into table " + dbName2 + "." + tableName2,
        statement.execute("load data local inpath '" + dataFilePath
            + "' into table " + dbName2 + "." + tableName2));
    assertEquals("user1 should be able to drop table " + dbName2 + "." + tableName3,
        statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName3));
    assertEquals(
        "user1 should be able to create table " + dbName2 + "." + tableName3,
        statement.execute("create table " + dbName2 + "." + tableName3
            + " (under_col int comment 'the under column', value string)"));
    assertEquals(
        "user1 should be able to load data into table " + dbName2 + "." + tableName3,
        statement.execute("load data local inpath '" + dataFilePath
            + "' into table " + dbName2 + "." + tableName3));

    // c
    assertFalse("user1 shouldn't be able to drop database",
        statement.execute("DROP DATABASE IF EXISTS " + dbName1));
    assertFalse("user1 shouldn't be able to drop database",
        statement.execute("DROP DATABASE IF EXISTS " + dbName2));

    // d
    editor.removePolicy("group1 = all_db2");
    editor.removePolicy("all_db2 = server=server1:db=db_2:*");
    assertEquals("user1 should be able to drop table " + dbName1 + "." + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1));
    assertEquals(
        "user1 should be able to create table " + dbName1 + "." + tableName1,
        statement.execute("create table " + dbName1 + "." + tableName1
            + " (under_col int comment 'the under column', value string)"));

    // e
    assertEquals("user1 should be able to drop view " + dbName1 + "." + viewName2,
        statement.execute("DROP VIEW IF EXISTS " + dbName1 + "." + viewName2));
    assertFalse(
        "user1 should not be able to create based on table " + dbName1 + "." + tableName2,
        statement.execute("CREATE VIEW " + dbName1 + "." + viewName2
            + " (value) AS SELECT value from " + dbName1 + "." + tableName2 + " LIMIT 10"));

    // f
    editor.addPolicy("group1 = select_tb2", "groups");
    editor.addPolicy("select_tb2 = server=server1:db=db_2:tb=tb_2:select",
        "roles");
    assertEquals("user1 should be able to drop view " + dbName1 + "." + viewName2,
        statement.execute("DROP VIEW IF EXISTS " + dbName1 + "." + viewName2));
    assertEquals(
        "user1 should be able to create view based on table " + dbName1 + "." + tableName2,
        statement.execute("CREATE VIEW " + dbName1 + "." + viewName2
            + " (value) AS SELECT value from " + dbName1 + "." + tableName2 + " LIMIT 10"));

    // g
    assertEquals("user1 should be able to drop view " + dbName1 + "." + viewName3,
        statement.execute("DROP VIEW IF EXISTS " + dbName1 + "." + viewName3));
    assertFalse(
        "user1 should not be able to create based on table " + dbName1 + "." + tableName3,
        statement.execute("CREATE VIEW " + dbName1 + "." + viewName3
            + " (value) AS SELECT value from " + tableName3 + " LIMIT 10"));

    statement.close();
    connection.close();
  }

}
