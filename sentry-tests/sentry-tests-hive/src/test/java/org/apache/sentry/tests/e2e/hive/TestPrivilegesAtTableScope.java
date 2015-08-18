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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at table scope within a single database.
 */

public class TestPrivilegesAtTableScope extends AbstractTestWithStaticConfiguration {

  private static PolicyFile policyFile;
  private final static String MULTI_TYPE_DATA_FILE_NAME = "emp.dat";

  @Before
  public void setup() throws Exception {
    policyFile = super.setupPolicy();
    super.setup();
    prepareDBDataForTest();
  }

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  protected static void prepareDBDataForTest() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");

    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");

    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2 Admin grants SELECT on TAB_1, TAB_2, INSERT on TAB_1 to
   * USER_GROUP of which user1 is a member.
   */
  @Test
  public void testInsertAndSelect() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "insert_tab1", "select_tab2")
        .addPermissionsToRole("select_tab1", "server=server1->db=DB_1->table=TAB_1->action=select")
        .addPermissionsToRole("insert_tab1", "server=server1->db=DB_1->table=TAB_1->action=insert")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=TAB_2->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can insert
    statement.execute("INSERT INTO TABLE TAB_1 SELECT A, B FROM TAB_2");
    // test user can query table
    statement.executeQuery("SELECT A FROM TAB_2");
    // negative test: test user can't drop
    try {
      statement.execute("DROP TABLE TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // connect as admin and drop tab_1
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    statement.execute("DROP TABLE TAB_1");
    statement.close();
    connection.close();

    // negative test: connect as user1 and try to recreate tab_1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    try {
      statement.execute("CREATE TABLE TAB_1(A STRING)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    // connect as admin to restore the tab_1
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("INSERT INTO TABLE TAB_1 SELECT A, B FROM TAB_2");
    statement.close();
    connection.close();

  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin grants INSERT on TAB_1, SELECT on TAB_2 to USER_GROUP
   * of which user1 is a member.
   */
  @Test
  public void testInsert() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "insert_tab1", "select_tab2")
        .addPermissionsToRole("insert_tab1", "server=server1->db=DB_1->table=TAB_1->action=insert")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=TAB_2->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // test user can execute insert on table
    statement.execute("INSERT INTO TABLE TAB_1 SELECT A, B FROM TAB_2");

    // negative test: user can't query table
    try {
      statement.executeQuery("SELECT A FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query view
    try {
      statement.executeQuery("SELECT A FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test case: show tables shouldn't list VIEW_1
    ResultSet resultSet = statement.executeQuery("SHOW TABLES");
    while (resultSet.next()) {
      String tableName = resultSet.getString(1);
      assertNotNull("table name is null in result set", tableName);
      assertFalse("Found VIEW_1 in the result set",
          "VIEW_1".equalsIgnoreCase(tableName));
    }

    // negative test: test user can't create a new view
    try {
      statement.executeQuery("CREATE VIEW VIEW_2(A) AS SELECT A FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin grants SELECT on TAB_1, TAB_2 to USER_GROUP of which
   * user1 is a member.
   */
  @Test
  public void testSelect() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "select_tab2")
        .addPermissionsToRole("select_tab1", "server=server1->db=DB_1->table=TAB_1->action=select")
        .addPermissionsToRole("insert_tab1", "server=server1->db=DB_1->table=TAB_1->action=insert")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=TAB_2->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // test user can execute query on table
    statement.executeQuery("SELECT A FROM TAB_1");

    // negative test: test insert into table
    try {
      statement.executeQuery("INSERT INTO TABLE TAB_1 SELECT A, B FROM TAB_2");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query view
    try {
      statement.executeQuery("SELECT A FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't create a new view
    try {
      statement.executeQuery("CREATE VIEW VIEW_2(A) AS SELECT A FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1
   * loads data into TAB_1, TAB_2. Admin grants SELECT on TAB_1,TAB_2 to
   * USER_GROUP of which user1 is a member.
   */
  @Test
  public void testTableViewJoin() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "select_tab2")
        .addPermissionsToRole("select_tab1", "server=server1->db=DB_1->table=TAB_1->action=select")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=TAB_2->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // test user can execute query TAB_1 JOIN TAB_2
    statement.executeQuery("SELECT T1.B FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT V1.B FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1
   * loads data into TAB_1, TAB_2. Admin grants SELECT on TAB_2 to USER_GROUP of
   * which user1 is a member.
   */
  @Test
  public void testTableViewJoin2() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab2")
        .addPermissionsToRole("select_tab1", "server=server1->db=DB_1->table=TAB_1->action=select")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=TAB_2->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // test user can execute query on TAB_2
    statement.executeQuery("SELECT A FROM TAB_2");

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT VIEW_1.B FROM VIEW_1 JOIN TAB_2 ON (VIEW_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT TAB_1.B FROM TAB_1 JOIN TAB_2 ON (TAB_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1
   * loads data into TAB_1, TAB_2. Admin grants SELECT on TAB_2, VIEW_1 to
   * USER_GROUP of which user1 is a member.
   */
  @Test
  public void testTableViewJoin3() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab2", "select_view1")
        .addPermissionsToRole("select_view1", "server=server1->db=DB_1->table=VIEW_1->action=select")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=TAB_2->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // test user can execute query on TAB_2
    statement.executeQuery("SELECT A FROM TAB_2");

    // test user can execute query VIEW_1 JOIN TAB_2
    statement.executeQuery("SELECT V1.B FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");

    // test user can execute query on VIEW_1
    statement.executeQuery("SELECT A FROM VIEW_1");

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT T1.B FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1
   * loads data into TAB_1, TAB_2. Admin grants SELECT on TAB_1, VIEW_1 to
   * USER_GROUP of which user1 is a member.
   */
  @Test
  public void testTableViewJoin4() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "select_view1")
        .addPermissionsToRole("select_view1", "server=server1->db=DB_1->table=VIEW_1->action=select")
        .addPermissionsToRole("select_tab1", "server=server1->db=DB_1->table=TAB_1->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    // test user can execute query VIEW_1 JOIN TAB_1
    statement.executeQuery("SELECT VIEW_1.B FROM VIEW_1 JOIN TAB_1 ON (VIEW_1.B = TAB_1.B)");

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT TAB_1.B FROM TAB_1 JOIN TAB_2 ON (TAB_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  /***
   * Verify truncate table permissions for different users with different
   * privileges
   * @throws Exception
   */
  @Test
  public void testTruncateTable() throws Exception {
    File dataDir = context.getDataDir();
    // copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "all_tab1")
        .addPermissionsToRole("all_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL2)
        .addRolesToGroup(USERGROUP2, "drop_tab1")
        .addPermissionsToRole("drop_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL3 + "->action=drop",
            "server=server1->db=" + DB1 + "->table=" + TBL3 + "->action=select")
        .addRolesToGroup(USERGROUP3, "select_tab1")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL1 + "->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE " + TBL1 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("CREATE TABLE " + TBL2 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("CREATE TABLE " + TBL3 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + TBL1);
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + TBL2);
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + TBL3);

    // verify admin can execute truncate table
    statement.execute("TRUNCATE TABLE " + TBL1);
    assertFalse(hasData(statement, TBL1));
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // verify all on tab can truncate table
    statement.execute("TRUNCATE TABLE " + TBL2);
    assertFalse(hasData(statement, TBL2));
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // verify drop on tab can truncate table
    statement.execute("TRUNCATE TABLE " + TBL3);
    assertFalse(hasData(statement, TBL3));
    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // verify select on tab can NOT truncate table
    context.assertAuthzException(statement, "TRUNCATE TABLE " + TBL3);
    statement.close();
    connection.close();
  }

  /**
   * Test queries without from clause. Hive rewrites the queries with dummy db and table
   * entities which should not trip authorization check.
   * @throws Exception
   */
  @Test
  public void testSelectWithoutFrom() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "all_tab1")
        .addPermissionsToRole("all_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL1)
        .addRolesToGroup(USERGROUP2, "select_tab1")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL1)
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);

    // test with implicit default database
    assertTrue(statement.executeQuery("SELECT 1 ").next());
    assertTrue(statement.executeQuery("SELECT current_database()").next());

    // test after switching database
    statement.execute("USE " + DB1);
    assertTrue(statement.executeQuery("SELECT 1 ").next());
    assertTrue(statement.executeQuery("SELECT current_database() ").next());
    statement.close();
    connection.close();
  }

  // verify that the given table has data
  private boolean hasData(Statement stmt, String tableName) throws Exception {
    ResultSet rs1 = stmt.executeQuery("SELECT * FROM " + tableName);
    boolean hasResults = rs1.next();
    rs1.close();
    return hasResults;
  }

  @Test
  public void testDummyPartition() throws Exception {

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "select_tab2")
        .addPermissionsToRole("select_tab1", "server=server1->db=DB_1->table=TAB_1->action=select")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=TAB_3->action=insert")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("USE " + DB1);
    statement.execute("CREATE table TAB_3 (a2 int) PARTITIONED BY (b2 string, c2 string)");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    statement.execute("USE " + DB1);
    statement.execute("INSERT OVERWRITE TABLE TAB_3 PARTITION(b2='abc', c2) select a, b as c2 from TAB_1");
    statement.close();
    connection.close();

  }
}
