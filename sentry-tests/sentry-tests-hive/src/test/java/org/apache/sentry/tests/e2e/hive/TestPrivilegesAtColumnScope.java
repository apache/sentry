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

import org.apache.sentry.provider.file.PolicyFile;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at table scope within a single database.
 */

public class TestPrivilegesAtColumnScope extends AbstractTestWithStaticConfiguration {

  private PolicyFile policyFile;

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private final String MULTI_TYPE_DATA_FILE_NAME = "emp.dat";

  @Before
  public void setup() throws Exception {
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2.
   * Admin grants SELECT on just one column of TAB_1, TAB_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   */
  @Test
  public void testSelectColumnOnTable() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1_A", "select_tab2_A")
        .addRolesToGroup(USERGROUP2, "select_tab1_A", "select_tab1_B", "select_tab2_A", "select_tab2_B")
        .addPermissionsToRole("select_tab1_A", "server=server1->db=DB_1->table=TAB_1->column=A->action=select")
        .addPermissionsToRole("select_tab1_B", "server=server1->db=DB_1->table=TAB_1->column=B->action=select")
        .addPermissionsToRole("select_tab2_A", "server=server1->db=DB_1->table=TAB_2->column=A->action=select")
        .addPermissionsToRole("select_tab2_B", "server=server1->db=DB_1->table=TAB_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING, B STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1(A) AS SELECT A FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING, B STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution on user1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can execute query count on column A on tab_1
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(A) FROM TAB_1");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 500);

    // test user can execute query column A on tab_1
    resultSet = statement.executeQuery("SELECT A FROM TAB_1");
    countRows = 0;

    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 500);

    // negative test: test user can't execute query count of column B on tab_1
    try {
      statement.execute("SELECT COUNT(B) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query column B on tab_1
    try {
      statement.execute("SELECT B FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query view
    try {
      statement.execute("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // test execution on user2
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query count of column A on tab_1
    resultSet = statement.executeQuery("SELECT COUNT(A) FROM TAB_1");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    // test user can execute query count of column B on tab_1
    resultSet = statement.executeQuery("SELECT COUNT(B) FROM TAB_1");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }

    // test user can't execute query count using * on tab_1
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can execute SELECT * on tab_1
    resultSet = statement.executeQuery("SELECT * FROM TAB_1");
    countRows = 0;

    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 500);

    statement.close();
    connection.close();

    // test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin view on TAB_1 and TAB_2
   * Admin grants SELECT on just one column of VIEW_1, VIEW_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   * Note: We don't support column level privilege on VIEW
   */
  @Test
  public void testSelectColumnOnView() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "select_view1_A", "select_view2_A")
        .addRolesToGroup(USERGROUP2, "select_view1_A", "select_view1_B", "select_view2_A", "select_view2_B")
        .addPermissionsToRole("select_view1_A", "server=server1->db=DB_1->table=VIEW_1->column=A->action=select")
        .addPermissionsToRole("select_view1_B", "server=server1->db=DB_1->table=VIEW_1->column=B->action=select")
        .addPermissionsToRole("select_view2_A", "server=server1->db=DB_1->table=VIEW_2->column=A->action=select")
        .addPermissionsToRole("select_view2_B", "server=server1->db=DB_1->table=VIEW_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING, B STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1(A,B) AS SELECT A,B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING, B STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_2");
    statement.execute("CREATE VIEW VIEW_2(A,B) AS SELECT A,B FROM TAB_2");
    statement.close();
    connection.close();

    // test execution on user1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    // negative test: test user can't execute query count of column B on tab_1
    try {
      statement.execute("SELECT COUNT(B) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // negative test: test user can't execute query count of column A on tab_1
    try {
      statement.execute("SELECT COUNT(A) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query column of view
    try {
      statement.execute("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // negative test: test user can't query column of view
    try {
      statement.execute("SELECT COUNT(B) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    // test execution on user2
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query count of column A on tab_1
    try {
      statement.execute("SELECT COUNT(A) FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // test user can execute query count of column B on tab_1
    try {
      statement.execute("SELECT COUNT(B) FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // test user can't execute query count using * on tab_1
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("SELECT * FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query view
    try {
      statement.execute("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("SELECT COUNT(B) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't create a new view
    try {
      statement.execute("CREATE VIEW VIEW_2(A) AS SELECT A FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1
   * loads data into TAB_1, TAB_2. Admin grants SELECT on TAB_1,TAB_2 to
   * USER_GROUPS.
   */
  @Test
  public void testSelectColumnOnTableJoin() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1_A", "select_tab1_B", "select_tab2_B")
        .addRolesToGroup(USERGROUP2, "select_tab1_B", "select_tab2_B")
        .addRolesToGroup(USERGROUP3, "select_tab1_B", "select_tab2_A")
        .addRolesToGroup(USERGROUP4, "select_tab1_A", "select_tab1_B", "select_tab2_A", "select_tab2_B")
        .addPermissionsToRole("select_tab1_A", "server=server1->db=DB_1->table=TAB_1->column=A->action=select")
        .addPermissionsToRole("select_tab1_B", "server=server1->db=DB_1->table=TAB_1->column=B->action=select")
        .addPermissionsToRole("select_tab2_A", "server=server1->db=DB_1->table=TAB_2->column=A->action=select")
        .addPermissionsToRole("select_tab2_B", "server=server1->db=DB_1->table=TAB_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_2");
    statement.execute("CREATE VIEW VIEW_2 AS SELECT A, B FROM TAB_2");
    statement.close();
    connection.close();

    // test execution user1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can execute query TAB_1 JOIN TAB_2
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(T1.B) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    count = 0;
    countRows = 0;
    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_2 T2 JOIN TAB_1 T1 ON (T1.B = T2.B)");
    count = 0;
    countRows = 0;
    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT T1.* FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT * FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select count *
    try {
      statement.execute("SELECT count(*) FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
      //Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT T1.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select * on TAB_1
    try {
      statement.execute("SELECT count(T1.*) FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select * on TAB_2
    try {
      statement.execute("SELECT T2.* FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT count(T2.*) FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT * FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT T2.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT T2.A FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test execution on user2
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT COUNT(T1.B) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT T2.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT * FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select * on TAB_1
    try {
      statement.execute("SELECT T1.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select column A on TAB_1
    try {
      statement.execute("SELECT T1.A FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement
          .executeQuery("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test execution on user3
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(T1.B) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(T2.B) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(T1.B) FROM TAB_2 T2 JOIN TAB_1 T1 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(T2.B) FROM TAB_2 T2 JOIN TAB_1 T1 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT T2.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT * FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select * on TAB_1
    try {
      statement.execute("SELECT T1.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can execute query TAB_1 JOIN TAB_2 use select *
    try {
      statement.execute("SELECT T1.A FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test execution on user4
    connection = context.createConnection(USER4_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT COUNT(T1.B) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_2 JOIN TAB_1
    resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_2 T2 JOIN TAB_1 T1 ON (T1.B = T2.B)");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query TAB_2 JOIN TAB_1
    resultSet = statement.executeQuery("SELECT T1.* FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // test user can execute query TAB_2 JOIN TAB_1
    resultSet = statement.executeQuery("SELECT * FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // test user can execute query TAB_2 JOIN TAB_1
    resultSet = statement.executeQuery("SELECT count(*) FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    try {
      statement.execute("SELECT count(T1.*) FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can execute query TAB_2 JOIN TAB_1
    resultSet = statement.executeQuery("SELECT T2.* FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
    countRows = 0;

    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    try {
      statement.execute("SELECT count(T2.*) FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can execute query TAB_1 JOIN TAB_2 use select *
    resultSet = statement.executeQuery("SELECT * FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // test user can execute query TAB_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT T1.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // test user can execute query TAB_1 JOIN TAB_2 use select *
    resultSet = statement.executeQuery("SELECT T2.* FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // test user can execute query TAB_1 JOIN TAB_2 use select *
    resultSet = statement.executeQuery("SELECT T2.A FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
    countRows = 0;
    while (resultSet.next()) {
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 12);

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    // test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin view on TAB_1 and TAB_2
   * Admin grants SELECT on just one column of VIEW_1, VIEW_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   * Note: We don't support column level privilege on VIEW
   */
  @Test
  public void testSelectColumnOnViewJoin() throws Exception {
    File dataDir = context.getDataDir();
    // copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "select_view1_A", "select_view1_B", "select_view2_B")
        .addRolesToGroup(USERGROUP2, "select_view1_B", "select_view2_B")
        .addRolesToGroup(USERGROUP3, "select_view1_B", "select_view2_A")
        .addPermissionsToRole("select_view1_A", "server=server1->db=DB_1->table=VIEW_1->column=A->action=select")
        .addPermissionsToRole("select_view1_B", "server=server1->db=DB_1->table=VIEW_1->column=B->action=select")
        .addPermissionsToRole("select_view2_A", "server=server1->db=DB_1->table=VIEW_2->column=A->action=select")
        .addPermissionsToRole("select_view2_B", "server=server1->db=DB_1->table=VIEW_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_2");
    statement.execute("CREATE VIEW VIEW_2 AS SELECT A, B FROM TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");


    // test user can't execute query VIEW_1 JOIN VIEW_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN VIEW_2 V2 ON (V1.B = V2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    // test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin view on TAB_1 and TAB_2
   * Admin grants SELECT on just one column of VIEW_1, VIEW_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   * Note: We don't support column level privilege on VIEW
   */
  @Test
  public void testSelectColumnOnTableViewJoin() throws Exception {
    File dataDir = context.getDataDir();
    // copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1_A", "select_tab1_B", "select_view2_B")
        .addPermissionsToRole("select_tab1_A", "server=server1->db=DB_1->table=VIEW_1->column=A->action=select")
        .addPermissionsToRole("select_tab1_B", "server=server1->db=DB_1->table=VIEW_1->column=B->action=select")
        .addPermissionsToRole("select_view2_B", "server=server1->db=DB_1->table=VIEW_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE TAB_2");
    statement.execute("CREATE VIEW VIEW_2 AS SELECT A, B FROM TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query VIEW_1 JOIN VIEW_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN VIEW_2 V2 ON (V1.B = V2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    // test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.close();
    connection.close();
  }
}
