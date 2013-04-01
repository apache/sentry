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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at table scope within a single database.
 * Implements test cases 2.1, 2.2, 2.3, 2.4.1, 2.4.2, 2.4.3, 2.4.5 in the test plan
 */

public class TestPrivilegesAtTableScope {

  private EndToEndTestContext testContext;
  private Map<String, String> testProperties;
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private final String MULTI_TYPE_DATA_FILE_NAME = "emp.dat";

  @Before
  public void setup() throws Exception {
    testProperties = new HashMap<String, String>();
  }

  @After
  public void teardown() throws Exception {
    if (testContext != null) {
      testContext.close();
    }
  }

  /* 2.1
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into TAB_1, TAB_2
   * Admin grants SELECT on TAB_1, TAB_2, INSERT on TAB_1 to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testInsertAndSelect() throws Exception {
    testContext = new EndToEndTestContext(false, testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group = select_tab1, insert_tab1, select_tab2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1:*");
    testContext.appendToPolicyFileWithNewLine("select_tab1 = server=server1:db=DB_1:table=TAB_1:select");
    testContext.appendToPolicyFileWithNewLine("insert_tab1 = server=server1:db=DB_1:table=TAB_1:insert");
    testContext.appendToPolicyFileWithNewLine("select_tab2 = server=server1:db=DB_1:table=TAB_2:select");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("admin = hive");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can insert
    statement.execute("INSERT INTO TABLE TAB_1 SELECT A FROM TAB_2");
    // test user can query table
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(A) FROM TAB_1");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 500);

    // negative test: test user can't drop
    try {
      statement.execute("DROP TABLE TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    statement.close();
    connection.close();

    //connect as admin and drop tab_1
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP TABLE TAB_1");
    statement.close();
    connection.close();

    //negative test: connect as user_1 and try to recreate the tab_1
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    try {
      statement.execute("CREATE TABLE TAB_1(A STRING)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1");
    statement.close();
    connection.close();

  }

  /* 2.2
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into TAB_1, TAB_2.
   * Admin grants INSERT on TAB_1, SELECT on TAB_2 to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testInsert() throws Exception {
    testContext = new EndToEndTestContext(false, testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group = insert_tab1, select_tab2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1:*");
    testContext.appendToPolicyFileWithNewLine("insert_tab1 = server=server1:db=DB_1:table=TAB_1:insert");
    testContext.appendToPolicyFileWithNewLine("select_tab2 = server=server1:db=DB_1:table=TAB_2:select");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);

    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1(A) AS SELECT A FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute insert on table
    statement.executeQuery("INSERT INTO TABLE TAB_1 SELECT A FROM TAB_2");

    // negative test: user can't query table
    try {
      statement.executeQuery("SELECT COUNT(A) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    // negative test: test user can't query view
    try {
      statement.executeQuery("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    // negative test case: show tables shouldn't list VIEW_1
    ResultSet resultSet = statement.executeQuery("SHOW TABLES");
    while(resultSet.next()) {
      assertNotNull("table name is null in result set", resultSet.getString(1));
      assertFalse("VIEW_1".equalsIgnoreCase(resultSet.getString(1)));
    }

    //negative test: test user can't create a new view
    try {
      statement.executeQuery("CREATE VIEW VIEW_2(A) AS SELECT A FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1");
    statement.close();
    connection.close();
  }

  /* 2.3
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into TAB_1, TAB_2.
   * Admin grants SELECT on TAB_1, TAB_2 to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testSelect() throws Exception {
    testContext = new EndToEndTestContext(false, testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group = select_tab1, select_tab2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1:*");
    testContext.appendToPolicyFileWithNewLine("select_tab1 = server=server1:db=DB_1:table=TAB_1:select");
    testContext.appendToPolicyFileWithNewLine("insert_tab1 = server=server1:db=DB_1:table=TAB_1:insert");
    testContext.appendToPolicyFileWithNewLine("select_tab2 = server=server1:db=DB_1:table=TAB_2:select");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);

    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1(A) AS SELECT A FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query on table
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(A) FROM TAB_1");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 500);

    // negative test: test insert into table
    try {
      statement.executeQuery("INSERT INTO TABLE TAB_1 SELECT A FROM TAB_2");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    // negative test: test user can't query view
    try {
      statement.executeQuery("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    //negative test: test user can't create a new view
    try {
      statement.executeQuery("CREATE VIEW VIEW_2(A) AS SELECT A FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1");
    statement.close();
    connection.close();
  }

  /* 2.4.1
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1 loads data into TAB_1, TAB_2.
   * Admin grants SELECT on TAB_1,TAB_2 to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testTableViewJoin() throws Exception {
    testContext = new EndToEndTestContext(false, testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group = select_tab1, select_tab2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1:*");
    testContext.appendToPolicyFileWithNewLine("select_tab1 = server=server1:db=DB_1:table=TAB_1:select");
    testContext.appendToPolicyFileWithNewLine("select_tab2 = server=server1:db=DB_1:table=TAB_2:select");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);

    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query TAB_1 JOIN TAB_2
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_1 JOIN TAB_2 ON (TAB_1.B = TAB_2.B)");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM VIEW_1 JOIN TAB_2 ON (VIEW_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1");
    statement.close();
    connection.close();
  }

  /* 2.4.2
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1 loads data into TAB_1, TAB_2.
   * Admin grants SELECT on TAB_2 to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testTableViewJoin2() throws Exception {
    testContext = new EndToEndTestContext(false, testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group = select_tab2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1:*");
    testContext.appendToPolicyFileWithNewLine("select_tab1 = server=server1:db=DB_1:table=TAB_1:select");
    testContext.appendToPolicyFileWithNewLine("select_tab2 = server=server1:db=DB_1:table=TAB_2:select");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);

    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query on TAB_2
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_2");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM VIEW_1 JOIN TAB_2 ON (VIEW_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM TAB_1 JOIN TAB_2 ON (TAB_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1");
    statement.close();
    connection.close();
  }

  /* 2.4.3
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1 loads data into TAB_1, TAB_2.
   * Admin grants SELECT on TAB_2, VIEW_1 to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testTableViewJoin3() throws Exception {
    testContext = new EndToEndTestContext(false, testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group = select_tab2, select_view1");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1:*");
    testContext.appendToPolicyFileWithNewLine("select_view1 = server=server1:db=DB_1:table=VIEW_1:select");
    testContext.appendToPolicyFileWithNewLine("select_tab2 = server=server1:db=DB_1:table=TAB_2:select");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);

    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query on TAB_2
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM TAB_2");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query VIEW_1 JOIN TAB_2
    resultSet = statement.executeQuery("SELECT COUNT(*) FROM VIEW_1 JOIN TAB_2 ON (VIEW_1.B = TAB_2.B)");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // test user can execute query on VIEW_1
    resultSet = statement.executeQuery("SELECT COUNT(*) FROM VIEW_1");
    count = 0;
    countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM TAB_1 JOIN TAB_2 ON (TAB_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1");
    statement.close();
    connection.close();
  }

  /* 2.5
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1 loads data into TAB_1, TAB_2.
   * Admin grants SELECT on TAB_1, VIEW_1 to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testTableViewJoin4() throws Exception {
    testContext = new EndToEndTestContext(false, testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group = select_tab1, select_view1");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1:*");
    testContext.appendToPolicyFileWithNewLine("select_view1 = server=server1:db=DB_1:table=VIEW_1:select");
    testContext.appendToPolicyFileWithNewLine("select_tab1 = server=server1:db=DB_1:table=TAB_1:select");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);

    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can execute query VIEW_1 JOIN TAB_1
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM VIEW_1 JOIN TAB_1 ON (VIEW_1.B = TAB_1.B)");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);

    // negative test: test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM TAB_1 JOIN TAB_2 ON (TAB_1.B = TAB_2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1");
    statement.close();
    connection.close();
  }
}