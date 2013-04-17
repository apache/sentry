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
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at table scope within a single database.
 */

public class TestPrivilegesAtDatabaseScope {

  private EndToEndTestContext testContext;
  private Map<String, String> testProperties;
  private static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
 
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

  /* Admin creates database DB_1
   * Admin grants ALL to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testAllPrivilege() throws Exception {
    testContext = new EndToEndTestContext(testProperties);
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
    testContext.appendToPolicyFileWithNewLine("user_group1 = all_db1, load_data");
    testContext.appendToPolicyFileWithNewLine("user_group2 = all_db2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1");
    testContext.appendToPolicyFileWithNewLine("all_db1 = server=server1->db=DB_1");
    testContext.appendToPolicyFileWithNewLine("all_db2 = server=server1->db=DB_2");
    testContext.appendToPolicyFileWithNewLine("load_data = server=server1->uri=file:" + dataFile.getPath());
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group1");
    testContext.appendToPolicyFileWithNewLine("user_2 = user_group2");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("CREATE DATABASE DB_2");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    // test user can switch db
    statement.execute("USE DB_1");
    // test user can create table
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    // test user can execute load
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");

    //test user can create view
    statement.execute("CREATE VIEW VIEW_1(A) AS SELECT A FROM TAB_1");

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
    assertTrue("Incorrect result", count == 1000);

    // test user can execute alter table rename
    statement.execute("ALTER TABLE TAB_1 RENAME TO TAB_3");

    // test user can execute create as select
    statement.execute("CREATE TABLE TAB_4 AS SELECT * FROM TAB_2");

    // test user can execute alter table rename cols
    statement.execute("ALTER TABLE TAB_3 ADD COLUMNS (B INT)");

    // test user can drop table
    statement.execute("DROP TABLE TAB_3");

    //negative test case: user can't drop another user's database
    try {
      statement.execute("DROP DATABASE DB_2 CASCADE");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      testContext.verifyAuthzException(e);
    }

    //negative test case: user can't switch into another user's database
    try {
      statement.execute("USE DB_2");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      testContext.verifyAuthzException(e);
    }

    //negative test case: user can't drop own database
    try {
      statement.execute("DROP DATABASE DB_1 CASCADE");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      testContext.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.execute("DROP DATABASE DB_2 CASCADE");
    statement.close();
    connection.close();
    testContext.close();
  }

  /* Admin creates database DB_1, creates table TAB_1, loads data into it
   * Admin grants ALL to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testAllPrivilegeOnObjectOwnedByAdmin() throws Exception {
    testContext = new EndToEndTestContext(testProperties);
    File policyFile = testContext.getPolicyFile();
    File dataDir = testContext.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    File externalTblDir = new File(dataDir, "exttab");
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, testContext.deletePolicyFile());
    // groups : role -> group
    testContext.appendToPolicyFileWithNewLine("[groups]");
    testContext.appendToPolicyFileWithNewLine("admin = all_server");
    testContext.appendToPolicyFileWithNewLine("user_group1 = all_db1, load_data");
    testContext.appendToPolicyFileWithNewLine("user_group2 = all_db2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1");
    testContext.appendToPolicyFileWithNewLine("all_db1 = server=server1->db=DB_1");
    testContext.appendToPolicyFileWithNewLine("all_db2 = server=server1->db=DB_2");
    testContext.appendToPolicyFileWithNewLine("load_data = server=server1->uri=file:" + dataFile.getPath());
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group1");
    testContext.appendToPolicyFileWithNewLine("user_2 = user_group2");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("CREATE DATABASE DB_2");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE PART_TAB_1(A STRING) partitioned by (B INT) STORED AS TEXTFILE");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE PART_TAB_1 PARTITION(B=1)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE PART_TAB_1 PARTITION(B=2)");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    // test user can switch db
    statement.execute("USE DB_1");
    // test user can execute load
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");

    //test user can create view
    statement.execute("CREATE VIEW VIEW_1(A) AS SELECT A FROM TAB_1");

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
    assertTrue("Incorrect result", count == 1500);

    // test user can execute alter table rename
    statement.execute("ALTER TABLE TAB_1 RENAME TO TAB_3");

    // test user can drop table
    statement.execute("DROP TABLE TAB_3");

    //negative test case: user can't drop db
    try {
      statement.execute("DROP DATABASE DB_1 CASCADE");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      testContext.verifyAuthzException(e);
    }

    //negative test case: user can't create external tables
    assertTrue("Unable to create directory for external table test" , externalTblDir.mkdir());
    try {
      statement.execute("CREATE EXTERNAL TABLE EXT_TAB_1(A STRING) STORED AS TEXTFILE LOCATION '"+
                        externalTblDir.getAbsolutePath() + "'");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      testContext.verifyAuthzException(e);
    }

    //negative test case: user can't execute alter table set location
    try {
      statement.execute("ALTER TABLE TAB_2 SET LOCATION 'hdfs://nn1.example.com/hive/warehouse'");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      testContext.verifyAuthzException(e);
    }


    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.execute("DROP DATABASE DB_2 CASCADE");
    statement.close();
    connection.close();
    testContext.close();
  }

  /* Admin creates database DB_1, DB_2, tables TAB_1, TAB_2 in DB_1 and TAB_3 and TAB_4 in DB_2
   * Admin grants ALL on DB_1 to USER_GROUP1 of which USER_1 is a member, ALL on DB_2 to USER_GROUP2 of which USER_2 is a member
   */
  @Test
  public void testPrivilegesForMetadataOperations() throws Exception {
    testContext = new EndToEndTestContext(testProperties);
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
    testContext.appendToPolicyFileWithNewLine("user_group1 = all_db1");
    testContext.appendToPolicyFileWithNewLine("user_group2 = all_db2");
    // roles: privileges -> role
    testContext.appendToPolicyFileWithNewLine("[roles]");
    testContext.appendToPolicyFileWithNewLine("all_server = server=server1");
    testContext.appendToPolicyFileWithNewLine("all_db1 = server=server1->db=DB_1");
    testContext.appendToPolicyFileWithNewLine("all_db2 = server=server1->db=DB_2");
    // users: users -> groups
    testContext.appendToPolicyFileWithNewLine("[users]");
    testContext.appendToPolicyFileWithNewLine("hive = admin");
    testContext.appendToPolicyFileWithNewLine("user_1 = user_group1");
    testContext.appendToPolicyFileWithNewLine("user_2 = user_group2");
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("CREATE DATABASE DB_2");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE PART_TAB_1(A STRING) partitioned by (B INT) STORED AS TEXTFILE");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE PART_TAB_1 PARTITION(B=1)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE PART_TAB_1 PARTITION(B=2)");

    statement.execute("USE DB_2");
    statement.execute("CREATE TABLE TAB_3(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_3");
    statement.execute("CREATE TABLE PART_TAB_4(A STRING) partitioned by (B INT) STORED AS TEXTFILE");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE PART_TAB_4 PARTITION(B=1)");

    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user_1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE DB_1");
    // TODO:test show * e.g., show tables, show databases etc.

    statement.close();
    connection.close();

    //test cleanup
    connection = testContext.createConnection("hive", "hive");
    statement = testContext.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.execute("DROP DATABASE DB_2 CASCADE");
    statement.close();
    connection.close();
    testContext.close();
  }

  @AfterClass
  public static void shutDown() throws IOException {
    EndToEndTestContext.shutdown();
  }
}
