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

package org.apache.sentry.tests.e2e;

import static org.junit.Assert.assertFalse;
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

import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at table scope within a single database.
 */

public class TestPrivilegesAtDatabaseScope extends AbstractTestWithHiveServer {

  private Context context;
  Map <String, String >testProperties;
  private static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  @Before
  public void setup() throws Exception {
    testProperties = new HashMap<String, String>();
  }

  @After
  public void teardown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /* Admin creates database DB_1
   * Admin grants ALL to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testAllPrivilege() throws Exception {
    context = createContext(testProperties);

    File policyFile = context.getPolicyFile();
    File dataDir = context.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, context.deletePolicyFile());
    // groups : role -> group
    context.append("[groups]");
    context.append("admin = all_server");
    context.append("user_group1 = all_db1, load_data");
    context.append("user_group2 = all_db2");
    // roles: privileges -> role
    context.append("[roles]");
    context.append("all_server = server=server1");
    context.append("all_db1 = server=server1->db=DB_1");
    context.append("all_db2 = server=server1->db=DB_2");
    context.append("load_data = server=server1->uri=file://" + dataFile.getPath());
    // users: users -> groups
    context.append("[users]");
    context.append("hive = admin");
    context.append("user_1 = user_group1");
    context.append("user_2 = user_group2");
    // setup db objects needed by the test
    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("CREATE DATABASE DB_2");
    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection("user_1", "password");
    statement = context.createStatement(connection);  
    // test user can create table
    statement.execute("CREATE TABLE DB_1.TAB_1(A STRING)");
    // test user can execute load
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE DB_1.TAB_1");
    statement.execute("CREATE TABLE DB_1.TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE DB_1.TAB_2");

    // test user can switch db
    statement.execute("USE DB_1");
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
      context.verifyAuthzException(e);
    }

    //negative test case: user can't switch into another user's database
    try {
      statement.execute("USE DB_2");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    //negative test case: user can't drop own database
    try {
      statement.execute("DROP DATABASE DB_1 CASCADE");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = context.createConnection("hive", "hive");
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.execute("DROP DATABASE DB_2 CASCADE");
    statement.close();
    connection.close();
    context.close();
  }

  /* Admin creates database DB_1, creates table TAB_1, loads data into it
   * Admin grants ALL to USER_GROUP of which USER_1 is a member.
   */
  @Test
  public void testAllPrivilegeOnObjectOwnedByAdmin() throws Exception {
    context = createContext(testProperties);

    File policyFile = context.getPolicyFile();
    File dataDir = context.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    File externalTblDir = new File(dataDir, "exttab");
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, context.deletePolicyFile());
    // groups : role -> group
    context.append("[groups]");
    context.append("admin = all_server");
    context.append("user_group1 = all_db1, load_data, exttab");
    context.append("user_group2 = all_db2");
    // roles: privileges -> role
    context.append("[roles]");
    context.append("all_server = server=server1");
    context.append("all_db1 = server=server1->db=DB_1");
    context.append("all_db2 = server=server1->db=DB_2");
    context.append("exttab = server=server1->uri=file://" + dataDir.getPath());
    context.append("load_data = server=server1->uri=file://" + dataFile.getPath());

    // users: users -> groups
    context.append("[users]");
    context.append("hive = admin");
    context.append("user_1 = user_group1");
    context.append("user_2 = user_group2");
    // setup db objects needed by the test
    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);
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
    connection = context.createConnection("user_1", "password");
    statement = context.createStatement(connection);
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
      context.verifyAuthzException(e);
    }

    //negative test case: user can't create external tables
    assertTrue("Unable to create directory for external table test" , externalTblDir.mkdir());
    statement.execute("CREATE EXTERNAL TABLE EXT_TAB_1(A STRING) STORED AS TEXTFILE LOCATION 'file:"+
                        externalTblDir.getAbsolutePath() + "'");

    //negative test case: user can't execute alter table set location
    try {
      statement.execute("ALTER TABLE TAB_2 SET LOCATION 'hdfs://nn1.example.com/hive/warehouse'");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    connection = context.createConnection("user_2", "password");
    statement = context.createStatement(connection);
    try {
      statement.execute("CREATE EXTERNAL TABLE EXT_TAB_1(A STRING) STORED AS TEXTFILE LOCATION 'file:"+
        externalTblDir.getAbsolutePath() + "'");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = context.createConnection("hive", "hive");
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.execute("DROP DATABASE DB_2 CASCADE");
    statement.close();
    connection.close();
    context.close();
  }

  /**
   * Test privileges for 'use <db>'
   * Admin should be able to run use <db> with server level access
   * User with db level access should be able to run use <db>
   * User with table level access should be able to run use <db>
   * User with no access to that db objects, should NOT be able run use <db>
   * @throws Exception
   */
  @Test
  public void testUseDbPrivilege() throws Exception {
    context = createContext(testProperties);

    // groups : role -> group
    context.append("[groups]");
    context.append("admin = all_server");
    context.append("user_group1 = all_db1");
    context.append("user_group2 = select_db2");
    context.append("user_group3 = all_db3");
    // roles: privileges -> role
    context.append("[roles]");
    context.append("all_server = server=server1");
    context.append("all_db1 = server=server1->db=DB_1");
    context.append("select_db2 = server=server1->db=DB_2->table=tab_2->action=select");
    context.append("all_db3 = server=server1->db=DB_3");

    // users: users -> groups
    context.append("[users]");
    context.append("hive = admin");
    context.append("user_1 = user_group1");
    context.append("user_2 = user_group2");
    context.append("user_3 = user_group3");

    // setup db objects needed by the test
    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("use DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_2");
    statement.execute("use DB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    context.close();

    // user_1 should be able to connect db_1
    connection = context.createConnection("user_1", "hive");
    statement = context.createStatement(connection);
    statement.execute("use DB_1");
    context.close();

    // user_2 should not be able to connect db_1
    connection = context.createConnection("user_2", "hive");
    statement = context.createStatement(connection);
    try {
      statement.execute("use DB_1");
      assertFalse("User_2 shouldn't be able switch to db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.execute("use DB_2");
    context.close();

    // user_3 who is not listed in policy file should not be able to connect db_2
    connection = context.createConnection("user_3", "hive");
    statement = context.createStatement(connection);
    try {
      statement.execute("use DB_2");
      assertFalse("User_3 shouldn't be able switch to db_2", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();
  }

  /**
   * Test access to default DB with out of box authz config
   * All users should be able to switch to default, including the users that don't have any
   * privilege on default db objects via policy file
   * @throws Exception
   */
  @Test
  public void testDefaultDbPrivilege() throws Exception {
    context = createContext(testProperties);

    // groups : role -> group
    context.append("[groups]");
    context.append("admin = all_server");
    context.append("user_group1 = all_db1");
    context.append("user_group2 = select_db2");
    context.append("user_group3 = all_default");
    // roles: privileges -> role
    context.append("[roles]");
    context.append("all_server = server=server1");
    context.append("all_db1 = server=server1->db=DB_1");
    context.append("select_db2 = server=server1->db=DB_2->table=tab_2->action=select");
    context.append("all_default = server=server1->db=default");
    // users: users -> groups
    context.append("[users]");
    context.append("hive = admin");
    context.append("user_1 = user_group1");
    context.append("user_2 = user_group2");
    context.append("user_3 = user_group3");

    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection("user_1", "hive");
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection("user_2", "hive");
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection("user_3", "hive");
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();
  }

  /**
   * Test access to default DB with explicit privilege requirement
   * Admin should be able to run use default with server level access
   * User with db level access should be able to run use default
   * User with table level access should be able to run use default
   * User with no access to default db objects, should NOT be able run use default
   * @throws Exception
   */
  @Test
  public void testDefaultDbRestrictivePrivilege() throws Exception {
    testProperties.put(AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "true");
    context = createContext(testProperties);

    // groups : role -> group
    context.append("[groups]");
    context.append("admin = all_server");
    context.append("user_group1 = all_default");
    context.append("user_group2 = select_default");
    context.append("user_group3 = all_db1");
    // roles: privileges -> role
    context.append("[roles]");
    context.append("all_server = server=server1");
    context.append("all_default = server=server1->db=default");
    context.append("select_default = server=server1->db=default->table=tab_2->action=select");
    context.append("all_db1 = server=server1->db=DB_1");
    // users: users -> groups
    context.append("[users]");
    context.append("hive = admin");
    context.append("user_1 = user_group1");
    context.append("user_2 = user_group2");
    context.append("user_3 = user_group3");

    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection("user_1", "hive");
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection("user_2", "hive");
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection("user_3", "hive");
    statement = context.createStatement(connection);
    try {
      // user_3 doesn't have any implicit permission for default
      statement.execute("use default");
      assertFalse("User_3 shouldn't be able switch to default", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();
  }

}
