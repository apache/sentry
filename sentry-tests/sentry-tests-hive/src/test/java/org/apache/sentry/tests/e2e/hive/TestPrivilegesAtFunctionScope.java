/*
printf_test_3 * Licensed to the Apache Software Foundation (ASF) under one or more
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.security.CodeSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.sentry.core.common.utils.PolicyFile;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPrivilegesAtFunctionScope extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
          .getLogger(TestPrivilegesAtFunctionScope.class);

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataDir;
  private File dataFile;
  private PolicyFile policyFile;
  private final String tableName1 = "tb_1";

  @Before
  public void setup() throws Exception {
    dataDir = context.getDataDir();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
  }

  /**
   * admin should be able to create/drop temp functions
   * user with db level access should be able to create/drop temp functions
   * user with table level access should be able to create/drop temp functions
   * user with no privilege should NOT be able to create/drop temp functions
   */
  private void verifyPrintFuncValues(Statement statement, String qry) throws Exception {
    ResultSet res = execQuery(statement, qry);
    if (res.next()) {
      String value = res.getString(1);
      LOGGER.info("Function returns: " + value);
      assertEquals("test1", value);
    }
    res.close();
  }

  private void setUpContext() throws Exception {
    String udfClassName = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf";
    CodeSource udfSrc = Class.forName(udfClassName).getProtectionDomain().getCodeSource();
    String udfLocation = System.getProperty(EXTERNAL_HIVE_LIB);
    if(udfLocation == null) {
      udfLocation = udfSrc.getLocation().getPath();
    }
    String udf1ClassName = "org.apache.sentry.tests.e2e.hive.TestUDF";
    CodeSource udf1Src = Class.forName(udf1ClassName).getProtectionDomain().getCodeSource();
    String udf1Location = udf1Src.getLocation().getPath();
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("USE " + DB1);
    statement.execute("create table " + DB1 + "." + tableName1
        + " (number INT comment 'column as a number', value STRING comment 'column as a string')");
    statement.execute("INSERT INTO TABLE " + DB1 + "." + tableName1 + " VALUES (1, 'test1')");
    statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test");
    statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test_2");
    context.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all", "UDF1_JAR", "UDF_JAR", "data_read")
        .addRolesToGroup(USERGROUP2, "db1_tab1", "UDF_JAR")
        .addRolesToGroup(USERGROUP3, "db1_tab1")
        .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
        .addPermissionsToRole("db1_tab1", "server=server1->db=" + DB1 + "->table=" + tableName1)
        .addPermissionsToRole("UDF_JAR", "server=server1->uri=file://" + udfLocation)
        .addPermissionsToRole("UDF1_JAR", "server=server1->uri=file://" + udf1Location)
        .addPermissionsToRole("data_read", "server=server1->URI=" + "file:///tmp");
    writePolicyFile(policyFile);
  }

  /**
   * Test the required permission to create/drop permanent function.
   * @throws Exception
   */
  @Test
  public void testCreateDropPermUdf() throws Exception {
    setUpContext();
    // user1 has URI privilege for the Jar and should be able create the perm function.
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);

    statement.execute("CREATE FUNCTION db_2.test_1 AS 'org.apache.sentry.tests.e2e.hive.TestUDF'");
    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);

    // user3 does not have URI privilege but still should be able drop the perm function.
    statement.execute("DROP FUNCTION IF EXISTS db_2.test_1");
    statement.close();
    connection.close();
  }

  /**
   * Test the required permission to create/drop temporary function.
   * @throws Exception
   */
  @Test
  public void testCreateDropTempUdf() throws Exception {
    setUpContext();
    // user1 should be able create/drop temp functions
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    statement.execute("CREATE TEMPORARY FUNCTION test_1 AS 'org.apache.sentry.tests.e2e.hive.TestUDF'");
    statement.close();
    connection.close();

    // user3 shouldn't be able to create/drop temp functions since it doesn't have permission for jar
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    try {
      statement.execute("CREATE TEMPORARY FUNCTION test_2 AS 'org.apache.sentry.tests.e2e.hive.TestUDF'");
      assertFalse("CREATE TEMPORARY FUNCTION should fail for user3", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);

    // user3 does not have URI privilege but still should be able drop the temp function.
    statement.execute("DROP FUNCTION IF EXISTS test_2");
  }

  @Test
  public void testAndVerifyFuncPrivilegesPart1() throws Exception {
    setUpContext();
    // user1 should be able create/drop temp functions
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    LOGGER.info("Testing select from temp func printf_test");
    try {
      statement.execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      verifyPrintFuncValues(statement, "SELECT printf_test('%s', value) FROM " + tableName1);
    } catch (Exception ex) {
      LOGGER.error("test temp func printf_test failed with reason: " + ex.getStackTrace() + " " + ex.getMessage());
      fail("fail to test temp func printf_test");
    }

    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    // user3 only has db1_tab1 privilege but still should be able execute the temp function.
    try {
      verifyPrintFuncValues(statement, "SELECT printf_test('%s', value) FROM " + tableName1);
    } catch (Exception ex) {
      LOGGER.error("test temp func printf_test failed with reason: " + ex.getStackTrace() + " " + ex.getMessage());
      fail("fail to test temp func printf_test");
    }

    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test");

    LOGGER.info("Testing select from perm func printf_test_perm");
    try {
      statement.execute(
          "CREATE FUNCTION printf_test_perm AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf' ");
      verifyPrintFuncValues(statement, "SELECT printf_test_perm('%s', value) FROM " + tableName1);
    } catch (Exception ex) {
      LOGGER.error("test perm func printf_test_perm failed with reason: " + ex.getStackTrace() + " " + ex.getMessage());
      fail("Fail to test perm func printf_test_perm");
    }

    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    // user3 only has db1_tab1 privilege but still should be able execute the perm function.
    try {
      verifyPrintFuncValues(statement, "SELECT printf_test_perm('%s', value) FROM " + tableName1);
    } catch (Exception ex) {
      LOGGER.error("test perm func printf_test_perm failed with reason: " + ex.getStackTrace() + " " + ex.getMessage());
      fail("Fail to test perm func printf_test_perm");
    }

    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("DROP FUNCTION IF EXISTS printf_test_perm");

    // test perm UDF with 'using file' syntax
    LOGGER.info("Testing select from perm func printf_test_perm_use_file");
    try {
      statement
          .execute("CREATE FUNCTION printf_test_perm_use_file AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf' "
              + " using file 'file:///tmp'");
    } catch (Exception ex) {
      LOGGER.error("test perm func printf_test_perm_use_file failed with reason: "
          + ex.getStackTrace() + " " + ex.getMessage());
      fail("Fail to test perm func printf_test_perm_use_file");
    } finally {
      statement.execute("DROP FUNCTION IF EXISTS printf_test_perm_use_file");
    }
    context.close();
  }

  @Test
  public void testAndVerifyFuncPrivilegesPart2() throws Exception {
    setUpContext();
    // user2 has select privilege on one of the tables in db1, should be able create/drop temp functions
    Connection connection = context.createConnection(USER2_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    try {
      statement.execute(
          "CREATE TEMPORARY FUNCTION printf_test_2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      verifyPrintFuncValues(statement, "SELECT printf_test_2('%s', value) FROM " + tableName1);
    } catch (Exception ex) {
      LOGGER.error("test perm func printf_test_2 failed with reason: "
          + ex.getStackTrace() + " " + ex.getMessage());
      fail("Fail to test temp func printf_test_2");
    } finally {
      statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test_2");
    }

    try {
      statement.execute(
          "CREATE FUNCTION " + DB1 + ".printf_test_2_perm AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      verifyPrintFuncValues(statement, "SELECT printf_test_2_perm('%s', value) FROM " + tableName1);
    } catch (Exception ex) {
      LOGGER.error("test perm func printf_test_2_perm failed with reason: "
          + ex.getStackTrace() + " " + ex.getMessage());
      fail("Fail to test temp func printf_test_2_perm");
    } finally {
      statement.execute("DROP FUNCTION IF EXISTS printf_test_2_perm");
    }

    // USER2 doesn't have URI perm on dataFile
    try {
      statement
          .execute("CREATE FUNCTION "
              + DB1
              + ".printf_test_2_perm AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"
              + " using file '" + "file://" + dataFile.getPath() + "'");
      assertFalse("CREATE TEMPORARY FUNCTION should fail for user3", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();
  }

  @Test
  public void testAndVerifyFuncPrivilegesPart3() throws Exception {
    setUpContext();
    // user3 shouldn't be able to create/drop temp functions since it doesn't have permission for jar
    Connection connection = context.createConnection(USER3_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    try {
      statement.execute(
          "CREATE TEMPORARY FUNCTION printf_test_bad AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      assertFalse("CREATE TEMPORARY FUNCTION should fail for user3", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      statement.execute(
          "CREATE FUNCTION printf_test_perm_bad AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      assertFalse("CREATE FUNCTION should fail for user3", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();
    // user4 (not part of any group ) shouldn't be able to create/drop temp functions
    connection = context.createConnection(USER4_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("USE default");
      statement.execute(
          "CREATE TEMPORARY FUNCTION printf_test_bad AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      assertFalse("CREATE TEMPORARY FUNCTION should fail for user4", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();
  }

  @Test
  public void testUdfWhiteList () throws Exception {
    String tableName1 = "tab1";

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("create table " + tableName1
        + " (under_col int comment 'the under column', value string)");

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all", "UDF_JAR")
        .addRolesToGroup(USERGROUP2, "db1_tab1", "UDF_JAR")
        .addRolesToGroup(USERGROUP3, "db1_tab1")
        .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
        .addPermissionsToRole("db1_tab1", "server=server1->db=" + DB1 + "->table=" + tableName1)
        .addPermissionsToRole("UDF_JAR", "server=server1->uri=file://${user.home}/.m2");
    writePolicyFile(policyFile);

    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE "
        + DB1 + "." + tableName1);
    statement.execute("SELECT rand(), concat(value, '_foo') FROM " + tableName1);

    context.assertAuthzException(statement,
        "SELECT  reflect('java.net.URLDecoder', 'decode', 'http://www.apache.org', 'utf-8'), value FROM " + tableName1);
    context.assertAuthzException(statement,
        "SELECT  java_method('java.net.URLDecoder', 'decode', 'http://www.apache.org', 'utf-8'), value FROM " + tableName1);
    statement.close();
    connection.close();
  }

  /**
   * User with db level access should be able to create/alter tables with buildin Serde.
   */
  @Test
  public void testSerdePrivileges() throws Exception {
    String tableName1 = "tab1";
    String tableName2 = "tab2";

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);

    context.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all")
        .addPermissionsToRole("db1_all", "server=server1->db=" + DB1);
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("create table " + DB1 + "." + tableName1
        + " (a string, b string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "
        + " STORED AS TEXTFILE");

    statement.execute("create table " + DB1 + "." + tableName2 + " (a string, b string)");
    statement.execute("alter table " + DB1 + "." + tableName2
        + " SET SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'");

    context.close();
  }
}
