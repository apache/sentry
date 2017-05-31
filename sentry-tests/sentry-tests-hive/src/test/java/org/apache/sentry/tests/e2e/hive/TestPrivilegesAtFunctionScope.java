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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.security.CodeSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.Ignore;
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
  @Ignore("Once CDH-54745 is fixed, enable this test again")
  @Test
  public void testFuncPrivileges1() throws Exception {
    String tableName1 = "tb_1";
    String udfClassName = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf";
    CodeSource udfSrc = Class.forName(udfClassName).getProtectionDomain().getCodeSource();
    String udfLocation = System.getProperty(EXTERNAL_HIVE_LIB);
    if(udfLocation == null) {
      udfLocation = udfSrc.getLocation().getPath();
    }
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("create table " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE "
        + DB1 + "." + tableName1);
    statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test");
    statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test_2");
    context.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all", "UDF_JAR", "data_read")
        .addRolesToGroup(USERGROUP2, "db1_tab1", "UDF_JAR")
        .addRolesToGroup(USERGROUP3, "db1_tab1")
        .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
        .addPermissionsToRole("db1_tab1", "server=server1->db=" + DB1 + "->table=" + tableName1)
        .addPermissionsToRole("UDF_JAR", "server=server1->uri=file://" + udfLocation)
        .addPermissionsToRole("data_read", "server=server1->URI=" + "file:///tmp");
    writePolicyFile(policyFile);

    // user1 should be able create/drop temp functions
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    try {
      statement.execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      LOGGER.info("Testing select from temp func printf_test");
      ResultSet res = statement.executeQuery("SELECT printf_test('%d', under_col) FROM " + tableName1);
      while (res.next()) {
        LOGGER.info(res.getString(1));
      }
      res.close();
      statement.execute("DROP TEMPORARY FUNCTION printf_test");
    } catch (Exception ex) {
      LOGGER.error("test temp func printf_test failed with reason: " + ex.getStackTrace() + " " + ex.getMessage());
      fail("fail to test temp func printf_test");
    }

    statement.execute(
        "CREATE FUNCTION printf_test_perm AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf' ");
    statement.execute("SELECT printf_test_perm(value) FROM " + tableName1);
    statement.execute("DROP FUNCTION printf_test_perm");

    // test perm UDF with 'using file' syntax
    statement
        .execute("CREATE FUNCTION printf_test_perm AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf' "
            + " using file 'file://" + udfLocation + "'");
    statement.execute("DROP FUNCTION printf_test_perm");

    context.close();

    // user2 has select privilege on one of the tables in db1, should be able create/drop temp functions
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute(
        "CREATE TEMPORARY FUNCTION printf_test_2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
    statement.execute("SELECT printf_test_2(value) FROM " + tableName1);
    statement.execute("DROP TEMPORARY FUNCTION printf_test_2");

    statement.execute(
        "CREATE FUNCTION " + DB1 + ".printf_test_2_perm AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
    statement.execute("SELECT printf_test_2_perm(value) FROM " + tableName1);
    statement.execute("DROP FUNCTION printf_test_2_perm");

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

    // user3 shouldn't be able to create/drop temp functions since it doesn't have permission for jar
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
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

  /**
   * Test create function using jar functionality
   * @throws Exception
   */
  @Test
  public void testAndVerifyFuncPrivilegesPart4() throws Exception {
    String tableName1 = "tb_1";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("create table " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("USE " + DB1);
    context.close();

    String udfLocation = new File("../data/xudf.jar").getCanonicalPath();
    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all", "UDF2_JAR", "data_read")
        .addRolesToGroup(USERGROUP2, "db1_tab1")
        .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
        .addPermissionsToRole("db1_tab1", "server=server1->db=" + DB1 + "->table=" + tableName1)
        .addPermissionsToRole("UDF2_JAR", "server=server1->uri=file://" + udfLocation)
        .addPermissionsToRole("data_read", "server=server1->URI=" + "file:///tmp");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    // USER1 has URI perm on jarFiles
    try {

      statement
          .execute("CREATE FUNCTION "
              + DB1
              + ".xadd AS 'xudf.XAdd'"
              + " using jar 'file://" + udfLocation + "'");
      ResultSet rs = statement.executeQuery("select xadd(1)");
      assertTrue(rs.next());
      assertTrue(rs.getInt(1) == 1);
    } catch (SQLException e) {
      assertFalse("CREATE FUNCTION should succeed for user1:" + e, true);
    } finally {
      connection.close();
    }

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);

    // USER2 doesn't have URI perm on jarFiles
    try {
      statement
          .execute("CREATE FUNCTION "
              + DB1
              + ".xadd AS 'xudf.XAdd'"
              + " using jar 'file://" + udfLocation + "'");
      assertFalse("CREATE FUNCTION should fail for user2", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    } finally {
      connection.close();
    }
    context.close();
  }

  @Test
  public void testUdfWhiteList () throws Exception {
    String tableName1 = "tab1";

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all", "UDF_JAR")
        .addRolesToGroup(USERGROUP2, "db1_tab1", "UDF_JAR")
        .addRolesToGroup(USERGROUP3, "db1_tab1")
        .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
        .addPermissionsToRole("db1_tab1", "server=server1->db=" + DB1 + "->table=" + tableName1)
        .addPermissionsToRole("UDF_JAR", "server=server1->uri=file://${user.home}/.m2");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("create table " + tableName1
        + " (under_col int comment 'the under column', value string)");
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
}
