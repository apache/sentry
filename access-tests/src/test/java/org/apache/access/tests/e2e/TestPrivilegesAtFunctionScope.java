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
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestPrivilegesAtFunctionScope {
  private EndToEndTestContext context;
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataDir;
  private File dataFile;

  @Before
  public void setup() throws Exception {
    context = new EndToEndTestContext(new HashMap<String, String>());
    dataDir = context.getDataDir();
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

  @AfterClass
  public static void shutDown() throws IOException {
    EndToEndTestContext.shutdown();
  }

  /**
   * Steps: 1. admin create database DB_1, create table tb_1 in it 2. admin
   * doesn't grant any privilege to user_1 3. user_1 try to create function
   * printf_test, expect to fail 4. admin grant user_1 select privilege on tb_1
   * 5. user_1 try to create function printf_test, expect to fail 6. admin is
   * able to create function printf_test since it requires A@Server 7. user_1 is
   * able to show function and describe function printf_test 8. user_1 cannot
   * drop function printf_test 9. admin remove user_1 select privilege on tb_1,
   * show function and describe function should fail 11. admin is abel to dorp
   * function printf_test since it requires All@Server
   */
  @Test
  public void testFuncPrivileges1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1:*", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1);
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + dbName1 + "." + tableName1);
    statement.close();
    connection.close();

    // 2,3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertFalse(
          "user_1 should not have privilge to create function printf_test",
          statement
              .execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
      Assert.fail("No valid privileges");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    statement.close();
    connection.close();

    // 4
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("select_tb1 = server=server1:db=db_1:tb=tb_1:select",
        "roles");

    // 5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertFalse(
          "user_1 should not have privilge to create function printf_test",
          statement
              .execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
      statement.close();
      Assert.fail("No valid privileges");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    connection.close();

    // 6
    connection = context.createConnection("admin1", "foo");
    statement = context.createStatement(connection);
    assertTrue(
        "admin should have privilge to create function printf_test",
        statement
            .execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
    statement.close();
    connection.close();

    // 7,8
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertTrue("user_1 should have privilge to describe function printf_test",
        statement.execute("DESCRIBE FUNCTION printf_test"));
    assertTrue(
        "user_1 should have privilege to execute function printf_test",
        statement
            .execute("SELECT printf_test(\"Hello World %d %s\", 100, \"days\") FROM "
                + tableName1 + " LIMIT 1"));
    try {
      assertFalse(
          "user_1 should not have privilege to drop function printf_test",
          statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
      Assert.fail("No valid privileges");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    statement.close();
    connection.close();

    // 9
    editor.removePolicy("group1 = select_tb1");
    editor.removePolicy("select_tb1 = server=server1:db=db_1:tb=tb_1:select");
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertFalse(
          "user_1 should not have privilge to describe function printf_test",
          statement.execute("DESCRIBE FUNCTION printf_test"));
      Assert.fail("No valid privileges");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    try {
      assertFalse(
          "user_1 should not have privilege to execute function printf_test",
          statement
              .execute("SELECT printf_test(\"Hello World %d %s\", 100, \"days\") FROM "
                  + tableName1 + " limit 1"));
      Assert.fail("No valid privileges");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    statement.close();
    connection.close();

    // 10
    connection = context.createConnection("admin1", "foo");
    statement = context.createStatement(connection);
    assertTrue("admin should have privilge to drop function printf_test",
        statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
    statement.close();
    connection.close();
  }

  /**
   * table insert privilege is not enough for user to do
   * describe function and execute function
   * Step 4, privilege is insert instead of select
   * Step 7 and 8 expected to fail instead of success
   */
  @Test
  public void testFuncPrivileges2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1:*", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1);
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + dbName1 + "." + tableName1);
    statement.close();
    connection.close();

    // 4
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("insert_tb1 = server=server1:db=db_1:tb=tb_1:insert",
        "roles");

    // 5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertFalse(
          "user_1 should not have privilge to create function printf_test",
          statement
              .execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
      statement.close();
      Assert.fail("No valid privileges");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    connection.close();

    // 6
    connection = context.createConnection("admin1", "foo");
    statement = context.createStatement(connection);
    assertTrue(
        "admin should have privilge to create function printf_test",
        statement
            .execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
    statement.close();
    connection.close();

    // 7,8
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertFalse("user_1 should not have privilge to describe function printf_test",
        statement.execute("DESCRIBE FUNCTION printf_test"));
    assertFalse(
        "user_1 should not have privilege to execute function printf_test",
        statement
            .execute("SELECT printf_test(\"Hello World %d %s\", 100, \"days\") FROM "
                + tableName1 + " LIMIT 1"));
    try {
      assertFalse(
          "user_1 should not have privilege to drop function printf_test",
          statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
      Assert.fail("No valid privileges");
    } catch (SQLException e) {
      assertEquals("42000", e.getSQLState());
    }
    statement.close();
    connection.close();

    // 10
    connection = context.createConnection("admin1", "foo");
    statement = context.createStatement(connection);
    assertTrue("admin should have privilge to drop function printf_test",
        statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
    statement.close();
    connection.close();
  }
}
