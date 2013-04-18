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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestMetadataObjectRetrieval {
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
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do show tables
   * 3. user_1 do show tables should fail
   * 4. grant insert@tb_1 to user_1, show tables fail
   * 5. grant read@tb_1 to user_1, show tables fail
   */
  @Test
  public void testShowTables1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    Assert.assertTrue(rs.next());
    assertTrue("admin should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();

    // 3,4,5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("user_1 should not be able to show tables", !statement.execute("SHOW TABLES"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy(
        "insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert",
        "roles");
    try {
      assertFalse("user_1 should not be able to show tables", !statement.execute("SHOW TABLES"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy(
        "select_tb1 = server=server1->db=db_1->table=tb_1->action=select",
        "roles");
    try {
      assertFalse("user_1 should not be able to show tables", !statement.execute("SHOW TABLES"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do show tables
   * 3. grant all@database to user_1, show table succeed
   */
  @Test
  public void testShowTables2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    Assert.assertTrue(rs.next());
    assertTrue("admin should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    rs = statement.executeQuery("SHOW TABLES");
    Assert.assertTrue(rs.next());
    assertTrue("user_1 should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and don't create any table
   * 2. admin can do show tables return 0 results
   * 3. grant all@database to user_1
   * 4. user_1 do show tables return 0 results, not error message
   */
  @Test
  public void testShowTables3() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    try {
      assertTrue("admin should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TABLES");
      Assert.assertFalse(rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    Assert.assertFalse(rs.next());
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do show tables
   * 3. grant select@view_1 to user_1, show table fail
   */
  @Test
  public void testShowTables4() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String viewName1 = "view_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    try {
      assertTrue("admin should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
      assertTrue(
          "admin should be able to load data to table tb_1",
          !statement.execute("load data local inpath '" + dataFile.getPath()
              + "' into table " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to drop view view_1", !statement.execute("DROP VIEW IF EXISTS " + viewName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to create view view_1", !statement.execute("CREATE VIEW " + viewName1
          + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TABLES");
      Assert.assertTrue(rs.next());
      assertTrue("admin should be able to retrieval table names", rs.getString(1)
          .equals(tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy(
        "select_view1 = server=server1->db=db_1->view=view_1->action=insert",
        "roles");
    try {
      assertFalse("user_1 should not be able to do show tables", !statement.execute("SHOW TABLES"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do show tables
   * 3. grant transfer@server to user_1, show table fail
   */
  @Test
  public void testShowTables5() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    Assert.assertTrue(rs.next());
    assertTrue("admin should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = transform_db1", "groups");
    editor.addPolicy("transform_db1 = server=server1->action=transform", "roles");
    try {
      assertFalse("user_1 should not be able to do show tables", !statement.execute("SHOW TABLES"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do describe table tb_1
   * 2.1 admin can do SHOW CREATE TABLE tb_1
   * 2.2 admin can do SHOW COLUMNS FROM TABLE
   * 2.3 admin can do DESCRIBE table column
   * 3. user_1 do describe table tb_1 should fail
   * 3.1 user_1 do SHOW CREATE TABLE tb_1 fail
   * 3.2 user_1 do SHOW COLUMNS FROM TABLE fail
   * 3.3 user_1 do describe table column fail
   * 4. grant insert@tb_1 to user_1, describe table tb_1 fail
   * 4.1 SHOW CREATE TABLE tb_1 fail
   * 4.2 SHOW COLUMNS FROM TABLE fail
   * 4.3 describe table column fail
   * 5. grant read@tb_1 to user_1, describe table tb_1 fail
   * 5.1 SHOW CREATE TABLE tb_1 fail
   * 5.2 SHOW COLUMNS FROM TABLE fail
   * 5.3 describe table column fail
   */
  @Test
  public void testTableMetaObjects1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " under_col");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " value");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertTrue("SHOW CREATE TABLE fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertTrue("SHOW TBLPROPERTIES fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3,4,5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("user_1 should not be able to describe table " + tableName1, !statement.execute("DESCRIBE " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE table column should fail", !statement.execute("DESCRIBE " + tableName1 + " under_col"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertFalse("SHOW CREATE TABLE should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertFalse("SHOW TBLPROPERTIES should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy(
        "insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert",
        "roles");
    try {
      assertFalse("user_1 should not be able to describe table " + tableName1, !statement.execute("DESCRIBE " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " under_col"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertFalse("SHOW CREATE TABLE should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertFalse("SHOW TBLPROPERTIES should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy(
        "select_tb1 = server=server1->db=db_1->table=tb_1->action=select",
        "roles");
    try {
      assertFalse("user_1 should not be able to describe table " + tableName1, !statement.execute("DESCRIBE " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " under_col"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertFalse("SHOW CREATE TABLE should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertFalse("SHOW TBLPROPERTIES should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do describe table
   * 2.1 admin can do SHOW CREATE TABLE tb_1
   * 2.3 admin can do SHOW COLUMNS FROM TABLE
   * 3. grant all@database to user_1, describe table succeed
   * 3.1 SHOW CRATE TABLE tb_1 should succeed
   * 3.2 SHOW COLUMNS FROM TABLE succeed
   * 3.3 describe table column succeed
   */
  @Test
  public void testTableMetaObjects2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int, value string)");
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " under_col");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " value");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertTrue("SHOW CREATE TABLE fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertTrue("SHOW TBLPROPERTIES fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " under_col");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " value");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertTrue("SHOW CREATE TABLE fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertTrue("SHOW TBLPROPERTIES fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do describe table
   * 2.1 admin can do SHOW CREATE TABLE tb_1
   * 2.2 admin can do SHOW COLUMNS FROM TABLE
   * 2.3 admin can do describe table column
   * 3. grant select@view_1 to user_1, describe table fail
   * 3.1 user_1 do SHOW CREATE TABLE tb_1 should fail
   * 3.2 user_1 do SHOW COLUMNS FROM TABLE should fail
   * 3.3 user_1 do describe table column should fail
   */
  @Test
  public void testTableMetaObjects4() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String viewName1 = "view_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    try {
      assertTrue("admin should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
      assertTrue(
          "admin should be able to load data to table tb_1",
          !statement.execute("load data local inpath '" + dataFile.getPath()
              + "' into table " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to drop view view_1", !statement.execute("DROP VIEW IF EXISTS " + viewName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to create view view_1", !statement.execute("CREATE VIEW " + viewName1
          + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " under_col");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " value");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertTrue("SHOW CREATE TABLE fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertTrue("SHOW TBLPROPERTIES fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy(
        "select_view1 = server=server1->db=db_1->view=view_1->action=insert",
        "roles");
    try {
      assertFalse("user_1 should not be able to describe table " + tableName1, !statement.execute("DESCRIBE " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " under_col"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertFalse("SHOW CREATE TABLE should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertFalse("SHOW TBLPROPERTIES should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do describe table
   * 2.1 admin can do SHOW CREATE TABLE tb_1
   * 2.2 admin can do SHOW COLUMNS FROM TABLE
   * 2.3 admin can do describe table column
   * 3. grant transfer@server to user_1, describe table fail
   * 3.1 user_1 do SHOW CREATE TABLE tb_1 should fail
   * 3.2 user_1 do SHOW COLUMNS FROM TABLE should fail
   * 3.3 user_1 do describe table column should fail
   */
  @Test
  public void testTableMetaObjects5() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " under_col");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("under_col"));
      assertTrue("describe table fail", rs.getString(2).equals("int"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE " + tableName1 + " value");
      Assert.assertTrue(rs.next());
      assertTrue("describe table fail", rs.getString(1).equals("value"));
      assertTrue("describe table fail", rs.getString(2).equals("string"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertTrue("show columns from fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertTrue("SHOW CREATE TABLE fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertTrue("SHOW TBLPROPERTIES fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = transform_db1", "groups");
    editor.addPolicy("transform_db1 = server=server1->action=transform", "roles");
    try {
      assertFalse("user_1 should not be able to describe table " + tableName1, !statement.execute("DESCRIBE " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " under_col"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertFalse("DESCRIBE TABLE COLUMN should fail", !statement.execute("DESCRIBE " + tableName1 + " value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName1);
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("under_col"));
      Assert.assertTrue(rs.next());
      assertFalse("show columns from should fail", rs.getString(1).equals("value"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + tableName1);
      assertFalse("SHOW CREATE TABLE should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW TBLPROPERTIES " + tableName1);
       assertFalse("SHOW TBLPROPERTIES should fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, create function
   * 2. admin can do show functions printf_test
   * 3. user_1 do show functions should fail
   * 4. grant insert@tb_1 to user_1, show tables pass
   * 5. delete insert@tab_1 and grant read@tb_1 to user_1, show functions fail
   */
  @Test
  public void testShowFunctions1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int, value string)");
    // it fail here now, all@server is not enough to drop function, need fix
    try {
      assertTrue("admin should have privilge to drop function printf_test", !statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should have privilege to create function", !statement.execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    ResultSet rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
    Assert.assertTrue(rs.next());
    assertTrue("admin should be able to retrieval function names", rs.getString(1)
        .equals("printf_test"));
    statement.close();
    connection.close();

    // 3,4,5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // it fail here now, no restricted privilege on 'show functions', need fix
    try {
      rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertTrue(rs.next());
      assertFalse("user_1 should not be able to retrieval function names", rs.getString(1)
          .equals("printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert","roles");
    try {
      rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertTrue(rs.next());
      assertFalse("user_1 should not be able to retrieval function names", rs.getString(1)
          .equals("printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select","roles");
    try {
      rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertTrue(rs.next());
      assertTrue("user_1 should be able to retrieval function names", rs.getString(1)
          .equals("printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
  * Steps:
  * 1. admin create db_1 and db_1.tb_1, create function
  * 2. admin can do show functions
  * 3. grant all@database to user_1, show functions succeed
  */
  @Test
  public void testShowFunctions2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int, value string)");
    try {
      assertTrue("admin should have privilge to drop function printf_test", !statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should have privilege to create function", !statement.execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertTrue(rs.next());
      assertTrue("user_1 should be able to retrieval function names", rs.getString(1)
          .equals("printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1, create table tb_1 but don't create any function
   * 2. admin can do show functions return 0 results
   * 3. grant all@database to user_1
   * 4. user_1 do show functions return 0 results, not error message
   */
  @Test
  public void testShowFunctions3() throws Exception {
   // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int, value string)");
    try {
      assertTrue("admin should have privilge to drop function printf_test", !statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertFalse(rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertFalse(rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do show functions
   * 3. grant select@view_1 to user_1, show functions fail
   */
  @Test
  public void testShowFunctions4() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String viewName1 = "view_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int, value string)");
    try {
      assertTrue("admin should be able to load data to table tb_1", !statement.execute("load data local inpath '" + dataFile.getPath()
          + "' into table " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to drop view view_1", !statement.execute("DROP VIEW IF EXISTS " + viewName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to create view view_1", !statement.execute("CREATE VIEW " + viewName1
          + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should have privilge to drop function printf_test", !statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should have privilege to create function", !statement.execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy(
        "select_view1 = server=server1->db=db_1->view=view_1->action=insert",
        "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertTrue(rs.next());
      assertFalse("user_1 should not be able to retrieval function names", rs.getString(1)
          .equals("printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do show functions
   * 3. grant transfer@server to user_1, show functions fail
   */
  @Test
  public void testShowFunctions5() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int, value string)");
    try {
      assertTrue("admin should have privilge to drop function printf_test", !statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should have privilege to create function", !statement.execute("CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = transform_db1", "groups");
    editor.addPolicy("transform_db1 = server=server1->action=transform", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW FUNCTIONS \"printf_test\"");
      Assert.assertTrue(rs.next());
      assertFalse("user_1 should not be able to retrieval function names", rs.getString(1)
          .equals("printf_test"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. all@server can do SHOW INDEX ON table FROM db_name
   * 2. all@database can do SHOW INDEX ON table FROM db_name
   * 3. select@table (index is based on table) can do SHOW INDEX ON table FROM db_name
   * 3.1 select@table (index is not based on table) cannot do SHOW INDEX ON table FROM db_name
   * 4. insert@table cannot do SHOW INDEX ON table FROM db_name
   * 5. select@view cannot do SHOW INDEX ON table FROM db_name
   * 6. transform@server cannot do SHOW INDEX ON table FROM db_name
   */
  @Test
  public void testShowIndexes1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String tableName2 = "tb_2";
    String viewName1 = "view_1";
    String indexName1 = "index_1";
    String indexName2 = "index_2";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    try {
      assertTrue("admin should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName2);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + dbName1 + "." + tableName2
        + " (under_col int comment 'the under column', value string)");
    try {
      assertTrue("admin should be able to load data to table tb_1", !statement.execute("load data local inpath '" + dataFile.getPath()
          + "' into table " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to load data to table tb_2", !statement.execute("load data local inpath '" + dataFile.getPath()
          + "' into table " + tableName2));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to drop view view_1", !statement.execute("DROP VIEW IF EXISTS " + viewName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to create view view_1", !statement.execute("CREATE VIEW " + viewName1
          + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to drop index", !statement.execute("DROP INDEX IF EXISTS " + indexName1 + " ON " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to drop index", !statement.execute("DROP INDEX IF EXISTS " + indexName2 + " ON " + tableName2));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to create index", !statement.execute("CREATE INDEX " + indexName1 + " ON TABLE " + tableName1 + "(value) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to create index", !statement.execute("CREATE INDEX " + indexName2 + " ON TABLE " + tableName2 + "(value) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // 1
    try {
      ResultSet rs = statement.executeQuery("SHOW INDEX ON " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW INDEX fail", rs.getString(1).trim().equals(indexName1));
      assertTrue("SHOW INDEX fail", rs.getString(2).trim().equals(tableName1));
      assertTrue("SHOW INDEX fail", rs.getString(3).trim().equals("value"));
      assertTrue("SHOW INDEX fail", rs.getString(4).trim().equals("db_1__tb_1_index_1__"));
      assertTrue("SHOW INDEX fail", rs.getString(5).trim().equals("compact"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW INDEX ON " + tableName2);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW INDEX fail", rs.getString(1).trim().equals(indexName2));
      assertTrue("SHOW INDEX fail", rs.getString(2).trim().equals(tableName2));
      assertTrue("SHOW INDEX fail", rs.getString(3).trim().equals("value"));
      assertTrue("SHOW INDEX fail", rs.getString(4).trim().equals("db_1__tb_2_index_2__"));
      assertTrue("SHOW INDEX fail", rs.getString(5).trim().equals("compact"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("admin should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // 2
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW INDEX ON " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW INDEX fail", rs.getString(1).trim().equals(indexName1));
      assertTrue("SHOW INDEX fail", rs.getString(2).trim().equals(tableName1));
      assertTrue("SHOW INDEX fail", rs.getString(3).trim().equals("value"));
      assertTrue("SHOW INDEX fail", rs.getString(4).trim().equals("db_1__tb_1_index_1__"));
      assertTrue("SHOW INDEX fail", rs.getString(5).trim().equals("compact"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("SHOW INDEX ON " + tableName2);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW INDEX fail", rs.getString(1).trim().equals(indexName2));
      assertTrue("SHOW INDEX fail", rs.getString(2).trim().equals(tableName2));
      assertTrue("SHOW INDEX fail", rs.getString(3).trim().equals("value"));
      assertTrue("SHOW INDEX fail", rs.getString(4).trim().equals("db_1__tb_2_index_2__"));
      assertTrue("SHOW INDEX fail", rs.getString(5).trim().equals("compact"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = all_db1");
    editor.removePolicy("all_db1 = server=server1->db=db_1");
    // 3
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW INDEX ON " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW INDEX fail", rs.getString(1).trim().equals(indexName1));
      assertTrue("SHOW INDEX fail", rs.getString(2).trim().equals(tableName1));
      assertTrue("SHOW INDEX fail", rs.getString(3).trim().equals("value"));
      assertTrue("SHOW INDEX fail", rs.getString(4).trim().equals("db_1__tb_1_index_1__"));
      assertTrue("SHOW INDEX fail", rs.getString(5).trim().equals("compact"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // 3.1
    try {
      assertFalse("SHOW INDEX should fail", statement.execute("SHOW INDEX ON " + tableName2));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = select_tb1");
    editor.removePolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select");
    // 4
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert", "roles");
    try {
      assertFalse("SHOW INDEX should fail", statement.execute("SHOW INDEX ON " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = insert_tb1");
    editor.removePolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert");
    // 5
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy("select_view1 = server=server1->db=db_1->table=view_1->action=select", "roles");
    try {
      assertFalse("SHOW INDEX should fail", statement.execute("SHOW INDEX ON " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = select_view1");
    editor.removePolicy("select_view1 = server=server1->db=db_1->table=view_1->action=select");
    // 6
    editor.addPolicy("group1 = transform_db_1", "groups");
    editor.addPolicy("transform_db_1 = server=server1->action=transform", "roles");
    try {
      assertFalse("SHOW INDEX should fail", statement.execute("SHOW INDEX ON " + tableName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = transform_db_1");
    editor.removePolicy("transform_db_1 = server=server1->action=transform");
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. all@server can do SHOW PARTITIONS
   * 2. all@database can do SHOW PARTITIONS
   * 3. select@table can do SHOW PARTITIONS
   * 4. insert@table cannot do SHOW PARTITIONS
   * 5. select@view cannot do SHOW PARTITIONS
   * 6. transform@server cannot do SHOW PARTITIONS
   */
  @Test
  public void testShowPartitions1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String viewName1 = "view_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    try {
      assertTrue("admin should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.execute("DROP TABLE IF EXISTS " + tableName1);
    statement.execute("create table " + tableName1
        + " (under_col int, value string) PARTITIONED BY (dt INT)");
    try {
      assertTrue("admin should be able to load data to table tb_1", !statement.execute("load data local inpath '" + dataFile.getPath()
          + "' into table " + tableName1 + " PARTITION (dt=3)"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to drop view view_1", !statement.execute("DROP VIEW IF EXISTS " + viewName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      assertTrue("admin should be able to create view view_1", !statement.execute("CREATE VIEW " + viewName1
          + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // 1
    try {
      ResultSet rs = statement.executeQuery("SHOW PARTITIONS " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW PARTITIONS fail", rs.getString(1).trim().equals("dt=3"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      assertTrue("admin should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // 2
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW PARTITIONS " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW PARTITIONS fail", rs.getString(1).trim().equals("dt=3"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = all_db1");
    editor.removePolicy("all_db1 = server=server1->db=db_1");
    // 3
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW PARTITIONS " + tableName1);
      Assert.assertTrue(rs.next());
      assertTrue("SHOW PARTITIONS fail", rs.getString(1).trim().equals("dt=3"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = select_tb1");
    editor.removePolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select");
    // 4
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW PARTITIONS " + tableName1);
      assertFalse("SHOW PARTITIONS fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = insert_tb1");
    editor.removePolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert");
    // 5
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy("select_view1 = server=server1->db=db_1->table=view_1->action=select", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW PARTITIONS " + tableName1);
      assertFalse("SHOW PARTITIONS fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = select_view1");
    editor.removePolicy("select_view1 = server=server1->db=db_1->table=view_1->action=select");
    // 6
    editor.addPolicy("group1 = transform_db_1", "groups");
    editor.addPolicy("transform_db_1 = server=server1->action=transform", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW PARTITIONS " + tableName1);
      assertFalse("SHOW PARTITIONS fail", rs.next());
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = transform_db_1");
    editor.removePolicy("transform_db_1 = server=server1->action=transform");
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do show/describe databases
   * 3. user_1 do show/describe databases should fail
   * 4. grant insert@tb_1 to user_1, show/describe databases fail
   * 5. grant read@tb_1 to user_1, show/describe databases fail
   */
  @Test
  public void testShowDatabases1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    boolean created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertTrue("database " + dbName1 + " is not created", created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe database fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3,4,5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertFalse("user_1 doesn't have privilege to retrieval database "
          + dbName1, created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertFalse("describe database should fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy(
        "insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert",
        "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertFalse("user_1 doesn't have privilege to retrieval database "
          + dbName1, created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertFalse("describe database should fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy(
        "select_tb1 = server=server1->db=db_1->table=tb_1->action=select",
        "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertFalse("user_1 doesn't have privilege to retrieval database "
          + dbName1, created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertFalse("describe database should fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do show/describe databases
   * 3. grant all@server to user_1, show/describe database succeed
   */
  @Test
  public void testShowDatabases2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertTrue("database " + dbName1 + " is not created", created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe database fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = all_server", "groups");
    editor.addPolicy("all_server = server=server1", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertTrue("user_1 cannot retrieval database " + dbName1, created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe database fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin doesn't create any database
   * 2. admin can do show databases, can only get 'default'
   * 2.1 describe database fail
   * 3. grant all@server to user_1
   * 4. user_1 do show databases can only get 'default', but no error message
   * 4.1 describe database fail
   */
  @Test
  public void testShowDatabases3() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals("default")) {
          created = true;
        }
      }
      assertTrue("cannot retrieval database default", created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertFalse("describe database should fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = all_server", "groups");
    editor.addPolicy("all_server = server=server1", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals("default")) {
          created = true;
        }
      }
      assertTrue("cannot retrieval database default", created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertFalse("describe database should fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do show/describe databases
   * 3. grant select@view_1 to user_1, show/describe databases fail
   */
  @Test
  public void testShowDatabases4() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    String viewName1 = "view_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    assertEquals(
        "user1 should be able to load data into table " + tableName1,
        statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName1));
    assertEquals("user1 should be able to drop view " + viewName1,
        statement.execute("DROP VIEW IF EXISTS " + viewName1));
    assertEquals(
        "user1 should be able to create view " + viewName1,
        statement.execute("CREATE VIEW " + viewName1
            + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertTrue("database " + dbName1 + " is not created", created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe database fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy(
        "select_view1 = server=server1->db=db_1->view=view_1->action=insert",
        "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertFalse("user_1 doesn't have privilege to retrieval database "
          + dbName1, created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertFalse("describe database should fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do show tables
   * 3. grant transfer@server to user_1, show table fail
   */
  @Test
  public void testShowDatabases5() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    // verify by SQL
    // 1, 2
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertTrue("database " + dbName1 + " is not created", created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertTrue("describe database fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = transform_db1", "groups");
    editor.addPolicy("transform_db1 = server=server1->action=transform", "roles");
    try {
      ResultSet rs = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (rs.next()) {
        if (rs.getString(1).equals(dbName1)) {
          created = true;
        }
      }
      assertFalse("user_1 doesn't have privilege to retrieval database "
          + dbName1, created);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      ResultSet rs = statement.executeQuery("DESCRIBE DATABASE " + dbName1);
      Assert.assertTrue(rs.next());
      assertFalse("describe database should fail",rs.getString(1).equals(dbName1));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }
}
