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
    rs.next();
    System.out.println(rs.getString(1));
    assertTrue("admin should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();

    // 3,4,5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      statement.execute("USE " + dbName1);
      assertTrue("user_1 should be able to switch to database db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("SHOW TABLES");
      assertFalse("user_1 should not be able to show tables", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy(
        "insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert",
        "roles");
    try {
      statement.execute("SHOW TABLES");
      assertFalse("user_1 should not be able to show tables", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy(
        "select_tb1 = server=server1->db=db_1->table=tb_1->action=select",
        "roles");
    try {
      statement.execute("SHOW TABLES");
      assertFalse("user_1 should not be able to show tables", false);
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
    rs.next();
    assertTrue("admin should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      statement.execute("USE " + dbName1);
      assertTrue("user_1 should be able to switch to database db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    rs = statement.executeQuery("SHOW TABLES");
    rs.next();
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
      statement.execute("USE " + dbName1);
      assertTrue("admin should be able to switch to database db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    rs.next();
    assertFalse(
        "admin should be able to retrieval tables, but result set is empty",
        rs.next());
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      statement.execute("USE " + dbName1);
      assertTrue("user_1 should be able to switch to database db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    rs = statement.executeQuery("SHOW TABLES");
    rs.next();
    assertFalse(
        "user_1 should be able to retrieval tables, but result set is empty",
        rs.next());
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
      statement.execute("USE " + dbName1);
      assertTrue("admin should be able to switch to database db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    try {
      statement.execute("load data local inpath '" + dataFile.getPath()
          + "' into table " + tableName1);
      assertTrue("admin should be able to load data to table tb_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("DROP VIEW IF EXISTS " + viewName1);
      assertTrue("admin should be able to drop view view_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("CREATE VIEW " + viewName1
          + " (value) AS SELECT value from " + tableName1 + " LIMIT 10");
      assertTrue("admin should be able to create view view_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    rs.next();
    assertTrue("admin should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      statement.execute("USE " + dbName1);
      assertTrue("user_1 should be able to switch to database db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy(
        "select_view1 = server=server1->db=db_1->view=view_1->action=insert",
        "roles");
    try {
      statement.execute("SHOW TABLES");
      assertFalse("user_1 should not be able to do show tables", false);
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
    rs.next();
    assertTrue("admin should be able to retrieval table names", rs.getString(1)
        .equals(tableName1));
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    try {
      statement.execute("USE " + dbName1);
      assertTrue("user_1 should be able to switch to database db_1", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.addPolicy("group1 = transform_db1", "groups");
    editor.addPolicy("transform_db1 = server=server1->action=transform", "roles");
    try {
      statement.execute("SHOW TABLES");
      assertFalse("user_1 should not be able to do show tables", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do show databases
   * 3. user_1 do show databases should fail
   * 4. grant insert@tb_1 to user_1, show databases fail
   * 5. grant read@tb_1 to user_1, show databases fail
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
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    boolean created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertTrue("database " + dbName1 + " is not created", created);
    statement.close();
    connection.close();

    // 3,4,5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    rs = statement.executeQuery("SHOW DATABASES");
    created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertFalse("user_1 doesn't have privilege to retrieval database " + dbName1,
        created);
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy(
        "insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert",
        "roles");
    rs = statement.executeQuery("SHOW DATABASES");
    created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertFalse("user_1 doesn't have privilege to retrieval database " + dbName1,
        created);
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy(
        "select_tb1 = server=server1->db=db_1->table=tb_1->action=select",
        "roles");
    rs = statement.executeQuery("SHOW DATABASES");
    created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertFalse("user_1 doesn't have privilege to retrieval database " + dbName1,
        created);
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin can do show databases
   * 3. grant all@server to user_1, show table succeed
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
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    boolean created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertTrue("database " + dbName1 + " is not created", created);
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = all_server", "groups");
    editor.addPolicy("all_server = server=server1", "roles");
    rs = statement.executeQuery("SHOW DATABASES");
    created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertTrue("user_1 cannot retrieval database " + dbName1, created);
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin doesn't create any database
   * 2. admin can do show databases, can only get 'default'
   * 3. grant all@server to user_1
   * 4. user_1 do show databases can only get 'default', but no error message
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
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    boolean created = false;
    while (rs.next()) {
      if (rs.getString(1).equals("default")) {
        created = true;
      }
    }
    assertTrue("cannot retrieval database default", created);
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = all_server", "groups");
    editor.addPolicy("all_server = server=server1", "roles");
    rs = statement.executeQuery("SHOW DATABASES");
    created = false;
    while (rs.next()) {
      if (rs.getString(1).equals("default")) {
        created = true;
      }
    }
    assertTrue("cannot retrieval database default", created);
    statement.close();
    connection.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1, and db_1.view_1
   * 2. admin can do show databases
   * 3. grant select@view_1 to user_1, show databases fail
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
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    boolean created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertTrue("database " + dbName1 + " is not created", created);
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy(
        "select_view1 = server=server1->db=db_1->view=view_1->action=insert",
        "roles");
    rs = statement.executeQuery("SHOW DATABASES");
    created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertFalse("user_1 doesn't have privilege to retrieval database " + dbName1,
        created);
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
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    boolean created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertTrue("database " + dbName1 + " is not created", created);
    statement.close();
    connection.close();

    // 3
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    editor.addPolicy("group1 = transform_db1", "groups");
    editor.addPolicy("transform_db1 = server=server1->action=transform", "roles");
    rs = statement.executeQuery("SHOW DATABASES");
    created = false;
    while (rs.next()) {
      if (rs.getString(1).equals(dbName1)) {
        created = true;
      }
    }
    assertFalse("user_1 doesn't have privilege to retrieval database " + dbName1,
        created);
    statement.close();
    connection.close();
  }
}
