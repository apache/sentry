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

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

/**
 * Metadata tests for show tables and show databases.  * Unlike rest of the access privilege
 * validation which is handled in semantic hooks, these statements are validaed via a
 * runtime fetch hook
 */
public class TestRuntimeMetadataRetrieval extends
    AbstractTestWithStaticHiveServer {
  private Context context;
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataDir;
  private File dataFile;

  @Before
  public void setup() throws Exception {
    context = createContext();
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


  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin should see all tables
   * 3. user_1 should only see the tables it has any level of privilege
   */
  @Test
  public void testShowTables1() throws Exception {
    String dbName1 = "db_1";
    // tables visible to user1 (not access to tb_4
    String tableNames[] = { "tb_1", "tb_2", "tb_3", "tb_4" };

    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("user_group = tab1_priv,tab2_priv,tab3_priv", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("tab1_priv = server=server1->db=" + dbName1 + "->table=" + tableNames[0] + "->action=select", "roles");
    editor.addPolicy("tab2_priv = server=server1->db=" + dbName1 + "->table=" + tableNames[1] + "->action=insert", "roles");
    editor.addPolicy("tab3_priv = server=server1->db=" + dbName1 + "->table=" + tableNames[2] + "->action=select", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = user_group", "users");

    String user1_tableNames[] = { "tb_1", "tb_2", "tb_3"};

    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    createTabs(statement, dbName1, tableNames);
    // Admin should see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    validateTables(rs, dbName1, tableNames);
    statement.close();
    context.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES");
    validateTables(rs, dbName1, user1_tableNames);
    statement.close();
    context.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and tables
   * 2. admin should see all tables
   * 3. user_1 should only see the all tables with db level privilege
   */
  @Test
  public void testShowTables2() throws Exception {
    String dbName1 = "db_1";
    // tables visible to user1 (not access to tb_4
    String tableNames[] = { "tb_1", "tb_2", "tb_3", "tb_4" };

    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("user_group = db_priv", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("db_priv = server=server1->db=" + dbName1, "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = user_group", "users");

    String user1_tableNames[] = { "tb_1", "tb_2", "tb_3", "tb_4" };

    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    createTabs(statement, dbName1, tableNames);
    // Admin should see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    validateTables(rs, dbName1, tableNames);
    statement.close();
    context.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES");
    validateTables(rs, dbName1, user1_tableNames);
    statement.close();
    context.close();
  }


  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin should see all tables with given pattern
   * 3. user_1 should only see the tables with given pattern it has any level of privilege
   */
  @Test
  public void testShowTables3() throws Exception {
    String dbName1 = "db_1";
    // tables visible to user1 (not access to tb_4
    String tableNames[] = { "tb_1", "tb_2", "tb_3", "newtab_3"};

    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("user_group = tab_priv", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("tab_priv = server=server1->db=" + dbName1 + "->table=" + tableNames[3] + "->action=insert", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = user_group", "users");

    String admin_tableNames[] = {"tb_3", "newtab_3"};
    String user1_tableNames[] = {"tb_3"};

    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    createTabs(statement, dbName1, tableNames);
    // Admin should see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES *3");
    validateTables(rs, dbName1, admin_tableNames);
    statement.close();
    context.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES *3");
    validateTables(rs, dbName1, user1_tableNames);
    statement.close();
    context.close();
  }

  /**
   * Steps:
   * 1. admin create db_1 and db_1.tb_1
   * 2. admin should see all tables with given pattern
   * 3. user_1 should only see the tables with given pattern with db level privilege
   */
  @Test
  public void testShowTables4() throws Exception {
    String dbName1 = "db_1";
    // tables visible to user1 (not access to tb_4
    String tableNames[] = { "tb_1", "tb_2", "tb_3", "newtab_3"};

    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("user_group = tab_priv", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("tab_priv = server=server1->db=" + dbName1, "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = user_group", "users");

    String admin_tableNames[] = {"tb_3", "newtab_3"};
    String user1_tableNames[] = {"tb_3", "newtab_3"};

    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    createTabs(statement, dbName1, tableNames);
    // Admin should see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES *3");
    validateTables(rs, dbName1, admin_tableNames);
    statement.close();
    context.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES *3");
    validateTables(rs, dbName1, user1_tableNames);
    statement.close();
    context.close();
  }

  /**
   * Steps:
   * 1. admin tables in default db
   * 3. user_1 shouldn't see any tables when it doesn't have any privilege on default
   */
  @Test
  public void testShowTables5() throws Exception {
    String tableNames[] = { "tb_1", "tb_2", "tb_3", "tb_4" };

    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("user_group = db_priv", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");

    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    createTabs(statement, "default", tableNames);
    context.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    // User1 should see tables with any level of access
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    // user1 doesn't have access to any tables in default db
    Assert.assertFalse(rs.next());
    statement.close();
    context.close();
  }

  /**
   * Steps:
   * 1. admin create few dbs
   * 2. admin can do show databases
   * 3. users with db level permissions should should only those dbs on 'show database'
   */
  @Test
  public void testShowDatabases1() throws Exception {
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("group1 = db1_all", "groups");
    editor.addPolicy("db1_all = server=server1->db=db_1", "roles");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");
    String[] dbNames = { "db_1", "db_2", "db_3" };
    String[] user1_dbNames = { "db_1"};

    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    createDBs(statement, dbNames); // create all dbs
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    validateDBs(rs, dbNames); // admin should see all dbs
    rs.close();
    context.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    createDBs(statement, dbNames); // create all dbs
    rs = statement.executeQuery("SHOW DATABASES");
    validateDBs(rs, user1_dbNames); // user should see only dbs with access
    rs.close();
    context.close();
  }

  /**
   * Steps:
   * 1. admin create few dbs
   * 2. admin can do show databases
   * 3. users with table level permissions should should only those parent dbs on 'show database'
   */
  @Test
  public void testShowDatabases2() throws Exception {
    File policyFile = context.getPolicyFile();
    String[] dbNames = { "db_1", "db_2", "db_3" };
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("group1 = db1_tab,db2_tab", "groups");
    editor.addPolicy("db1_tab = server=server1->db=db_1->table=tb_1->action=select", "roles");
    editor.addPolicy("db2_tab = server=server1->db=db_2->table=tb_1->action=insert", "roles");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");
    String[] user1_dbNames = { "db_1", "db_2" };

    // verify by SQL
    // 1, 2
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    createDBs(statement, dbNames); // create all dbs
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    validateDBs(rs, dbNames); // admin should see all dbs
    rs.close();
    context.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    createDBs(statement, dbNames); // create all dbs
    rs = statement.executeQuery("SHOW DATABASES");
    validateDBs(rs, user1_dbNames); // user should see only dbs with access
    rs.close();
    context.close();
  }


  // create given dbs
  private void createDBs(Statement statement, String dbNames[])
      throws SQLException {
    for (String dbName : dbNames) {
      statement.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
      statement.execute("CREATE DATABASE " + dbName);
    }
  }

  // compare the table resultset with given array of table names
  private void validateDBs(ResultSet rs, String dbNames[])
        throws SQLException {
    for (String dbName : dbNames) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(rs.getString(1), dbName);
    }
    Assert.assertFalse(rs.next());
    rs.close();
  }

  // Create the give tables
  private void createTabs(Statement statement, String dbName, String tableNames[])
        throws SQLException {
    for (String tabName : tableNames) {
      statement.execute("DROP TABLE IF EXISTS " + dbName + "." + tabName);
      statement.execute("create table " + dbName + "." + tabName
          + " (under_col int comment 'the under column', value string)");
    }
  }

  // compare the table resultset with given array of table names
  private void validateTables(ResultSet rs, String dbName, String tableNames[])
        throws SQLException {
    for (String tabName : tableNames) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(rs.getString(1), tabName);
    }
    Assert.assertFalse(rs.next());
    rs.close();
  }

}
