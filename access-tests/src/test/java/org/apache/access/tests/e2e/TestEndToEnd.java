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
import java.sql.Statement;
import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestEndToEnd {
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
   * 1. admin create a new experimental database
   * 2. admin create a new production database, create table, load data
   * 3. admin create new user group, and add user into it
   * 4. admin grant privilege all@'experimental database' to group
   * 5. user create table, load data in experimental DB
   * 6. user create view based on table in experimental DB
   * 7. admin create table (same name) in production DB
   * 8. admin grant read@productionDB.table to group
   *    admin grant select@productionDB.table to group
   * 9. user load data from experimental table to production table
   */
  @Test
  public void testEndToEnd1() throws Exception {
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = adminPri", "groups");
    editor.addPolicy("adminPri = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");

    String dbName1 = "db_1";
    String dbName2 = "productionDB";
    String tableName1 = "tb_1";
    String tableName2 = "tb_2";
    String viewName1 = "view_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    // 1
    assertTrue("DROP DATABASE fail", !statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE"));
    assertTrue("CREATE DATABASE fail", !statement.execute("CREATE DATABASE " + dbName1));
    // 2
    assertTrue("DROP DATABASE fail", !statement.execute("DROP DATABASE IF EXISTS " + dbName2 + " CASCADE"));
    assertTrue("CREATE DATABASE fail", !statement.execute("CREATE DATABASE " + dbName2));
    assertTrue("switch database fail", !statement.execute("USE " + dbName2));
    assertTrue("DROP TABLE fail", !statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName2));
    assertTrue("CREATE TABLE fail", !statement.execute("create table " + dbName2 + "." + tableName2
        + " (under_col int comment 'the under column', value string)"));
    assertTrue(
        "admin should be able to load data to table " + tableName2,
        !statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName2));
    statement.close();
    connection.close();

    // 3
    editor.addPolicy("user1 = group1", "users");

    // 4
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");

    // 5
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName1));
    assertTrue("DROP TABLE fail", !statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1));
    assertTrue("CREATE TABLE fail", !statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)"));
    assertTrue(
        "admin should be able to load data to table " + tableName1,
        !statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName1));
    // 6
    assertTrue("user1 should be able to create view view_1", !statement.execute("CREATE VIEW " + viewName1
        + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));
    statement.close();
    connection.close();

    // 7
    connection = context.createConnection("admin1", "foo");
    statement = context.createStatement(connection);
    assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName2));
    assertTrue("DROP TABLE fail", !statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1));
    assertTrue("CREATE TABLE fail", !statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)"));
    statement.close();
    connection.close();

    // 8
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("select_tb1 = server=server1->db=productionDB->table=tb_1->action=select","roles");
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("insert_tb1 = server=server1->db=productionDB->table=tb_1->action=insert","roles");

    // 9
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertTrue("user_1 should be able to switch to database db_1", !statement.execute("USE " + dbName2));
    assertTrue("load data between database fail", !statement.execute("INSERT OVERWRITE TABLE "
        + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1));
    statement.close();
    connection.close();
  }
}
