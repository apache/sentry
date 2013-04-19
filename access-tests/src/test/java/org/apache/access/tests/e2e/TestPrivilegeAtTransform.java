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

public class TestPrivilegeAtTransform {
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
   * 1. admin create database, create table, load data into it
   * 2. all@server can issue transforms command
   * 3. all@database cannot issue transform command
   * 4. insert@table select@table cannot issue transform command
   * 5. select@view cannot issue transform command
   * 6. transform@server can issue the transform command
   */
  @Test
  public void testTransform1() throws Exception {
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
    assertTrue("admin should be able to switch to database db_1",
        !statement.execute("USE " + dbName1));
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int, value string)");
    assertTrue(
        "admin should be able to load data to table tb_1",
        !statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName1));
    assertTrue("admin should be able to drop view view_1",
        !statement.execute("DROP VIEW IF EXISTS " + viewName1));

    assertTrue(
        "admin should be able to create view view_1",
        !statement.execute("CREATE VIEW " + viewName1
            + " (value) AS SELECT value from " + tableName1 + " LIMIT 10"));

    ResultSet rs = statement
        .executeQuery("select TRANSFORM(a.under_col, a.value) USING 'cat' AS (tunder_col, tvalue) FROM "
            + tableName1 + " a");
    assertTrue("TRANSFORM fail", rs.next());

    statement.close();
    connection.close();

    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);

    assertTrue("user_1 should be able to switch to database db_1",
        !statement.execute("USE " + dbName1));
    // 3
    editor.addPolicy("group1 = all_db1", "groups");
    editor.addPolicy("all_db1 = server=server1->db=db_1", "roles");
    try {
      assertFalse(
          "TRANSFORM fail",
          statement
              .execute("select TRANSFORM(a.under_col, a.value) USING 'cat' AS (tunder_col, tvalue) FROM "
                  + tableName1 + " a"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = all_db1");
    editor.removePolicy("all_db1 = server=server1->db=db_1");

    // 4
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy(
        "select_tb1 = server=server1->db=db_1->table=tb_1->action=select",
        "roles");
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy(
        "insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert",
        "roles");
    try {
      assertFalse(
          "TRANSFORM fail",
          statement
              .execute("select TRANSFORM(a.under_col, a.value) USING 'cat' AS (tunder_col, tvalue) FROM "
                  + tableName1 + " a"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = select_tb1");
    editor
        .removePolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select");
    editor.removePolicy("group1 = insert_tb1");
    editor
        .removePolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert");

    // 5
    editor.addPolicy("group1 = select_view1", "groups");
    editor.addPolicy(
        "select_view1 = server=server1->db=db_1->table=view_1->action=select",
        "roles");
    try {
      assertFalse(
          "TRANSFORM fail",
          statement
              .execute("select TRANSFORM(a.under_col, a.value) USING 'cat' AS (tunder_col, tvalue) FROM "
                  + tableName1 + " a"));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    editor.removePolicy("group1 = select_view1");
    editor
        .removePolicy("select_view1 = server=server1->db=db_1->table=view_1->action=select");

    // 6
    editor.addPolicy("group1 = transform_db_1", "groups");
    editor
        .addPolicy("transform_db_1 = server=server1->action=transform", "roles");
    rs = statement
        .executeQuery("select TRANSFORM(a.under_col, a.value) USING 'cat' AS (tunder_col, tvalue) FROM "
            + tableName1 + " a");
    assertTrue("TRANSFORM fail", rs.next());
    editor.removePolicy("group1 = transform_db_1");
    editor.removePolicy("transform_db_1 = server=server1->action=transform");
    statement.close();
    connection.close();
  }
}
