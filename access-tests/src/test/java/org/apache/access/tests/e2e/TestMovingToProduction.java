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
import java.sql.Connection;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMovingToProduction extends AbstractTestWithStaticHiveServer {
  private Context context;
  private String dataFilePath = this.getClass().getResource("/kv1.dat").getFile();

  @Before
  public void setUp() throws Exception {
    context = createContext();
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /**
   * Steps:
   * 1. admin create DB_1, admin create GROUP_1, GROUP_2
   * 2. admin grant all to GROUP_1 on DB_1
   * 3. user in GROUP_1 create table tb_1 and load data into
   * 4. user in GROUP_1 create table production.tb_1.
   * 5. admin grant all to GROUP_1 on production.tb_1.
   *   positive test cases:
   *     a)verify user in GROUP_1 can load data from DB_1.tb_1 to production.tb_1
   *     b)verify user in GROUP_1 has proper privilege on production.tb_1
   *     (read and insert)
   *   negative test cases:
   *     c)verify user in GROUP_2 cannot load data from DB_1.tb_1
   *     to production.tb_1
   *     d)verify user in GROUP_1 cannot drop production.tb_1
   * @throws Exception
   */
  @Test
  public void testMovingTable1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("group1 = insert_produTB", "groups");
    editor.addPolicy("group1 = load_data", "groups");
    editor.addPolicy("select_produTB = server=server1->db=produdb->table=tb_1->action=select", "roles");
    editor.addPolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select", "roles");
    editor.addPolicy("insert_produTB = server=server1->db=produdb->table=tb_1->action=insert", "roles");
    editor.addPolicy("load_data = server=server1->uri=file:" + dataFilePath.toString(), "roles");
    editor.addPolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert", "roles");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group2", "users");

    String dbName1 = "db_1";
    String dbName2 = "productdb";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("DROP DATABASE IF EXISTS " + dbName2 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("CREATE DATABASE " + dbName2);
    statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName1);
    statement.execute("create table " + dbName2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.close();
    connection.close();

    // a
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertTrue("user1 should be able to switch to " + dbName1,
        statement.execute("USE " + dbName1));
    assertTrue("user1 should be able to drop table " + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + tableName1));
    assertTrue(
        "user1 should be able to create table " + tableName1,
        statement.execute("create table " + tableName1
            + " (under_col int comment 'the under column', value string)"));
    assertTrue(
        "user1 has privilege to load data from " + dataFilePath.toString(),
        statement.execute("LOAD DATA INPATH '" + dataFilePath.toString()
            + "' INTO TABLE " + tableName1));

    assertTrue("user1 should be able to switch to " + dbName2,
        statement.execute("USE " + dbName2));
    assertTrue("user1 should be able to load data to "
        + tableName1, statement.execute("INSERT OVERWRITE TABLE "
        + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1));

    // b
    editor.addPolicy("group1 = select_produTB", "groups");
    assertTrue("user1 should be able to select data from "
        + dbName2 + "." + tableName1, statement.execute("SELECT * FROM "
        + tableName1 + " LIMIT 10"));
    assertTrue("user1 should be able to describe table " + tableName1,
        statement.execute("DESCRIBE " + tableName1));

    // d
    assertFalse("user1 should not be able to drop table " + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + tableName1));
    statement.close();
    connection.close();

    // c
    connection = context.createConnection("user2", "foo");
    statement = context.createStatement(connection);
    assertTrue("user2 should be able to switch to " + dbName2,
        statement.execute("USE " + dbName2));
    assertFalse("user2 should not be able to load data to "
        + tableName1, statement.execute("INSERT OVERWRITE TABLE "
        + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1));
    assertFalse("user2 should not be able to select data from "
        + tableName1, statement.execute("SELECT * FROM "
        + tableName1 + " LIMIT 10"));
    statement.close();
    connection.close();
  }

  /**
   * repeat above tests, only difference is don't do 'USE <database>'
   * in this test. Instead, access table objects across database by
   * database.table
   * @throws Exception
   */
  @Test
  public void testMovingTable2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("group1 = insert_tb1", "groups");
    editor.addPolicy("group1 = select_tb1", "groups");
    editor.addPolicy("group1 = insert_produTB", "groups");
    editor.addPolicy("group1 = load_data", "groups");
    editor.addPolicy("select_produTB = server=server1->db=produdb->table=tb_1->action=select", "roles");
    editor.addPolicy("select_tb1 = server=server1->db=db_1->table=tb_1->action=select", "roles");
    editor.addPolicy("insert_produTB = server=server1->db=produdb->table=tb_1->action=insert", "roles");
    editor.addPolicy("insert_tb1 = server=server1->db=db_1->table=tb_1->action=insert", "roles");
    editor.addPolicy("load_data = server=server1->uri=file:" + dataFilePath.toString(), "roles");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group2", "users");

    String dbName1 = "db_1";
    String dbName2 = "productdb";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("DROP DATABASE IF EXISTS " + dbName2 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("CREATE DATABASE " + dbName2);
    statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName1);
    statement.execute("create table " + dbName2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.close();
    connection.close();

    // a
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    assertTrue("user1 should be able to drop table " + dbName1 + "." + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1));
    assertTrue(
        "user1 should be able to create table " + dbName1 + "." + tableName1,
        statement.execute("create table " + dbName1 + "." + tableName1
            + " (under_col int comment 'the under column', value string)"));
    assertTrue(
        "user1 has privilege to load data from " + dataFilePath.toString(),
        statement.execute("LOAD DATA INPATH '" + dataFilePath.toString()
            + "' INTO TABLE " + dbName1 + "." + tableName1));

    assertTrue("user1 should be able to load data to "
        + dbName2 + "." + tableName1, statement.execute("INSERT OVERWRITE TABLE "
        + dbName2 + "." + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1));

    // b
    editor.addPolicy("group1 = select_produTB", "groups");
    assertTrue("user1 should be able to select data from "
        + dbName2 + "." + dbName2 + "." + tableName1, statement.execute("SELECT * FROM "
        + dbName2 + "." + tableName1 + " LIMIT 10"));
    assertTrue("user1 should be able to describe table " + dbName2 + "." + tableName1,
        statement.execute("DESCRIBE " + dbName2 + "." + tableName1));

    // d
    assertFalse("user1 should not be able to drop table " + dbName2 + "." + tableName1,
        statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName1));
    statement.close();
    connection.close();

    // c
    connection = context.createConnection("user2", "foo");
    statement = context.createStatement(connection);

    assertFalse("user2 should not be able to load data to "
        + dbName2 + "." + tableName1, statement.execute("INSERT OVERWRITE TABLE "
        + dbName2 + "." + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1));
    assertFalse("user2 should not be able to select data from "
        + dbName2 + "." + tableName1, statement.execute("SELECT * FROM "
        + dbName2 + "." + tableName1 + " LIMIT 10"));
    statement.close();
    connection.close();
  }
}
