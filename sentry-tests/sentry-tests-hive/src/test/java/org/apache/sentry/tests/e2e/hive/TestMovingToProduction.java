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

package org.apache.sentry.tests.e2e.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestMovingToProduction extends AbstractTestWithStaticConfiguration {
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private PolicyFile policyFile;


  @Before
  public void setUp() throws Exception {
    context = createContext();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
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
   * 4. admin create table production.tb_1.
   * 5. admin grant all to GROUP_1 on production.tb_1.
   *   positive test cases:
   *     a)verify user in GROUP_1 can load data from DB_1.tb_1 to production.tb_1
   *     b)verify user in GROUP_1 has proper privilege on production.tb_1
   *     (read and insert)
   *   negative test cases:
   *     c)verify user in GROUP_2 cannot load data from DB_1.tb_1
   *     to production.tb_1
   *     d)verify user in GROUP_1 cannot drop production.tb_1
   */
  @Test
  public void testMovingTable1() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "all_db1", "load_data", "select_proddb_tbl1", "insert_proddb_tbl1")
        .addPermissionsToRole("load_data", "server=server1->uri=file://" + dataDir.getPath())
        .addPermissionsToRole("all_db1", "server=server1->db=db_1")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    String dbName1 = "db_1";
    String dbName2 = "proddb";
    String tableName1 = "tb_1";

    Connection connection = context.createConnection(ADMIN1);
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
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + tableName1);
    statement.execute("create table " + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA LOCAL INPATH 'file://" + dataDir.getPath()
        + "' INTO TABLE " + tableName1);

    policyFile
        .addPermissionsToRole("insert_proddb_tbl1", "server=server1->db=proddb->table=tb_1->action=insert");
    writePolicyFile(policyFile);
    statement.execute("USE " + dbName2);
    statement.execute("INSERT OVERWRITE TABLE "
        + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1);

    // b
    policyFile
        .addPermissionsToRole("select_proddb_tbl1", "server=server1->db=proddb->table=tb_1->action=select");
    writePolicyFile(policyFile);

    ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName1 + " LIMIT 10");
    int count = 0;
    while(resultSet.next()) {
      count++;
    }
    assertEquals(10, count);
    statement.execute("DESCRIBE " + tableName1);

    // c
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    context.assertAuthzException(statement, "USE " + dbName2);
    context.assertAuthzException(statement, "INSERT OVERWRITE TABLE "
        + dbName2 + "." + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1);
    context.assertAuthzException(statement, "SELECT * FROM " + dbName2 + "." + tableName1 + " LIMIT 10");
    statement.close();
    connection.close();

    // d
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName2);
    context.assertAuthzException(statement, "DROP TABLE " + tableName1);
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
    policyFile
        .addRolesToGroup(USERGROUP1, "all_db1", "load_data", "select_proddb_tbl1", "insert_proddb_tbl1")
        .addPermissionsToRole("all_db1", "server=server1->db=db_1")
        .addPermissionsToRole("load_data", "server=server1->uri=file://" + dataDir.getPath())
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    String dbName1 = "db_1";
    String dbName2 = "proddb";
    String tableName1 = "tb_1";
    Connection connection = context.createConnection(ADMIN1);
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
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA LOCAL INPATH 'file://" + dataDir.getPath()
        + "' INTO TABLE " + dbName1 + "." + tableName1);

    policyFile
        .addPermissionsToRole("insert_proddb_tbl1", "server=server1->db=proddb->table=tb_1->action=insert");
    writePolicyFile(policyFile);

    statement.execute("INSERT OVERWRITE TABLE "
        + dbName2 + "." + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1);

    // b
    policyFile
        .addPermissionsToRole("select_proddb_tbl1", "server=server1->db=proddb->table=tb_1->action=select");
    writePolicyFile(policyFile);

    assertTrue("user1 should be able to select data from "
        + dbName2 + "." + dbName2 + "." + tableName1, statement.execute("SELECT * FROM "
            + dbName2 + "." + tableName1 + " LIMIT 10"));
    assertTrue("user1 should be able to describe table " + dbName2 + "." + tableName1,
        statement.execute("DESCRIBE " + dbName2 + "." + tableName1));

    // c
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);

    context.assertAuthzException(statement, "INSERT OVERWRITE TABLE "
        + dbName2 + "." + tableName1 + " SELECT * FROM " + dbName1
        + "." + tableName1);

    context.assertAuthzException(statement, "SELECT * FROM "
        + dbName2 + "." + tableName1 + " LIMIT 10");
    statement.close();
    connection.close();

    // d
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName2);
    context.assertAuthzException(statement, "DROP TABLE " + tableName1);
    statement.close();
    connection.close();
  }
}
