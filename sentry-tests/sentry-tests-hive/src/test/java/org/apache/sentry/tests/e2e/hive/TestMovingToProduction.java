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

import org.apache.sentry.provider.file.PolicyFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestMovingToProduction extends AbstractTestWithStaticConfiguration {
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private PolicyFile policyFile;


  @Before
  public void setUp() throws Exception {
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP)
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
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
    String tableName1 = "tb_1";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("DROP DATABASE IF EXISTS " + DB2 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("DROP TABLE IF EXISTS " + DB2 + "." + tableName1);
    statement.execute("create table " + DB2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.close();
    connection.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "all_db1", "load_data")
        .addPermissionsToRole("load_data", "server=server1->uri=file://" + dataDir.getPath())
        .addPermissionsToRole("all_db1", "server=server1->db="  + DB1);
    writePolicyFile(policyFile);


    // a
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("DROP TABLE IF EXISTS " + tableName1);
    statement.execute("create table " + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA LOCAL INPATH 'file://" + dataDir.getPath()
        + "' INTO TABLE " + tableName1);

    policyFile
        .addRolesToGroup(USERGROUP1, "insert_proddb_tbl1")
        .addPermissionsToRole("insert_proddb_tbl1", "server=server1->db="  + DB2 + "->table=tb_1->action=insert");
    writePolicyFile(policyFile);
    statement.execute("USE " + DB2);
    statement.execute("INSERT OVERWRITE TABLE "
        + tableName1 + " SELECT * FROM " + DB1
        + "." + tableName1);

    // b
    policyFile
        .addRolesToGroup(USERGROUP1, "select_proddb_tbl1")
        .addPermissionsToRole("select_proddb_tbl1", "server=server1->db="  + DB2 + "->table=tb_1->action=select");
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
    context.assertAuthzException(statement, "USE " + DB2);
    context.assertAuthzException(statement, "INSERT OVERWRITE TABLE "
        + DB2 + "." + tableName1 + " SELECT * FROM " + DB1
        + "." + tableName1);
    context.assertAuthzException(statement, "SELECT * FROM " + DB2 + "." + tableName1 + " LIMIT 10");
    statement.close();
    connection.close();

    // d
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB2);
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
    String tableName1 = "tb_1";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("DROP DATABASE IF EXISTS " + DB2 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("DROP TABLE IF EXISTS " + DB2 + "." + tableName1);
    statement.execute("create table " + DB2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.close();
    connection.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "all_db1", "load_data")
        .addPermissionsToRole("all_db1", "server=server1->db="  + DB1)
        .addPermissionsToRole("load_data", "server=server1->uri=file://" + dataDir.getPath());
    writePolicyFile(policyFile);

    // a
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("DROP TABLE IF EXISTS " + DB1 + "." + tableName1);
    statement.execute("create table " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA LOCAL INPATH 'file://" + dataDir.getPath()
        + "' INTO TABLE " + DB1 + "." + tableName1);

    policyFile
        .addRolesToGroup(USERGROUP1, "insert_proddb_tbl1")
        .addPermissionsToRole("insert_proddb_tbl1", "server=server1->db="  + DB2 + "->table=tb_1->action=insert");
    writePolicyFile(policyFile);

    statement.execute("INSERT OVERWRITE TABLE "
        + DB2 + "." + tableName1 + " SELECT * FROM " + DB1
        + "." + tableName1);

    // b
    policyFile
        .addRolesToGroup(USERGROUP1, "select_proddb_tbl1")
        .addPermissionsToRole("select_proddb_tbl1", "server=server1->db="  + DB2 + "->table=tb_1->action=select");
    writePolicyFile(policyFile);

    assertTrue("user1 should be able to select data from "
        + DB2 + "." + DB2 + "." + tableName1, statement.execute("SELECT * FROM "
            + DB2 + "." + tableName1 + " LIMIT 10"));
    assertTrue("user1 should be able to describe table " + DB2 + "." + tableName1,
        statement.execute("DESCRIBE " + DB2 + "." + tableName1));

    // c
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);

    context.assertAuthzException(statement, "INSERT OVERWRITE TABLE "
        + DB2 + "." + tableName1 + " SELECT * FROM " + DB1
        + "." + tableName1);

    context.assertAuthzException(statement, "SELECT * FROM "
        + DB2 + "." + tableName1 + " LIMIT 10");
    statement.close();
    connection.close();

    // d
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB2);
    context.assertAuthzException(statement, "DROP TABLE " + tableName1);
    statement.close();
    connection.close();
  }
}
