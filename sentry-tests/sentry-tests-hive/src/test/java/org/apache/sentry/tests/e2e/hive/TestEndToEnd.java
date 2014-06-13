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

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestEndToEnd extends AbstractTestWithStaticConfiguration {
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataFile;
  private PolicyFile policyFile;


  @Before
  public void setup() throws Exception {
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);

  }

  /**
   * Steps:
   * 1. admin create a new experimental database
   * 2. admin create a new production database, create table, load data
   * 3. admin grant privilege all@'experimental database' to usergroup1
   * 4. user create table, load data in experimental DB
   * 5. user create view based on table in experimental DB
   * 6. admin create table (same name) in production DB
   * 7. admin grant read@productionDB.table to group
   *    admin grant select@productionDB.table to group
   * 8. user load data from experimental table to production table
   */
  @Test
  public void testEndToEnd1() throws Exception {
    policyFile
      .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
    String dbName1 = "db_1";
    String dbName2 = "productionDB";
    String tableName1 = "tb_1";
    String tableName2 = "tb_2";
    String viewName1 = "view_1";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    // 1
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    // 2
    statement.execute("DROP DATABASE IF EXISTS " + dbName2 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName2);
    statement.execute("USE " + dbName2);
    statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName2);
    statement.execute("create table " + dbName2 + "." + tableName2
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName2);
    statement.close();
    connection.close();

    // 3
    policyFile
        .addRolesToGroup(USERGROUP1, "all_db1", "data_uri", "select_tb1", "insert_tb1")
        .addPermissionsToRole("all_db1", "server=server1->db=db_1")
        .addPermissionsToRole("select_tb1", "server=server1->db=productionDB->table=tb_1->action=select")
        .addPermissionsToRole("insert_tb2", "server=server1->db=productionDB->table=tb_2->action=insert")
        .addPermissionsToRole("insert_tb1", "server=server1->db=productionDB->table=tb_2->action=insert")
        .addPermissionsToRole("data_uri", "server=server1->uri=file://" + dataDir.getPath());
    writePolicyFile(policyFile);

    // 4
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName1);
    // 5
    statement.execute("CREATE VIEW " + viewName1 + " (value) AS SELECT value from " + tableName1 + " LIMIT 10");
    statement.close();
    connection.close();

    // 7
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName2);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.close();
    connection.close();

    // 8
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName2);
    statement.execute("INSERT OVERWRITE TABLE " +
        dbName2 + "." + tableName2 + " SELECT * FROM " + dbName1
        + "." + tableName1);
    statement.close();
    connection.close();
  }
}
