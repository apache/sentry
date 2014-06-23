/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.dbprovider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class TestDbPrivilegeCleanupOnDrop extends
    AbstractTestWithStaticConfiguration {

  private final static int SHOW_GRANT_TABLE_POSITION = 2;
  private final static int SHOW_GRANT_DB_POSITION = 1;

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  private final static String dbName1 = "db_1";
  private final static String dbName2 = "prod";
  private final static String tableName1 = "tb_1";
  private final static String tableName2 = "tb_2";
  private final static String tableName3 = "tb_3";
  private final static String tableName4 = "tb_4";
  private final static String renameTag = "_new";

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    setMetastoreListener = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Before
  public void setUp() throws Exception {
    // context = createContext();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    setupAdmin();
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
   * drop table and verify that the no privileges are referring to it drop db
   * and verify that the no privileges are referring to it drop db cascade
   * verify that the no privileges are referring to db and tables under it
   * 
   * @throws Exception
   */
  @Test
  public void testDropObjects() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    setupRoles(statement); // create required roles
    setupDbObjects(statement); // create test DBs and Tables
    setupPrivileges(statement); // setup privileges for USER1
    dropDbObjects(statement); // drop objects
    verifyPrivilegesDropped(statement); // verify privileges are removed

    statement.close();
    connection.close();
  }

  /**
   * drop table and verify that the no privileges are referring to it drop db
   * and verify that the no privileges are referring to it drop db cascade
   * verify that the no privileges are referring to db and tables under it
   * 
   * @throws Exception
   */
  @Test
  public void testReCreateObjects() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    setupRoles(statement); // create required roles
    setupDbObjects(statement); // create test DBs and Tables
    setupPrivileges(statement); // setup privileges for USER1
    dropDbObjects(statement); // drop DB and tables

    setupDbObjects(statement); // recreate same DBs and tables
    verifyPrivilegesDropped(statement); // verify the stale privileges removed
  }

  /**
   * rename table and verify that the no privileges are referring to it old table
   * verify that the same privileges are created for the new table name
   * 
   * @throws Exception
   */
  @Test
  public void testRenameTables() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    setupRoles(statement); // create required roles
    setupDbObjects(statement); // create test DBs and Tables
    setupPrivileges(statement); // setup privileges for USER1

    // verify privileges on the created tables
    statement.execute("USE " + dbName2);
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1", "insert_tbl1", "all_tbl1"),
        tableName1);
    verifyTablePrivilegeExist(statement, Lists.newArrayList("all_tbl2"),
        tableName2);

    renameTables(statement); // alter tables to rename

    // verify privileges removed for old tables
    verifyTablePrivilegesDropped(statement);

    // verify privileges created for new tables
    statement.execute("USE " + dbName2);
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1", "insert_tbl1", "all_tbl1"),
        tableName1 + renameTag);
    verifyTablePrivilegeExist(statement, Lists.newArrayList("all_tbl2"),
        tableName2 + renameTag);

    statement.close();
    connection.close();
  }

  // Create test roles
  private void setupRoles(Statement statement) throws Exception {
    statement.execute("CREATE ROLE all_db1");
    statement.execute("CREATE ROLE read_db1");
    statement.execute("CREATE ROLE select_tbl1");
    statement.execute("CREATE ROLE insert_tbl1");
    statement.execute("CREATE ROLE all_tbl1");
    statement.execute("CREATE ROLE all_tbl2");
    statement.execute("CREATE ROLE all_prod");

    statement.execute("GRANT ROLE all_db1, read_db1, select_tbl1, insert_tbl1,"
        + " all_tbl1, all_tbl2, all_prod to GROUP " + USERGROUP1);

    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("DROP DATABASE IF EXISTS " + dbName2 + " CASCADE");
  }

  // create test DBs and Tables
  private void setupDbObjects(Statement statement) throws Exception {
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("CREATE DATABASE " + dbName2);
    statement.execute("create table " + dbName2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + dbName2 + "." + tableName2
        + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + dbName1 + "." + tableName3
        + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + dbName1 + "." + tableName4
        + " (under_col int comment 'the under column', value string)");
  }

  // Create privileges on DB and Tables
  private void setupPrivileges(Statement statement) throws Exception {
    statement.execute("GRANT ALL ON DATABASE " + dbName1 + " TO ROLE all_db1");
    statement.execute("GRANT SELECT ON DATABASE " + dbName1
        + " TO ROLE read_db1");
    statement.execute("GRANT ALL ON DATABASE " + dbName2 + " TO ROLE all_prod");
    statement.execute("USE " + dbName2);
    statement.execute("GRANT SELECT ON TABLE " + tableName1
        + " TO ROLE select_tbl1");
    statement.execute("GRANT INSERT ON TABLE " + tableName1
        + " TO ROLE insert_tbl1");
    statement.execute("GRANT ALL ON TABLE " + tableName1 + " TO ROLE all_tbl1");
    statement.execute("GRANT ALL ON TABLE " + tableName2 + " TO ROLE all_tbl2");
  }

  // Drop test DBs and Tables
  private void dropDbObjects(Statement statement) throws Exception {
    statement.execute("DROP TABLE " + dbName2 + "." + tableName1);
    statement.execute("DROP TABLE " + dbName2 + "." + tableName2);
    statement.execute("DROP DATABASE " + dbName2);
    statement.execute("DROP DATABASE " + dbName1 + " CASCADE");
  }

  // rename tables
  private void renameTables(Statement statement) throws Exception {
    statement.execute("USE " + dbName2);
    statement.execute("ALTER TABLE " + tableName1 + " RENAME TO " + tableName1
        + renameTag);
    statement.execute("ALTER TABLE " + tableName2 + " RENAME TO " + tableName2
        + renameTag);
    statement.execute("USE " + dbName1);
    statement.execute("ALTER TABLE " + tableName3 + " RENAME TO " + tableName3
        + renameTag);
    statement.execute("ALTER TABLE " + tableName4 + " RENAME TO " + tableName4
        + renameTag);
  }

  // verify all the test privileges are dropped as we drop the objects
  private void verifyPrivilegesDropped(Statement statement)
      throws Exception {
    verifyDbPrivilegesDropped(statement);
    verifyTablePrivilegesDropped(statement);
  }

  // verify all the test privileges are dropped as we drop the objects
  private void verifyTablePrivilegesDropped(Statement statement)
      throws Exception {
    List<String> roles = getRoles(statement);
    verifyPrivilegeDropped(statement, roles, tableName1,
        SHOW_GRANT_TABLE_POSITION);
    verifyPrivilegeDropped(statement, roles, tableName2,
        SHOW_GRANT_TABLE_POSITION);
    verifyPrivilegeDropped(statement, roles, tableName3,
        SHOW_GRANT_TABLE_POSITION);
    verifyPrivilegeDropped(statement, roles, tableName4,
        SHOW_GRANT_TABLE_POSITION);

  }

  // verify all the test privileges are dropped as we drop the objects
  private void verifyDbPrivilegesDropped(Statement statement) throws Exception {
    List<String> roles = getRoles(statement);
    verifyPrivilegeDropped(statement, roles, dbName2, SHOW_GRANT_DB_POSITION);
    verifyPrivilegeDropped(statement, roles, dbName1, SHOW_GRANT_DB_POSITION);

  }

  // verify given table/DB has no longer permissions
  private void verifyPrivilegeDropped(Statement statement, List<String> roles,
      String objectName, int resultPos) throws Exception {
    for (String roleName : roles) {
      ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE "
          + roleName);
      while (resultSet.next()) {
        assertFalse(objectName.equalsIgnoreCase(resultSet.getString(resultPos)));
      }
      resultSet.close();
    }
  }

  // verify given table is part of the role
  private void verifyTablePrivilegeExist(Statement statement,
      List<String> roles, String tableName) throws Exception {
    for (String roleName : roles) {
      ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE "
          + roleName + " ON TABLE " + tableName);
      assertTrue(resultSet.next());
      resultSet.close();
    }
  }

  private List<String> getRoles(Statement statement) throws Exception {
    ArrayList<String> roleList = Lists.newArrayList();
    ResultSet resultSet = statement.executeQuery("SHOW ROLES ");
    while (resultSet.next()) {
      roleList.add(resultSet.getString(1));
    }
    return roleList;
  }
}
