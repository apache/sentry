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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.sentry.service.thrift.HMSFollower;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.*;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDbPrivilegeCleanupOnDrop extends
    AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
          .getLogger(TestDbPrivilegeCleanupOnDrop.class);

  private final static int SHOW_GRANT_TABLE_POSITION = 2;
  private final static int SHOW_GRANT_DB_POSITION = 1;

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  private final static String tableName1 = "tb_1";
  private final static String tableName2 = "tb_2";
  private final static String tableName3 = "tb_3";
  private final static String tableName4 = "tb_4";
  private final static String renameTag = "_new";

  static final long WAIT_FOR_NOTIFICATION_PROCESSING = 10000;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    if (!setMetastoreListener) {
      setMetastoreListener = true;
    }
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
    // context = createContext();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    // Check the HMS connection only when notification log is enabled.
    if (enableNotificationLog) {
      while (!HMSFollower.isConnectedToHMS()) {
        Thread.sleep(1000);
      }
    }
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
    if (enableNotificationLog) {
      Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    }
    verifyPrivilegesDropped(statement); // verify privileges are removed

    statement.close();
    connection.close();
  }

  /**
   * Return the remaining rows of the current resultSet
   * Cautiously it will modify the cursor position of the resultSet
   *
   */
  private void assertRemainingRows(ResultSet resultSet, int expected) throws SQLException{
    int count = 0;
    while(resultSet.next()) {
      count++;
    }
    assertThat(count, is(expected));
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
    if (enableNotificationLog) {
      Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    }
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
    if (enableNotificationLog) {
      Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    }

    // verify privileges on the created tables
    statement.execute("USE " + DB2);
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1", "insert_tbl1", "all_tbl1"),
        tableName1);
    verifyTablePrivilegeExist(statement, Lists.newArrayList("all_tbl2"),
        tableName2);

    renameTables(statement); // alter tables to rename
    // verify privileges removed for old tables
    if (enableNotificationLog) {
      Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    }
    verifyTablePrivilegesDropped(statement);

    // verify privileges created for new tables
    statement.execute("USE " + DB2);
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1", "insert_tbl1", "all_tbl1"),
        tableName1 + renameTag);
    verifyTablePrivilegeExist(statement, Lists.newArrayList("all_tbl2"),
        tableName2 + renameTag);

    statement.close();
    connection.close();
  }

  /**
   * After we drop/rename table, we will drop/rename all privileges(ALL,SELECT,INSERT,ALTER,DROP...)
   * from this role
   *
   * @throws Exception
   */
  @Test
  public void testDropAndRenameWithMultiAction() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);

    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string)");

    // Grant SELECT/INSERT to TABLE t1
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("ALTER TABLE t1 RENAME TO t2");
    if (enableNotificationLog) {
      Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    }

    statement.execute("drop table t2");
    if (enableNotificationLog) {
      Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    }
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    // user_role will revoke all privilege from table t2
    assertRemainingRows(resultSet, 0);

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

    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("DROP DATABASE IF EXISTS " + DB2 + " CASCADE");
  }

  // create test DBs and Tables
  private void setupDbObjects(Statement statement) throws Exception {
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("create table " + DB2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + DB2 + "." + tableName2
        + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + DB1 + "." + tableName3
        + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + DB1 + "." + tableName4
        + " (under_col int comment 'the under column', value string)");
  }

  // Create privileges on DB and Tables
  private void setupPrivileges(Statement statement) throws Exception {
    statement.execute("GRANT ALL ON DATABASE " + DB1 + " TO ROLE all_db1");
    statement.execute("GRANT SELECT ON DATABASE " + DB1
        + " TO ROLE read_db1");
    statement.execute("GRANT ALL ON DATABASE " + DB2 + " TO ROLE all_prod");
    statement.execute("USE " + DB2);
    statement.execute("GRANT SELECT ON TABLE " + tableName1
        + " TO ROLE select_tbl1");
    statement.execute("GRANT INSERT ON TABLE " + tableName1
        + " TO ROLE insert_tbl1");
    statement.execute("GRANT ALL ON TABLE " + tableName1 + " TO ROLE all_tbl1");
    statement.execute("GRANT ALL ON TABLE " + tableName2 + " TO ROLE all_tbl2");
  }

  // Drop test DBs and Tables
  private void dropDbObjects(Statement statement) throws Exception {
    statement.execute("DROP TABLE " + DB2 + "." + tableName1);
    statement.execute("DROP TABLE " + DB2 + "." + tableName2);
    statement.execute("DROP DATABASE " + DB2);
    statement.execute("DROP DATABASE " + DB1 + " CASCADE");
  }

  // rename tables
  private void renameTables(Statement statement) throws Exception {
    statement.execute("USE " + DB2);
    statement.execute("ALTER TABLE " + tableName1 + " RENAME TO " + tableName1
        + renameTag);
    statement.execute("ALTER TABLE " + tableName2 + " RENAME TO " + tableName2
        + renameTag);
    statement.execute("USE " + DB1);
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
    verifyPrivilegeDropped(statement, roles, DB2, SHOW_GRANT_DB_POSITION);
    verifyPrivilegeDropped(statement, roles, DB1, SHOW_GRANT_DB_POSITION);

  }

  // verify given table/DB has no longer permissions
  private void verifyPrivilegeDropped(Statement statement, List<String> roles,
      String objectName, int resultPos) throws Exception {
    for (String roleName : roles) {
      ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE "
          + roleName);
      while (resultSet.next()) {
        String returned = resultSet.getString(resultPos);
        assertFalse("value " + objectName + " shouldn't be detected, but actually " + returned + " is found from resultSet",
                objectName.equalsIgnoreCase(returned));
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
