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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.sentry.tests.e2e.hdfs.TestHDFSIntegrationBase;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestDbPrivilegeCleanupOnDrop extends TestHDFSIntegrationBase {

  private final static int SHOW_GRANT_TABLE_POSITION = 2;
  private final static int SHOW_GRANT_DB_POSITION = 1;

  private final static String tableName1 = "tb_1";
  private final static String tableName2 = "tb_2";
  private final static String tableName3 = "tb_3";
  private final static String tableName4 = "tb_4";
  private final static String renameTag = "_new";

  protected static final String ALL_DB1 = "server=server1->db=db_1",
          ADMIN1 = StaticUserGroup.ADMIN1,
          ADMINGROUP = StaticUserGroup.ADMINGROUP,
          USER1_1 = StaticUserGroup.USER1_1,
          USER2_1 = StaticUserGroup.USER2_1,
          USER3_1 = StaticUserGroup.USER3_1,
          USERGROUP1 = StaticUserGroup.USERGROUP1,
          USERGROUP2 = StaticUserGroup.USERGROUP2,
          USERGROUP3 = StaticUserGroup.USERGROUP3,
          DB1 = "db_1",
          DB2 = "db_2";

  private Connection connection;
  private Statement statement;

  @BeforeClass
  public static void setup() throws Exception {
    // TODO: once HIVE-18783 is fixed, set high interval
    // set high interval to test if table rename is synced with fetching notification from HMS
    // HMSFOLLOWER_INTERVAL_MILLS = 2000;
    WAIT_FOR_NOTIFICATION_PROCESSING = HMSFOLLOWER_INTERVAL_MILLS * 3;

    TestHDFSIntegrationBase.setup();
  }

  @Before
  public void initialize() throws Exception{
    super.setUpTempDir();
    admin = "hive";
    connection = hiveServer2.createConnection(admin, admin);
    statement = connection.createStatement();
    statement.execute("create role admin_role");
    statement.execute("grant role admin_role to group hive");
    statement.execute("grant all on server server1 to role admin_role");
  }

  @Test
  public void BasicSanity() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "all_db1", "all_tbl1", "all_tbl2"};

    statement.execute("CREATE ROLE all_db1");
    statement.execute("CREATE ROLE all_tbl1");
    statement.execute("CREATE ROLE all_tbl2");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("create table " + DB1 + "." + tableName3
            + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + DB1 + "." + tableName4
            + " (under_col int comment 'the under column', value string)");


    statement.execute("GRANT all ON DATABASE " + DB1 + " TO ROLE all_db1");
    statement.execute("USE " + DB1);
    statement.execute("GRANT all ON TABLE " + tableName3 + " TO ROLE all_tbl1");
    statement.execute("GRANT all ON TABLE " + tableName4 + " TO ROLE all_tbl2");

    statement.execute("DROP DATABASE " + DB1 + " CASCADE");

    verifyDbPrivilegesDropped(statement);
  }
  /**
   *
   * drop table and verify that the no privileges are referring to it drop db
   * and verify that the no privileges are referring to it drop db cascade
   * verify that the no privileges are referring to db and tables under it
   *
   * @throws Exception
   */
  @Test
  public void testDropObjects() throws Exception {
    dbNames = new String[]{DB1, DB2};
    roles = new String[]{"admin_role", "read_db1", "all_db1", "select_tbl1",
            "insert_tbl1", "all_tbl1", "all_tbl2", "all_prod"};

    setupRoles(statement); // create required roles
    setupDbObjects(statement); // create test DBs and Tables
    setupPrivileges(statement); // setup privileges for USER1
    dropDbObjects(statement); // drop objects
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
    dbNames = new String[]{DB1, DB2};
    roles = new String[]{"admin_role", "read_db1", "all_db1", "select_tbl1",
            "insert_tbl1", "all_tbl1", "all_tbl2", "all_prod"};

    setupRoles(statement); // create required roles
    setupDbObjects(statement); // create test DBs and Tables
    setupPrivileges(statement); // setup privileges for USER1
    dropDbObjects(statement); // drop DB and tables

    setupDbObjects(statement); // recreate same DBs and tables
    verifyPrivilegesDropped(statement); // verify the stale privileges removed
  }

  /**
   * rename table and verify that the no privileges are referring to it old table
   * verify that the same privileges are created for the new table name within the same DB
   *
   * @throws Exception
   */
  @Test
  public void testRenameTablesWithinDB() throws Exception {
    dbNames = new String[]{DB1, DB2};
    roles = new String[]{"admin_role", "read_db1", "all_db1", "select_tbl1",
            "insert_tbl1", "all_tbl1", "all_tbl2", "all_prod"};

    setupRoles(statement); // create required roles
    setupDbObjects(statement); // create test DBs and Tables
    setupPrivileges(statement); // setup privileges for USER1

    // verify privileges on the created tables
    statement.execute("USE " + DB2);
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1", "insert_tbl1", "all_tbl1"),
        tableName1);
    verifyTablePrivilegeExist(statement, Lists.newArrayList("all_tbl2"),
        tableName2);

    // alter tables to rename
    renameTables(statement);

    // verify privileges removed for old tables
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
   * rename table and verify that the no privileges are referring to it old table
   * verify that the same privileges are created for the new table name at different DB
   *
   * @throws Exception
   */
  @Test
  public void testRenameTablesCrossDB() throws Exception {
    dbNames = new String[]{DB1, DB2};
    roles = new String[]{"admin_role", "read_db1", "all_db1", "select_tbl1",
        "insert_tbl1", "all_tbl1", "all_tbl2", "all_prod"};

    // create required roles
    setupRoles(statement);

    // create test DBs and Tables
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("create table " + DB2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");

    // setup privileges for USER1
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

    // verify privileges on the created tables
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1", "insert_tbl1", "all_tbl1"),
        DB2 + "." + tableName1);

    // rename table across the DB
    statement.execute("ALTER TABLE " + DB2 + "." + tableName1 + " RENAME TO "
        + DB1 + "." + tableName1 + renameTag);

    // verify privileges removed for old table
    List<String> roles = getRoles(statement);
    verifyIfAllPrivilegeAreDropped(statement, roles, DB2 + "." + tableName1,
        SHOW_GRANT_TABLE_POSITION);

    // verify privileges created for new table
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1", "insert_tbl1", "all_tbl1"),
        DB1 + "." + tableName1 + renameTag);

    statement.close();
    connection.close();
  }

  /**
   * add single privilege, rename table within the same DB and verify privilege moved to
   * new table name right away
   *
   * @throws Exception
   */
  @Test
  public void testRenameTablesWithinDBSinglePrivilege() throws Exception {
    dbNames = new String[]{DB1, DB2};
    roles = new String[]{"admin_role", "read_db1", "all_db1", "select_tbl1",
        "insert_tbl1", "all_tbl1", "all_tbl2", "all_prod"};

    // create required roles
    setupRoles(statement);

    // create test DBs and Tables
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("create table " + DB2 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");

    // setup privileges for USER1
    statement.execute("GRANT ALL ON DATABASE " + DB1 + " TO ROLE all_db1");
    statement.execute("GRANT SELECT ON DATABASE " + DB1
        + " TO ROLE read_db1");
    statement.execute("GRANT ALL ON DATABASE " + DB2 + " TO ROLE all_prod");
    statement.execute("USE " + DB2);

    // grant priviledges
    statement.execute("GRANT SELECT ON TABLE " + tableName1
        + " TO ROLE select_tbl1");

    // rename table
    statement.execute("ALTER TABLE " + tableName1 + " RENAME TO " +
        tableName1 + renameTag);

    // verify privileges created for new table
    verifyTablePrivilegeExist(statement,
        Lists.newArrayList("select_tbl1"),
        tableName1 + renameTag);

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
  @Ignore("once HIVE-18783 is fixed in hive version, enable this test")
  public void testDropAndRenameWithMultiAction() throws Exception {
    dbNames = new String[]{DB1, DB2};
    roles = new String[]{"admin_role", "user_role"};

    statement.execute("CREATE ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);

    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string)");

    // Grant SELECT/INSERT/DROP/ALTER to TABLE t1
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALTER ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT DROP ON TABLE t1 TO ROLE user_role");
    // For rename, grant DROP/CREATE to DB1
    statement.execute("GRANT DROP ON DATABASE " + DB1 + " TO ROLE user_role");
    statement.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE user_role");

    // After rename table t1 to t2
    connection = hiveServer2.createConnection(USER1_1, USER1_1);
    statement = connection.createStatement();
    statement.execute("USE " + DB1);
    statement.execute("ALTER TABLE t1 RENAME TO t2");

    // After rename table t1 to t2, user_role should have permission to drop t2
    statement.execute("drop table t2");
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    // user_role will revoke all privilege from table t2, only remain DROP/CREATE on db_1
    assertRemainingRows(resultSet, 2);

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
    verifyIfAllPrivilegeAreDropped(statement, roles, tableName1,
        SHOW_GRANT_TABLE_POSITION);
    verifyIfAllPrivilegeAreDropped(statement, roles, tableName2,
        SHOW_GRANT_TABLE_POSITION);
    verifyIfAllPrivilegeAreDropped(statement, roles, tableName3,
        SHOW_GRANT_TABLE_POSITION);
    verifyIfAllPrivilegeAreDropped(statement, roles, tableName4,
        SHOW_GRANT_TABLE_POSITION);

  }

  // verify all the test privileges are dropped as we drop the objects
  private void verifyDbPrivilegesDropped(Statement statement) throws Exception {
    List<String> roles = getRoles(statement);
    verifyIfAllPrivilegeAreDropped(statement, roles, DB2, SHOW_GRANT_DB_POSITION);
    verifyIfAllPrivilegeAreDropped(statement, roles, DB1, SHOW_GRANT_DB_POSITION);

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
}
