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

import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import org.apache.sentry.tests.e2e.hdfs.TestHDFSIntegrationBase;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOwnerPrivileges extends TestHDFSIntegrationBase {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestHDFSIntegrationBase.class);
  private final static String tableName1 = "tb_1";

  protected static final String ALL_DB1 = "server=server1->db=db_1",
      ADMIN1 = StaticUserGroup.ADMIN1,
      ADMINGROUP = StaticUserGroup.ADMINGROUP,
      USER1_1 = StaticUserGroup.USER1_1,
      USER1_2 = StaticUserGroup.USER1_2,
      USERGROUP1 = StaticUserGroup.USERGROUP1,
      DB1 = "db_1";

  private final static String renameTag = "_new";
  private Connection connection;
  private Statement statement;

  @BeforeClass
  public static void setup() throws Exception {
    ownerPrivilegeEnabled = true;

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

  /**
   * Verify that the user who creases database has owner privilege on this database
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatabase() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // setup privileges for USER1
    statement.execute("GRANT CREATE ON SERVER server1" + " TO ROLE create_db1");

    // USER1 creates test DB
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE DATABASE " + DB1);

    // verify privileges created for new database
    verifyTablePrivilegeExistForUser(statementUSER1_1, Lists.newArrayList(USER1_1),
        DB1, null, 1);

    // verify that user has all privilege on this database, i.e., "OWNER" means "ALL"
    // for authorization
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");
    statementUSER1_1.execute("INSERT INTO TABLE " + DB1 + "." + tableName1 + " VALUES (35)");
    statementUSER1_1.execute("ALTER TABLE " + DB1 + "." + tableName1 + " RENAME TO " +
        DB1 + "." + tableName1 + renameTag );
    statementUSER1_1.execute("DROP TABLE " + DB1 + "." + tableName1 + renameTag);
    statementUSER1_1.execute("DROP DATABASE " + DB1 + " CASCADE");

    statement.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();
  }

  /**
   * Verify that the user who does not creases database has no owner privilege on this database
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatabaseNegative() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // setup privileges for USER1
    statement.execute("GRANT CREATE ON SERVER server1" + " TO ROLE create_db1");

    // USER1 creates test DB
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE DATABASE " + DB1);

    // verify user user1_2 has no privileges created for new database
    Connection connectionUSER1_2 = hiveServer2.createConnection(USER1_2, USER1_2);
    Statement statementUSER1_2 = connectionUSER1_2.createStatement();
    verifyTablePrivilegeExistForUser(statementUSER1_2, Lists.newArrayList(USER1_2),
        DB1, null, 0);

    // verify that user user1_2 does not have any privilege on this database except create
    try {
      statementUSER1_2.execute("DROP DATABASE " + DB1 + " CASCADE");
      Assert.fail("Expect dropping database to fail");
    } catch  (Exception ex) {
      LOGGER.info("Expected Exception when dropping database " + ex.getMessage());
    }


    statement.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();

    statementUSER1_2.close();
    connectionUSER1_2.close();
  }

  /**
   * Verify that no owner privilege is created when its creator is an admin user
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatabaseAdmin() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // admin user creates test DB
    statement.execute("CREATE DATABASE " + DB1);

    // verify no privileges created for new database
    verifyTablePrivilegeExistForUser(statement, Lists.newArrayList(admin),
        DB1, null, 0);

    statement.close();
    connection.close();
  }

  /**
   * Verify that after dropping a database, the user who creases database has no owner privilege
   * on this dropped database
   *
   * @throws Exception
   */
  @Test
  public void testDropDatabase() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // setup privileges for USER1
    statement.execute("GRANT CREATE ON SERVER server1" + " TO ROLE create_db1");

    // USER1 creates test DB and then drop it
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE DATABASE " + DB1);
    statementUSER1_1.execute("DROP DATABASE " + DB1 + " CASCADE");

    // verify owner privileges created for new database no longer exists
    verifyTablePrivilegeExistForUser(statementUSER1_1, Lists.newArrayList(USER1_1),
        DB1, null, 0);

    statement.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();
  }


  /**
   * Verify that the user who creases table has owner privilege on this table
   *
   * @throws Exception
   */
  @Test
  public void testCreateTable() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    // create test DB
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1
    statement.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");
    statement.execute("USE " + DB1);

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");


    // verify privileges created for new table
    verifyTablePrivilegeExistForUser(statementUSER1_1, Lists.newArrayList(USER1_1),
        DB1, tableName1, 1);

    // verify that user has all privilege on this table, i.e., "OWNER" means "ALL"
    // for authorization
    statementUSER1_1.execute("INSERT INTO TABLE " + DB1 + "." + tableName1 + " VALUES (35)");
    statementUSER1_1.execute("ALTER TABLE " + DB1 + "." + tableName1 + " RENAME TO " +
        DB1 + "." + tableName1 + renameTag );

    // alter table rename is not blocked for notification processing in upstream due to
    // hive bug HIVE-18783, which is fixed in Hive 2.4.0 and 3.0
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);
    statementUSER1_1.execute("DROP TABLE " + DB1 + "." + tableName1 + renameTag);

    statement.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();
  }

  /**
   * Verify that the user who creases table has owner privilege on this table, but cannot
   * access tables created by others
   *
   * @throws Exception
   */
  @Test
  public void testCreateTableNegative() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    // create test DB
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1 and USER2
    statement.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");
    statement.execute("USE " + DB1);

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");

    // verify user1_2 does not have privileges on table created by user1_1
    Connection connectionUSER1_2 = hiveServer2.createConnection(USER1_2, USER1_2);
    Statement statementUSER1_2 = connectionUSER1_2.createStatement();
    verifyTablePrivilegeExistForUser(statementUSER1_2, Lists.newArrayList(USER1_2),
        DB1, tableName1, 0);

    // verify that user user1_2 does not have any privilege on this table
    try {
      statementUSER1_2.execute("INSERT INTO TABLE " + DB1 + "." + tableName1 + " VALUES (35)");
      Assert.fail("Expect table insert to fail");
    } catch  (Exception ex) {
      LOGGER.info("Expected Exception when inserting table: " + ex.getMessage());
    }

    try {
      statementUSER1_2.execute("ALTER TABLE " + DB1 + "." + tableName1 + " RENAME TO " +
          DB1 + "." + tableName1 + renameTag);
      Assert.fail("Expect table rename to fail");
    } catch  (Exception ex) {
      LOGGER.info("Expected Exception when renaming table: " + ex.getMessage());
    }

    try {
      statementUSER1_2.execute("DROP TABLE " + DB1 + "." + tableName1 );
      Assert.fail("Expect table drop to fail");
    } catch  (Exception ex) {
      LOGGER.info("Expected Exception when dropping table: " + ex.getMessage());
    }

    statement.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();

    statementUSER1_2.close();
    connectionUSER1_2.close();
  }

  /**
   * Verify that no owner privilege is created on table created by an admin user
   *
   * @throws Exception
   */
  @Test
  public void testCreateTableAdmin() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // admin creates test DB and then drop it
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");

    // verify no owner privileges created for new table
    verifyTablePrivilegeExistForUser(statement, Lists.newArrayList(admin),
        DB1, tableName1, 0);

    statement.close();
    connection.close();
  }

  /**
   * Verify that the user who creases table and then drops it has no owner privilege on this table
   *
   * @throws Exception
   */
  @Test
  public void testDropTable() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statement);

    // create test DB
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1
    statement.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statementUSER1_1.execute("DROP TABLE " + DB1 + "." + tableName1);

    // verify privileges created for new table
    verifyTablePrivilegeExistForUser(statementUSER1_1, Lists.newArrayList(USER1_1),
        DB1, tableName1, 0);

    statement.close();
    connection.close();
  }

  

  // TODO: once hive supports alter table set owner, need to add testing cases for owner
  // privilege associated with role

  // Create test roles
  private void setupUserRoles(String[] roles, Statement statement) throws Exception {
    Set<String> userRoles = Sets.newHashSet(roles);
    userRoles.remove("admin_role");

    for (String roleName : userRoles) {
      statement.execute("CREATE ROLE " + roleName);
      statement.execute("GRANT ROLE " + roleName + " to GROUP " + USERGROUP1);
    }
  }

  // verify given table is part of every user in the list
  private void verifyTablePrivilegeExistForUser(Statement statement,
      List<String> users, String dbName, String tableName, int expectedResultCount) throws Exception {

    for (String userName : users) {
      String command;

      if (tableName == null) {
        command = "SHOW GRANT USER " + userName + " ON DATABASE " + dbName;
      } else {
        command = "SHOW GRANT USER " + userName + " ON TABLE " + dbName + "." + tableName;
      }

      ResultSet resultSet = statement.executeQuery(command);

      int resultSize = 0;
      while(resultSet.next()) {
        resultSize ++;

        assertThat(resultSet.getString(1), equalToIgnoringCase(dbName)); // db name

        if (tableName != null) {
          assertThat(resultSet.getString(2), equalToIgnoringCase(tableName)); // table name
        }

        assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
        assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
        assertThat(resultSet.getString(5), equalToIgnoringCase(userName));//principalName
        assertThat(resultSet.getString(6), equalToIgnoringCase("user"));//principalType
        assertThat(resultSet.getString(7), equalToIgnoringCase("owner"));
        assertThat(resultSet.getBoolean(8), is(ownerPrivilegeGrantEnabled));//grantOption
      }

      assertEquals(expectedResultCount, resultSize);

      resultSet.close();
    }
  }

}
