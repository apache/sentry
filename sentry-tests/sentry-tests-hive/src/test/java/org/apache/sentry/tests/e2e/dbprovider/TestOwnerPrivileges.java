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
import com.google.common.collect.Lists;
import com.google.common.base.Strings;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.tests.e2e.hdfs.TestHDFSIntegrationBase;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.service.common.ServiceConstants.SentryPrincipalType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOwnerPrivileges extends TestHDFSIntegrationBase {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestHDFSIntegrationBase.class);
  protected final static String tableName1 = "tb_1";

  protected static final String ADMIN1 = StaticUserGroup.ADMIN1,
      ADMINGROUP = StaticUserGroup.ADMINGROUP,
      USER1_1 = StaticUserGroup.USER1_1,
      USER1_2 = StaticUserGroup.USER1_2,
      USERGROUP1 = StaticUserGroup.USERGROUP1,
      USERGROUP2 = StaticUserGroup.USERGROUP2,
      USER2_1 = StaticUserGroup.USER2_1,
      DB1 = "db_1";

  private final static String renameTag = "_new";
  protected Connection connection;
  protected Statement statementAdmin;

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
    statementAdmin = connection.createStatement();
    statementAdmin.execute("create role admin_role");
    statementAdmin.execute("grant role admin_role to group hive");
    statementAdmin.execute("grant all on server server1 to role admin_role");
  }

  /**
   * Verify that the user who creases database has owner privilege on this database
   * and also makes sure that HDFS ACL rules are updated.
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatabase() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON SERVER server1" + " TO ROLE create_db1");

    // USER1 creates test DB
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE DATABASE " + DB1);

    // verify privileges created for new database
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, "", 1);

    // Verify that HDFS ACL are added.
    verifyHdfsAcl(Lists.newArrayList(USER1_1), null, DB1, null, null, true);

    // verify that user has all privilege on this database, i.e., "OWNER" means "ALL"
    // for authorization
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");
    statementUSER1_1.execute("INSERT INTO TABLE " + DB1 + "." + tableName1 + " VALUES (35)");
    statementUSER1_1.execute("ALTER TABLE " + DB1 + "." + tableName1 + " RENAME TO " +
        DB1 + "." + tableName1 + renameTag );
    statementUSER1_1.execute("DROP TABLE " + DB1 + "." + tableName1 + renameTag);
    statementUSER1_1.execute("DROP DATABASE " + DB1 + " CASCADE");

    statementAdmin.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();
  }

  /**
   * Verify that the user who does not creases database has no owner privilege on this database and
   * also makes sure that there are not HDFS ACL.
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatabaseNegative() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON SERVER server1" + " TO ROLE create_db1");

    // USER1 creates test DB
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE DATABASE " + DB1);

    // verify user user1_2 has no privileges created for new database
    Connection connectionUSER1_2 = hiveServer2.createConnection(USER1_2, USER1_2);
    Statement statementUSER1_2 = connectionUSER1_2.createStatement();
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_2, SentryPrincipalType.USER, Lists.newArrayList(USER1_2),
        DB1, "", 0);

    // verify that user user1_2 does not have any privilege on this database except create
    try {
      statementUSER1_2.execute("DROP DATABASE " + DB1 + " CASCADE");
      Assert.fail("Expect dropping database to fail");
    } catch  (Exception ex) {
      LOGGER.info("Expected Exception when dropping database " + ex.getMessage());
    }

    // Verify that HDFS ACL are not set.
    verifyHdfsAcl(Lists.newArrayList(USER1_2), null, DB1, null, null, false);

    statementAdmin.close();
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
  public void testCreateDatabaseAdmin() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // admin user creates test DB
    statementAdmin.execute("CREATE DATABASE " + DB1);

    // verify privileges created for new database
    verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER, Lists.newArrayList(admin),
        DB1, "", 1);

    // Verify that HDFS ACL are set.
    verifyHdfsAcl(Lists.newArrayList(admin), null, DB1, null, null, true);

    statementAdmin.close();
    connection.close();
  }

  /**
   * Verify that after dropping a database, the user who creases database has no owner privilege
   * on this dropped database and makes sure that HDFS ACLs are updated accordingly.
   *
   * @throws Exception
   */
  @Test
  public void testDropDatabase() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON SERVER server1" + " TO ROLE create_db1");

    // USER1 creates test DB and then drop it
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE DATABASE " + DB1);
    statementUSER1_1.execute("DROP DATABASE " + DB1 + " CASCADE");

    // verify owner privileges created for new database no longer exists
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, "", 0);

    // Verify that HDFS ACL are not set.
    verifyHdfsAcl(Lists.newArrayList(USER1_1), null, DB1, null, null, false);

    statementAdmin.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();
  }

  /**
   * Verify that the user who can call alter database set owner on this table
   *
   * @throws Exception
   */
  @Ignore("Enable the test once HIVE-18031 is in the hiver version integrated with Sentry")
  @Test
  public void testAuthorizeAlterDatabaseSetOwner() throws Throwable {
    String ownerRole = "owner_role";
    String allWithGrantRole = "allWithGrant_role";
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_on_server", ownerRole};

    // create required roles, and assign them to USERGROUP1
    setupUserRoles(roles, statementAdmin);

    // create test DB
    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON SERVER " + SERVER_NAME + " TO ROLE create_on_server");

    // USER1_1 create database
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE DATABASE " + DB1);

    if (!ownerPrivilegeGrantEnabled) {
      try {
        statementUSER1_1.execute("ALTER DATABASE " + DB1 + " SET OWNER ROLE " + ownerRole);
        Assert.fail("Expect altering database set owner to fail for owner without grant option");
      } catch(Exception ex){
        // owner without grant option cannot issue this command
      }
    }


    // admin issues alter database set owner
    try {
      statementAdmin.execute("ALTER DATABASE " + DB1 + " SET OWNER ROLE " + ownerRole);
      Assert.fail("Expect altering database set owner to fail for admin");
    } catch (Exception ex) {
      // admin does not have all with grant option, so cannot issue this command
    }

    Connection connectionUSER2_1 = hiveServer2.createConnection(USER2_1, USER2_1);
    Statement statementUSER2_1 = connectionUSER2_1.createStatement();

    try {
      // create role that has all with grant on the table
      statementAdmin.execute("create role " + allWithGrantRole);
      statementAdmin.execute("grant role " + allWithGrantRole + " to group " + USERGROUP2);
      statementAdmin.execute("GRANT ALL ON DATABASE " + DB1 + " to role " +
          allWithGrantRole + " with grant option");

      // cannot issue command on a different database
      try {
        statementUSER2_1.execute("ALTER DATABASE NON_EXIST_DB" + " SET OWNER ROLE " + ownerRole);
        Assert.fail("Expect altering database set owner to fail on db that USER2_1 has no all with grant");
      } catch (Exception ex) {
        // USER2_1 does not have all with grant option on NON_EXIST_DB, so cannot issue this command
      }

      // user2_1 having all with grant on this DB and can issue command: alter database set owner
      // alter database set owner to a role
      statementUSER2_1
          .execute("ALTER DATABASE " + DB1 + " SET OWNER ROLE " + ownerRole);

      // verify privileges is transferred to role owner_role, which is associated with USERGROUP1,
      // therefore to USER1_1
      verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.ROLE,
          Lists.newArrayList(ownerRole),
          DB1, "", 1);

      // Verify that HDFS ACL are not set.
      verifyHdfsAcl(Lists.newArrayList(USER1_1), null, DB1, null, null, false);

      // Verify that HDFS ACL are set.
      verifyHdfsAcl(null, Lists.newArrayList(USERGROUP2), DB1, null, null, true);

      // alter database set owner to user USER1_1 and verify privileges is transferred to USER USER1_1
      statementUSER2_1
          .execute("ALTER DATABASE " + DB1 + " SET OWNER USER " + USER1_1);
      verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER,
          Lists.newArrayList(USER1_1), DB1, "", 1);

      // alter database set owner to user USER2_1, who already has explicit all with grant
      statementUSER2_1
          .execute("ALTER DATABASE " + DB1 + " SET OWNER USER " + USER2_1);
      verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER,
          Lists.newArrayList(USER2_1),
          DB1, "", 1);

      // Verify that HDFS ACL are set.
      verifyHdfsAcl(Lists.newArrayList(USER2_1), null, DB1, tableName1, null, true);


    } finally {
      statementAdmin.execute("drop role " + allWithGrantRole);

      statementAdmin.close();
      connection.close();

      statementUSER1_1.close();
      connectionUSER1_1.close();

      statementUSER2_1.close();
      connectionUSER2_1.close();
    }
  }


  /**
   * Verify that the user who creases table has owner privilege on this table and
   * and makes sure that HDFS ACLs are updated accordingly.
   *
   * @throws Exception
   */
  @Test
  public void testCreateTable() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    // create test DB
    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statementAdmin.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");
    statementAdmin.execute("USE " + DB1);

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");


    // verify privileges created for new table
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, tableName1, 1);

    // Verify that HDFS ACL are added.
    verifyHdfsAcl(Lists.newArrayList(USER1_1), null, DB1, tableName1, null, true);

    // verify that user has all privilege on this table, i.e., "OWNER" means "ALL"
    // for authorization
    statementUSER1_1.execute("INSERT INTO TABLE " + DB1 + "." + tableName1 + " VALUES (35)");
    statementUSER1_1.execute("ALTER TABLE " + DB1 + "." + tableName1 + " RENAME TO " +
        DB1 + "." + tableName1 + renameTag );

    // alter table rename is not blocked for notification processing in upstream due to
    // hive bug HIVE-18783, which is fixed in Hive 2.4.0 and 3.0
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);
    statementUSER1_1.execute("DROP TABLE " + DB1 + "." + tableName1 + renameTag);


    statementAdmin.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();
  }

  /**
   * Verify that the user who creases table has owner privilege on this table, but cannot
   * access tables created by others and makes sure that HDFS ACLs are updated accordingly.
   *
   * @throws Exception
   */
  @Test
  public void testCreateTableNegative() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    // create test DB
    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statementAdmin.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1 and USER2
    statementAdmin.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");
    statementAdmin.execute("USE " + DB1);

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");

    // verify user1_2 does not have privileges on table created by user1_1
    Connection connectionUSER1_2 = hiveServer2.createConnection(USER1_2, USER1_2);
    Statement statementUSER1_2 = connectionUSER1_2.createStatement();
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_2, SentryPrincipalType.USER, Lists.newArrayList(USER1_2),
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
    // Verify that HDFS ACL are not set.
    verifyHdfsAcl(Lists.newArrayList(USER1_2), null, DB1, tableName1, null, false);

    statementAdmin.close();
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
  public void testCreateTableAdmin() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");

    // admin creates test DB and then drop it
    statementAdmin.execute("CREATE DATABASE " + DB1);
    statementAdmin.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");

    // verify owner privileges created for new table
    verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER, Lists.newArrayList(admin),
        DB1, tableName1, 1);

    // Verify that HDFS ACL are set.
    verifyHdfsAcl(Lists.newArrayList(admin), null, DB1, tableName1, null, true);

    statementAdmin.close();
    connection.close();
  }

  /**
   * Verify that the user who creases table and then drops it has no owner privilege on this table
   * and makes sure that HDFS ACLs are updated accordingly.
   *
   * @throws Exception
   */
  @Test
  public void testDropTable() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    // create test DB
    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statementAdmin.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statementUSER1_1.execute("DROP TABLE " + DB1 + "." + tableName1);

    // verify privileges created for new table
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, tableName1, 0);

    statementAdmin.close();
    connection.close();
  }

  /**
   * Verify that the owner privilege is updated when the ownership is changed
   *
   * @throws Exception
   */
  @Ignore("Enable the test once HIVE-18762 is in the hiver version integrated with Sentry")
  @Test
  public void testAlterTable() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1", "owner_role"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    // create test DB
    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statementAdmin.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");
    statementAdmin.execute("USE " + DB1);

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");


    // verify privileges created for new table
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, tableName1, 1);

    // Verify that HDFS ACL are set.
    verifyHdfsAcl(Lists.newArrayList(USER1_1), null, DB1, tableName1, null, true);

    // verify that user has all privilege on this table, i.e., "OWNER" means "ALL"
    // for authorization
    statementUSER1_1.execute("INSERT INTO TABLE " + DB1 + "." + tableName1 + " VALUES (35)");

    // Changing the owner to a role
    statementUSER1_1.execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER ROLE " +
        "owner_role");

    // Verify that HDFS ACL are not set.
    verifyHdfsAcl(Lists.newArrayList(USER1_1), null, DB1, tableName1, null, false);

    // Verify that HDFS ACL are set.
    verifyHdfsAcl(null, Lists.newArrayList(USERGROUP1), DB1, tableName1, null, true);


    // alter table rename is not blocked for notification processing in upstream due to
    // hive bug HIVE-18783, which is fixed in Hive 2.4.0 and 3.0
    Thread.sleep(WAIT_BEFORE_TESTVERIFY);

    // Verify that old owner does not have owner privilege
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, tableName1, 0);
    // Verify that new owner has owner privilege

    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.ROLE, Lists.newArrayList("owner_role"),
        DB1, tableName1, 1);


    // Changing the owner to a user
    statementUSER1_1.execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER USER " +
        USER1_1);

    // Verify that old owner does not have owner privilege
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.ROLE, Lists.newArrayList("owner_role"),
        DB1, tableName1, 0);

    // Verify that new owner has owner privilege
    verifyTableOwnerPrivilegeExistForPrincipal(statementUSER1_1, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, tableName1, 1);

    statementAdmin.close();

    statementAdmin.close();
    connection.close();

    statementUSER1_1.close();
    connectionUSER1_1.close();
  }

  /**
   * Verify that the user who can call alter table set owner on this table
   *
   * @throws Exception
   */
  @Ignore("Enable the test once HIVE-18762 is in the hiver version integrated with Sentry")
  @Test
  public void testAuthorizeAlterTableSetOwner() throws Exception {
    String ownerRole = "owner_role";
    String allWithGrantRole = "allWithGrant_role";
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1", ownerRole};

    // create required roles, and assign them to USERGROUP1
    setupUserRoles(roles, statementAdmin);

    // create test DB
    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statementAdmin.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");
    statementAdmin.execute("USE " + DB1);

    // USER1_1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");

    // owner issues alter table set owner
    if (!ownerPrivilegeGrantEnabled) {
      try {
        statementUSER1_1
            .execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER ROLE " + ownerRole);
        Assert.fail("Expect altering table set owner to fail for owner without grant option");
      } catch (Exception ex) {
        // owner without grant option cannot issue this command
      }
    }

    // owner issues alter table set owner
    if (!ownerPrivilegeGrantEnabled) {
      try {
        statementUSER1_1
            .execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER ROLE " + ownerRole);
        Assert.fail("Expect altering table set owner to fail for owner without grant option");
      } catch (Exception ex) {
        // owner without grant option cannot issue this command
      }
    }

    // admin issues alter table set owner
    try {
      statementAdmin.execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER ROLE " + ownerRole);
      Assert.fail("Expect altering table set owner to fail for admin");
    } catch (Exception ex) {
      // admin does not have grant option, so cannot issue this command
    }

    Connection connectionUSER2_1 = hiveServer2.createConnection(USER2_1, USER2_1);
    Statement statementUSER2_1 = connectionUSER2_1.createStatement();

    try {
      // create role that has all with grant on the table
      statementAdmin.execute("create role " + allWithGrantRole);
      statementAdmin.execute("grant role " + allWithGrantRole + " to group " + USERGROUP2);
      statementAdmin.execute("grant all on table " + DB1 + "." + tableName1 + " to role " +
          allWithGrantRole + " with grant option");

      // cannot issue command on a different table
      try {
        statementUSER2_1.execute("ALTER TABLE " + DB1 + ".non_exit_table" + " SET OWNER ROLE " + ownerRole);
        Assert.fail("Expect altering table set owner to fail on non-exist table");
      } catch (Exception ex) {
        // admin does not have grant option, so cannot issue this command
      }

      // user2_1 having all with grant on this table and can issue command: alter table set owner
      // alter table set owner to a role
      statementUSER2_1
          .execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER ROLE " + ownerRole);

      // verify privileges is transferred to role owner_role, which is associated with USERGROUP1,
      // therefore to USER1_1
      verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.ROLE,
          Lists.newArrayList(ownerRole),
          DB1, tableName1, 1);

      // alter table set owner to user USER1_1 and verify privileges is transferred to USER USER1_1
      statementUSER2_1
          .execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER USER " + USER1_1);
      verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER,
          Lists.newArrayList(USER1_1), DB1, tableName1, 1);

      // alter table set owner to user USER2_1, who already has explicit all with grant
      statementUSER2_1
          .execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER USER " + USER2_1);
      verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER,
          Lists.newArrayList(USER2_1),
          DB1, tableName1, 1);

    } finally {
      statementAdmin.execute("drop role " + allWithGrantRole);

      statementAdmin.close();
      connection.close();

      statementUSER1_1.close();
      connectionUSER1_1.close();

      statementUSER2_1.close();
      connectionUSER2_1.close();
    }
  }

  /**
   * Verify that no owner privilege is granted when the ownership is changed to sentry admin user
   * @throws Exception
   */
  @Ignore("Enable the test once HIVE-18762 is in the hiver version integrated with Sentry")
  @Test
  public void testAlterTableAdmin() throws Exception {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "create_db1"};

    // create required roles
    setupUserRoles(roles, statementAdmin);

    // create test DB
    statementAdmin.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statementAdmin.execute("CREATE DATABASE " + DB1);

    // setup privileges for USER1
    statementAdmin.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE create_db1");
    statementAdmin.execute("USE " + DB1);

    // USER1 create table
    Connection connectionUSER1_1 = hiveServer2.createConnection(USER1_1, USER1_1);
    Statement statementUSER1_1 = connectionUSER1_1.createStatement();
    statementUSER1_1.execute("CREATE TABLE " + DB1 + "." + tableName1
        + " (under_col int comment 'the under column')");

    // verify owner privileges created for new table
    verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER, Lists.newArrayList(USER1_1),
        DB1, tableName1, 1);

    // Changing the owner to an admin user
    statementUSER1_1.execute("ALTER TABLE " + DB1 + "." + tableName1 + " SET OWNER USER " +
        admin);

    // verify no owner privileges to the new owner as the owner is admin user

    verifyTableOwnerPrivilegeExistForPrincipal(statementAdmin, SentryPrincipalType.USER, Lists.newArrayList(admin),
        DB1, tableName1, 1);

    statementAdmin.close();
    connection.close();
  }

  // Create test roles
  protected void setupUserRoles(String[] roles, Statement statement) throws Exception {
    Set<String> userRoles = Sets.newHashSet(roles);
    userRoles.remove("admin_role");

    for (String roleName : userRoles) {
      statementAdmin.execute("CREATE ROLE " + roleName);
      statementAdmin.execute("GRANT ROLE " + roleName + " to GROUP " + USERGROUP1);
    }
  }

  // verify given table is part of every user in the list
  // verify that each entity in the list has owner privilege on the given database or table
  protected void verifyTableOwnerPrivilegeExistForPrincipal(Statement statement, SentryPrincipalType principalType,
      List<String> principals, String dbName, String tableName, int expectedResultCount) throws Exception {

    for (String principal : principals) {
      String command;

      if (Strings.isNullOrEmpty(tableName)) {
        command = "SHOW GRANT " + principalType.toString() + " " + principal + " ON DATABASE " + dbName;
      } else {
        command = "SHOW GRANT " + principalType.toString() + " " + principal + " ON TABLE " + dbName + "." + tableName;
      }

      ResultSet resultSet = statement.executeQuery(command);

      int resultSize = 0;
      while(resultSet.next()) {
        String actionValue = resultSet.getString(7);
        if (!actionValue.equalsIgnoreCase("owner")) {
          // only check owner privilege, and skip other privileges
          continue;
        }
        if(!resultSet.getString(1).equalsIgnoreCase(dbName)) {
          continue;
        }

        if (!StringUtils.equalsIgnoreCase(tableName, resultSet.getString(2))) {
          // it is possible the entity has owner privilege on both DB and table
          // only check the owner privilege on intended table. If tableName is "",
          // resultSet.getString(2) should be "" as well
          continue;
        }

        assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
        assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
        assertThat(resultSet.getString(5), equalToIgnoringCase(principal));//principalName
        assertThat(resultSet.getString(6), equalToIgnoringCase(principalType.toString()));//principalType
        assertThat(resultSet.getBoolean(8), is(ownerPrivilegeGrantEnabled));//grantOption
        resultSize ++;
      }

      assertEquals(expectedResultCount, resultSize);

      resultSet.close();
    }
  }

  /**
   * Verifies HDFS ACL for users and groups.
   * ACL could be because of explicit privilege grants or implicit owner privileges
   *
   * @param users list of users for which the ACL entries should be verified
   * @param groups list of groups for which the ACL entries should be verified
   * @param dbName Database name
   * @param tableName  Table Name
   * @param location Location of the database/table
   * @param areAclExpected whether ACL entries are expected
   * @throws Throwable If verification fails.
   */
   protected void verifyHdfsAcl(List<String> users, List<String> groups,
      String dbName, String tableName, String location, boolean areAclExpected) throws Throwable {
     String locationToVerify = location;
     try {
       if (Strings.isNullOrEmpty(locationToVerify)) {
         if (tableName == null) {
           locationToVerify = hiveWarehouseLocation + "/" + dbName + ".db";
         } else {
           locationToVerify = hiveWarehouseLocation + "/" + dbName + ".db" + "/" + tableName;
         }
       }

       if (users != null && !users.isEmpty()) {
         for (String user : users) {
           verifyUserPermOnAllSubDirs(locationToVerify, FsAction.ALL, user, areAclExpected);
         }
       }

       if (groups != null && !groups.isEmpty()) {
         for (String group : groups) {
           verifyGroupPermOnAllSubDirs(locationToVerify, FsAction.ALL, group, areAclExpected);
         }
       }
     } catch (FileNotFoundException e) {
       // If ACL's are not expected, This exception is consumed.
       if(areAclExpected) {
         throw e;
       }
     }
   }
}
