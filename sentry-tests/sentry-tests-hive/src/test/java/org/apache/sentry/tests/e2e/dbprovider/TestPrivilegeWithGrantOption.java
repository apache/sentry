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

package org.apache.sentry.tests.e2e.dbprovider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.DummySentryOnFailureHook;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPrivilegeWithGrantOption extends AbstractTestWithStaticConfiguration {

  private static boolean isInternalServer = false;
  private static int SHOW_GRANT_ROLE_DB_POSITION = 1;
  private static int SHOW_GRANT_ROLE_TABLE_POSITION = 2;
  private static int SHOW_GRANT_ROLE_WITH_GRANT_POSITION = 8;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    String hiveServer2Type = System
        .getProperty(HiveServerFactory.HIVESERVER2_TYPE);
    if ((hiveServer2Type == null)
        || HiveServerFactory.isInternalServer(HiveServerFactory.HiveServer2Type
            .valueOf(hiveServer2Type.trim()))) {
      System.setProperty(
        HiveAuthzConf.AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(),
        DummySentryOnFailureHook.class.getName());
      isInternalServer = true;
    }
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    DummySentryOnFailureHook.invoked = false;
    super.setupAdmin();
    super.setup();
  }

  /*
   * Admin grant DB_1 user1 without grant option, grant user3 with grant option,
   * user1 tries to grant it to user2, but failed.
   * user3 can grant it to user2.
   * user1 tries to revoke, but failed.
   * user3 tries to revoke user2, user3 and user1, user3 revoke user1 will failed.
   * permissions for DB_1.
   */
  @Test
  public void testOnGrantPrivilege() throws Exception {

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS db_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS db_2 CASCADE");
    statement.execute("CREATE DATABASE db_1");
    statement.execute("CREATE ROLE group1_role");
    statement.execute("GRANT ALL ON DATABASE db_1 TO ROLE group1_role");
    statement.execute("GRANT ROLE group1_role TO GROUP " + USERGROUP1);
    statement.execute("CREATE ROLE group3_grant_role");
    statement.execute("GRANT ALL ON DATABASE db_1 TO ROLE group3_grant_role WITH GRANT OPTION");
    statement.execute("GRANT ROLE group3_grant_role TO GROUP " + USERGROUP3);
    statement.execute("CREATE ROLE group2_role");
    statement.execute("GRANT ROLE group2_role TO GROUP " + USERGROUP2);

    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    statement.execute("USE db_1");
    statement.execute("CREATE TABLE foo (id int)");
    runSQLWithError(statement, "GRANT ALL ON DATABASE db_1 TO ROLE group2_role",
        HiveOperation.GRANT_PRIVILEGE, null, null, true);
    runSQLWithError(statement,
        "GRANT ALL ON DATABASE db_1 TO ROLE group2_role WITH GRANT OPTION",
        HiveOperation.GRANT_PRIVILEGE, null, null, true);
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("GRANT ALL ON DATABASE db_1 TO ROLE group2_role");
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    runSQLWithError(statement, "REVOKE ALL ON Database db_1 FROM ROLE admin_role",
        HiveOperation.REVOKE_PRIVILEGE, null, null, true);
    runSQLWithError(statement, "REVOKE ALL ON Database db_1 FROM ROLE group2_role",
        HiveOperation.REVOKE_PRIVILEGE, null, null, true);
    runSQLWithError(statement,
        "REVOKE ALL ON Database db_1 FROM ROLE group3_grant_role",
        HiveOperation.REVOKE_PRIVILEGE, null, null, true);
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("REVOKE ALL ON Database db_1 FROM ROLE group2_role");
    statement.execute("REVOKE ALL ON Database db_1 FROM ROLE group3_grant_role");
    runSQLWithError(statement, "REVOKE ALL ON Database db_1 FROM ROLE group1_role",
        HiveOperation.REVOKE_PRIVILEGE, null, null, true);

    connection.close();
    context.close();
  }

  /**
   * Test privileges with grant on parent objects are sufficient for operation
   * on child objects
   * @throws Exception
   */
  @Test
  public void testImpliedPrivilegesWithGrant() throws Exception {
    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS db_1 CASCADE");
    statement.execute("CREATE DATABASE db_1");

    statement.execute("CREATE ROLE role1");
    statement
        .execute("GRANT ALL ON DATABASE db_1 TO ROLE role1 WITH GRANT OPTION");
    statement.execute("GRANT ROLE role1 TO GROUP " + USERGROUP1);

    statement.execute("CREATE ROLE role2");
    statement.execute("GRANT ROLE role2 TO GROUP " + USERGROUP2);

    statement.execute("CREATE ROLE role3_1");
    statement.execute("GRANT ROLE role3_1 TO GROUP " + USERGROUP3);

    statement.execute("CREATE ROLE role3_2");
    statement.execute("GRANT ROLE role3_2 TO GROUP " + USERGROUP3);
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    statement.execute("USE db_1");
    statement.execute("CREATE TABLE foo (id int)");
    // user1 with grant option of ALL on DB should be able grant ALL on TABLE
    statement.execute("GRANT ALL ON TABLE foo TO ROLE role2");
    // user1 with grant option of ALL on DB should be able grant SELECT on DB
    statement.execute("GRANT SELECT ON DATABASE db_1 TO ROLE role3_1");
    // user1 with grant option of ALL on DB should be able grant INSERT on TABLE
    statement.execute("GRANT INSERT ON TABLE foo TO ROLE role3_2");
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE db_1");
    runSQLWithError(statement, "GRANT ALL ON TABLE foo TO ROLE role3_2",
        HiveOperation.GRANT_PRIVILEGE, null, null, true);
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("use db_1");
    verifySingleGrantWithGrantOption(statement,
        "SHOW GRANT ROLE role2 ON TABLE foo", SHOW_GRANT_ROLE_TABLE_POSITION,
        "foo");
    verifySingleGrantWithGrantOption(statement,
        "SHOW GRANT ROLE role3_1 ON DATABASE db_1",
        SHOW_GRANT_ROLE_DB_POSITION, "db_1");
    verifySingleGrantWithGrantOption(statement,
        "SHOW GRANT ROLE role3_2 ON TABLE foo", SHOW_GRANT_ROLE_TABLE_POSITION,
        "foo");

    // test 'with grant option' status
    verifySingleGrantWithGrantOption(statement, "show grant role role1",
        SHOW_GRANT_ROLE_WITH_GRANT_POSITION, "true");
    verifySingleGrantWithGrantOption(statement,
        "SHOW GRANT ROLE role1 ON DATABASE db_1",
        SHOW_GRANT_ROLE_WITH_GRANT_POSITION, "true");
    verifySingleGrantWithGrantOption(statement,
        "SHOW GRANT ROLE role1 ON TABLE foo",
        SHOW_GRANT_ROLE_WITH_GRANT_POSITION, "true");
    verifySingleGrantWithGrantOption(statement, "show grant role role2",
        SHOW_GRANT_ROLE_WITH_GRANT_POSITION, "false");
    verifySingleGrantWithGrantOption(statement,
        "SHOW GRANT ROLE role2 ON TABLE foo",
        SHOW_GRANT_ROLE_WITH_GRANT_POSITION, "false");
    statement.close();
    connection.close();
  }

  // run the given statement and verify that failure hook is invoked as expected
  private void runSQLWithError(Statement statement, String sqlStr,
      HiveOperation expectedOp, String dbName, String tableName,
      boolean checkSentryAccessDeniedException) throws Exception {
    // negative test case: non admin user can't create role
    assertFalse(DummySentryOnFailureHook.invoked);
    try {
      statement.execute(sqlStr);
      Assert.fail("Expected SQL exception for " + sqlStr);
    } catch (SQLException e) {
      verifyFailureHook(expectedOp, dbName, tableName, checkSentryAccessDeniedException);
    } finally {
      DummySentryOnFailureHook.invoked = false;
    }

  }

  // run the given statement and verify that failure hook is invoked as expected
  private void verifyFailureHook(HiveOperation expectedOp,
      String dbName, String tableName, boolean checkSentryAccessDeniedException)
      throws Exception {
    if (!isInternalServer) {
      return;
    }

    assertTrue(DummySentryOnFailureHook.invoked);
    if (expectedOp != null) {
      Assert.assertNotNull("Hive op is null for op: " + expectedOp, DummySentryOnFailureHook.hiveOp);
      Assert.assertTrue(expectedOp.equals(DummySentryOnFailureHook.hiveOp));
    }
    if (checkSentryAccessDeniedException) {
      Assert.assertTrue("Expected SentryDeniedException for op: " + expectedOp,
          DummySentryOnFailureHook.exception.getCause() instanceof SentryAccessDeniedException);
    }
    if(tableName != null) {
      Assert.assertNotNull("Table object is null for op: " + expectedOp, DummySentryOnFailureHook.table);
      Assert.assertTrue(tableName.equalsIgnoreCase(DummySentryOnFailureHook.table.getName()));
    }
    if(dbName != null) {
      Assert.assertNotNull("Database object is null for op: " + expectedOp, DummySentryOnFailureHook.db);
      Assert.assertTrue(dbName.equalsIgnoreCase(DummySentryOnFailureHook.db.getName()));
    }
  }

  // verify the expected object name at specific position in the SHOW GRANT result
  private void verifySingleGrantWithGrantOption(Statement statetment,
      String statementSql, int dbObjectPosition, String dbObjectName)
      throws Exception {
    ResultSet res = statetment.executeQuery(statementSql);
    assertTrue(res.next());
    assertEquals(dbObjectName, res.getString(dbObjectPosition));
    res.close();
  }

}
