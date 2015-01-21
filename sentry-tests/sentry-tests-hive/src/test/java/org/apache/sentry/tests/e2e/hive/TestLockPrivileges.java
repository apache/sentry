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

package org.apache.sentry.tests.e2e.hive;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLockPrivileges extends AbstractTestWithStaticConfiguration {
  private PolicyFile policyFile;
  final String tableName = "tb1";

  static Map<String, String> privileges = new HashMap<String, String>();
  static {
    privileges.put("all_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=all");
    privileges.put("select_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=select");
    privileges.put("insert_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=insert");
    privileges.put("alter_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=alter");
    privileges.put("lock_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=lock");

    privileges.put("all_db1", "server=server1->db=" + DB1 + "->action=all");
    privileges.put("select_db1", "server=server1->db=" + DB1 + "->action=select");
    privileges.put("insert_db1", "server=server1->db=" + DB1 + "->action=insert");
    privileges.put("alter_db1", "server=server1->db=" + DB1 + "->action=alter");
    privileges.put("lock_db1", "server=server1->db=" + DB1 + "->action=lock");
  }

  @BeforeClass
  public static void setHiveConcurrency() throws Exception {
    enableHiveConcurrency = true;
    setupTestStaticConfiguration();
  }

  private void adminCreate(String db, String table) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + db + " CASCADE");
    statement.execute("CREATE DATABASE " + db);
    if (table != null) {
      statement.execute("CREATE table " + db + "." + table + " (a string)");
    }
    statement.close();
    connection.close();
  }

  @Before
  public void setup() throws Exception {
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP).setUserGroupMapping(
        StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
  }

  @Test
  public void testLockDatabase() throws Exception {
    String partialErrorMsgForNoPrivilege = "No valid privileges";
    String assertErrorException = "The exception is not the same as the expectation.";
    String assertExceptionThrown = "SQLException will be thrown.";

    adminCreate(DB1, null);
    policyFile.addPermissionsToRole("lock_db1", privileges.get("lock_db1"))
        .addRolesToGroup(USERGROUP1, "lock_db1")
        .addPermissionsToRole("insert_db1", privileges.get("insert_db1"))
        .addRolesToGroup(USERGROUP2, "insert_db1")
        .addPermissionsToRole("select_db1", privileges.get("select_db1"))
        .addRolesToGroup(USERGROUP2, "select_db1")
        .addPermissionsToRole("alter_db1", privileges.get("alter_db1"))
        .addRolesToGroup(USERGROUP2, "alter_db1")
        .addPermissionsToRole("all_db1", privileges.get("all_db1"))
        .addRolesToGroup(USERGROUP3, "all_db1");
    writePolicyFile(policyFile);

    // user1 has lock privilege only
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("LOCK DATABASE db_1 SHARED");
    try {
      statement.execute("UNLOCK DATABASE db_1");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is successful.
      assertTrue(assertErrorException, se.getMessage().indexOf(partialErrorMsgForNoPrivilege) == -1);
    }

    // user2 has privileges with insert, select, alter, but has no lock privilege
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    try {
      statement.execute("LOCK DATABASE db_1 SHARED");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is failed, the error message include "No valid privileges"
      assertTrue(assertErrorException, se.getMessage().indexOf(partialErrorMsgForNoPrivilege) > 0);
    }
    try {
      statement.execute("UNLOCK DATABASE db_1");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is failed, the error message include "No valid privileges"
      assertTrue(assertErrorException, se.getMessage().indexOf(partialErrorMsgForNoPrivilege) > 0);
    }

    // user3 has All privilege
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("LOCK DATABASE db_1 SHARED");
    try {
      statement.execute("UNLOCK DATABASE db_1");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is successful.
      assertTrue(assertErrorException, se.getMessage().indexOf(partialErrorMsgForNoPrivilege) == -1);
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testLockTable() throws Exception {
    String partialErrorMsgForNoPrivilege = "No valid privileges";
    String assertErrorException = "The exception is not the same as the expectation.";
    String assertExceptionThrown = "SQLException will be thrown.";

    adminCreate(DB1, tableName);
    policyFile.addPermissionsToRole("lock_db1_tb1", privileges.get("lock_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "lock_db1_tb1")
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "insert_db1_tb1")
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "select_db1_tb1")
        .addPermissionsToRole("alter_db1_tb1", privileges.get("alter_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "alter_db1_tb1")
        .addPermissionsToRole("all_db1_tb1", privileges.get("all_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "all_db1_tb1");
    writePolicyFile(policyFile);

    // user1 has lock privilege only
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("LOCK TABLE tb1 SHARED");
    try {
      statement.execute("UNLOCK TABLE tb1");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is successful.
      assertTrue(assertErrorException, se.getMessage().indexOf(partialErrorMsgForNoPrivilege) == -1);
    }

    // user2 has privileges with insert, select, alter, but has no lock privilege
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    try {
      statement.execute("LOCK TABLE tb1 SHARED");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is failed, the error message include "No valid privileges"
      assertTrue(assertErrorException,
          se.getMessage().indexOf(partialErrorMsgForNoPrivilege) > 0);
    }
    try {
      statement.execute("UNLOCK TABLE tb1");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is failed, the error message include "No valid privileges"
      assertTrue(assertErrorException,
          se.getMessage().indexOf(partialErrorMsgForNoPrivilege) > 0);
    }

    // user3 has All privilege
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("LOCK TABLE tb1 SHARED");
    try {
      statement.execute("UNLOCK TABLE tb1");
      fail(assertExceptionThrown);
    } catch (SQLException se) {
      // Authorization is successful.
      assertTrue(assertErrorException, se.getMessage().indexOf(partialErrorMsgForNoPrivilege) == -1);
    }
    statement.close();
    connection.close();
  }
}
