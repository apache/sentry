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

import static org.junit.Assert.assertFalse;
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

public class TestUserManagement extends AbstractTestWithStaticLocalFS {
  private static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private static final String dbName = "db1";
  private static final String tableName = "t1";
  private static final String tableComment = "Test table";
  private File dataFile;
  private Context context;
  private PolicyFile policyFile;

  @Before
  public void setUp() throws Exception {
    context = createContext();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
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
  private void doCreateDbLoadDataDropDb(String admin, String...users) throws Exception {
    doDropDb(admin);
    for (String user : users) {
      doCreateDb(user);
      Connection connection = context.createConnection(user, "password");
      Statement statement = context.createStatement(connection);
      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertTrue("database " + dbName + " is not created", created);
      doCreateTableLoadData(user);
      doDropDb(user);
      statement.close();
      connection.close();
    }
  }
  private void doDropDb(String user) throws Exception {
    Connection connection = context.createConnection(user, "password");
    Statement statement = connection.createStatement();
    statement.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    statement.close();
    connection.close();
  }
  private void doCreateDb(String user) throws Exception {
    Connection connection = context.createConnection(user, "password");
    Statement statement = connection.createStatement();
    statement.execute("CREATE DATABASE " + dbName);
    statement.close();
    connection.close();
  }
  private void doCreateTableLoadData(String user) throws Exception {
    Connection connection = context.createConnection(user, "password");
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + dbName);
    statement.execute("CREATE TABLE " + tableName +
        " (under_col int comment 'the under column', value string) comment '"
        + tableComment + "'");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' into table " + tableName);
    assertTrue(statement.execute("SELECT * FROM " + tableName));
    statement.close();
    connection.close();
  }
  /**
   * Basic sanity test
   */
  @Test
  public void testSanity() throws Exception {
    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile.write(context.getPolicyFile());
    doCreateDbLoadDataDropDb("admin1", "admin1");
  }

  /**
   * Tests admin privileges allow admins to create/drop dbs
   **/
  @Test
  public void testAdmin1() throws Exception {
    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile
        .addGroupsToUser("admin2", "admin")
        .addGroupsToUser("admin3", "admin")
        .write(context.getPolicyFile());

    doCreateDbLoadDataDropDb("admin1", "admin1", "admin2", "admin3");
  }

  /**
   * Negative case: Tests that when a user is removed
   * from the policy file their permissions have no effect
   **/
  @Test
  public void testAdmin3() throws Exception {
    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile
        .addGroupsToUser("admin2", "admin")
        .addGroupsToUser("admin3", "admin")
        .write(context.getPolicyFile());
    doCreateDbLoadDataDropDb("admin1", "admin1", "admin2", "admin3");

    // remove admin1 from admin group
    policyFile
        .removeGroupsFromUser("admin1", "admin")
        .write(context.getPolicyFile());
    // verify admin1 doesn't have admin privilege
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = connection.createStatement();
    context.assertAuthzException(statement, "CREATE DATABASE somedb");
    statement.close();
    connection.close();
  }

  /**
   * Tests that users in two groups work correctly
   **/
  @Test
  public void testAdmin5() throws Exception {
    policyFile = new PolicyFile();
    policyFile
        .addRolesToGroup("admin_group1", "admin")
        .addRolesToGroup("admin_group2", "admin")
        .addPermissionsToRole("admin", "server=server1")
        .addGroupsToUser("admin1", "admin_group1", "admin_group2")
        .addGroupsToUser("admin2", "admin_group1", "admin_group2")
        .addGroupsToUser("admin3", "admin_group1", "admin_group2")
        .write(context.getPolicyFile());
    doCreateDbLoadDataDropDb("admin1", "admin1", "admin2", "admin3");
  }

  /**
   * Tests admin group does not infect non-admin group
   **/
  @Test
  public void testAdmin6() throws Exception {
    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile
        .addRolesToGroup("group1", "non_admin_role")
        .addPermissionsToRole("non_admin_role", "server=server1->db=" + dbName)
        .addGroupsToUser("user1", "group1")
        .write(context.getPolicyFile());

    doCreateDbLoadDataDropDb("admin1", "admin1");
    Connection connection = context.createConnection("user1", "password");
    Statement statement = connection.createStatement();
    context.assertAuthzException(statement, "CREATE DATABASE " + dbName);
    statement.close();
    connection.close();
  }

  /**
   * Tests that user with two roles the most powerful role takes effect
   **/
  @Test
  public void testGroup2() throws Exception {
    policyFile = new PolicyFile();
    policyFile
        .addRolesToGroup("group1", "admin", "analytics")
        .addPermissionsToRole("admin", "server=server1")
        .addPermissionsToRole("analytics", "server=server1->db=" + dbName)
        .addGroupsToUser("user1", "group1")
        .addGroupsToUser("user2", "group1")
        .addGroupsToUser("user3", "group1")
        .write(context.getPolicyFile());
    doCreateDbLoadDataDropDb("user1", "user1", "user2", "user3");
  }
  /**
   * Tests that user without uri privilege can create table but not load data
   **/
  @Test
  public void testGroup4() throws Exception {
    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile
        .addRolesToGroup("group1", "non_admin_role", "load_data")
        .addPermissionsToRole("non_admin_role", "server=server1->db=" + dbName)
        .addGroupsToUser("user1", "group1")
        .addGroupsToUser("user2", "group1")
        .addGroupsToUser("user3", "group1")
        .write(context.getPolicyFile());

    doDropDb("admin1");
    for(String user : new String[]{"user1", "user2", "user3"}) {
      doCreateDb("admin1");
      Connection connection = context.createConnection(user, "password");
      Statement statement = context.createStatement(connection);
      statement.execute("USE " + dbName);
      statement.execute("CREATE TABLE " + tableName +
          " (under_col int comment 'the under column', value string) comment '"
          + tableComment + "'");
      context.assertAuthzException(statement,
          "LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' into table " + tableName);
      assertTrue(statement.execute("SELECT * FROM " + tableName));
      statement.close();
      connection.close();
      doDropDb("admin1");
    }
  }
  /**
   * Tests users can have same name as groups
   **/
  @Test
  public void testGroup5() throws Exception {

    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile
        .addRolesToGroup("group1", "non_admin_role", "load_data")
        .addPermissionsToRole("non_admin_role", "server=server1->db=" + dbName)
        .addPermissionsToRole("load_data", "server=server1->URI=file://" + dataFile.getPath())
        .addGroupsToUser("group1", "group1")
        .addGroupsToUser("user2", "group1")
        .addGroupsToUser("user3", "group1")
        .write(context.getPolicyFile());

    doDropDb("admin1");
    for(String user : new String[]{"group1", "user2", "user3"}) {
      doCreateDb("admin1");
      doCreateTableLoadData(user);
      doDropDb("admin1");
    }
  }

  /**
   * Tests that group names with special characters are handled correctly
   **/
  @Test
  public void testGroup6() throws Exception {
    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile
        .addRolesToGroup("group1~!@#$%^&*()+-", "analytics", "load_data")
        .addPermissionsToRole("analytics", "server=server1->db=" + dbName)
        .addPermissionsToRole("load_data", "server=server1->URI=file://" + dataFile.getPath())
        .addGroupsToUser("user1", "group1~!@#$%^&*()+-")
        .addGroupsToUser("user2", "group1~!@#$%^&*()+-")
        .addGroupsToUser("user3", "group1~!@#$%^&*()+-")
        .write(context.getPolicyFile());

    doDropDb("admin1");
    for(String user : new String[]{"user1", "user2", "user3"}) {
      doCreateDb("admin1");
      doCreateTableLoadData(user);
      doDropDb("admin1");
    }
  }

  /**
   * Tests that user names with special characters are handled correctly
   **/
  @Test
  public void testGroup7() throws Exception {
    policyFile = new PolicyFile();
    policyFile
        .addRolesToGroup("group1", "admin")
        .addPermissionsToRole("admin", "server=server1")
        .addGroupsToUser("user1~!@#$%^&*()+-", "group1")
        .addGroupsToUser("user2", "group1")
        .addGroupsToUser("user3", "group1")
        .write(context.getPolicyFile());
    doCreateDbLoadDataDropDb("user1~!@#$%^&*()+-", "user1~!@#$%^&*()+-", "user2", "user3");
  }

  /**
   * Tests that users with no privileges cannot list any tables
   **/
  @Test
  public void testGroup8() throws Exception {
    policyFile = PolicyFile.createAdminOnServer1(ADMIN1);
    policyFile
        .addRolesToGroup("group1", "analytics")
        .addGroupsToUser("user1", "group1")
        .addGroupsToUser("user2", "group1")
        .addGroupsToUser("user3", "group1")
        .write(context.getPolicyFile());

    Connection connection = context.createConnection("admin1", "password");
    Statement statement = connection.createStatement();
    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");
    statement.execute("CREATE TABLE t1 (under_col int, value string)");
    statement.close();
    connection.close();
    String[] users = { "user1", "user2", "user3" };
    for (String user : users) {
      connection = context.createConnection(user, "foo");
      statement = context.createStatement(connection);
      assertFalse("No results should be returned",
          statement.executeQuery("SHOW TABLES").next());
      statement.close();
      connection.close();
    }
  }
}
