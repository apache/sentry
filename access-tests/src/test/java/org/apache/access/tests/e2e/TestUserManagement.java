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

package org.apache.access.tests.e2e;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestUserManagement {
  private EndToEndTestContext context;
  private Map<String, String> properties;
  private final String dataFileDir = "src/test/resources";
  private String tableName = "Test_Tab007";
  private String tableComment = "Test table";

  @Before
  public void setUp() throws Exception {
    properties = Maps.newHashMap();
    context = new EndToEndTestContext(properties);
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void testSanity() throws Exception {
    // per test setup
    Connection connection = context.createConnection("foo", "foo");
    Statement statement = context.createStatement(connection);
    Path dataFilePath = new Path(dataFileDir, "kv1.dat");

    // drop table
    statement.execute("drop table if exists " + tableName);

    // create table
    statement.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string) comment '"
        + tableComment + "'");

    // load data
    statement.execute("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName);
    // query table
    statement.executeQuery("select under_col from " + tableName);

    // drop table
    statement.execute("drop table " + tableName);

    // per test tear down
    statement.close();
    statement.close();
  }

  /**
   * Steps:
   * 1. in policy file, create a admin group, ADMIN_GROUP_1.
   * 2. add a list of users (USER_1, USER_2, USER_3....) in it.
   * 3. verify every user has admin privilege
   * (execute CREATE DATABASE and DROP DATABASE)
   **/
  @Test
  public void testAdmin1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("admin2 = admin", "users");
    editor.addPolicy("admin3 = admin", "users");

    // verify by SQL
    String[] users = { "admin1", "admin2", "admin3" };
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertEquals("database " + dbName + " is not created", true,
          created);
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * Steps:
   * 1. in policy file, create a admin group.
   * 2. ADMIN_GROUP_1 don't add any user into it at this
   * moment make sure nothing is broken.
   * 3. And non-admin user is created.
   * 4. Add a list of users (USER_1, USER_2, USER_3....) in
   * ADMIN_GROUP_1 in policy file
   * 5. verify every user has admin privilege (execute
   * CREATE DATABASE and DROP DATABASE)
   **/
  @Test
  public void testAdmin2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1", "roles");

    // verify by SQL
    // although no user is added to admin group, nothing should broken
    testSanity();

    // add user to admin group now
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("admin2 = admin", "users");
    editor.addPolicy("admin3 = admin", "users");

    String[] users = { "admin1", "admin2", "admin3" };
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertEquals("database " + dbName + " is not created", true,
          created);
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /** Negative case
   * Steps:
   * 1. in policy file, create a admin group, ADMIN_GROUP_1.
   * 2. add a list of users (USER_1, USER_2, USER_3....) in it.
   * 3. verify every user has admin privilege
   * (execute CREATE DATABASE and DROP DATABASE)
   * 4. move one user USER_1 from that group.
   * 5. verify user USER_1 doesn't have admin privilege
   * (execute CREATE DATABASE and DROP DATABASE)
   **/
  @Test
  public void testAdmin3() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("admin2 = admin", "users");
    editor.addPolicy("admin3 = admin", "users");

    // verify by SQL
    String[] users = { "admin1", "admin2", "admin3" };
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertEquals("database " + dbName + " is not created", true,
          created);
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }

    // remove admin1 from admin group
    editor.removePolicy("admin1 = admin");

    // verify admin1 doesn't have admin privilege
    String dbName = "db_1";
    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = connection.createStatement();

    // should fail
    assertFalse(
        "admin1 has been remove from the admin group, should not be able to create database",
        statement.execute("CREATE DATABASE " + dbName));
  }

  /**
   * Steps:
   * 1. in policy file, create a admin group, ADMIN_GROUP_1, ADMIN_GROUP_2.
   * 2. add a list of same users (USER_1, USER_2, USER_3....) in both
   * ADMIN_GROUP_1 and ADMIN_GROUP_2.
   * 3. verify every user has admin privilege
   * (execute CREATE DATABASE and DROP DATABASE)
   **/
  @Test
  public void testAdmin5() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin_group1, admin_group2", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("admin1 = admin_group1, admin_group2", "users");
    editor.addPolicy("admin2 = admin_group1, admin_group2", "users");
    editor.addPolicy("admin3 = admin_group1, admin_group2", "users");

    // verify by SQL
    String[] users = { "admin1", "admin2", "admin3" };
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertEquals("database " + dbName + " is not created", true,
          created);
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * Steps:
   * 1. create several user groups, GROUP_1, GROUP_2, GROUP_3.
   * 2. add users into it apply admin privilege to GROUP_1.
   * 3. verify every user in GROUP_1 has admin privilege
   * (execute CREATE DATABASE and DROP DATABASE)
   **/
  @Test
  public void testGroup1() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = admin", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group1", "users");
    editor.addPolicy("user3 = group1", "users");

    // verify by SQL
    String[] users = { "user1", "user2", "user3" };
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertEquals("database " + dbName + " is not created", true,
          created);
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * Steps:
   * 1. in policy file, create user groups, GROUP_1.
   * 2. add user USER_1 into GROUP_1.
   * 3. apply admin privileges to GROUP_1,
   * apply NON-ADMIN_1 to user_1 in GROUP_1.
   * 4. verify user_1 has admin privilege
   **/
  @Test
  public void testGroup2() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = admin", "groups");
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("analytics = server=server1->db=default", "roles");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group1", "users");
    editor.addPolicy("user3 = group1", "users");

    // verify by SQL
    String[] users = { "user1", "user2", "user3" };
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertEquals("database " + dbName + " is not created", true,
          created);
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /** Negative case
   * Steps:
   * in policy file, create a group with the duplicated users, but different
   * users in it should see exception
   **/
  @Test
  public void testGroup3() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("analytics = server=server1->db=default", "roles");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group1", "users");
    editor.addPolicy("user3 = group1", "users");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group1", "users");
    editor.addPolicy("user3 = group1", "users");

    // verify by SQL
    String[] users = { "user1", "user2", "user3" };
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      // should fail
      assertFalse("two groups with the same name should fail",
          statement.execute("SHOW TABLES"));

      statement.close();
      connection.close();
    }
  }

  /** Negative case
   * Steps:
   * in policy file, create two group with the same name, but same users
   * in it should see exception
   **/
  @Test
  public void testGroup4() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("analytics = server=server1->db=default", "roles");
    editor.addPolicy("user1 = group1, group1", "users");
    editor.addPolicy("user2 = group1, group1", "users");
    editor.addPolicy("user3 = group1, group1", "users");
    // verify by SQL
    String[] users = { "user1", "user2", "user3" };
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      // should fail
      assertFalse("two groups with the same name should fail",
          statement.execute("SHOW TABLES"));

      statement.close();
      connection.close();
    }
  }

  /**
   * Steps:
   * in policy file, create a user group, 'GROUP_1', and add user with
   * same name 'GROUP_1' into it should NOT see exception
   **/
  @Test
  public void testGroup5() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("analytics = server=server1->db=default", "roles");
    editor.addPolicy("group1 = group1", "users");
    editor.addPolicy("user2 = group1", "users");
    editor.addPolicy("user3 = group1", "users");

    // verify by SQL
    String[] users = { "group1", "user2", "user3" };
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      Path dataFilePath = new Path(dataFileDir, "kv1.dat");

      statement.execute("drop table if exists " + tableName);

      statement
          .execute("create table "
              + tableName
              + " (under_col int comment 'the under column', value string) comment '"
              + tableComment + "'");

      statement.execute("load data local inpath '" + dataFilePath.toString()
          + "' into table " + tableName);

      statement.execute("drop table " + tableName);

      statement.close();
      connection.close();
    }
  }

  /**
   * Steps:
   * in policy file, create a user group, its name includes all the
   * special charactors should NOT see exception
   **/
  @Test
  public void testGroup6() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1~!@#$%^&*()+- = analytics", "groups");
    editor.addPolicy("analytics = server=server1->db=default", "roles");
    editor.addPolicy("group1~!@#$%^&*()+- = user1, user2, user3", "users");
    editor.addPolicy("user1 = group1~!@#$%^&*()+-", "users");
    editor.addPolicy("user2 = group1~!@#$%^&*()+-", "users");
    editor.addPolicy("user3 = group1~!@#$%^&*()+-", "users");

    // verify by SQL
    String[] users = { "user1", "user2", "user3" };
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      Path dataFilePath = new Path(dataFileDir, "kv1.dat");

      statement.execute("drop table if exists " + tableName);

      statement
          .execute("create table "
              + tableName
              + " (under_col int comment 'the under column', value string) comment '"
              + tableComment + "'");

      statement.execute("load data local inpath '" + dataFilePath.toString()
          + "' into table " + tableName);

      statement.execute("DROP TABLE " + tableName);

      statement.close();
      connection.close();
    }
  }

  /**
   * Steps:
   * in policy file, create a user group, add one user into it, user's
   * name includes all the special charactors make this group an admin group (or
   * non-admin group) should NOT see exception
   **/
  @Test
  public void testGroup7() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = admin", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("user1~!@#$%^&*()+- = group1", "users");
    editor.addPolicy("user2 = group1", "users");
    editor.addPolicy("user3 = group1", "users");

    // verify by SQL
    String[] users = { "user1~!@#$%^&*()+-", "user2", "user3" };
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES");
      boolean created = false;
      while (res.next()) {
        if (res.getString(1).equals(dbName)) {
          created = true;
        }
      }
      assertEquals("database " + dbName + " is not created", true,
          created);
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /** Negative case
   * Steps:
   * in policy file, create a new group admin try to apply a doesn't
   * existing role (either admin or non-admin role) to this group will fail
   **/
  @Test
  public void testGroup8() throws Exception {
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group1", "users");
    editor.addPolicy("user3 = group1", "users");

    // verify by SQL
    String[] users = { "user1", "user2", "user3" };
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      // should fail
      assertFalse("doesn't grant any privilege to group, should fail",
          statement.execute("SHOW TABLES"));

      statement.close();
      connection.close();
    }
  }
}
