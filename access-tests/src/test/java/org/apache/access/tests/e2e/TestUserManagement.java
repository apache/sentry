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
import static org.junit.Assert.assertNotNull;

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
//  private boolean standAloneServer = false;
  private EndToEndTestContext context;
  private Map<String, String> properties;
  private final String dataFileDir = "src/test/resources";
  private String tableName = "Test_Tab007";
  private String tableComment = "Test table";

  @Before
  public void setUp() throws Exception {
    properties = Maps.newHashMap();
    context = new EndToEndTestContext(false, properties);
  }

  @After
  public void tearDown() throws Exception {
    if(context != null) {
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
    statement.execute("load data local inpath '" + dataFilePath.toString() + "' into table "
        + tableName);
    // query table
    statement.executeQuery("select under_col from " + tableName);

    //drop table
    statement.execute("drop table " + tableName);

    // per test tear down
    statement.close();
    statement.close();
  }

  /**
   * 1.1
   *
   * Steps:
   * in policy file, create a admin group, ADMIN_GROUP_1
   * add a list of users (USER_1, USER_2, USER_3....) in it
   * verify every user has admin privilege (execute CREATE DATABASE and DROP DATABASE)
  **/
  @Test
  public void testAdmin1() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1:*","roles");
    editor.addPolicy("admin = admin1, admin2, admin3","users");

    //verify by SQL
    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
      while (res.next()) {
          assertEquals("database name is not as expected", dbName, res.getString(1));
      }
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.2
   *
   * Steps:
   * in policy file, create a admin group, ADMIN_GROUP_1
   * don't add any user into it at this moment
   * make sure nothing is broken, and no admin user is created
   * add a list of users (USER_1, USER_2, USER_3....) in ADMIN_GROUP_1 in policy file
   * verify every user has admin privilege (execute CREATE DATABASE and DROP DATABASE)
  **/
  @Test
  public void testAdmin2() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1:*","roles");

    //verify by SQL
    //although no user is added to admin group, nothing should broken
    testSanity();

    //add user to admin group now
    editor.addPolicy("admin = admin1, admin2, admin3","users");

    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
      while (res.next()) {
          assertEquals("database name is not as expected", dbName, res.getString(1));
      }
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.3
   * Negative case
   *
   * Steps:
   * in policy file, create a admin group, ADMIN_GROUP_1
   * add a list of users (USER_1, USER_2, USER_3....) in it
   * verify every user has admin privilege (execute CREATE DATABASE and DROP DATABASE)
   * move one user USER_1 from that group
   * verify user USER_1 doesn't have admin privilege (execute CREATE DATABASE and DROP DATABASE)
  **/
  @Test
  public void testAdmin3() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("admin = server=server1:*","roles");
    editor.addPolicy("admin = admin1, admin2, admin3","users");

    //verify by SQL
    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
      while (res.next()) {
          assertEquals("database name is not as expected", dbName, res.getString(1));
      }
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }

    //remove admin1 from admin group
    editor.removePolicy("admin = admin1, admin2, admin3");
    editor.addPolicy("admin = admin2, admin3", "users");

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
   * 1.4
   *
   * Steps:
   *
   * in policy file, create a admin group, ADMIN_GROUP_1, ADMIN_GROUP_2
   * add a list of same users (USER_1, USER_2, USER_3....) in both ADMIN_GROUP_1 and ADMIN_GROUP_2
   * verify every user has admin privilege (execute CREATE DATABASE and DROP DATABASE)
  **/
  @Test
  public void testAdmin5() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin_group1, admin_group2", "groups");
    editor.addPolicy("admin = server=server1:*","roles");
    editor.addPolicy("admin_group1 = admin1, admin2, admin3","users");
    editor.addPolicy("admin_group2 = admin1, admin2, admin3","users");

    //verify by SQL
    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
      while (res.next()) {
          assertEquals("database name is not as expected", dbName, res.getString(1));
      }
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.5
   *
   * Steps:
   * create several user groups, GROUP_1, GROUP_2, GROUP_3, add users into it
   * apply admin privilege to GROUP_1
   * verify every user in GROUP_1 has admin privilege (execute CREATE DATABASE and DROP DATABASE)
  **/
  @Test
  public void testGroup1() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = admin", "groups");
    editor.addPolicy("admin = server=server1:*","roles");
    editor.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
      while (res.next()) {
          assertEquals("database name is not as expected", dbName, res.getString(1));
      }
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.6
   *
   * Steps:
   *
   * in policy file, create several user groups, GROUP_1 add user USER_1 into GROUP_1
   * apply admin privileges to GROUP_1, apply NON-ADMIN_1 to user_1 in GROUP_1
   * verify user_1 has admin privilege
  **/
  @Test
  public void testGroup2() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = admin", "groups");
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("admin = server=server1:*","roles");
    editor.addPolicy("analytics = server=server1:db=default:*","roles");
    editor.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
      while (res.next()) {
          assertEquals("database name is not as expected", dbName, res.getString(1));
      }
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.7
   * Negative case
   *
   * Steps:
   * in policy file, create two group with the same name, but different users in it
   * should see exception
  **/
  @Test
  public void testGroup3() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("analytics = server=server1:db=default:*","roles");
    editor.addPolicy("group1 = user1, user2, user3","users");
    editor.addPolicy("group1 = user3, user4, user5","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3",
                      "user4", "user5", "user6"};
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      // should fail
      assertFalse(
              "two groups with the same name should fail",
              statement.execute("SHOW TABLES"));

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.8
   * Negative case
   *
   * Steps:
   * in policy file, create two group with the same name, but same users in it
   * should see exception
  **/
  @Test
  public void testGroup4() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("analytics = server=server1:db=default:*","roles");
    editor.addPolicy("group1 = user1, user2, user3","users");
    editor.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      // should fail
      assertFalse(
              "two groups with the same name should fail",
              statement.execute("SHOW TABLES"));

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.9
   *
   * Steps:
   * in policy file, create a user group, 'GROUP_1', and add user with same name 'GROUP_1' into it
   * should NOT see exception
  **/
  @Test
  public void testGroup5() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("analytics = server=server1:db=default:*","roles");
    editor.addPolicy("group1 = group1, user2, user3","users");

    //verify by SQL
    String[] users = {"group1", "user2", "user3"};
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      Path dataFilePath = new Path(dataFileDir, "kv1.dat");

      statement.execute("drop table if exists " + tableName);

      statement.execute("create table " + tableName
                     + " (under_col int comment 'the under column', value string) comment '"
                     + tableComment + "'");

      statement.execute("load data local inpath '" + dataFilePath.toString() + "' into table "
                     + tableName);

      statement.execute("drop table " + tableName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.10
   *
   * Steps:
   * in policy file, create a user group, its name includes all the special charactors
   * should NOT see exception
  **/
  @Test
  public void testGroup6() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1~!@#$%^&*()+- = analytics", "groups");
    editor.addPolicy("analytics = server=server1:db=default:*","roles");
    editor.addPolicy("group1~!@#$%^&*()+- = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      Path dataFilePath = new Path(dataFileDir, "kv1.dat");

      statement.execute("drop table if exists " + tableName);

      statement.execute("create table " + tableName
                     + " (under_col int comment 'the under column', value string) comment '"
                     + tableComment + "'");

      statement.execute("load data local inpath '" + dataFilePath.toString() + "' into table "
                     + tableName);

      statement.execute("DROP TABLE " + tableName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.11
   *
   * Steps:
   * in policy file, create a user group, add one user into it, user's name includes all the special charactors
   * make this group an admin group (or non-admin group)
   * should NOT see exception
  **/
  @Test
  public void testGroup7() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = admin", "groups");
    editor.addPolicy("admin = server=server1:*","roles");
    editor.addPolicy("group1 = user1~!@#$%^&*()+-, user2, user3","users");

    //verify by SQL
    String[] users = {"user1~!@#$%^&*()+-", "user2", "user3"};
    for (String admin : users) {
      String dbName = "db_1";
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      statement.execute("DROP DATABASE IF EXISTS " + dbName);
      statement.execute("CREATE DATABASE " + dbName);

      ResultSet res = statement.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
      while (res.next()) {
          assertEquals("database name is not as expected", dbName, res.getString(1));
      }
      statement.execute("DROP DATABASE " + dbName);

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.12
   * not be able to automate this test case
   *
   * Steps:
   * a non-admin user USER_1 try to read/write to privilege file should fail
   * admin user edit policy file to add USER_1 into admin group
   * USER_1 should be able to read and edit policy file
  **/

  /**
   * 1.13
   * not be able to automate this test case
   *
   * Steps:
   * a admin user USER_1 try to read/write to privilege file should OK
   * admin user edit policy file to remove USER_1 from admin group
   * USER_1 should NOT be able to read and edit policy file
  **/

  /**
   * 1.14.1
   * Negative case
   *
   * Steps:
   * admin try to delete policy file should fail
  **/
  @Test
  public void testFileManipulation1() throws Exception {
    File file = new File("test-authz-provider.ini");

    assertFalse("policy file shouldn't be deleted by admin", file.delete());
  }

  /**
   * 1.14.2
   * Negative case
   *
   * Steps:
   * admin try to rename policy file should fail
  **/
  @Test
  public void testFileManipulation2() throws Exception {
    File file = new File("test-authz-provider.ini");

    assertFalse("policy file shouldn't be renameed by admin",
        file.renameTo(new File("test-authz-provider_rename.ini")));
  }

  /**
   * 1.14.3
   * Negative case
   *
   * Steps:
   * admin try to change policy file permission should fail
  **/
  @Test
  public void testFileManipulation3() throws Exception {
    File file = new File("test-authz-provider.ini");

    assertFalse("policy file shouldn't be set writable by admin",
        file.setWritable(true));
  }

  /**
   * 1.15
   * Negative case
   *
   * Steps:
   * in policy file, create a new group
   * admin try to apply a doesn't existing role (either admin or non-admin role) to this group will fail
  **/
  @Test
  public void testGroup8() throws Exception {
    //edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("group1 = analytics", "groups");
    editor.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
      Connection connection = context.createConnection(admin, "foo");
      Statement statement = context.createStatement(connection);

      //should fail
      assertFalse(
              "doesn't grant any privilege to group, should fail",
              statement.execute("SHOW TABLES"));

      statement.close();
      connection.close();
    }
  }

  /**
   * 1.16
   * cannot automate
   *
   * Steps:
   * any change to policy file doesn't require restart HS2 to take effect
   * edit policy file, don't restart HS2, make sure the effect is as expected
   * edit policy file, restart HS2, make sure the effect is still as expected
  **/
}
