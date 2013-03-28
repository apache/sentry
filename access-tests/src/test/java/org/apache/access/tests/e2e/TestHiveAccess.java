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

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHiveAccess {
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private boolean standAloneServer = false;
  private Connection adminCon;
  private Statement adminStmt;
  private final String dataFileDir = "src/test/resources";

  private String tableName = "Test_Tab007";
  private String tableComment = "Test table";

  private Connection getConnection(String username, String password) throws Exception {
    Class.forName(driverName);
    if (standAloneServer) {
      // get connection
      return DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
          username, password);
    } else {
      return DriverManager.getConnection("jdbc:hive2://", username, password);
    }
  }

  public void setUpHS2() throws Exception {
    adminCon = getConnection("admin", "admin");
    assertNotNull("Connection is null", adminCon);
    assertFalse("Connection should not be closed", adminCon.isClosed());

    adminStmt  = adminCon.createStatement();
    assertNotNull("Statement is null", adminStmt);

    adminStmt.execute("show tables");
  }

  public void setUpAccess() throws Exception {
    adminStmt.execute("set hive.support.concurrency = false");
    adminStmt.execute("set hive.semantic.analyzer.hook = org.apache.access.binding.hive.HiveAuthzBindingHook");
  }

  @Before
  public void setUp() throws Exception {
    /* To run end to end tests against HS2 from within the Access project,
     * 1. Setup Embedded HS2 and JDBC driver to talk to HS2
     * 2. Set the semantic analyzer hooks to the hive binding class in the access project
     * 3. Setup the access-site and policy files for the access provider
     */
    setUpHS2();
    setUpAccess();
  }

  @Test
  public void testSanity() throws Exception {
    // per test setup
    Connection userCon = getConnection("foo", "foo");
    Statement userStmt = userCon.createStatement();
    Path dataFilePath = new Path(dataFileDir, "kv1.dat");

    // drop table
    userStmt.execute("drop table if exists " + tableName);

    // create table
    userStmt.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string) comment '"
        + tableComment + "'");

    // load data
    userStmt.execute("load data local inpath '" + dataFilePath.toString() + "' into table "
        + tableName);
    // query table
    userStmt.executeQuery("select under_col from " + tableName + " limit 10");

    //drop table
    userStmt.execute("drop table " + tableName);

    // per test tear down
    userStmt.close();
    userCon.close();
  }

  /**
   * 1.1
  **/
  @Test
  public void testAdmin1() throws Exception {
    //edit policy file
    Util.addPolicy("admin = admin", "groups");
    Util.addPolicy("admin = server=server1:*","roles");
    Util.addPolicy("admin = admin1, admin2, admin3","users");

    //verify by SQL
    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        userStmt.execute("DROP DATABASE IF EXISTS " + dbName);
        userStmt.execute("CREATE DATABASE " + dbName);

        ResultSet res = userStmt.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
        while (res.next()) {
            assertEquals("database name is not as expected", dbName, res.getString(1));
        }
        userStmt.execute("DROP DATABASE " + dbName);

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("admin = admin");
    Util.removePolicy("admin = server=server1:*");
    Util.removePolicy("admin = admin1, admin2, admin3");
  }

  /**
   * 1.2
  **/
  @Test
  public void testAdmin2() throws Exception {
    //edit policy file
    Util.addPolicy("admin = admin", "groups");
    Util.addPolicy("admin = server=server1:*","roles");

    //verify by SQL
    //although no user is added to admin group, nothing should broken
    testSanity();

    //add user to admin group now
    Util.addPolicy("admin = admin1, admin2, admin3","users");

    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        userStmt.execute("DROP DATABASE IF EXISTS " + dbName);
        userStmt.execute("CREATE DATABASE " + dbName);

        ResultSet res = userStmt.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
        while (res.next()) {
            assertEquals("database name is not as expected", dbName, res.getString(1));
        }
        userStmt.execute("DROP DATABASE " + dbName);

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("admin = admin");
    Util.removePolicy("admin = server=server1:*");
    Util.removePolicy("admin = admin1, admin2, admin3");
  }

  /**
   * 1.3
   * Negative case
  **/
  @Test
  public void testAdmin3() throws Exception {
    //edit policy file
    Util.addPolicy("admin = admin", "groups");
    Util.addPolicy("admin = server=server1:*","roles");
    Util.addPolicy("admin = admin1, admin2, admin3","users");

    //verify by SQL
    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        userStmt.execute("DROP DATABASE IF EXISTS " + dbName);
        userStmt.execute("CREATE DATABASE " + dbName);

        ResultSet res = userStmt.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
        while (res.next()) {
            assertEquals("database name is not as expected", dbName, res.getString(1));
        }
        userStmt.execute("DROP DATABASE " + dbName);

        userStmt.close();
        userCon.close();
    }

    //remove admin1 from admin group
    Util.removePolicy("admin = admin1, admin2, admin3");
    Util.addPolicy("admin = admin2, admin3", "users");

    // verify admin1 doesn't have admin privilege
    String dbName = "db_1";
    Connection userCon = getConnection("admin1", "foo");
    Statement userStmt = userCon.createStatement();

    // should fail
    assertFalse(
            "admin1 has been remove from the admin group, should not be able to create database",
            userStmt.execute("CREATE DATABASE " + dbName));

    // restore policy file
    Util.removePolicy("admin = admin");
    Util.removePolicy("admin = server=server1:*");
    Util.removePolicy("admin = admin2, admin3");
  }

  /**
   * 1.4
  **/
  @Test
  public void testAdmin5() throws Exception {
    //edit policy file
    Util.addPolicy("admin = admin_group1, admin_group2", "groups");
    Util.addPolicy("admin = server=server1:*","roles");
    Util.addPolicy("admin_group1 = admin1, admin2, admin3","users");
    Util.addPolicy("admin_group2 = admin1, admin2, admin3","users");

    //verify by SQL
    String[] users = {"admin1", "admin2", "admin3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        userStmt.execute("DROP DATABASE IF EXISTS " + dbName);
        userStmt.execute("CREATE DATABASE " + dbName);

        ResultSet res = userStmt.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
        while (res.next()) {
            assertEquals("database name is not as expected", dbName, res.getString(1));
        }
        userStmt.execute("DROP DATABASE " + dbName);

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("admin = admin_group1, admin_group2");
    Util.removePolicy("admin = server=server1:*");
    Util.removePolicy("admin_group1 = admin1, admin2, admin3");
    Util.removePolicy("admin_group2 = admin1, admin2, admin3");
  }

  /**
   * 1.5
  **/
  @Test
  public void testGroup1() throws Exception {
    //edit policy file
    Util.addPolicy("group1 = admin", "groups");
    Util.addPolicy("admin = server=server1:*","roles");
    Util.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        userStmt.execute("DROP DATABASE IF EXISTS " + dbName);
        userStmt.execute("CREATE DATABASE " + dbName);

        ResultSet res = userStmt.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
        while (res.next()) {
            assertEquals("database name is not as expected", dbName, res.getString(1));
        }
        userStmt.execute("DROP DATABASE " + dbName);

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1 = admin");
    Util.removePolicy("admin = server=server1:*");
    Util.removePolicy("group1 = user1, user2, user3");
  }

  /**
   * 1.6
  **/
  @Test
  public void testGroup2() throws Exception {
    //edit policy file
    Util.addPolicy("group1 = admin", "groups");
    Util.addPolicy("group1 = analytics", "groups");
    Util.addPolicy("admin = server=server1:*","roles");
    Util.addPolicy("analytics = server=server1:db=default:*","roles");
    Util.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        userStmt.execute("DROP DATABASE IF EXISTS " + dbName);
        userStmt.execute("CREATE DATABASE " + dbName);

        ResultSet res = userStmt.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
        while (res.next()) {
            assertEquals("database name is not as expected", dbName, res.getString(1));
        }
        userStmt.execute("DROP DATABASE " + dbName);

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1 = admin");
    Util.removePolicy("group1 = analytics");
    Util.removePolicy("admin = server=server1:*");
    Util.removePolicy("analytics = server=server1:db=default:*");
    Util.removePolicy("group1 = user1, user2, user3");
  }

  /**
   * 1.7
   * Negative case
  **/
  @Test
  public void testGroup3() throws Exception {
    //edit policy file
    Util.addPolicy("group1 = analytics", "groups");
    Util.addPolicy("analytics = server=server1:db=default:*","roles");
    Util.addPolicy("group1 = user1, user2, user3","users");
    Util.addPolicy("group1 = user3, user4, user5","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3",
                      "user4", "user5", "user6"};
    for (String admin : users) {
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        // should fail
        assertFalse(
                "two groups with the same name should fail",
                userStmt.execute("SHOW TABLES"));

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1 = analytics");
    Util.removePolicy("analytics = server=server1:db=default:*");
    Util.removePolicy("group1 = user1, user2, user3");
    Util.removePolicy("group1 = user3, user4, user5");
  }

  /**
   * 1.8
   * Negative case
  **/
  @Test
  public void testGroup4() throws Exception {
    //edit policy file
    Util.addPolicy("group1 = analytics", "groups");
    Util.addPolicy("analytics = server=server1:db=default:*","roles");
    Util.addPolicy("group1 = user1, user2, user3","users");
    Util.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

       // should fail
        assertFalse(
                "two groups with the same name should fail",
                userStmt.execute("SHOW TABLES"));

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1 = analytics");
    Util.removePolicy("analytics = server=server1:db=default:*");
    Util.removePolicy("group1 = user1, user2, user3");
    Util.removePolicy("group1 = user1, user2, user3");
  }

  /**
   * 1.9
  **/
  @Test
  public void testGroup5() throws Exception {
    //edit policy file
    Util.addPolicy("group1 = analytics", "groups");
    Util.addPolicy("analytics = server=server1:db=default:*","roles");
    Util.addPolicy("group1 = group1, user2, user3","users");

    //verify by SQL
    String[] users = {"group1", "user2", "user3"};
    for (String admin : users) {
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        Path dataFilePath = new Path(dataFileDir, "kv1.dat");

        userStmt.execute("drop table if exists " + tableName);

        userStmt.execute("create table " + tableName
                         + " (under_col int comment 'the under column', value string) comment '"
                         + tableComment + "'");

        userStmt.execute("load data local inpath '" + dataFilePath.toString() + "' into table "
                     + tableName);

        userStmt.execute("drop table " + tableName);

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1 = analytics");
    Util.removePolicy("analytics = server=server1:db=default:*");
    Util.removePolicy("group1 = group1, user2, user3");
  }

  /**
   * 1.10
  **/
  @Test
  public void testGroup6() throws Exception {
    //edit policy file
    Util.addPolicy("group1~!@#$%^&*()+- = analytics", "groups");
    Util.addPolicy("analytics = server=server1:db=default:*","roles");
    Util.addPolicy("group1~!@#$%^&*()+- = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        Path dataFilePath = new Path(dataFileDir, "kv1.dat");

        userStmt.execute("drop table if exists " + tableName);

        userStmt.execute("create table " + tableName
                         + " (under_col int comment 'the under column', value string) comment '"
                         + tableComment + "'");

        userStmt.execute("load data local inpath '" + dataFilePath.toString() + "' into table "
                     + tableName);

        userStmt.execute("DROP TABLE " + tableName);


        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1~!@#$%^&*()+- = analytics");
    Util.removePolicy("analytics = server=server1:db=default:*");
    Util.removePolicy("group1~!@#$%^&*()+- = user1, user2, user3");
  }

  /**
   * 1.11
  **/
  @Test
  public void testGroup7() throws Exception {
    //edit policy file
    Util.addPolicy("group1 = admin", "groups");
    Util.addPolicy("admin = server=server1:*","roles");
    Util.addPolicy("group1 = user1~!@#$%^&*()+-, user2, user3","users");

    //verify by SQL
    String[] users = {"user1~!@#$%^&*()+-", "user2", "user3"};
    for (String admin : users) {
        String dbName = "db_1";
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        userStmt.execute("DROP DATABASE IF EXISTS " + dbName);
        userStmt.execute("CREATE DATABASE " + dbName);

        ResultSet res = userStmt.executeQuery("SHOW DATABASES LIKE '" + dbName + "'");
        while (res.next()) {
            assertEquals("database name is not as expected", dbName, res.getString(1));
        }
        userStmt.execute("DROP DATABASE " + dbName);

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1 = admin");
    Util.removePolicy("admin = server=server1:*");
    Util.removePolicy("group1 = user1~!@#$%^&*()+-, user2, user3");
  }

  /**
   * 1.12
   * not be able to automate this test case
  **/

  /**
   * 1.13
   * not be able to automate this test case
  **/

  /**
   * 1.14.1
   * Negative case
  **/
  @Test
  public void testFileManipulation1() throws Exception {
      File file = new File("test-authz-provider.ini");

      assertFalse("policy file shouldn't be deleted by admin", file.delete());
  }

  /**
   * 1.14.2
   * Negative case
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
  **/
  @Test
  public void testGroup8() throws Exception {
    //edit policy file
    Util.addPolicy("group1 = analytics", "groups");
    Util.addPolicy("group1 = user1, user2, user3","users");

    //verify by SQL
    String[] users = {"user1", "user2", "user3"};
    for (String admin : users) {
        Connection userCon = getConnection(admin, "foo");
        Statement userStmt = userCon.createStatement();

        //should fail
        assertFalse(
                "doesn't grant any privilege to group, should fail",
                userStmt.execute("SHOW TABLES"));

        userStmt.close();
        userCon.close();
    }

    //restore policy file
    Util.removePolicy("group1 = analytics");
    Util.removePolicy("group1 = user1, user2, user3");
  }

  /**
   * 1.16
   * cannot automate
  **/

  @After
  public void tearDown() throws Exception {
    adminStmt.close();
    adminCon.close();
  }
}
