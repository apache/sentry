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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;


public class TestMedataPermissions {
  private EndToEndTestContext context;
  @Before
  public void setup() throws Exception {
    context = new EndToEndTestContext(new HashMap<String, String>());
    // edit policy file
    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1 = db1_all,db2_all",
        "user_group2 = db1_all",
        "[roles]",
        "db1_all = server=server1->db=db1->table=*->action=insert",
        "db2_all = server=server1->db=db2->table=*->action=select",
        "admin_role = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        "admin = admin_group"
        };
    context.makeNewPolicy(testPolicies);
    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    for (String dbName : new String[] { "db1", "db2" }) {
      adminStmt.execute("USE default");
      adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
      adminStmt.execute("CREATE DATABASE " + dbName);
      adminStmt.execute("USE " + dbName);
      for (String tabName : new String[] { "tab1", "tab2" }) {
        adminStmt.execute("CREATE TABLE " + tabName + " (id int)");
      }
    }
    context.close();
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  @AfterClass
  public static void shutDown() throws IOException {
    EndToEndTestContext.shutdown();
  }

  /**
   * Test Case 2.14
   * Positive test:  Describe <object>
   */
  @Test
  public void testDescPrivilegesNegative() throws Exception {
  // verify user2 doesn't can't run describe agains db2 objects
    Connection user2Con = context.createConnection("user2", "foo");
    Statement user2Stmt = context.createStatement(user2Con);
    String dbName = "db2";

    user2Stmt.execute("USE " + dbName);

    for (String tabName : new String[] { "tab1", "tab2" }) {
      try {
        user2Stmt.execute("DESCRIBE " + tabName);
        Assert.assertTrue("user2 shouldn't be able to run describe on db2 objects", false);
      } catch (SQLException e) {
        context.verifyAuthzException(e);
      }
      try {
        user2Stmt.execute("DESCRIBE EXTENDED " + tabName);
        Assert.assertTrue("user2 shouldn't be able to run describe on db2 objects", false);
      } catch (SQLException e) {
        context.verifyAuthzException(e);
      }
    }
  }

  /**
   * Test Case 2.14
   * Negative test:  Describe database <object>
   */
  @Test
  public void testDescDbPrivilegesNegative() throws Exception {
  // verify user2 doesn't can't run describe db2 database
    Connection user2Con = context.createConnection("user2", "foo");
    Statement user2Stmt = context.createStatement(user2Con);
    String dbName = "db2";
    try {
      Assert.assertTrue(user2Stmt.execute("DESCRIBE DATABASE " + dbName));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
    Assert.assertTrue(user2Stmt.execute("DESCRIBE DATABASE EXTENDED " + dbName));
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
  }

  /**
   * Test Case 2.14
   * Positive test:  Describe database <object>
   */
  @Test
  public void testDescDbPrivilegesPositive() throws Exception {
    // verify user1 can describe both dbs and tables
    Connection user1Con = context.createConnection("user1", "foo");
    Statement user1Stmt = context.createStatement(user1Con);
    for (String dbName : new String[] { "db1", "db2" }) {
      user1Stmt.execute("USE " + dbName);
      Assert.assertTrue(user1Stmt.execute("DESCRIBE DATABASE " + dbName));
      Assert.assertTrue(user1Stmt.execute("DESCRIBE DATABASE EXTENDED " + dbName));
    }
  }

  /**
   * Test Case 2.14
   * Positive test:  Describe <object>
   */
  @Test
  public void testDescPrivilegesPositive() throws Exception {
    // verify user1 can describe both tables
    Connection user1Con = context.createConnection("user1", "foo");
    Statement user1Stmt = context.createStatement(user1Con);
    for (String dbName : new String[] { "db1", "db2" }) {
      user1Stmt.execute("USE " + dbName);
      Assert.assertTrue(user1Stmt.execute("DESCRIBE DATABASE " + dbName));
      for (String tabName : new String[] { "tab1", "tab2" }) {
        Assert.assertTrue(user1Stmt.execute("DESCRIBE " + tabName));
        Assert.assertTrue(user1Stmt.execute("DESCRIBE EXTENDED " + tabName));

      }
    }
  }

}
