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

import java.sql.Connection;
import java.sql.Statement;

import junit.framework.Assert;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestMetadataPermissions extends AbstractTestWithStaticConfiguration {
  private Context context;
  private PolicyFile policyFile;

  @Before
  public void setup() throws Exception {
    context = createContext();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all", "db2_all")
        .addRolesToGroup(USERGROUP2, "db1_all")
        .addPermissionsToRole("db1_all", "server=server1->db=db1")
        .addPermissionsToRole("db2_all", "server=server1->db=db2")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    Connection adminCon = context.createConnection(ADMIN1);
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
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /**
   * Ensure that a user with no privileges on a database cannot
   * query that databases metadata.
   */
  @Test
  public void testDescPrivilegesNegative() throws Exception {
    String dbName = "db2";
    Connection connection = context.createConnection(USER2_1);
    Statement statement = context.createStatement(connection);
    context.assertAuthzException(statement, "USE " + dbName);
//    TODO when DESCRIBE db.table is supported tests should be uncommented
//    for (String tabName : new String[] { "tab1", "tab2" }) {
//      context.assertAuthzException(statement, "DESCRIBE " + dbName + "." + tabName);
//      context.assertAuthzException(statement, "DESCRIBE EXTENDED " + dbName + "." + tabName);
//    }
    statement.close();
    connection.close();
  }

  /**
   * Ensure that a user cannot describe databases to which the user
   * has no privilege.
   */
  @Test
  public void testDescDbPrivilegesNegative() throws Exception {
    String dbName = "db2";
    Connection connection = context.createConnection(USER2_1);
    Statement statement = context.createStatement(connection);
    context.assertAuthzException(statement, "DESCRIBE DATABASE " + dbName);
    context.assertAuthzException(statement, "DESCRIBE DATABASE EXTENDED " + dbName);
    statement.close();
    connection.close();
  }

  /**
   * Ensure that a user with privileges on a database can describe
   * the database.
   */
  @Test
  public void testDescDbPrivilegesPositive() throws Exception {
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    for (String dbName : new String[] { "db1", "db2" }) {
      statement.execute("USE " + dbName);
      Assert.assertTrue(statement.executeQuery("DESCRIBE DATABASE " + dbName).next());
      Assert.assertTrue(statement.executeQuery("DESCRIBE DATABASE EXTENDED " + dbName).next());
    }
    statement.close();
    connection.close();
  }

  /**
   * Ensure that a user with privileges on a table can describe the table.
   */
  @Test
  public void testDescPrivilegesPositive() throws Exception {
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    for (String dbName : new String[] { "db1", "db2" }) {
      statement.execute("USE " + dbName);
      Assert.assertTrue(statement.executeQuery("DESCRIBE DATABASE " + dbName).next());
      for (String tabName : new String[] { "tab1", "tab2" }) {
        Assert.assertTrue(statement.executeQuery("DESCRIBE " + tabName).next());
        Assert.assertTrue(statement.executeQuery("DESCRIBE EXTENDED " + tabName).next());

      }
    }
    statement.close();
    connection.close();
  }

}
