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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// uses mini DFS cluster
public class TestExportImportPrivileges extends AbstractTestWithStaticDFS {
  private Context context;
  private static final String dataFile = "/kv1.dat";
  private String dataFilePath = this.getClass().getResource(dataFile).getFile();

  @Before
  public void setup() throws Exception {
    context = createContext();
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  // test insert overwrite directory
  @Test
  public void testInsertToDirPrivileges() throws Exception {
    String dbName = "db1";
    String tabName = "tab1";
    Connection userConn = null;
    Statement userStmt = null;
    String dumpDir = context.getDFSUri().toString() + "/hive_data_dump";

    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = db1_read, db1_write, data_dump",
        "user_group2  = db1_read, db1_write",
        "[roles]",
        "db1_write = server=server1->db=" + dbName + "->table=" + tabName + "->action=INSERT",
        "db1_read = server=server1->db=" + dbName + "->table=" + tabName + "->action=SELECT",
        "data_dump = server=server1->URI=" + dumpDir,
        "admin_role = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        "admin = admin_group"
        };
    context.makeNewPolicy(testPolicies);

    // create dbs and load data
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);
    adminStmt.execute("CREATE TABLE " + tabName + "(id int)");
    adminStmt.execute("load data local inpath '" + dataFilePath +
        "' into table " + tabName);
    adminStmt.execute("select * from " + tabName + " limit 1");
    ResultSet res = adminStmt.getResultSet();
    Assert.assertTrue("Table should have data after load", res.next());
    res.close();
    context.close();

    // Negative test, user2 doesn't have access to write to dir
    userConn = context.createConnection("user2", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    try {
    userStmt.execute("INSERT OVERWRITE DIRECTORY '" + dumpDir +
        "' SELECT * FROM " + tabName);
    Assert.assertTrue("load should fail for user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();

    // positive test, user1 has access to write to dir
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("SELECT * FROM " + tabName);
    userStmt.execute("INSERT OVERWRITE DIRECTORY '" + dumpDir +
        "' SELECT * FROM " + tabName);
  }

  // test export/import
  @Test
  public void testExportPrivileges() throws Exception {
    String dbName = "db1";
    String tabName = "tab1";
    String newTabName = "tab2";
    Connection userConn = null;
    Statement userStmt = null;
    String exportDir = context.getDFSUri().toString() + "/hive_export1";

    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = db1_read, db1_write, data_read, data_export",
        "user_group2  = db1_write, db1_read",
        "[roles]",
        "db1_write = server=server1->db=" + dbName + "->table=" + tabName + "->action=INSERT",
        "db1_read = server=server1->db=" + dbName + "->table=" + tabName + "->action=SELECT",
        "data_read = server=server1->URI=file://" + dataFilePath,
        "data_export = server=server1->URI=" + exportDir,
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
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);
    adminStmt.execute("CREATE TABLE " + tabName + "(id int)");
    adminStmt.execute("load data local inpath '" + dataFilePath +
        "' into table " + tabName);
    context.close();

    // Negative test, user2 doesn't have access to the file being loaded
    userConn = context.createConnection("user2", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    try {
      userStmt.execute("EXPORT TABLE " + tabName + " TO '" + exportDir + "'");
      Assert.assertTrue("export should fail for user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();

    // Positive test, user1 have access to the target directory
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("EXPORT TABLE " + tabName + " TO '" + exportDir + "'");
    context.close();

    // Negative test, user2 doesn't have access to the directory loading from
    userConn = context.createConnection("user2", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    try {
      userStmt.execute("IMPORT TABLE " + newTabName + " FROM '" + exportDir + "'");
      Assert.assertTrue("export should fail for user2", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();

    // Positive test, user1 have access to the target directory
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("IMPORT TABLE " + newTabName + " FROM '" + exportDir + "'");
    context.close();

  }
}
