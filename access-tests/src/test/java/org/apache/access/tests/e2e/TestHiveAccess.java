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
import static org.junit.Assert.assertNotNull;

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
  public void test() throws Exception {
    String tableName = "Test_Tab007";
    String tableComment = "Test table";

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
    ResultSet   res = userStmt.executeQuery("select under_col from " + tableName);

    //drop table
    userStmt.execute("drop table " + tableName);

    // per test tear down
    userStmt.close();
    userCon.close();
  }

  @After
  public void tearDown() throws Exception {
    adminStmt.close();
    adminCon.close();
  }
}