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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJDBCInterface extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory.
          getLogger(TestJDBCInterface.class);
  private static PolicyFile policyFile;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    LOGGER.info("TestJDBCInterface setupTestStaticConfiguration");
    policyOnHdfs = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Before
  public void setup() throws Exception {
    LOGGER.info("TestJDBCInterface setup");
    policyFile = super.setupPolicy();
    super.setup();
  }

  /*
   * Admin creates DB_1, DB2, tables (tab_1 ) and (tab_2, tab_3) in DB_1 and
   * DB_2 respectively. User user1 has select on DB_1.tab_1, insert on
   * DB2.tab_2 User user2 has select on DB2.tab_3 Test show database and show
   * tables for both user1 and user2
   */
  @Test
  public void testJDBCGetSchemasAndGetTables() throws Exception {
    // admin create two databases
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB2 CASCADE");

    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE TAB1(id int)");
    statement.executeQuery("SHOW TABLES");
    statement.execute("USE " + DB2);
    statement.execute("CREATE TABLE TAB2(id int)");
    statement.execute("CREATE TABLE TAB3(id int)");

    // edit policy file
    policyFile
            .addRolesToGroup(USERGROUP1, "select_tab1", "insert_tab2")
            .addRolesToGroup(USERGROUP2, "select_tab3")
            .addPermissionsToRole("select_tab1",
                    "server=server1->db=" + DB1 + "->table=tab1->action=select")
            .addPermissionsToRole("select_tab3",
                    "server=server1->db=" + DB2 + "->table=tab3->action=select")
            .addPermissionsToRole("insert_tab2",
                    "server=server1->db=" + DB2 + "->table=tab2->action=insert");
    writePolicyFile(policyFile);

    // test show databases
    // show databases shouldn't filter any of the dbs from the resultset
    Connection conn = context.createConnection(USER1_1);
    List<String> expectedResult = new ArrayList<String>();
    List<String> returnedResult = new ArrayList<String>();

    // test direct JDBC metadata API
    ResultSet res = conn.getMetaData().getSchemas();
    ResultSetMetaData resMeta = res.getMetaData();
    assertEquals(2, resMeta.getColumnCount());
    assertEquals("TABLE_SCHEM", resMeta.getColumnName(1));
    assertEquals("TABLE_CATALOG", resMeta.getColumnName(2));

    expectedResult.add(DB1);
    expectedResult.add(DB2);
    expectedResult.add("default");

    while (res.next()) {
      returnedResult.add(res.getString(1));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    // test direct JDBC metadata API
    res = conn.getMetaData().getTables(null, DB1, "tab%", null);
    expectedResult.add("tab1");

    while (res.next()) {
      returnedResult.add(res.getString(3));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    // test direct JDBC metadata API
    res = conn.getMetaData().getTables(null, DB2, "tab%", null);
    expectedResult.add("tab2");

    while (res.next()) {
      returnedResult.add(res.getString(3));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    res = conn.getMetaData().getTables(null, "DB%", "tab%", null);
    expectedResult.add("tab2");
    expectedResult.add("tab1");

    while (res.next()) {
      returnedResult.add(res.getString(3));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    // test show columns
    res = conn.getMetaData().getColumns(null, "DB%", "tab%", "i%");
    expectedResult.add("id");
    expectedResult.add("id");

    while (res.next()) {
      returnedResult.add(res.getString(4));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    conn.close();

    // test show databases and show tables for user2
    conn = context.createConnection(USER2_1);

    // test direct JDBC metadata API
    res = conn.getMetaData().getSchemas();
    resMeta = res.getMetaData();
    assertEquals(2, resMeta.getColumnCount());
    assertEquals("TABLE_SCHEM", resMeta.getColumnName(1));
    assertEquals("TABLE_CATALOG", resMeta.getColumnName(2));

    expectedResult.add(DB2);
    expectedResult.add("default");

    while (res.next()) {
      returnedResult.add(res.getString(1));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    // test JDBC direct API
    res = conn.getMetaData().getTables(null, "DB%", "tab%", null);
    expectedResult.add("tab3");

    while (res.next()) {
      returnedResult.add(res.getString(3));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    // test show columns
    res = conn.getMetaData().getColumns(null, "DB%", "tab%", "i%");
    expectedResult.add("id");

    while (res.next()) {
      returnedResult.add(res.getString(4));
    }
    validateReturnedResult(expectedResult, returnedResult);
    expectedResult.clear();
    returnedResult.clear();
    res.close();

    // test show columns
    res = conn.getMetaData().getColumns(null, DB1, "tab%", "i%");

    while (res.next()) {
      returnedResult.add(res.getString(4));
    }
    assertTrue("returned result shouldn't contain any value, actually returned result = " + returnedResult.toString(),
            returnedResult.isEmpty());
    res.close();

    context.close();
  }

}
