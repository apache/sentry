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

package org.apache.sentry.tests.e2e.dbprovider;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestColumnEndToEnd extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory.
          getLogger(TestColumnEndToEnd.class);

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataFile;
  private PolicyFile policyFile;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    LOGGER.info("TestColumnEndToEnd setupTestStaticConfiguration");
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
    policyFile = super.setupPolicy();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
  }

  @Test
  public void testBasic() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE database " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("CREATE ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    context.assertSentryException(statement, "CREATE ROLE r2",
        SentryAccessDeniedException.class.getSimpleName());

    statement.execute("SELECT * FROM " + DB1 + ".t1");
    statement.close();
    connection.close();
  }

  @Test
  public void testDescribeTbl() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE IF NOT EXISTS t1 (c1 string, c2 string)");
    statement.execute("CREATE TABLE t2 (c1 string, c2 string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("GRANT SELECT (c1) ON TABLE t1 TO ROLE user_role1");
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    // Expect that DESCRIBE table works with only column-level privileges, but other
    // DESCRIBE variants like DESCRIBE FORMATTED fail. Note that if a user has privileges
    // on any column they can describe all columns.
    ResultSet rs = statement.executeQuery("DESCRIBE t1");
    assertTrue(rs.next());
    assertEquals("c1", rs.getString(1));
    assertEquals("string", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("c2", rs.getString(1));
    assertEquals("string", rs.getString(2));

    statement.executeQuery("DESCRIBE t1 c1");
    statement.executeQuery("DESCRIBE t1 c2");

    try {
      statement.executeQuery("DESCRIBE t2");
      fail("Expected DESCRIBE to fail on t2");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      statement.executeQuery("DESCRIBE FORMATTED t1");
      fail("Expected DESCRIBE FORMATTED to fail");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      statement.executeQuery("DESCRIBE EXTENDED t1");
      fail("Expected DESCRIBE EXTENDED to fail");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // Cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP TABLE t1");
    statement.execute("DROP TABLE t2");
    statement.execute("DROP ROLE user_role1");
    statement.close();
    connection.close();
  }

  @Test
  public void testNegative() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE t1 (c1 string, c2 string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("CREATE ROLE user_role2");
    statement.execute("GRANT SELECT (c1) ON TABLE t1 TO ROLE user_role1");
    statement.execute("GRANT SELECT (c1,c2) ON TABLE t1 TO ROLE user_role2");

    //Make sure insert/all are not supported
    try {
      statement.execute("GRANT INSERT (c2) ON TABLE t1 TO ROLE user_role2");
      assertTrue("Sentry should not support privilege: Insert on Column", false);
    } catch (Exception e) {
      assertTrue("The error should be 'Sentry does not support privilege: Insert on Column'",
          e.getMessage().contains("Sentry does not support privilege: Insert on Column"));
    }
    try {
      statement.execute("GRANT ALL (c2) ON TABLE t1 TO ROLE user_role2");
      assertTrue("Sentry should not support privilege: ALL on Column", false);
    } catch (Exception e) {
      assertTrue("The error should be 'Sentry does not support privilege: All on Column'",
          e.getMessage().contains("Sentry does not support privilege: All on Column"));
    }
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.close();
    connection.close();

    /*
    Behavior of select col, select count(col), select *, and select count(*), count(1)
     */
    // 1.1 user_role1 select c1,c2 from t1, will throw exception
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("SELECT c1,c2 FROM t1");
      assertTrue("User with privilege on one column is able to access other column!!", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // 1.2 user_role1 count(col) works, *, count(*) and count(1) fails
    statement.execute("SELECT count(c1) FROM t1");
    try {
      statement.execute("SELECT * FROM t1");
      assertTrue("Select * should fail - only SELECT allowed on t1.c1!!", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("SELECT count(*) FROM t1");
      assertTrue("Select count(*) should fail - only SELECT allowed on t1.c1!!", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("SELECT count(1) FROM t1");
      assertTrue("Select count(1) should fail - only SELECT allowed on t1.c1!!", false);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();


    // 2.1 user_role2 can do *, count(col), but count(*) and count(1) fails
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("SELECT count(c1) FROM t1");
    statement.execute("SELECT * FROM t1");

    //SENTRY-838
    try {
      statement.execute("SELECT count(*) FROM t1");
      assertTrue("Select count(*) works only with table level privileges - User has select on all columns!!", false);
    } catch (Exception e) {
      // Ignore
    }
    try {
      statement.execute("SELECT count(1) FROM t1");
      assertTrue("Select count(1) works only with table level privileges - User has select on all columns!!", false);
    } catch (Exception e) {
      // Ignore
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testPositive() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE database " + DB1);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string, c2 string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("CREATE ROLE user_role2");
    statement.execute("CREATE ROLE user_role3");
    statement.execute("GRANT SELECT (c1) ON TABLE t1 TO ROLE user_role1");
    statement.execute("GRANT SELECT (c1, c2) ON TABLE t1 TO ROLE user_role2");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role3");
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE user_role3 TO GROUP " + USERGROUP3);
    statement.close();
    connection.close();

    // 1 user_role1 select c1 on t1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("SELECT c1 FROM t1");
    statement.execute("DESCRIBE t1");

    // 2.1 user_role2 select c1,c2 on t1
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("SELECT c1,c2 FROM t1");
    // 2.2 user_role2 select * on t1
    statement.execute("SELECT * FROM t1");

    // 3.1 user_role3 select * on t1
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("SELECT * FROM t1");
    // 3.2 user_role3 select c1,c2 on t1
    statement.execute("SELECT c1,c2 FROM t1");

    statement.close();
    connection.close();
  }

  @Test
  public void testCreateTableAsSelect() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE database " + DB1);
    statement.execute("CREATE database " + DB2);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string, c2 string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("CREATE ROLE user_role2");
    statement.execute("CREATE ROLE user_role3");
    statement.execute("GRANT SELECT (c1) ON TABLE t1 TO ROLE user_role1");
    statement.execute("GRANT SELECT (c1, c2) ON TABLE t1 TO ROLE user_role2");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role3");
    statement.execute("GRANT ALL ON DATABASE " + DB2 + " TO ROLE user_role1");
    statement.execute("GRANT ALL ON DATABASE " + DB2 + " TO ROLE user_role2");
    statement.execute("GRANT ALL ON DATABASE " + DB2 + " TO ROLE user_role3");
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE user_role3 TO GROUP " + USERGROUP3);
    statement.close();
    connection.close();

    // 1 user_role1 create table as select
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB2);
    statement.execute("CREATE TABLE t1_1 AS SELECT c1 FROM " + DB1 + ".t1");
    try {
      statement.execute("CREATE TABLE t1_2 AS SELECT * FROM " + DB1 + ".t1");
      assertTrue("no permission on table t1!!", false);
    } catch (Exception e) {
      // Ignore
    }

    // 2 user_role2 create table as select
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB2);
    statement.execute("CREATE TABLE t2_1 AS SELECT c1 FROM " + DB1 + ".t1");
    statement.execute("CREATE TABLE t2_2 AS SELECT * FROM " + DB1 + ".t1");

    // 3 user_role3 create table as select
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB2);
    statement.execute("CREATE TABLE t3_1 AS SELECT c1 FROM " + DB1 + ".t1");
    statement.execute("CREATE TABLE t3_2 AS SELECT * FROM " + DB1 + ".t1");

    statement.close();
    connection.close();
  }

  @Test
  public void testShowColumns() throws Exception {
    // grant select on test_tb(s) to USER1_1
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE database " + DB1);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE test_tb (s string, i string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("GRANT SELECT (s) ON TABLE test_tb TO ROLE user_role1");
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    // USER1_1 executes "show columns in test_tb" and gets the s column information
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    ResultSet res = statement.executeQuery("show columns in test_tb");

    List<String> expectedResult = new ArrayList<String>();
    List<String> returnedResult = new ArrayList<String>();
    expectedResult.add("s");
    while (res.next()) {
      returnedResult.add(res.getString(1).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    statement.close();
    connection.close();

    // grant select on test_tb(s, i) to USER2_1
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("CREATE ROLE user_role2");
    statement.execute("GRANT SELECT(s, i) ON TABLE test_tb TO ROLE user_role2");
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.close();
    connection.close();

    // USER2_1 executes "show columns in test_tb" and gets the s,i columns information
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    res = statement.executeQuery("show columns in test_tb");

    expectedResult.add("s");
    expectedResult.add("i");
    while (res.next()) {
      returnedResult.add(res.getString(1).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    statement.close();
    connection.close();

    // USER3_1 executes "show columns in test_tb" and the exception will be thrown
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    try {
      // USER3_1 has no privilege on any column, so "show columns in test_tb" will throw an exception
      statement.execute("show columns in db_1.test_tb");
      fail("No valid privileges exception should have been thrown");
    } catch (Exception e) {
    }

    statement.close();
    connection.close();
  }
}
