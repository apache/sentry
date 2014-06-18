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

import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestDatabaseProvider extends AbstractTestWithStaticConfiguration {

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  /**
   * This test is only used for manual testing of beeline with Sentry Service
   * @throws Exception
   */
  @Override
  @After
  public void clearDB() throws Exception {
    Connection connection;
    Statement statement;
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    ResultSet resultSet;
    resultSet = statement.executeQuery("SHOW roles");
    List<String> roles = new ArrayList<String>();
    while ( resultSet.next()) {
      roles.add(resultSet.getString(1));
    }
    for(String role:roles) {
      statement.execute("DROP Role " + role);
    }

    statement.close();
    connection.close();
    if (context != null) {
      context.close();
    }
  }

  @Ignore
  @Test
  public void beelineTest() throws Exception{
    while(true) {}
  }

  @Test
  public void testBasic() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE admin_role");
    statement.execute("GRANT ALL ON DATABASE default TO ROLE admin_role");
    statement.execute("GRANT ROLE admin_role TO GROUP " + ADMINGROUP);
    statement.execute("DROP TABLE t1");
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
    // test default of ALL
    statement.execute("SELECT * FROM t1");
    // test a specific role
    statement.execute("SET ROLE user_role");
    statement.execute("SELECT * FROM t1");
    // test NONE
    statement.execute("SET ROLE NONE");
    context.assertAuthzException(statement, "SELECT * FROM t1");
    // test ALL
    statement.execute("SET ROLE ALL");
    statement.execute("SELECT * FROM t1");
    statement.close();
    connection.close();
  }


  @Test
  public void testRevokeDbALLAfterGrantTable() throws Exception {
    doSetup();

    // Revoke ALL on Db
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("REVOKE ALL ON DATABASE db1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("SELECT * FROM t1");
    try {
      statement.execute("SELECT * FROM db1.t2");
      assertTrue("SELECT should not be allowed after revoke on parent!!", false);
    } catch (Exception e) {
      // Ignore
    }
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    assertResultSize(resultSet, 1);
    statement.close();
    connection.close();
  }

  @Test
  public void testRevokeServerAfterGrantTable() throws Exception {
    doSetup();

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    assertResultSize(resultSet, 2);
    statement.close();
    connection.close();;

    // Revoke on Server
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("REVOKE ALL ON SERVER server1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("SELECT * FROM t1");
      assertTrue("SELECT should not be allowed after revoke on parent!!", false);
    } catch (Exception e) {
      // Ignore
    }
    try {
      statement.execute("SELECT * FROM db1.t2");
      assertTrue("SELECT should not be allowed after revoke on parent!!", false);
    } catch (Exception e) {
      // Ignore
    }

    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    assertResultSize(resultSet, 0);
    statement.close();
    connection.close();
  }


  /**
   * - Create db db1
   * - Create role user_role
   * - Create tables (t1, db1.t2)
   * - Grant all on table t2 to user_role
   * @throws Exception
   */
  private void doSetup() throws Exception {
    super.setupAdmin();

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("CREATE ROLE user_role");

    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");
    statement.execute("DROP TABLE IF EXISTS t2");
    statement.execute("CREATE TABLE t2 (c1 string)");
    statement.execute("GRANT ALL ON TABLE t2 TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("SELECT * FROM t1");
    statement.execute("SELECT * FROM db1.t2");

    statement.close();
    connection.close();
  }

  /**
   * SENTRY-299
   *
   * 1. Create 2 Roles (user_role & user_role2)
   * 2. Create a Table t1
   * 3. grant ALL on t1 to user_role
   * 4. grant INSERT on t1 to user_role2
   * 5. Revoke INSERT on t1 from user_role
   *     - This would imply user_role can still SELECT
   *     - But user_role should NOT be allowed to LOAD
   * 6. Ensure Presense of another role will still enforce the revoke
   * @throws Exception
   */

  @Test
  public void testRevokeFailAnotherRoleExist() throws Exception {
    super.setupAdmin();

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();


    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");
    statement.execute("CREATE ROLE user_role2");

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");
    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role2");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role2");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role2");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("SELECT * FROM db1.t1");
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    assertResultSize(resultSet, 2);
    statement.close();
    connection.close();

    // Revoke ALL on Db
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("REVOKE INSERT ON TABLE t1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // This Should pass
    statement.execute("SELECT * FROM db1.t1");

    try {
      statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
      assertTrue("INSERT Should Not be allowed since we Revoked INSERT privileges on the table !!", false);
    } catch (Exception e) {

    } finally {
      statement.close();
      connection.close();
    }

    // user_role2 can still insert into table
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
    statement.close();
    connection.close();

    // Grant changed from ALL to SELECT
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    assertResultSize(resultSet, 2);
    statement.close();
    connection.close();

  }


  /**
   * SENTRY-302
   *
   * 1. Create Role user_role
   * 2. Create a Table t1
   * 3. grant ALL on t1 to user_role
   * 4. grant INSERT on t1 to user_role
   * 5. Revoke INSERT on t1 from user_role
   *     - This would imply user_role can still SELECT
   *     - But user_role should NOT be allowed to LOAD
   * 6. Ensure INSERT is revoked on table
   * @throws Exception
   */

  @Test
  public void testRevokeFailMultipleGrantsExist() throws Exception {
    super.setupAdmin();

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();


    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");
    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("SELECT * FROM db1.t1");
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 2);
    statement.close();
    connection.close();

    // Revoke INSERT on Db
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("REVOKE INSERT ON TABLE t1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // This Should pass
    statement.execute("SELECT * FROM db1.t1");

    try {
      statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
      assertTrue("INSERT Should Not be allowed since we Revoked INSERT privileges on the table !!", false);
    } catch (Exception e) {

    } finally {
      statement.close();
      connection.close();
    }

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 2);
    statement.close();
    connection.close();

  }


  /**
   * Revoke all on server after:
   *  - grant all on db
   *  - grant all on table
   *  - grant select on table
   *  - grant insert on table
   *  - grant all on URI
   *  - grant select on URI
   *  - grant insert on URI
   */
  @Test
  public void testRevokeAllOnServer() throws Exception{
    super.setupAdmin();

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();


    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");

    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");

    statement.execute("GRANT ALL ON DATABASE db1 TO ROLE user_role");
    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT SELECT ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT INSERT ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure everything works
    statement.execute("SELECT * FROM db1.t1");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 3);
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("REVOKE ALL ON SERVER server1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure nothing works
    try {
      statement.execute("SELECT * FROM db1.t1");
      assertTrue("SELECT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }

    try {
      statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
      assertTrue("INSERT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 0);
    statement.close();
    connection.close();
  }


  /**
   * Revoke all on database after:
   *  - grant all on db
   *  - grant all on table
   *  - grant select on table
   *  - grant insert on table
   */
  @Test
  public void testRevokeAllOnDb() throws Exception{
    super.setupAdmin();

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");

    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");

    statement.execute("GRANT ALL ON DATABASE db1 TO ROLE user_role");
    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 3);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure everything works
    statement.execute("SELECT * FROM db1.t1");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("REVOKE ALL ON DATABASE db1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure nothing works
    try {
      statement.execute("SELECT * FROM db1.t1");
      assertTrue("SELECT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }

    try {
      statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
      assertTrue("INSERT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 1);
    statement.close();
    connection.close();
  }

  /**
   * Revoke all on table after:
   *  - grant all on table
   *  - grant select on table
   *  - grant insert on table
   */
  @Test
  public void testRevokeAllOnTable() throws Exception{
    super.setupAdmin();

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");

    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");

    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure everything works
    statement.execute("SELECT * FROM db1.t1");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 2);
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("REVOKE ALL ON TABLE t1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure nothing works
    try {
      statement.execute("SELECT * FROM db1.t1");
      assertTrue("SELECT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }

    try {
      statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
      assertTrue("INSERT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 1);
    statement.close();
    connection.close();
  }

  /**
   * Revoke select on table after:
   *  - grant all on table
   *  - grant select on table
   *  - grant insert on table
   */
  @Test
  public void testRevokeSELECTOnTable() throws Exception{
    super.setupAdmin();

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");

    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");

    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure everything works
    statement.execute("SELECT * FROM db1.t1");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 2);
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("REVOKE SELECT ON TABLE t1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure select not allowed
    try {
      statement.execute("SELECT * FROM db1.t1");
      assertTrue("SELECT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }

    // Ensure insert allowed
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
    statement.close();
    connection.close();

    // This removes the ALL and SELECT privileges
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 2);
    statement.close();
    connection.close();

  }


  /**
   * Revoke insert on table after:
   *  - grant all on table
   *  - grant select on table
   *  - grant insert on table
   */
  @Test
  public void testRevokeINSERTOnTable() throws Exception{
    super.setupAdmin();

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE user_role");

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");

    statement.execute("DROP TABLE IF EXISTS t1");
    statement.execute("CREATE TABLE t1 (c1 string)");

    statement.execute("GRANT ALL ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ALL ON URI 'file://" + dataFile.getPath() + "' TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure everything works
    statement.execute("SELECT * FROM db1.t1");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 2);
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("REVOKE INSERT ON TABLE t1 from ROLE user_role");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // Ensure insert not allowed
    try {
      statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE db1.t1");
      assertTrue("INSERT should not be allowed !!", false);
    } catch (SQLException se) {
      // Ignore
    }

    // Ensure select allowed
    statement.execute("SELECT * FROM db1.t1");
    statement.close();
    connection.close();

    // This removes the INSERT and ALL privileges
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    assertResultSize(statement.executeQuery("SHOW GRANT ROLE user_role"), 2);
    statement.close();
    connection.close();
  }




  /**
   * Grant/Revoke privilege - Positive cases
   * @throws Exception
    1.1. All on server
    1.2. All on database
    1.3. All on URI
    1.4. All on table
    1.5. Insert on table
    1.6. Select on table
    1.7. Partial privileges on table
    1.7.1. Grant all, revoke insert leads to select on table
    1.7.2. Grant all, revoke select leads to select on table
     */
  @Test
  public void testGrantRevokePrivileges() throws Exception {
    Connection connection;
    Statement statement;
    ResultSet resultSet;

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");

    //Grant/Revoke All on server by admin
    statement.execute("GRANT ALL ON SERVER server1 to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("*"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    statement.execute("REVOKE ALL ON SERVER server1 from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);

    //Grant/Revoke All on database by admin
    statement.execute("GRANT ALL ON DATABASE default to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    statement.execute("REVOKE ALL ON DATABASE default from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);

    //Grant/Revoke All on URI by admin
    statement.execute("GRANT ALL ON URI 'file:///path' to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("file://path"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }
    statement.execute("REVOKE ALL ON URI 'file:///path' from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);

    //Grant/Revoke All on table by admin
    statement.execute("GRANT ALL ON TABLE tab1 to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("tab1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    statement.execute("REVOKE ALL ON TABLE tab1 from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);

    //Grant/Revoke INSERT on table by admin
    statement.execute("GRANT INSERT ON TABLE tab1 to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("tab1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("insert"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    statement.execute("REVOKE INSERT ON TABLE tab1 from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);

    //Grant/Revoke SELECT on table by admin
    statement.execute("GRANT SELECT ON TABLE tab1 to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("tab1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("select"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    statement.execute("REVOKE SELECT ON TABLE tab1 from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);

    //Revoke Partial privilege on table by admin
    statement.execute("GRANT ALL ON TABLE tab1 to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    statement.execute("REVOKE INSERT ON TABLE tab1 from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("tab1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("select"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    //Revoke Partial privilege on table by admin
    statement.execute("GRANT ALL ON TABLE tab1 to role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    statement.execute("REVOKE SELECT ON TABLE tab1 from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while(resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("tab1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("insert"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor

    }
    statement.close();
    connection.close();
  }

  private void assertResultSize(ResultSet resultSet, int expected) throws SQLException{
    int count = 0;
    while(resultSet.next()) {
      count++;
    }
    assertThat(count, is(expected));
  }

  /**
   * Create and Drop role by admin
   * @throws Exception
   */
  @Test
  public void testCreateDropRole() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    ResultSet resultSet = statement.executeQuery("SHOW roles");
    assertResultSize(resultSet, 1);
    statement.execute("DROP ROLE role1");
    resultSet = statement.executeQuery("SHOW roles");
    assertResultSize(resultSet, 0);
  }

  /**
   * Corner cases
   * @throws Exception
    7.1. Drop role which doesn't exist, throws SentryNoSuchObjectException
    7.2. Create role which already exists, throws SentryAlreadyExitsException
    7.3. Drop role when privileges mapping exists and create role with same name, old
    mappings should not exist
    7.4. Grant role, when role doesn't exist, throws SentryNoSuchObjectException
    7.5. Grant role when mapping exists, silently allows
    7.6. Grant multiple roles to a group
    7.7. Revoke role after role has been dropped, SentryNoSuchObjectException
    7.8. Revoke role from a group when mapping doesn't exist, silently allows
    7.9. Grant privilege to a role, privilege already exists, silently allows
    7.10. Grant privilege to a role, mapping already exists, silently allows
    7.11. Multiple privileges to a role
    7.12. Revoke privilege when privilege doesn't exist, silently allows
    7.13. Revoke privilege, when role doesn't exist, SentryNoSuchObjectException
    7.14. Revoke privilege when mapping doesn't exist, silently allows
    7.15. Drop role should remove role-group mapping
   */
  @Test
  public void testCornerCases() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    //Drop a role which does not exist
    context.assertSentryException(statement, "DROP ROLE role1",
        SentryNoSuchObjectException.class.getSimpleName());

    //Create a role which already exists
    statement.execute("CREATE ROLE role1");
    context.assertSentryException(statement, "CREATE ROLE role1",
        SentryAlreadyExistsException.class.getSimpleName());

    //Drop role when privileges mapping exists and create role with same name, old mappings should not exist
    //state: role1
    statement.execute("GRANT ROLE role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ALL ON SERVER server1 TO ROLE role1");
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    statement.execute("DROP ROLE role1");
    statement.execute("CREATE ROLE role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);


    //Grant role, when role doesn't exist
    //state: role1
    context.assertSentryException(statement, "GRANT role role2 TO GROUP " + USERGROUP1,
        SentryNoSuchObjectException.class.getSimpleName());


    //Grant multiple roles to a group
    //state: role1
    statement.execute("CREATE ROLE role2");
    statement.execute("GRANT ROLE role2 to GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE role1 to GROUP " + USERGROUP1);
    resultSet = statement.executeQuery("SHOW ROLE GRANT GROUP " + USERGROUP1);
    assertResultSize(resultSet, 2);

    //Grant role when mapping exists
    //state: role1, role2 -> usergroup1
    statement.execute("GRANT ROLE role1 to GROUP " + USERGROUP1);

    //Revoke role after role has been dropped
    //state: role1, role2 -> usergroup1
    statement.execute("DROP ROLE role2");
    context.assertSentryException(statement, "REVOKE role role2 from group " + USERGROUP1,
        SentryNoSuchObjectException.class.getSimpleName());

    //Revoke role from a group when mapping doesnt exist
    //state: role1 -> usergroup1
    statement.execute("REVOKE ROLE role1 from GROUP " + USERGROUP1);
    statement.execute("REVOKE ROLE role1 from GROUP " + USERGROUP1);

    //Grant privilege to a role, privilege already exists, mapping already exists
    //state: role1
    //TODO: Remove this comment SENTRY-181
    statement.execute("CREATE ROLE role2");
    statement.execute("GRANT ALL ON SERVER server1 TO ROLE role1");
    statement.execute("GRANT ALL ON SERVER server1 TO ROLE role1");
    statement.execute("GRANT ALL ON SERVER server1 TO ROLE role2");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    resultSet = statement.executeQuery("SHOW GRANT ROLE role2");
    assertResultSize(resultSet, 1);

    //Multiple privileges to a role
    //state: role1,role2 -> grant all on server
    statement.execute("GRANT ALL ON TABLE tab1 to ROLE role2");
    statement.execute("GRANT ALL,INSERT ON TABLE tab1 to ROLE role2");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role2");
    assertResultSize(resultSet, 2);
    statement.execute("DROP role role2");

    //Revoke privilege when privilege doesnt exist
    //state: role1 -> grant all on server
    statement.execute("REVOKE ALL ON TABLE tab1 from role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    int count = 0;
    //Verify we still have all on server
    while(resultSet.next()) {
      count++;
      assertThat(resultSet.getString(1), equalToIgnoringCase("*"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));

    }
    assertThat(count, is(1));

    //Revoke privilege, when role doesnt exist
    //state: role1 -> grant all on server
    context.assertSentryException(statement, "REVOKE ALL ON SERVER server1 from role role2",
        SentryNoSuchObjectException.class.getSimpleName());

    //Revoke privilege when privilege exists but mapping doesnt exist
    //state: role1 -> grant all on server
    statement.execute("CREATE ROLE role2");
    statement.execute("GRANT ALL on TABLE tab1 to ROLE role2");
    statement.execute("REVOKE ALL on TABLE tab1 from Role role1");
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    statement.execute("DROP role role2");

    //Drop role should remove role-group mapping
    //state: role1 -> grant all on server
    statement.execute("GRANT ROLE role1 to GROUP " + USERGROUP1);
    statement.execute("DROP ROLE role1");
    resultSet = statement.executeQuery("SHOW ROLE GRANT GROUP " + USERGROUP1);
    assertResultSize(resultSet, 0);
  }
  /**
   * SHOW ROLES
   * @throws Exception
   3.1. When there are no roles, returns empty list
   3.2. When there are roles, returns correct list with correct schema.
   */
  @Test
  public void testShowRoles() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    ResultSet resultSet = statement.executeQuery("SHOW ROLES");
    assertResultSize(resultSet, 0);
    statement.execute("CREATE ROLE role1");
    statement.execute("CREATE ROLE role2");
    resultSet = statement.executeQuery("SHOW ROLES");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertThat(resultSetMetaData.getColumnCount(), is(1));
    assertThat(resultSetMetaData.getColumnName(1), equalToIgnoringCase("role"));

    Set<String> roles = new HashSet<String>();
    while ( resultSet.next()) {
      roles.add(resultSet.getString(1));
    }
    assertThat(roles.size(), is(2));
    assertTrue(roles.contains("role1"));
    assertTrue(roles.contains("role2"));
    statement.close();
    connection.close();
  }

  /**
   * SHOW ROLE GRANT GROUP groupName
   * @throws Exception
   4.1. When there are no roles and group, throws SentryNoSuchObjectException
   4.2. When there are roles, returns correct list with correct schema.
   */
  @Test
  public void testShowRolesByGroup() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    //This is non deterministic as we are now using same sentry service across the tests
    // and orphan groups are not cleaned up.
    //context.assertSentryException(statement,"SHOW ROLE GRANT GROUP " + ADMINGROUP,
    //    SentryNoSuchObjectException.class.getSimpleName());
    statement.execute("CREATE ROLE role1");
    statement.execute("CREATE ROLE role2");
    statement.execute("CREATE ROLE role3");
    statement.execute("GRANT ROLE role1 to GROUP " + ADMINGROUP);

    ResultSet resultSet = statement.executeQuery("SHOW ROLE GRANT GROUP " + ADMINGROUP);
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertThat(resultSetMetaData.getColumnCount(), is(4));
    assertThat(resultSetMetaData.getColumnName(1), equalToIgnoringCase("role"));
    assertThat(resultSetMetaData.getColumnName(2), equalToIgnoringCase("grant_option"));
    assertThat(resultSetMetaData.getColumnName(3), equalToIgnoringCase("grant_time"));
    assertThat(resultSetMetaData.getColumnName(4), equalToIgnoringCase("grantor"));
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("role1"));
      assertThat(resultSet.getBoolean(2), is(new Boolean("False")));
      //Create time is not tested
      //assertThat(resultSet.getLong(3), is(new Long(0)));
      assertThat(resultSet.getString(4), equalToIgnoringCase(ADMIN1));
    }
    statement.close();
    connection.close();
  }

  /**
   * SHOW GRANT ROLE roleName
   * @throws Exception
    5.1. When there are no privileges granted to a role, returns an empty list
    5.2. When there are privileges, returns correct list with correct schema.
    5.3. Given privileges on table, show grant on table should return table privilege.
    5.4. Privileges on database
    5.4.1. Show grant on database should return correct priv
    5.4.2. Show grant on table should return correct priv
    5.5. Privileges on server
    5.5.1. Show grant on database should return correct priv
    5.5.2. Show grant on table should return correct priv
    5.5.3. Show grant on server should return correct priv (sql not supported yet in hive)
    5.6. Show grant on uri (sql not supported yet in hive)
  */
  @Test
  public void testShowPrivilegesByRole() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 0);
    statement.execute("CREATE ROLE role2");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE role1");

    resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    //| database  | table  | partition  | column  | principal_name  |
    // principal_type | privilege  | grant_option  | grant_time  | grantor  |
    assertThat(resultSetMetaData.getColumnCount(), is(10));
    assertThat(resultSetMetaData.getColumnName(1), equalToIgnoringCase("database"));
    assertThat(resultSetMetaData.getColumnName(2), equalToIgnoringCase("table"));
    assertThat(resultSetMetaData.getColumnName(3), equalToIgnoringCase("partition"));
    assertThat(resultSetMetaData.getColumnName(4), equalToIgnoringCase("column"));
    assertThat(resultSetMetaData.getColumnName(5), equalToIgnoringCase("principal_name"));
    assertThat(resultSetMetaData.getColumnName(6), equalToIgnoringCase("principal_type"));
    assertThat(resultSetMetaData.getColumnName(7), equalToIgnoringCase("privilege"));
    assertThat(resultSetMetaData.getColumnName(8), equalToIgnoringCase("grant_option"));
    assertThat(resultSetMetaData.getColumnName(9), equalToIgnoringCase("grant_time"));
    assertThat(resultSetMetaData.getColumnName(10), equalToIgnoringCase("grantor"));

    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("t1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("select"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }
    statement.close();
    connection.close();
  }

  /**
   * SHOW GRANT ROLE roleName ON TABLE tableName
   * @throws Exception
   */
  @Test
  public void testShowPrivilegesByRoleOnObjectGivenTable() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE role1");

    //On table - positive
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON TABLE t1");
    int rowCount = 0 ;
    while ( resultSet.next()) {
      rowCount++;
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("t1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("select"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }
    assertThat(rowCount, is(1));
    //On table - negative
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON TABLE tab1");
    rowCount = 0 ;
    while (resultSet.next()) {
      rowCount++;
    }
    assertThat(rowCount, is(0));
    statement.close();
    connection.close();
  }

    /**
     * SHOW GRANT ROLE roleName ON TABLE tableName
     * @throws Exception
     */
  @Test
  public void testShowPrivilegesByRoleOnObjectGivenDatabase() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("GRANT ALL ON DATABASE default TO ROLE role1");

    //On Table - positive
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON TABLE tab1");
    int rowCount = 0 ;
    while ( resultSet.next()) {
      rowCount++;
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    //On Database - positive
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON DATABASE default");
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));//table
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    //On Database - negative
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON DATABASE db1");
    rowCount = 0 ;
    while (resultSet.next()) {
      rowCount++;
    }
    assertThat(rowCount, is(0));
    statement.close();
    connection.close();
  }

  /**
   * SHOW GRANT ROLE roleName ON TABLE tableName
   * @throws Exception
   */
  @Test
  public void testShowPrivilegesByRoleObObjectGivenServer() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("GRANT ALL ON SERVER server1 TO ROLE role1");

    //On table - positive
    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON TABLE tab1");
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("*"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    //On Database - postive
    resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON DATABASE default");
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("*"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }

    statement.close();
    connection.close();
  }

  /**
   * SHOW GRANT ROLE roleName ON DATABASE dbName: Needs Hive patch
   * @throws Exception
   */
  @Ignore
  @Test
  public void testShowPrivilegesByRoleOnUri() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("GRANT ALL ON URI 'file:///tmp/file.txt' TO ROLE role1");

    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON URI 'file:///tmp/file.txt'");
    assertTrue("Expecting SQL Exception", false);
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("file:///tmp/file.txt"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));//table
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }
    statement.close();
    connection.close();
  }

  /**
   * SHOW CURRENT ROLE
   * @throws Exception
   */
  @Test
  public void testShowCurrentRole() throws Exception {
    //TODO: Add more test cases once we fix SENTRY-268
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("SET ROLE role1");
    ResultSet resultSet = statement.executeQuery("SHOW CURRENT ROLES");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertThat(resultSetMetaData.getColumnCount(), is(1));
    assertThat(resultSetMetaData.getColumnName(1), equalToIgnoringCase("role"));

    while( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("role1"));
    }
    statement.close();
    connection.close();
  }

  // See SENTRY-166
  @Test
  public void testUriWithEquals() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("GRANT ALL ON URI 'file:///tmp/partition=value/file.txt' TO ROLE role1");

    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    assertResultSize(resultSet, 1);
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("file:///tmp/partition=value/file.txt"));
      assertThat(resultSet.getString(2), equalToIgnoringCase(""));//table
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("*"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }
    statement.close();
    connection.close();
  }

}
