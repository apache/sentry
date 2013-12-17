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

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestMetadataObjectRetrieval extends
AbstractTestWithStaticConfiguration {
  private PolicyFile policyFile;
  private File dataFile;

  @Before
  public void setup() throws Exception {
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
    context = createContext();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /**
   * Method called to run positive tests:
   *  describe table
   *  describe table column
   *  show columns from table
   *  show create table table
   *  show tblproperties table
   *
   * The table is assumed to have two colums under_col int and value string.
   */
  private void positiveDescribeShowTests(String user, String db, String table) throws Exception {
    Connection connection = context.createConnection(user);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + db);
    ResultSet rs = statement.executeQuery("DESCRIBE " + table);
    assertTrue(rs.next());

    assertTrue("describe table fail", rs.getString(1).trim().equals("under_col"));
    assertTrue("describe table fail", rs.getString(2).trim().equals("int"));
    assertTrue(rs.next());
    assertTrue("describe table fail", rs.getString(1).trim().equals("value"));
    assertTrue("describe table fail", rs.getString(2).trim().equals("string"));

    rs = statement.executeQuery("DESCRIBE " + table + " under_col");
    assertTrue(rs.next());
    assertTrue("describe table fail", rs.getString(1).trim().equals("under_col"));
    assertTrue("describe table fail", rs.getString(2).trim().equals("int"));

    rs = statement.executeQuery("DESCRIBE " + table + " value");
    assertTrue(rs.next());
    assertTrue("describe table fail", rs.getString(1).trim().equals("value"));
    assertTrue("describe table fail", rs.getString(2).trim().equals("string"));

    rs = statement.executeQuery("SHOW COLUMNS FROM " + table);
    assertTrue(rs.next());
    assertTrue("show columns from fail", rs.getString(1).trim().equals("under_col"));
    assertTrue(rs.next());
    assertTrue("show columns from fail", rs.getString(1).trim().equals("value"));

    rs = statement.executeQuery("SHOW CREATE TABLE " + table);
    assertTrue("SHOW CREATE TABLE fail", rs.next());

    rs = statement.executeQuery("SHOW TBLPROPERTIES " + table);
    assertTrue("SHOW TBLPROPERTIES fail", rs.next());

    statement.close();
    connection.close();
  }
  /**
   * Method called to run negative tests:
   *  describe table
   *  describe table column
   *  show columns from table
   *  show create table table
   *  show tblproperties table
   *
   * The table is assumed to have two columns under_col int and value string.
   */
  private void negativeDescribeShowTests(String user, String db, String table) throws Exception {
    Connection connection = context.createConnection(user);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + db);
    context.assertAuthzException(statement, "DESCRIBE " + table);
    context.assertAuthzException(statement, "DESCRIBE " + table + " under_col");
    context.assertAuthzException(statement, "DESCRIBE " + table + " value");
    context.assertAuthzException(statement, "SHOW COLUMNS FROM " + table);
    context.assertAuthzException(statement, "SHOW CREATE TABLE " + table);
    context.assertAuthzException(statement, "SHOW TBLPROPERTIES " + table);
    statement.close();
    connection.close();
  }


  /**
   * Tests to ensure a user with all on server,
   * insert|select on table can view metadata while
   * a user with all on a different table cannot
   * view the metadata.

   * Test both positive and negative of:
   *  describe table
   *  describe table column
   *  show columns from table
   *  show create table table
   *  show tblproperties table
   *
   * Positive tests are run with:
   *  all@server
   *  select@table
   *  insert@table
   * Negative tests are run three times:
   *  none
   *  insert@different table
   */
  @Test
  public void testAllOnServerSelectInsertNegativeNoneAllOnDifferentTable()
      throws Exception {
    policyFile
        .addPermissionsToRole(GROUP1_ROLE, "server=server1->db=" + DB1 + "->table=" + TBL2)
        .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);
    positiveDescribeShowTests(ADMIN1, DB1, TBL1);
    negativeDescribeShowTests(USER1_1, DB1, TBL1);
    policyFile
    .addPermissionsToRole(GROUP1_ROLE, SELECT_DB1_TBL1)
    .write(context.getPolicyFile());
    positiveDescribeShowTests(USER1_1, DB1, TBL1);
    policyFile.removePermissionsFromRole(GROUP1_ROLE, SELECT_DB1_TBL1);
    policyFile
    .addPermissionsToRole(GROUP1_ROLE, INSERT_DB1_TBL1)
    .write(context.getPolicyFile());
    positiveDescribeShowTests(USER1_1, DB1, TBL1);
  }

  /**
   * Tests to ensure that a user is able to view metadata
   * with all on db
   *
   * Test positive:
   *  describe table
   *  describe table column
   *  show columns from table
   *  show create table table
   *  show tblproperties table
   *
   * Positive tests are run twice:
   *  all@server
   *  all@db
   */
  @Test
  public void testAllOnServerAndAllOnDb() throws Exception {
    policyFile
      .addPermissionsToRole(GROUP1_ROLE, "server=server1->db=" + DB1)
      .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);
    positiveDescribeShowTests(ADMIN1, DB1, TBL1);
    positiveDescribeShowTests(USER1_1, DB1, TBL1);
  }

  /**
   * Test to ensure that all on view do not result in
   * metadata privileges on the underlying table
   *
   * Test both positive and negative of:
   *  describe table
   *  describe table column
   *  show columns from table
   *  show create table table
   *  show tblproperties table
   *
   * Positive tests are run with all@server
   * Negative tests are run three times:
   *  none
   *  all@view
   */
  @Test
  public void testAllOnServerNegativeAllOnView() throws Exception {
    policyFile
      .addPermissionsToRole(GROUP1_ROLE, "server=server1->db=" + DB1 + "->table=" + VIEW1)
      .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("DROP VIEW IF EXISTS " + VIEW1);
    statement.execute("CREATE VIEW " + VIEW1 + " (value) AS SELECT value from " + TBL1 + " LIMIT 10");
    positiveDescribeShowTests(ADMIN1, DB1, TBL1);
    statement.close();
    connection.close();
    negativeDescribeShowTests(USER1_1, DB1, TBL1);
  }

  /**
   * Tests to ensure that a user is able to view metadata
   * with all on table
   *
   * Test positive:
   *  describe table
   *  describe table column
   *  show columns from table
   *  show create table table
   *  show tblproperties table
   *
   * Positive tests are run twice:
   *  all@server
   *  all@table
   */
  @Test
  public void testAllOnServerAndAllOnTable() throws Exception {
    policyFile
      .addPermissionsToRole(GROUP1_ROLE, "server=server1->db=" + DB1 + "->table=" + TBL1)
      .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);
    positiveDescribeShowTests(ADMIN1, DB1, TBL1);
    positiveDescribeShowTests(USER1_1, DB1, TBL1);
  }


  /**
   * Tests that admin and all@db can describe database
   * and describe database extended. Also tests that a user
   * with no privileges on a db cannot describe database.
   */
  @Test
  public void testDescribeDatabasesWithAllOnServerAndAllOnDb()
      throws Exception {
    policyFile
      .addPermissionsToRole(GROUP1_ROLE, "server=server1->db=" + DB1)
      .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1, DB2);
    createDb(ADMIN1, DB1, DB2);
    createTable(ADMIN1, DB1, dataFile, TBL1);

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    assertTrue(statement.executeQuery("DESCRIBE DATABASE " + DB1).next());
    assertTrue(statement.executeQuery("DESCRIBE DATABASE EXTENDED " + DB1).next());
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    assertTrue(statement.executeQuery("DESCRIBE DATABASE " + DB1).next());
    assertTrue(statement.executeQuery("DESCRIBE DATABASE EXTENDED " + DB1).next());
    context.assertAuthzException(statement, "DESCRIBE DATABASE " + DB2);
    context.assertAuthzException(statement, "DESCRIBE DATABASE EXTENDED " + DB2);
    policyFile.addPermissionsToRole(GROUP1_ROLE, INSERT_DB2_TBL1)
    .write(context.getPolicyFile());
    context.assertAuthzException(statement, "DESCRIBE DATABASE " + DB2);
    context.assertAuthzException(statement, "DESCRIBE DATABASE EXTENDED " + DB2);
    statement.close();
    connection.close();
  }

  /**
   * Tests that a user without db level privileges cannot describe default
   */
  @Test
  public void testDescribeDefaultDatabase() throws Exception {
    policyFile
      .addPermissionsToRole(GROUP1_ROLE, "server=server1->db=default->table=" + TBL1 + "->action=select",
        "server=server1->db=" + DB1 + "->table=" + TBL1 + "->action=select")
      .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1, DB2);
    createDb(ADMIN1, DB1, DB2);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    assertTrue(statement.executeQuery("DESCRIBE DATABASE default").next());
    statement.execute("USE " + DB1);
    assertTrue(statement.executeQuery("DESCRIBE DATABASE default").next());
    assertTrue(statement.executeQuery("DESCRIBE DATABASE " + DB1).next());
    assertTrue(statement.executeQuery("DESCRIBE DATABASE " + DB2).next());
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    context.assertAuthzException(statement, "DESCRIBE DATABASE default");
    context.assertAuthzException(statement, "DESCRIBE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    context.assertAuthzException(statement, "DESCRIBE DATABASE " + DB1);
    context.assertAuthzException(statement, "DESCRIBE DATABASE " + DB2);
    statement.close();
    connection.close();
  }

  /**
   * Tests that users without privileges cannot execute show indexes
   * and that users with all on table can execute show indexes
   */
  @Test
  public void testShowIndexes1() throws Exception {
    // grant privilege to non-existent table to allow use db1
    policyFile.addPermissionsToRole(GROUP1_ROLE, SELECT_DB1_NONTABLE)
      .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("DROP INDEX IF EXISTS " + INDEX1 + " ON " + TBL1);
    statement
    .execute("CREATE INDEX "
        + INDEX1
        + " ON TABLE "
        + TBL1
        + "(value) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD");
    statement.execute("DROP VIEW IF EXISTS " + VIEW1);
    statement.execute("CREATE VIEW " + VIEW1 + " (value) AS SELECT value from " + TBL1 + " LIMIT 10");
    statement.close();
    connection.close();
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    context.assertAuthzException(statement, "SHOW INDEX ON " + TBL1);
    policyFile
    .addPermissionsToRole(GROUP1_ROLE, SELECT_DB1_VIEW1)
    .write(context.getPolicyFile());
    context.assertAuthzException(statement, "SHOW INDEX ON " + TBL1);
    policyFile.removePermissionsFromRole(GROUP1_ROLE, SELECT_DB1_VIEW1)
    .addPermissionsToRole(GROUP1_ROLE, SELECT_DB1_TBL1)
    .write(context.getPolicyFile());
    verifyIndex(statement, TBL1, INDEX1);
    policyFile.removePermissionsFromRole(GROUP1_ROLE, SELECT_DB1_TBL1)
    .addPermissionsToRole(GROUP1_ROLE, INSERT_DB1_TBL1)
    .write(context.getPolicyFile());
    verifyIndex(statement, TBL1, INDEX1);
    statement.close();
    connection.close();
  }

  private void verifyIndex(Statement statement, String table, String index) throws Exception {
    ResultSet rs = statement.executeQuery("SHOW INDEX ON " + table);
    assertTrue(rs.next());
    assertEquals(index, rs.getString(1).trim());
    assertEquals(table, rs.getString(2).trim());
    assertEquals("value", rs.getString(3).trim());
    assertEquals("db_1__tb_1_index_1__", rs.getString(4).trim());
    assertEquals("compact", rs.getString(5).trim());
  }

  /**
   * Tests that users without privileges cannot execute show partitions
   * and that users with select on table can execute show partitions
   */
  @Test
  public void testShowPartitions1() throws Exception {
    // grant privilege to non-existent table to allow use db1
    policyFile.addPermissionsToRole(GROUP1_ROLE, SELECT_DB1_NONTABLE)
      .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("DROP TABLE IF EXISTS " + TBL1);
    statement.execute("create table " + TBL1
        + " (under_col int, value string) PARTITIONED BY (dt INT)");
    statement.execute("load data local inpath '" + dataFile.getPath()
        + "' into table " + TBL1 + " PARTITION (dt=3)");
    statement.execute("DROP VIEW IF EXISTS " + VIEW1);
    statement.execute("CREATE VIEW " + VIEW1 + " (value) AS SELECT value from " + TBL1 + " LIMIT 10");
    statement.close();
    connection.close();
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    context.assertAuthzException(statement, "SHOW PARTITIONS " + TBL1);
    policyFile
    .addPermissionsToRole(GROUP1_ROLE, SELECT_DB1_VIEW1)
    .write(context.getPolicyFile());
    context.assertAuthzException(statement, "SHOW PARTITIONS " + TBL1);
    policyFile
    .removePermissionsFromRole(GROUP1_ROLE, SELECT_DB1_VIEW1)
    .addPermissionsToRole(GROUP1_ROLE, SELECT_DB1_TBL1)
    .write(context.getPolicyFile());
    verifyParition(statement, TBL1);
    policyFile.removePermissionsFromRole(GROUP1_ROLE, SELECT_DB1_TBL1)
    .addPermissionsToRole(GROUP1_ROLE, INSERT_DB1_TBL1)
    .write(context.getPolicyFile());
    verifyParition(statement, TBL1);
    statement.close();
    connection.close();
  }

  private void verifyParition(Statement statement, String table) throws Exception {
    ResultSet rs = statement.executeQuery("SHOW PARTITIONS " + TBL1);
    assertTrue(rs.next());
    assertEquals("dt=3", rs.getString(1).trim());
  }
}