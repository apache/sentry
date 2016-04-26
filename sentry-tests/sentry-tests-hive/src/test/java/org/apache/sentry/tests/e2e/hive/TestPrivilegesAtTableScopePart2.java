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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

/**
 * The second test class of testing privileges at table level.
 */
public class TestPrivilegesAtTableScopePart2 extends AbstractTestWithStaticConfiguration {
  private static PolicyFile policyFile;
  private final static String MULTI_TYPE_DATA_FILE_NAME = "emp.dat";

  @Before
  public void setup() throws Exception {
    policyFile = super.setupPolicy();
    super.setup();
    prepareDBDataForTest();
  }

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  protected static void prepareDBDataForTest() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");

    statement.execute("CREATE TABLE " + TBL1 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE " + TBL1);
    statement.execute("CREATE TABLE " + TBL2 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE " + TBL2);
    statement.execute("CREATE VIEW VIEW_1 AS SELECT A, B FROM " + TBL1);

    statement.close();
    connection.close();
  }

  /***
   * Verify truncate table permissions for different users with different
   * privileges
   * @throws Exception
   */
  @Test
  public void testTruncateTable() throws Exception {
    File dataDir = context.getDataDir();
    // copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("USE " + DB1);
    statement.execute("DROP TABLE if exists " + TBL1);
    statement.execute("DROP TABLE if exists " + TBL2);
    statement.execute("DROP TABLE if exists " + TBL3);
    statement.execute("CREATE TABLE " + TBL1 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("CREATE TABLE " + TBL2 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("CREATE TABLE " + TBL3 + "(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + TBL1);
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + TBL2);
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + TBL3);

    // verify admin can execute truncate table
    statement.execute("TRUNCATE TABLE " + TBL1);
    assertFalse(hasData(statement, TBL1));

    statement.close();
    connection.close();

    // add roles and grant permissions
    updatePolicyFile();

    // test truncate table without partitions
    truncateTableTests(false);
  }

  /***
   * Verify truncate partitioned permissions for different users with different
   * privileges
   * @throws Exception
   */
  @Test
  public void testTruncatePartitionedTable() throws Exception {
    File dataDir = context.getDataDir();
    // copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // create partitioned tables
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("DROP TABLE if exists " + TBL1);
    statement.execute("CREATE TABLE " + TBL1 + " (i int) PARTITIONED BY (j int)");
    statement.execute("DROP TABLE if exists " + TBL2);
    statement.execute("CREATE TABLE " + TBL2 + " (i int) PARTITIONED BY (j int)");
    statement.execute("DROP TABLE if exists " + TBL3);
    statement.execute("CREATE TABLE " + TBL3 + " (i int) PARTITIONED BY (j int)");

    // verify admin can execute truncate empty partitioned table
    statement.execute("TRUNCATE TABLE " + TBL1);
    assertFalse(hasData(statement, TBL1));
    statement.close();
    connection.close();

    // add roles and grant permissions
    updatePolicyFile();

    // test truncate empty partitioned tables
    truncateTableTests(false);

    // add partitions to tables
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    statement.execute("ALTER TABLE " + TBL1 + " ADD PARTITION (j=1) PARTITION (j=2)");
    statement.execute("ALTER TABLE " + TBL2 + " ADD PARTITION (j=1) PARTITION (j=2)");
    statement.execute("ALTER TABLE " + TBL3 + " ADD PARTITION (j=1) PARTITION (j=2)");

    // verify admin can execute truncate NOT empty partitioned table
    statement.execute("TRUNCATE TABLE " + TBL1 + " partition (j=1)");
    statement.execute("TRUNCATE TABLE " + TBL1);
    assertFalse(hasData(statement, TBL1));
    statement.close();
    connection.close();

    // test truncate NOT empty partitioned tables
    truncateTableTests(true);
  }

  /**
   * Test queries without from clause. Hive rewrites the queries with dummy db and table
   * entities which should not trip authorization check.
   * @throws Exception
   */
  @Test
  public void testSelectWithoutFrom() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "all_tab1")
        .addPermissionsToRole("all_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL1)
        .addRolesToGroup(USERGROUP2, "select_tab1")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL1)
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);

    // test with implicit default database
    assertTrue(statement.executeQuery("SELECT 1 ").next());
    assertTrue(statement.executeQuery("SELECT current_database()").next());

    // test after switching database
    statement.execute("USE " + DB1);
    assertTrue(statement.executeQuery("SELECT 1 ").next());
    assertTrue(statement.executeQuery("SELECT current_database() ").next());
    statement.close();
    connection.close();
  }

  // verify that the given table has data
  private boolean hasData(Statement stmt, String tableName) throws Exception {
    ResultSet rs1 = stmt.executeQuery("SELECT * FROM " + tableName);
    boolean hasResults = rs1.next();
    rs1.close();
    return hasResults;
  }

  @Test
  public void testDummyPartition() throws Exception {

    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("USE " + DB1);

    statement.execute("DROP TABLE if exists " + TBL1);
    statement.execute("CREATE table " + TBL1 + " (a int) PARTITIONED BY (b string, c string)");
    statement.execute("DROP TABLE if exists " + TBL3);
    statement.execute("CREATE table " + TBL3 + " (a2 int) PARTITIONED BY (b2 string, c2 string)");
    statement.close();
    connection.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "select_tab2")
        .addPermissionsToRole("select_tab1", "server=server1->db=DB_1->table=" + TBL1 + "->action=select")
        .addPermissionsToRole("select_tab2", "server=server1->db=DB_1->table=" + TBL3 + "->action=insert");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    statement.execute("USE " + DB1);
    statement.execute("INSERT OVERWRITE TABLE " + TBL3 + " PARTITION(b2='abc', c2) select a, b as c2 from " + TBL1);
    statement.close();
    connection.close();

  }

  /**
   * update policy file for truncate table tests
   */
  private void updatePolicyFile() throws Exception{
    policyFile
        .addRolesToGroup(USERGROUP1, "all_tab1")
        .addPermissionsToRole("all_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL2)
        .addRolesToGroup(USERGROUP2, "drop_tab1")
        .addPermissionsToRole("drop_tab1",
            // Because of CDH-28502, here use 'action=all' instead of 'action-drop'
            "server=server1->db=" + DB1 + "->table=" + TBL3 + "->action=all",
            "server=server1->db=" + DB1 + "->table=" + TBL3 + "->action=select")
        .addRolesToGroup(USERGROUP3, "select_tab1")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=" + DB1 + "->table=" + TBL1 + "->action=select");
    writePolicyFile(policyFile);
  }

  /**
   * Test truncate table with or without partitions for users with different privileges.
   * Only test truncate table partition if truncPartition is true.
   */
  private void truncateTableTests(boolean truncPartition) throws Exception{
    Connection connection = null;
    Statement statement = null;
    try {
      connection = context.createConnection(USER1_1);
      statement = context.createStatement(connection);
      statement.execute("USE " + DB1);
      // verify all privileges on table can truncate table
      if (truncPartition) {
        statement.execute("TRUNCATE TABLE " + TBL2 + " PARTITION (j=1)");
      }
      statement.execute("TRUNCATE TABLE " + TBL2);
      assertFalse(hasData(statement, TBL2));
      statement.close();
      connection.close();

      connection = context.createConnection(USER2_1);
      statement = context.createStatement(connection);
      statement.execute("USE " + DB1);
      // verify drop privilege on table can truncate table
      if (truncPartition) {
        statement.execute("TRUNCATE TABLE " + TBL3 + " partition (j=1)");
      }
      statement.execute("TRUNCATE TABLE " + TBL3);
      assertFalse(hasData(statement, TBL3));
      statement.close();
      connection.close();

      connection = context.createConnection(USER3_1);
      statement = context.createStatement(connection);
      statement.execute("USE " + DB1);
      // verify select privilege on table can NOT truncate table
      if (truncPartition) {
        context.assertAuthzException(
            statement, "TRUNCATE TABLE " + TBL1 + " PARTITION (j=1)");
      }
      context.assertAuthzException(statement, "TRUNCATE TABLE " + TBL1);
    } finally {
      if (statement != null) {
        statement.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }
}
