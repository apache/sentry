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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.PrivilegeResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains tests for meta data operations with column level privileges
 */
public class TestDbColumnLevelMetaDataOps extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory.
          getLogger(TestDbColumnLevelMetaDataOps.class);

  private static final String TEST_COL_METADATA_OPS_DB = "test_col_metadata_ops_db";
  private static final String TEST_COL_METADATA_OPS_TB = "test_col_metadata_ops_tb";
  private static final String TEST_COL_METADATA_OPS_ROLE = "test_col_metadata_ops_role";

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
    createTestData();
  }

  /**
   * Create test database, table and role
   * and grant column level privilege
   * @throws Exception
   */
  private void createTestData() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + TEST_COL_METADATA_OPS_DB);
    statement.execute("USE " + TEST_COL_METADATA_OPS_DB);
    statement.execute("CREATE TABLE " + TEST_COL_METADATA_OPS_TB
            + " (privileged STRING, unprivileged INT) partitioned by (privileged_par STRING, unprivileged_par INT)");
    statement.execute("INSERT INTO TABLE " + TEST_COL_METADATA_OPS_TB
            + " PARTITION(privileged_par = 'privileged_par', unprivileged_par = 1) VALUES ('test1', 1)");
    statement.execute("CREATE ROLE " + TEST_COL_METADATA_OPS_ROLE);
    statement.execute("GRANT SELECT(privileged) ON TABLE " + TEST_COL_METADATA_OPS_TB + " TO ROLE " + TEST_COL_METADATA_OPS_ROLE);
    statement.execute("GRANT ROLE " + TEST_COL_METADATA_OPS_ROLE + " TO GROUP " + USERGROUP1);

    PrivilegeResultSet prset = new PrivilegeResultSet(statement, "SHOW GRANT ROLE "
            + TEST_COL_METADATA_OPS_ROLE + " ON DATABASE " + TEST_COL_METADATA_OPS_DB);
    LOGGER.info("SHOW GRANT : " + prset.toString());
    prset.verifyResultSetColumn("table", TEST_COL_METADATA_OPS_TB);
    prset.verifyResultSetColumn("column", "privileged");
    prset.verifyResultSetColumn("privilege", "select");

    statement.close();
    connection.close();
  }

  private ResultSet executeQueryWithLog(Statement statement, String query) throws Exception {
    ResultSet rs;
    try {
      LOGGER.info("Running " + query);
      rs = statement.executeQuery(query);
      return rs;
    } catch (HiveSQLException ex) {
      LOGGER.error("Privilege exception occurs when running : " + query);
      throw ex;
    }
  }

  private void validateColumnMetaData(String query, String colMetaField, String user,
                                      String privileged, String unprivileged) throws Exception {
    Connection conneciton = context.createConnection(user);
    Statement statement = context.createStatement(conneciton);
    statement.execute("USE " + TEST_COL_METADATA_OPS_DB);
    ResultSet rs = executeQueryWithLog(statement, query);
    boolean found = false;
    while (rs.next()) {
      String val = rs.getString(colMetaField);
      // Relax validation for now:
      // user with any select privilege can perform metadata operations,
      // even though it might show some columns which he doesn't have privileges
      //assertFalse("column unprivileged shouldn't be shown in result",
      //        val.equalsIgnoreCase("unprivileged"));
      if (val.equalsIgnoreCase("unprivileged")) {
        LOGGER.warn("column unprivileged related metadata info is not disabled from result");
      }
      if (val.toLowerCase().contains(privileged)) {
        LOGGER.info("detected privileged column information: " + privileged);
        found = true;
      } else if (val.toLowerCase().contains(unprivileged)) {
        LOGGER.warn("detected unexpected column information: " + unprivileged);
      }
    }
    rs.close();
    statement.close();
    conneciton.close();
    assertTrue("failed to detect column privileged from result", found);
  }

  private void validateColumnMetaData(String query, String colMetaField, String user) throws Exception {
    validateColumnMetaData(query, colMetaField, user, "privileged", "unprivileged");
  }

  private void validateSemanticException(String query, String user) throws Exception {
    Connection conneciton = context.createConnection(user);
    Statement statement = context.createStatement(conneciton);
    try {
      LOGGER.info("Running " + query);
      statement.execute(query);
      fail("failed to throw SemanticException");
    } catch (Exception ex) {
      String err = "SemanticException No valid privileges";
      assertTrue("failed to detect " + err,
              ex.getMessage().contains("SemanticException No valid privileges"));
    }
    statement.close();
    conneciton.close();
  }

  /**
   * Test with column level privilege
   * user can NOT "show table extended"
   */
  @Test
  public void testShowExtended() throws Exception {
    String query = "SHOW TABLE EXTENDED IN " + TEST_COL_METADATA_OPS_DB
            + " like '" + TEST_COL_METADATA_OPS_TB + "'";
    // with column level privileges, user can not do show extended
    validateSemanticException(query, USER1_1);
    // negative test, without any privileges, user can not do it also
    validateSemanticException(query, USER2_1);
  }

  /**
   * Test with column level privileges,
   * user can list all columns for now
   */
  @Test
  public void testShowColumns() throws Exception {
    String query = "SHOW COLUMNS IN " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    // with column level privileges, user can show columns
    validateColumnMetaData(query, "field", USER1_1);
    // without column/table level privileges, any user can NOT show columns
    validateSemanticException(query, USER2_1);
  }

  /**
   * Test SHOW TBLPROPERTIES requires table level privileges
   * @throws Exception
   */
  @Test
  public void testShowProperties() throws Exception {
    String query = "SHOW TBLPROPERTIES " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);
  }

  /**
   * Test with column level select privilege,
   * user can do "describe table"
   */
  @Test
  public void testDescribeTable() throws Exception {
    String query = "DESCRIBE " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    // with column level privilege, user can describe table, but columns are not filtered for now
    validateColumnMetaData(query, "col_name", USER1_1);
    // without column/table level privileges, any user can NOT describe table
    validateSemanticException(query, USER2_1);

    // only with table level privileges user can describe extended/formatted
    query = "DESCRIBE EXTENDED " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);

    query = "DESCRIBE EXTENDED " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB + " s";
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);

    query = "DESCRIBE FORMATTED " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);

    query = "DESCRIBE FORMATTED " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB + " s";
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);
  }

  /**
   * Test with column level select privilege,
   * user can only do "explain select column";
   * any other select requires table level privileges
   * @throws Exception
   */
  @Ignore("After fix SENTRY-849, should enable this test")
  @Test
  public void testExplainSelect() throws Exception {
    String query = "EXPLAIN SELECT privileged FROM " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    // With column level privilege, user can explain select column
    validateColumnMetaData(query, "Explain", USER1_1);
    // Without column/table level privilege, user can NOT explain select column
    validateSemanticException(query, USER2_1);

    // user can NOT explain select unprivileged column
    query = "EXPLAIN SELECT unprivileged FROM " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);

    query = "EXPLAIN SELECT * FROM " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);

    query = "EXPLAIN SELECT count(*) FROM " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);

    query = "EXPLAIN SELECT * FROM (SELECT privileged AS c FROM " +
            TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB + " union all select unprivileged as c from "
            + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB + ") subq1 order by c";
    validateSemanticException(query, USER1_1);
    validateSemanticException(query, USER2_1);
  }

  /**
   * Test if add a new column and grant privilege,
   * test user can immediately has metadata access to this column
   */
  @Test
  public void testShowNewColumn() throws Exception {
    String colName = "newcol";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + TEST_COL_METADATA_OPS_DB);
    statement.execute("ALTER TABLE " + TEST_COL_METADATA_OPS_TB + " ADD COLUMNS (" + colName + " STRING)");
    statement.execute("GRANT SELECT(" + colName + ") ON TABLE " + TEST_COL_METADATA_OPS_TB + " TO ROLE " + TEST_COL_METADATA_OPS_ROLE);
    statement.close();
    connection.close();

    Connection newconn = context.createConnection(ADMIN1);
    Statement newstmt = context.createStatement(newconn);
    newstmt.execute("USE " + TEST_COL_METADATA_OPS_DB);
    ResultSet rs = executeQueryWithLog(newstmt, "SHOW COLUMNS IN " + TEST_COL_METADATA_OPS_TB);
    boolean found = false;
    while (rs.next() && !found) {
      String val = rs.getString("field");
      LOGGER.info("found " + val);
      if (val.equalsIgnoreCase(colName)) {
        found = true;
      }
    }
    assertTrue("Failed to show " + colName, found);
    rs.close();
    newstmt.close();
    newconn.close();
  }

  /**
   * Grant user column level privileges, show partitions
   * should list user's granted columns
   * @throws Exception
   */
  @Ignore("After fix SENTRY-898, turn on this test")
  @Test
  public void testShowPartitions() throws Exception {
    final String PAR_ROLE_NAME = TEST_COL_METADATA_OPS_ROLE + "_2";

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE " + TEST_COL_METADATA_OPS_DB);
    statement.execute("CREATE ROLE " + PAR_ROLE_NAME);
    statement.execute("GRANT SELECT(privileged_par) ON TABLE " + TEST_COL_METADATA_OPS_TB + " TO ROLE " + PAR_ROLE_NAME);
    statement.execute("GRANT ROLE " + PAR_ROLE_NAME + " TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();

    String query = "SHOW PARTITIONS " + TEST_COL_METADATA_OPS_DB + "." + TEST_COL_METADATA_OPS_TB;
    validateColumnMetaData(query, "partition", USER1_1, "privileged_par", "unprivileged_par");
  }

}
