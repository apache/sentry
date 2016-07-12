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

import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.core.common.utils.PolicyFile;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;

import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import static org.junit.Assume.assumeTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * The test class implements concurrency tests to test:
 * Sentry client, HS2 jdbc client etc.
 */
public class TestConcurrentClients extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
          .getLogger(TestConcurrentClients.class);

  private PolicyFile policyFile;

  // define scale for tests
  private final int NUM_OF_TABLES = Integer.parseInt(System.getProperty(
          "sentry.e2e.concurrency.test.tables-per-db", "1"));
  private final int NUM_OF_PAR = Integer.parseInt(System.getProperty(
          "sentry.e2e.concurrency.test.partitions-per-tb", "3"));
  private final int NUM_OF_THREADS = Integer.parseInt(System.getProperty(
          "sentry.e2e.concurrency.test.threads", "30"));
  private final int NUM_OF_TASKS = Integer.parseInt(System.getProperty(
          "sentry.e2e.concurrency.test.tasks", "100"));
  private final Long HS2_CLIENT_TEST_DURATION_MS = Long.parseLong(System.getProperty(
          "sentry.e2e.concurrency.test.hs2client.test.time.ms", "10000")); //millis
  private final Long SENTRY_CLIENT_TEST_DURATION_MS = Long.parseLong(System.getProperty(
          "sentry.e2e.concurrency.test.sentryclient.test.time.ms", "10000")); //millis

  private static Map<String, String> privileges = new HashMap<String, String>();
  static {
    privileges.put("all_db1", "server=server1->db=" + DB1 + "->action=all");
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP)
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
  }

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    assumeTrue(Boolean.parseBoolean(System.getProperty("sentry.scaletest.oncluster", "false")));
    useSentryService = true;    // configure sentry client
    clientKerberos = true; // need to get client configuration from testing environments
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
 }

  static String randomString( int len ){
    return RandomStringUtils.random(len, true, false);
  }

  private void execStmt(Statement stmt, String sql) throws Exception {
    LOGGER.info("Running [" + sql + "]");
    stmt.execute(sql);
  }

  private void createDbTb(String user, String db, String tb) throws Exception{
    Connection connection = context.createConnection(user);
    Statement statement = context.createStatement(connection);
    try {
      execStmt(statement, "DROP DATABASE IF EXISTS " + db + " CASCADE");
      execStmt(statement, "CREATE DATABASE " + db);
      execStmt(statement, "USE " + db);
      for (int i = 0; i < NUM_OF_TABLES; i++) {
        String tbName = tb + "_" + Integer.toString(i);
        execStmt(statement, "CREATE TABLE  " + tbName + " (a string) PARTITIONED BY (b string)");
      }
    } catch (Exception ex) {
      LOGGER.error("caught exception: " + ex);
    } finally {
      statement.close();
      connection.close();
    }
  }

  private void createPartition(String user, String db, String tb) throws Exception{
    Connection connection = context.createConnection(user);
    Statement statement = context.createStatement(connection);
    try {
      execStmt(statement, "USE " + db);
      for (int j = 0; j < NUM_OF_TABLES; j++) {
        String tbName = tb + "_" + Integer.toString(j);
        for (int i = 0; i < NUM_OF_PAR; i++) {
          String randStr = randomString(4);
          String sql = "ALTER TABLE " + tbName + " ADD IF NOT EXISTS PARTITION (b = '" + randStr + "') ";
          LOGGER.info("[" + i + "] " + sql);
          execStmt(statement, sql);
        }
      }
    } catch (Exception ex) {
      LOGGER.error("caught exception: " + ex);
    } finally {
      statement.close();
      connection.close();
    }
  }

  private void adminCreateRole(String roleName) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement stmt = context.createStatement(connection);
    try {
      execStmt(stmt, "DROP ROLE " + roleName);
    } catch (Exception ex) {
      LOGGER.warn("Role does not exist " + roleName);
    } finally {
      try {
        execStmt(stmt, "CREATE ROLE " + roleName);
      } catch (Exception ex) {
        LOGGER.error("caught exception when create new role: " + ex);
      } finally {
        stmt.close();
        connection.close();
      }
    }
  }

  private void adminCleanUp(String db, String roleName) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement stmt = context.createStatement(connection);
    try {
      execStmt(stmt, "DROP DATABASE IF EXISTS " + db + " CASCADE");
      execStmt(stmt, "DROP ROLE " + roleName);
    } catch (Exception ex) {
      LOGGER.warn("Failed to clean up ", ex);
    } finally {
      stmt.close();
      connection.close();
    }
  }

  private void adminShowRole(String roleName) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement stmt = context.createStatement(connection);
    boolean found = false;
    try {
      ResultSet rs = stmt.executeQuery("SHOW ROLES ");
      while (rs.next()) {
        if (rs.getString("role").equalsIgnoreCase(roleName)) {
          LOGGER.info("Found role " + roleName);
          found = true;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("caught exception when show roles: " + ex);
    } finally {
      stmt.close();
      connection.close();
    }
    assertTrue("failed to detect " + roleName, found);
  }

  private void adminGrant(String test_db, String test_tb,
                          String roleName, String group) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement stmt = context.createStatement(connection);
    try {
      execStmt(stmt, "USE " + test_db);
      for (int i = 0; i < NUM_OF_TABLES; i++) {
        String tbName = test_tb + "_" + Integer.toString(i);
        execStmt(stmt, "GRANT ALL ON TABLE " + tbName + " TO ROLE " + roleName);
      }
      execStmt(stmt, "GRANT ROLE " + roleName + " TO GROUP " + group);
    } catch (Exception ex) {
      LOGGER.error("caught exception when grant permission and role: " + ex);
    } finally {
      stmt.close();
      connection.close();
    }
  }

  /**
   * A synchronized state class to track concurrency test status from each thread
   */
  private final static class TestRuntimeState {
    private int numSuccess = 0;
    private boolean failed = false;
    private Throwable firstException = null;

    public synchronized void setFirstException(Throwable e) {
      failed = true;
      if (firstException == null) {
        firstException = e;
      }
    }
    public synchronized void setNumSuccess() {
      numSuccess += 1;
    }
    public synchronized int getNumSuccess() {
      return numSuccess;
    }
    public synchronized Throwable getFirstException() {
      return firstException;
    }
   }

  /**
   * Test when concurrent HS2 clients talking to server,
   * Privileges are correctly created and updated.
   * @throws Exception
   */
  @Test
  public void testConccurentHS2Client() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(NUM_OF_THREADS);
    final TestRuntimeState state = new TestRuntimeState();

    for (int i = 0; i < NUM_OF_TASKS; i ++) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          LOGGER.info("Starting tests: create role, show role, create db and tbl, and create partitions");
          if (state.failed) {
            return;
          }
          try {
            Long startTime = System.currentTimeMillis();
            Long elapsedTime = 0L;
            while (Long.compare(elapsedTime, HS2_CLIENT_TEST_DURATION_MS) <= 0) {
              String randStr = randomString(5);
              String test_role = "test_role_" + randStr;
              String test_db = "test_db_" + randStr;
              String test_tb = "test_tb_" + randStr;
              LOGGER.info("Start to test sentry with hs2 client with role " + test_role);
              adminCreateRole(test_role);
              adminShowRole(test_role);
              createDbTb(ADMIN1, test_db, test_tb);
              adminGrant(test_db, test_tb, test_role, USERGROUP1);
              createPartition(USER1_1, test_db, test_tb);
              adminCleanUp(test_db, test_role);
              elapsedTime = System.currentTimeMillis() - startTime;
              LOGGER.info("elapsedTime = " + elapsedTime);
            }
            state.setNumSuccess();
          } catch (Exception e) {
            LOGGER.error("Exception: " + e);
            state.setFirstException(e);
          }
        }
      });
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
      Thread.sleep(1000); //millisecond
    }
    Throwable ex = state.getFirstException();
    assertFalse( ex == null ? "Test failed" : ex.toString(), state.failed);
    assertEquals(NUM_OF_TASKS, state.getNumSuccess());
  }

  /**
   * Test when concurrent sentry clients talking to sentry server, threads data are synchronized
   * @throws Exception
   */
  @Test
  public void testConcurrentSentryClient() throws Exception {
    final String HIVE_KEYTAB_PATH =
            System.getProperty("sentry.e2etest.hive.policyOwnerKeytab");
    final SentryPolicyServiceClient client = getSentryClient("hive", HIVE_KEYTAB_PATH);
    ExecutorService executor = Executors.newFixedThreadPool(NUM_OF_THREADS);

    final TestRuntimeState state = new TestRuntimeState();
    for (int i = 0; i < NUM_OF_TASKS; i ++) {
      LOGGER.info("Start to test sentry client with task id [" + i + "]");
      executor.execute(new Runnable() {
        @Override
        public void run() {
          if (state.failed) {
            LOGGER.error("found one failed state, abort test from here.");
            return;
          }
          try {
            String randStr = randomString(5);
            String test_role = "test_role_" + randStr;
            LOGGER.info("Start to test role: " + test_role);
            Long startTime = System.currentTimeMillis();
            Long elapsedTime = 0L;
            while (Long.compare(elapsedTime, SENTRY_CLIENT_TEST_DURATION_MS) <= 0) {
              LOGGER.info("Test role " + test_role + " runs " + elapsedTime + " ms.");
              client.createRole(ADMIN1, test_role);
              client.listRoles(ADMIN1);
              client.grantServerPrivilege(ADMIN1, test_role, "server1", false);
              client.listAllPrivilegesByRoleName(ADMIN1, test_role);
              client.dropRole(ADMIN1, test_role);
              elapsedTime = System.currentTimeMillis() - startTime;
            }
            state.setNumSuccess();
          } catch (Exception e) {
            LOGGER.error("Sentry Client Testing Exception: ", e);
            state.setFirstException(e);
          }
        }
      });
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
      Thread.sleep(1000); //millisecond
    }
    Throwable ex = state.getFirstException();
    assertFalse( ex == null ? "Test failed" : ex.toString(), state.failed);
    assertEquals(NUM_OF_TASKS, state.getNumSuccess());
  }
}
