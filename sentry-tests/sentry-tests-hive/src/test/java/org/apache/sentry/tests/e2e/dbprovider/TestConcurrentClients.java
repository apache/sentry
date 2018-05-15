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

import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.api.service.thrift.TSentryRole;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;

import org.apache.sentry.tests.e2e.hive.StaticUserGroup;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.containsString;

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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;

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
  // number of threads < half of number of tasks, so that there will be
  // more than 1 threads for each task to be able to test synchronization
  private final int NUM_OF_THREADS = Integer.parseInt(System.getProperty(
          "sentry.e2e.concurrency.test.threads", "2"));
  // since test time out in 10 mins, no more than 600 / 10 = 60 tasks
  private final int NUM_OF_TASKS = Integer.parseInt(System.getProperty(
          "sentry.e2e.concurrency.test.tasks", "8"));
  private final Long HS2_CLIENT_TEST_DURATION_MS = Long.parseLong(System.getProperty(
          "sentry.e2e.concurrency.test.hs2client.test.time.ms", "4000")); //millis
  private final Long SENTRY_CLIENT_TEST_DURATION_MS = Long.parseLong(System.getProperty(
          "sentry.e2e.concurrency.test.sentryclient.test.time.ms", "4000")); //millis
  private final Long EXECUTOR_THREADS_MAX_WAIT_TIME = Long.parseLong(System.getProperty(
      "sentry.e2e.concurrency.test.max.wait.time", Integer.toString(NUM_OF_TASKS * 10))); // secs

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
    useSentryService = true;    // configure sentry client
    clientKerberos = true; // need to get client configuration from testing environments
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
 }

  static String randomString( int len ){
    return RandomStringUtils.random(len, true, false).toLowerCase();
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
  @Test(timeout=600000) // time out in 10 mins
  public void testConcurrentHS2Client() throws Exception {
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
    while (!executor.awaitTermination(EXECUTOR_THREADS_MAX_WAIT_TIME, TimeUnit.SECONDS)) {
      LOGGER.info("Awaiting completion of threads.");
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
  @Test(timeout=600000) // time out in 10 mins
  public void testConcurrentSentryClient() throws Exception {
    final String HIVE_KEYTAB_PATH =
        System.getProperty("sentry.e2etest.hive.policyOwnerKeytab");
    final SentryPolicyServiceClient client = getSentryClient("hive", HIVE_KEYTAB_PATH);
    ExecutorService executor = Executors.newFixedThreadPool(NUM_OF_THREADS);

    final TestRuntimeState state = new TestRuntimeState();
    String scratchLikeDir = context.getProperty(HiveConf.ConfVars.SCRATCHDIR.varname);
    final String uriPrefix = scratchLikeDir.contains("://") ?
        scratchLikeDir : (fileSystem.getUri().toString() + scratchLikeDir);
    LOGGER.info("uriPrefix = " + uriPrefix);
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
            String test_uri = uriPrefix + randStr;

            LOGGER.info("Start to test role: " + test_role);
            Long startTime = System.currentTimeMillis();
            Long elapsedTime = 0L;
            while (Long.compare(elapsedTime, SENTRY_CLIENT_TEST_DURATION_MS) <= 0) {
              LOGGER.info("Test role " + test_role + " runs " + elapsedTime + " ms.");
              client.createRole(ADMIN1, test_role);
              client.grantRoleToGroup(ADMIN1, ADMINGROUP, test_role);

              // validate role
              Set<TSentryRole> sentryRoles = client.listAllRoles(ADMIN1);
              String results = "";
              for (TSentryRole role : sentryRoles) {
                results += role.toString() + "|";
              }
              LOGGER.info("listRoles = " + results);
              assertThat(results, containsString("roleName:" + test_role));

              // validate privileges
              results = "";
              client.grantServerPrivilege(ADMIN1, test_role, "server1", false);
              client.grantURIPrivilege(ADMIN1, test_role, "server1", test_uri);
              Set<TSentryPrivilege> sPerms = client.listAllPrivilegesByRoleName(ADMIN1, test_role);
              for (TSentryPrivilege sp : sPerms) {
                results += sp.toString() + "|";
              }
              LOGGER.info("listAllPrivilegesByRoleName = " + results);
              assertThat(results, containsString("serverName:server1"));
              assertThat(results, containsString("URI:" + test_uri));

              client.revokeURIPrivilege(ADMIN1, test_role, "server1", test_uri);
              client.revokeServerPrivilege(ADMIN1, test_role, "server1", false);

              results = "";
              Set<TSentryPrivilege> rPerms = client.listAllPrivilegesByRoleName(ADMIN1, test_role);
              for (TSentryPrivilege rp : rPerms) {
                results += rp.toString() + "|";
              }
              assertThat(results, not(containsString("URI:" + test_uri)));
              assertThat(results, not(containsString("serverName:server1")));

              client.dropRole(ADMIN1, test_role);

              results = "";
              Set<TSentryRole> removedRoles = client.listUserRoles(ADMIN1);
              for (TSentryRole role : removedRoles) {
                results += role.toString() + "|";
              }
              LOGGER.info("listUserRoles = " + results);
              assertThat(results, not(containsString(test_role)));

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
    while (!executor.awaitTermination(EXECUTOR_THREADS_MAX_WAIT_TIME, TimeUnit.SECONDS)) {
      LOGGER.info("Awaiting completion of threads.");
      Thread.sleep(1000); //millisecond
    }
    Throwable ex = state.getFirstException();
    assertFalse( ex == null ? "Test failed" : ex.toString(), state.failed);
    assertEquals(NUM_OF_TASKS, state.getNumSuccess());
  }
}
