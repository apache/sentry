/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.ha;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.HAClientInvocationHandler;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

/**
 * End2End tests with Sentry service HA enabled.
 */
public class TestHaEnd2End extends AbstractTestWithStaticConfiguration {

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataFile;
  private PolicyFile policyFile;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    enableSentryHA = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  /**
   * Basic test with two Sentry service running.
   * @throws Exception
   */
  @Test
  public void testBasic() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
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

    // test ALL
    statement.execute("SET ROLE ALL");
    statement.execute("SELECT * FROM t1");
    statement.close();
    connection.close();

    // cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP ROLE user_role");
    statement.close();
    connection.close();
  }

  /**
   * Test service failover. Run Sentry operations with shutting down one or more
   * of the services.
   * @throws Exception
   */
  @Test
  public void testFailover() throws Exception {
    String roleName1 = "test_role_1";
    String roleName2 = "test_role_2";
    String roleName3 = "test_role_3";

    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);

    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    // access the new databases
    adminStmt.execute("use " + DB1);

    // stop server1 and verify sentry continues to work
    getSentrySrv().stop(0);
    adminStmt.execute("CREATE ROLE " + roleName1);
    verifyRoleExists(adminStmt, roleName1);

    // restart server1 and stop server2
    getSentrySrv().start(0);
    getSentrySrv().stop(1);
    adminStmt.execute("CREATE ROLE " + roleName2);
    verifyRoleExists(adminStmt, roleName2);

    // stop both servers and verify it fails
    getSentrySrv().stop(0);
    getSentrySrv().stop(1);
    context.assertAuthzExecHookException(adminStmt, "CREATE ROLE " + roleName3,
        HAClientInvocationHandler.SENTRY_HA_ERROR_MESSAGE);

    getSentrySrv().start(0);
    getSentrySrv().start(1);
    adminStmt.execute("CREATE ROLE " + roleName3);
    verifyRoleExists(adminStmt, roleName3);

    // cleanup

    dropDb(ADMIN1, DB1);
    adminStmt.execute("DROP ROLE " + roleName1);
    adminStmt.execute("DROP ROLE " + roleName2);
    adminStmt.execute("DROP ROLE " + roleName3);
    adminStmt.close();
    adminCon.close();

  }

  private void verifyRoleExists(Statement statement, String roleName)
      throws Exception {
    ResultSet resultSet = null;
    try {
      resultSet = statement.executeQuery("SHOW ROLES ");
      while (resultSet.next()) {
        if (roleName.equalsIgnoreCase(resultSet.getString(1))) {
          return;
        }
      }
      throw new Exception("Role " + roleName + " does not exist");
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }
}
