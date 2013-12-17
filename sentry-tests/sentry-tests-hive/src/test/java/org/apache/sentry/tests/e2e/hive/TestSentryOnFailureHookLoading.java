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

import com.google.common.io.Resources;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestSentryOnFailureHookLoading extends AbstractTestWithHiveServer {

  private Context context;
  private PolicyFile policyFile;

  Map<String, String > testProperties;
  private static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  @Before
  public void setup() throws Exception {
    testProperties = new HashMap<String, String>();
    testProperties.put(HiveAuthzConf.AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(),
        DummySentryOnFailureHook.class.getName());
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  @After
  public void teardown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /* Admin creates database DB_2
   * user1 tries to drop DB_2, but it has permissions for DB_1.
   */
  @Test
  public void testOnFailureHookLoading() throws Exception {

    // Do not run this test if run with external HiveServer2
    // This test checks for a static member, which will not
    // be set if HiveServer2 and the test run in different JVMs
    String hiveServer2Type = System.getProperty(
        HiveServerFactory.HIVESERVER2_TYPE);
    if (hiveServer2Type != null &&
        HiveServerFactory.HiveServer2Type.valueOf(hiveServer2Type.trim()) !=
        HiveServerFactory.HiveServer2Type.InternalHiveServer2) {
      return;
    }

    context = createContext(testProperties);

    File dataDir = context.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "all_db1", "load_data")
        .addPermissionsToRole("all_db1", "server=server1->db=DB_1")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("CREATE DATABASE DB_2");
    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    //negative test case: user can't drop another user's database
    assertFalse(DummySentryOnFailureHook.invoked);
      try {
      statement.execute("DROP DATABASE DB_2 CASCADE");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      assertTrue(DummySentryOnFailureHook.invoked);
    }

    statement.close();
    connection.close();

    //test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.execute("DROP DATABASE DB_2 CASCADE");
    statement.close();
    connection.close();
    context.close();
  }
}
