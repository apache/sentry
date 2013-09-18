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

package org.apache.sentry.tests.e2e;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class TestServerConfiguration extends AbstractTestWithHiveServer {

  private Context context;
  private Map<String, String> properties;
  private PolicyFile policyFile;

  @Before
  public void setup() throws Exception {
    properties = Maps.newHashMap();
    policyFile = PolicyFile.createAdminOnServer1("admin1");

  }

  @After
  public void tearDown() throws Exception {
    if(context != null) {
      context.close();
    }
  }

  /**
   * hive.server2.enable.impersonation must be disabled
   */
  @Test
  public void testImpersonationIsDisabled() throws Exception {
    properties.put(HiveServerFactory.ACCESS_TESTING_MODE, "false");
    properties.put("hive.server2.enable.impersonation", "true");
    context = createContext(properties);
    policyFile.write(context.getPolicyFile());
    Connection connection = context.createConnection("admin1", "hive");
    Statement statement = context.createStatement(connection);
    try {
      statement.execute("create table test (a string)");
      Assert.fail("Expected SQLException");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
  }

  /**
   * hive.server2.authentication must be set to LDAP or KERBEROS
   */
  @Test
  public void testAuthenticationIsStrong() throws Exception {
    properties.put(HiveServerFactory.ACCESS_TESTING_MODE, "false");
    properties.put("hive.server2.authentication", "NONE");
    context = createContext(properties);
    policyFile.write(context.getPolicyFile());
    System.out.println(Files.toString(context.getPolicyFile(), Charsets.UTF_8));
    Connection connection = context.createConnection("admin1", "hive");
    Statement statement = context.createStatement(connection);
    try {
      statement.execute("create table test (a string)");
      Assert.fail("Expected SQLException");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
  }

  /**
   * Test removal of policy file
   */
  @Test
  public void testRemovalOfPolicyFile() throws Exception {
    context = createContext(properties);
    File policyFile = context.getPolicyFile();
    assertTrue("Could not delete " + policyFile, policyFile.delete());
    Connection connection = context.createConnection("admin1", "hive");
    Statement statement = context.createStatement(connection);
    try {
      statement.execute("create table test (a string)");
      Assert.fail("Expected SQLException");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
  }

  /**
   * Test corruption of policy file
   */
  @Test
  public void testCorruptionOfPolicyFile() throws Exception {
    context = createContext(properties);
    File policyFile = context.getPolicyFile();
    assertTrue("Could not delete " + policyFile, policyFile.delete());
    FileOutputStream out = new FileOutputStream(policyFile);
    out.write("this is not valid".getBytes(Charsets.UTF_8));
    out.close();
    Connection connection = context.createConnection("admin1", "hive");
    Statement statement = context.createStatement(connection);
    try {
      statement.execute("create table test (a string)");
      Assert.fail("Expected SQLException");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
  }

  @Test
  public void testAddDeleteDFSRestriction() throws Exception {
    context = createContext(properties);

    policyFile
        .addRolesToGroup("group1", "all_db1")
        .addRolesToGroup("group2", "select_tb1")
        .addPermissionsToRole("select_tb1", "server=server1->db=db_1->table=tbl_1->action=select")
        .addPermissionsToRole("all_db1", "server=server1->db=db_1")
        .addGroupsToUser("user1", "group1")
        .write(context.getPolicyFile());

    Connection connection = context.createConnection("user1", "password");
    Statement statement = context.createStatement(connection);

    // disallow external executables. The external.exec is set to false by session hooks
    context.assertAuthzException(statement, "ADD JAR /usr/lib/hive/lib/hbase.jar");
    context.assertAuthzException(statement, "ADD FILE /tmp/tt.py");
    context.assertAuthzException(statement, "DFS -ls");
    context.assertAuthzException(statement, "DELETE JAR /usr/lib/hive/lib/hbase.jar");
    context.assertAuthzException(statement, "DELETE FILE /tmp/tt.py");
    statement.close();
    connection.close();
  }

  /**
   * Test that the required access configs are set by session hook
   */
  @Test
  public void testAccessConfigRestrictions() throws Exception {
    context = createContext(properties);
    policyFile.write(context.getPolicyFile());

    String testUser = "user1";
    // verify the config is set correctly by session hook
    verifyConfig(testUser, ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HiveAuthzBindingSessionHook.SEMANTIC_HOOK);
    verifyConfig(testUser, ConfVars.PREEXECHOOKS.varname,
        HiveAuthzBindingSessionHook.PRE_EXEC_HOOK);
    verifyConfig(testUser, ConfVars.HIVE_EXEC_FILTER_HOOK.varname,
        HiveAuthzBindingSessionHook.FILTER_HOOK);
    verifyConfig(testUser, ConfVars.HIVE_EXTENDED_ENITITY_CAPTURE.varname, "true");
    verifyConfig(testUser, ConfVars.HIVE_SERVER2_AUTHZ_EXTERNAL_EXEC.varname, "false");
    verifyConfig(testUser, ConfVars.SCRATCHDIRPERMISSION.varname, HiveAuthzBindingSessionHook.SCRATCH_DIR_PERMISSIONS);
    verifyConfig(testUser, HiveConf.ConfVars.HIVE_CONF_RESTRICTED_LIST.varname,
        HiveAuthzBindingSessionHook.ACCESS_RESTRICT_LIST);
    verifyConfig(testUser, HiveAuthzConf.HIVE_ACCESS_SUBJECT_NAME, testUser);
   }

  private void verifyConfig(String userName, String confVar, String expectedValue) throws Exception {
    Connection connection = context.createConnection(userName, "password");
    Statement statement = context.createStatement(connection);
    statement.execute("set " + confVar);
    ResultSet res = statement.getResultSet();
    assertTrue(res.next());
    String configValue = res.getString(1);
    assertNotNull(configValue);
    String restrictListValues = (configValue.split("="))[1];
    assertFalse(restrictListValues.isEmpty());
    for (String restrictConfig: expectedValue.split(",")) {
      assertTrue(restrictListValues.toLowerCase().contains(restrictConfig.toLowerCase()));
    }

  }
}