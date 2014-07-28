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
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class TestServerConfiguration extends AbstractTestWithHiveServer {

  private Context context;
  private Map<String, String> properties;
  private PolicyFile policyFile;

  @Before
  public void setup() throws Exception {
    properties = Maps.newHashMap();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);

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
    verifyInvalidConfigurationException();
  }

  /**
   * hive.server2.authentication must be set to LDAP or KERBEROS
   */
  @Test
  public void testAuthenticationIsStrong() throws Exception {
    properties.put(HiveServerFactory.ACCESS_TESTING_MODE, "false");
    properties.put("hive.server2.authentication", "NONE");
    verifyInvalidConfigurationException();
  }

  private void verifyInvalidConfigurationException() throws Exception{
    context = createContext(properties);
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    try {
      statement.execute("create table test (a string)");
      Assert.fail("Expected SQLException");
    } catch (SQLException e) {
      context.verifyInvalidConfigurationException(e);
    }
  }

  /**
   * Test removal of policy file
   */
  @Test
  public void testRemovalOfPolicyFile() throws Exception {
    context = createContext(properties);
    Connection connection = context.createConnection(ADMIN1);
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
    FileOutputStream out = new FileOutputStream(policyFile);
    out.write("this is not valid".getBytes(Charsets.UTF_8));
    out.close();
    Connection connection = context.createConnection(ADMIN1);
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
        .addRolesToGroup(USERGROUP1, "all_db1")
        .addRolesToGroup(USERGROUP2, "select_tb1")
        .addPermissionsToRole("select_tb1", "server=server1->db=db_1->table=tbl_1->action=select")
        .addPermissionsToRole("all_db1", "server=server1->db=db_1")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    Connection connection = context.createConnection(USER1_1);
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
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    String testUser = USER1_1;
    // verify the config is set correctly by session hook
    verifyConfig(testUser, ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HiveAuthzBindingSessionHook.SEMANTIC_HOOK);
    verifyConfig(testUser, ConfVars.PREEXECHOOKS.varname,
        HiveAuthzBindingSessionHook.PRE_EXEC_HOOK);
    verifyConfig(testUser, ConfVars.HIVE_SECURITY_COMMAND_WHITELIST.varname, "set");
    verifyConfig(testUser, ConfVars.SCRATCHDIRPERMISSION.varname, HiveAuthzBindingSessionHook.SCRATCH_DIR_PERMISSIONS);
    verifyConfig(testUser, HiveConf.ConfVars.HIVE_CONF_RESTRICTED_LIST.varname,
        HiveAuthzBindingSessionHook.ACCESS_RESTRICT_LIST);
    verifyConfig(testUser, HiveAuthzConf.HIVE_ACCESS_SUBJECT_NAME, testUser);
   }

  private void verifyConfig(String userName, String confVar, String expectedValue) throws Exception {
    Connection connection = context.createConnection(userName);
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

  /**
   * Test access to default DB with explicit privilege requirement
   * Admin should be able to run use default with server level access
   * User with db level access should be able to run use default
   * User with table level access should be able to run use default
   * User with no access to default db objects, should NOT be able run use default
   * @throws Exception
   */
  @Test
  public void testDefaultDbRestrictivePrivilege() throws Exception {
    properties.put(HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "true");
    context = createContext(properties);

    policyFile
        .addRolesToGroup(USERGROUP1, "all_default")
        .addRolesToGroup(USERGROUP2, "select_default")
        .addRolesToGroup(USERGROUP3, "all_db1")
        .addPermissionsToRole("all_default", "server=server1->db=default")
        .addPermissionsToRole("select_default", "server=server1->db=default->table=tab_2->action=select")
        .addPermissionsToRole("all_db1", "server=server1->db=DB_1")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    try {
      // user3 doesn't have any implicit permission for default
      statement.execute("use default");
      assertFalse("user3 shouldn't be able switch to default", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();
  }

}
