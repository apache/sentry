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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.sentry.binding.hive.authz.SentryConfigTool;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.provider.file.PolicyFile;

import com.google.common.io.Resources;

public class TestConfigTool extends AbstractTestWithStaticConfiguration {
  private static final String DB2_POLICY_FILE = "db2-policy-file.ini";
  private static String prefix;

  private PolicyFile policyFile;
  private SentryConfigTool configTool;

  @Before
  public void setup() throws Exception {
    context = createContext();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
    configTool = new SentryConfigTool();
    String hiveServer2 = System.getProperty("sentry.e2etest.hiveServer2Type",
        "InternalHiveServer2");
    String policyOnHDFS = System.getProperty(
        "sentry.e2etest.hive.policyOnHDFS", "true");
    if (policyOnHDFS.trim().equalsIgnoreCase("true")
        && (hiveServer2.equals("UnmanagedHiveServer2"))) {
      String policyLocation = System.getProperty(
          "sentry.e2etest.hive.policy.location", "/user/hive/sentry");
      prefix = "hdfs://" + policyLocation + "/";
    } else {
      prefix = "file://" + context.getPolicyFile().getParent() + "/";
    }

  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /**
   * Verify errors/warnings from malformed policy file
   * @throws Exception
   */
  @Test
  public void testInvalidPolicy() throws Exception {
    // policy file, missing insert_tab2 and select_tab3 role definition
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "insert_tab2")
        .addRolesToGroup(USERGROUP2, "select_tab3")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=db1->table=tab1->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(context.getPolicyFile());

    configTool.setPolicyFile(context.getPolicyFile().getPath());
    configTool.setupConfig();
    try {
      configTool.getSentryProvider().validateResource(true);
      fail("Policy validation should fail for malformed policy");
    } catch (SentryConfigurationException e) {
      assertTrue(e
          .getConfigWarnings()
          .get(0)
          .contains(
              "Role select_tab3 for group " + USERGROUP2 + " does not exist"));
      assertTrue(e
          .getConfigWarnings()
          .get(1)
          .contains(
              "Role insert_tab2 for group " + USERGROUP1 + " does not exist"));
    }
  }

  /**
   * Verify errors/warnings from malformed policy file with per-DB policy
   * @throws Exception
   */
  @Test
  public void testInvalidPerDbPolicy() throws Exception {
    PolicyFile db2PolicyFile = new PolicyFile();
    File db2PolicyFileHandle = new File(context.getPolicyFile().getParent(),
        DB2_POLICY_FILE);
    // invalid db2 policy file with missing roles
    db2PolicyFile
        .addRolesToGroup(USERGROUP2, "select_tbl2", "insert_db2_tab2")
        .addPermissionsToRole("select_tbl2",
            "server=server1->db=db2->table=tbl2->action=select")
        .write(db2PolicyFileHandle);

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tbl1")
        .addRolesToGroup(USERGROUP2, "select_tbl3")
        .addPermissionsToRole("select_tbl1",
            "server=server1->db=db1->table=tbl1->action=select")
        .addDatabase("db2", prefix + db2PolicyFileHandle.getName())
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    configTool.setPolicyFile(context.getPolicyFile().getPath());
    configTool.setupConfig();
    try {
      configTool.getSentryProvider().validateResource(true);
      fail("Policy validation should fail for malformed policy");
    } catch (SentryConfigurationException e) {
      assertTrue(e
          .getConfigWarnings()
          .get(0)
          .contains(
              "Role select_tbl3 for group " + USERGROUP2 + " does not exist"));
      assertTrue(e.getConfigWarnings().get(0)
          .contains(context.getPolicyFile().getName()));
      assertTrue(e
          .getConfigWarnings()
          .get(1)
          .contains(
              "Role insert_db2_tab2 for group " + USERGROUP2
                  + " does not exist"));
      assertTrue(e.getConfigWarnings().get(1)
          .contains(db2PolicyFileHandle.getName()));
    }
  }

  /**
   * Validate user permissions listing
   * @throws Exception
   */
  @Test
  public void testUserPermissions() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "insert_tab2")
        .addRolesToGroup(USERGROUP2, "select_tab3")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=db1->table=tab1->action=select")
        .addPermissionsToRole("insert_tab2",
            "server=server1->db=db1->table=tab2->action=insert")
        .addPermissionsToRole("select_tab3",
            "server=server1->db=db1->table=tab3->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(context.getPolicyFile());

    configTool.setPolicyFile(context.getPolicyFile().getPath());
    configTool.setupConfig();
    configTool.validatePolicy();

    Set<String> permList = configTool.getSentryProvider()
        .listPermissionsForSubject(new Subject(USER1_1));
    assertTrue(permList
        .contains("server=server1->db=db1->table=tab1->action=select"));
    assertTrue(permList
        .contains("server=server1->db=db1->table=tab2->action=insert"));

    permList = configTool.getSentryProvider().listPermissionsForSubject(
        new Subject(USER2_1));
    assertTrue(permList
        .contains("server=server1->db=db1->table=tab3->action=select"));

    permList = configTool.getSentryProvider().listPermissionsForSubject(
        new Subject(ADMIN1));
    assertTrue(permList.contains("server=server1"));
  }

  /***
   * Verify the mock compilation config setting forces query to abort
   * @throws Exception
   */
  @Test
  public void testMockCompilation() throws Exception {
    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(context.getPolicyFile());
    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP TABLE IF EXISTS tab1");
    statement.execute("CREATE TABLE tab1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("SELECT * FROM tab1");

    statement.execute("SET " + HiveAuthzConf.HIVE_SENTRY_MOCK_COMPILATION
        + "=true");
    try {
      statement.execute("SELECT * FROM tab1");
      fail("Query should fail with mock error config enabled");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains(HiveAuthzConf.HIVE_SENTRY_MOCK_ERROR));
    }
    statement.close();

  }

  /**
   * verify missing permissions for query using remote query validation
   * @throws Exception
   */
  @Test
  public void testQueryPermissions() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1", "insert_tab2")
        .addRolesToGroup(USERGROUP2, "select_tab3")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=default->table=tab1->action=select")
        .addPermissionsToRole("insert_tab2",
            "server=server1->db=default->table=tab2->action=insert")
        .addPermissionsToRole("select_tab3",
            "server=server1->db=default->table=tab3->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(context.getPolicyFile());

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP TABLE IF EXISTS tab1");
    statement.execute("DROP TABLE IF EXISTS tab2");
    statement.execute("DROP TABLE IF EXISTS tab3");
    statement.execute("CREATE TABLE tab1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("CREATE TABLE tab2(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("CREATE TABLE tab3(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.close();
    connection.close();

    configTool.setPolicyFile(context.getPolicyFile().getPath());
    configTool.setJdbcURL(context.getConnectionURL());
    configTool.setUser(USER1_1);
    configTool.setupConfig();
    ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();

    // user1_1 can query table1
    configTool.setUser(USER1_1);
    configTool.verifyRemoteQuery("SELECT COUNT(*) FROM tab1");

    // user1_1 can't select from tab3
    try {
      System.setOut(new PrintStream(errBuffer));
      configTool.setUser(USER1_1);
      configTool.verifyRemoteQuery("SELECT COUNT(*) FROM tab3");
      fail("Query should have failed with insufficient perms");
    } catch (SQLException e) {
      assertTrue(errBuffer.toString().contains(
          "Server=server1->Db=default->Table=tab3->action=select"));
      errBuffer.flush();
    }

    // user2_1 can select from tab3, but can't insert into tab2
    try {
      configTool.setUser(USER2_1);
      configTool
          .verifyRemoteQuery("INSERT OVERWRITE TABLE tab2 SELECT * FROM tab3");
      fail("Query should have failed with insufficient perms");
    } catch (SQLException e) {
      assertTrue(errBuffer.toString().contains(
          "Server=server1->Db=default->Table=tab2->action=insert"));
    }

  }
}
