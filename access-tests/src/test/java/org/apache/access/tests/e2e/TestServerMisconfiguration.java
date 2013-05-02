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

package org.apache.access.tests.e2e;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.access.tests.e2e.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class TestServerMisconfiguration extends AbstractTestWithHiveServer {

  private Context context;
  private Map<String, String> properties;
  private PolicyFile policyFile;

  @Before
  public void setup() throws Exception {
    properties = Maps.newHashMap();
    policyFile = PolicyFile.createAdminOnServer1("hive");

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
    Connection connection = context.createConnection("hive", "hive");
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
    Connection connection = context.createConnection("hive", "hive");
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
    Connection connection = context.createConnection("hive", "hive");
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
    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);
    try {
      statement.execute("create table test (a string)");
      Assert.fail("Expected SQLException");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
  }

}