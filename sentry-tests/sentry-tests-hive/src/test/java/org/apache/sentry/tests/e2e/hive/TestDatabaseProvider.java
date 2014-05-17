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

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

public class TestDatabaseProvider extends AbstractTestWithHiveServer {
  protected static final String SERVER_HOST = "localhost";

  private Context context;
  private Map<String, String> properties;
  private File dbDir;
  private SentryService server;
  private Configuration conf;
  private PolicyFile policyFile;

  @Before
  public void setup() throws Exception {
    properties = Maps.newHashMap();
    conf = new Configuration(false);
    policyFile = new PolicyFile();
    properties.put(HiveServerFactory.AUTHZ_PROVIDER_BACKEND, SimpleDBProviderBackend.class.getName());
    properties.put(ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
      SentryHiveAuthorizationTaskFactoryImpl.class.getName());
    properties.put(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    properties.put(ServerConfig.ADMIN_GROUPS, ADMINGROUP);
    properties.put(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    properties.put(ServerConfig.RPC_PORT, String.valueOf(0));
    dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    properties.put(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    properties.put(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    server = new SentryServiceFactory().create(conf);
    properties.put(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress().getHostString());
    properties.put(ClientConfig.SERVER_RPC_PORT, String.valueOf(server.getAddress().getPort()));
    startSentryService();
    context = createContext(properties);
  }

  @After
  public void tearDown() throws Exception {
    if(context != null) {
      context.close();
    }
    if (dbDir != null) {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  private void startSentryService() throws Exception {
    server.start();
    final long start = System.currentTimeMillis();
    while(!server.isRunning()) {
      Thread.sleep(1000);
      if(System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  /**
   * This test is only used for manual testing of beeline with Sentry Service
   * @throws Exception
   */
  @Ignore
  @Test
  public void beelineTest() throws Exception{
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    while(true) {}
  }

  @Test
  public void testBasic() throws Exception {
    policyFile
      .setUserGroupMapping(StaticUserGroup.getStaticMapping())
      .write(context.getPolicyFile());
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE admin_role");
    statement.execute("GRANT ALL ON DATABASE default TO ROLE admin_role");
    statement.execute("GRANT ROLE admin_role TO GROUP " + ADMINGROUP);
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("CREATE ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    context.assertSentryServiceAccessDenied(statement, "CREATE ROLE r2");
    // test default of ALL
    statement.execute("SELECT * FROM t1");
    // test a specific role
    statement.execute("SET ROLE user_role");
    statement.execute("SELECT * FROM t1");
    // test NONE
    statement.execute("SET ROLE NONE");
    context.assertAuthzException(statement, "SELECT * FROM t1");
    // test ALL
    statement.execute("SET ROLE ALL");
    statement.execute("SELECT * FROM t1");
    statement.close();
    connection.close();
  }

  /**
   * SHOW ROLES
   * @throws Exception
   */
  @Test
  public void testShowRoles() throws Exception {
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("CREATE ROLE role2");
    ResultSet resultSet = statement.executeQuery("SHOW ROLES");
    Set<String> roles = new HashSet<String>();
    while ( resultSet.next()) {
      roles.add(resultSet.getString(1));
    }
    assertThat(roles.size(), is(new Integer(2)));
    assertTrue(roles.contains("role1"));
    assertTrue(roles.contains("role2"));
    statement.close();
    connection.close();
  }

  /**
   * SHOW ROLE GRANT GROUP groupName
   * @throws Exception
   */
  @Test
  public void testShowRolesByGroup() throws Exception {
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("CREATE ROLE role2");
    statement.execute("CREATE ROLE role3");
    statement.execute("GRANT ROLE role1 to GROUP " + ADMINGROUP);

    ResultSet resultSet = statement.executeQuery("SHOW ROLE GRANT GROUP " + ADMINGROUP);
    Set<String> roles = new HashSet<String>();
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("role1"));
      assertThat(resultSet.getBoolean(2), is(new Boolean("False")));
      //Create time is not tested
      //assertThat(resultSet.getLong(3), is(new Long(0)));
      assertThat(resultSet.getString(4), equalToIgnoringCase(ADMIN1));
    }
    statement.close();
    connection.close();
  }

  /**
   * SHOW GRANT ROLE roleName
   * @throws Exception
   */
  @Test
  public void testShowPrivilegesByRole() throws Exception {
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("CREATE ROLE role2");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE role1");

    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1");
    while ( resultSet.next()) {
      assertThat(resultSet.getString(1), equalToIgnoringCase("default"));
      assertThat(resultSet.getString(2), equalToIgnoringCase("t1"));
      assertThat(resultSet.getString(3), equalToIgnoringCase(""));//partition
      assertThat(resultSet.getString(4), equalToIgnoringCase(""));//column
      assertThat(resultSet.getString(5), equalToIgnoringCase("role1"));//principalName
      assertThat(resultSet.getString(6), equalToIgnoringCase("role"));//principalType
      assertThat(resultSet.getString(7), equalToIgnoringCase("select"));
      assertThat(resultSet.getBoolean(8), is(new Boolean("False")));//grantOption
      //Create time is not tested
      //assertThat(resultSet.getLong(9), is(new Long(0)));
      assertThat(resultSet.getString(10), equalToIgnoringCase(ADMIN1));//grantor
    }
    statement.close();
    connection.close();
  }

  /**
   * SHOW GRANT ROLE roleName ON OBJECT PRIVILEGE not supported yet
   * @throws Exception
   */
  @Test
  public void testShowPrivilegesByRoleAndObject() throws Exception {
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE role1");

    try {
      ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE role1 ON TABLE tab1");
      assertTrue("Expected an exception", false);
    } catch(SQLException e) {
      statement.close();
      connection.close();
    }
  }
  /**
   * SHOW CURRENT ROLE not supported yet
   * @throws Exception
   */
  @Ignore
  @Test
  public void testShowCurrentRole() throws Exception {
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE role1");
    statement.execute("SET ROLE role1");

    try {
      ResultSet resultSet = statement.executeQuery("SHOW CURRENT ROLE");
      assertTrue("Expected an exception", false);
    } catch(SQLException e) {
      statement.close();
      connection.close();
    }
  }
}
