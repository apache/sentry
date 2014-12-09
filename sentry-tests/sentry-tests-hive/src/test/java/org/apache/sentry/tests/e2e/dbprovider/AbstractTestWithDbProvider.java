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

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithHiveServer;
import org.apache.sentry.tests.e2e.hive.Context;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.BeforeClass;

public abstract class AbstractTestWithDbProvider extends AbstractTestWithHiveServer {

  protected static final String SERVER_HOST = "localhost";

  private Map<String, String> properties = Maps.newHashMap();
  private File dbDir;
  private int sentryServerCount = 1;
  private List<SentryService> servers = new ArrayList<SentryService>(sentryServerCount);
  private Configuration conf;
  private PolicyFile policyFile;
  private File policyFilePath;
  protected Context context;

  protected boolean haEnabled;
  private TestingServer zkServer;

  @BeforeClass
  public static void setupTest() throws Exception {
  }

  @Override
  public Context createContext(Map<String, String> properties) throws Exception {
    this.properties = properties;
    return createContext();
  }

  public Context createContext() throws Exception {
    conf = new Configuration(false);
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
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
    properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    policyFilePath = new File(Files.createTempDir(), "sentry-policy-file.ini");
    properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    if (haEnabled) {
      zkServer = new TestingServer();
      zkServer.start();
      properties.put(ServerConfig.SENTRY_HA_ENABLED, "true");
      properties.put(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE, "sentry-test");
      properties.put(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    for (int i = 0; i < sentryServerCount; i++) {
      SentryService server = new SentryServiceFactory().create(new Configuration(conf));
      servers.add(server);
      properties.put(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress()
          .getHostName());
      properties.put(ClientConfig.SERVER_RPC_PORT,
          String.valueOf(server.getAddress().getPort()));
    }

    context = super.createContext(properties);
    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile(), policyFilePath);

    startSentryService();
    return context;
  }

  @After
  public void tearDown() throws Exception {
    for (SentryService server : servers) {
      if (server != null) {
        server.stop();
      }
    }
    if (context != null) {
      context.close();
    }
    if (dbDir != null) {
      FileUtils.deleteQuietly(dbDir);
    }
    if (zkServer != null) {
      zkServer.stop();
    }
  }

  protected void setupAdmin(Context context) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    statement.execute("CREATE ROLE admin_role");
    statement.execute("GRANT ALL ON SERVER "
        + HiveServerFactory.DEFAULT_AUTHZ_SERVER_NAME + " TO ROLE admin_role");
    statement.execute("GRANT ROLE admin_role TO GROUP " + ADMINGROUP);
    statement.close();
    connection.close();
  }

  private void startSentryService() throws Exception {
    for (SentryService server : servers) {
      server.start();
      final long start = System.currentTimeMillis();
      while(!server.isRunning()) {
        Thread.sleep(1000);
        if(System.currentTimeMillis() - start > 60000L) {
          throw new TimeoutException("Server did not start after 60 seconds");
        }
      }
    }
  }

  protected void shutdownAllSentryService() throws Exception {
    for (SentryService server : servers) {
      if (server != null) {
        server.stop();
      }
    }
    servers = null;
  }

  protected void startSentryService(int serverCount) throws Exception {
    Preconditions.checkArgument((serverCount > 0), "Server count should > 0.");
    servers = new ArrayList<SentryService>(serverCount);
    for (int i = 0; i < sentryServerCount; i++) {
      SentryService server = new SentryServiceFactory().create(new Configuration(conf));
      servers.add(server);
    }
    startSentryService();
  }

}
