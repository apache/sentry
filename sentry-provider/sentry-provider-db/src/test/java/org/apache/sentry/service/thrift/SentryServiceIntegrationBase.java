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

package org.apache.sentry.service.thrift;
import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.HAContext;
import org.apache.sentry.provider.db.service.thrift.SentryMiniKdcTestcase;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public abstract class SentryServiceIntegrationBase extends SentryMiniKdcTestcase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceIntegrationBase.class);

  static {
    if (System.getProperty("sun.security.krb5.debug", "").trim().isEmpty()) {
      System.setProperty("sun.security.krb5.debug", String.valueOf("true"));
    }
  }

  protected static final String SERVER_HOST = NetUtils.createSocketAddr("localhost:80").getAddress().getCanonicalHostName();
  protected static final String REALM = "EXAMPLE.COM";
  protected static final String SERVER_PRINCIPAL = "sentry/" + SERVER_HOST;
  protected static String SERVER_KERBEROS_NAME = "sentry/" + SERVER_HOST + "@" + REALM;
  protected static final String HTTP_PRINCIPAL = "HTTP/" + SERVER_HOST;
  protected static final String CLIENT_PRINCIPAL = "hive/" + SERVER_HOST;
  protected static final String CLIENT_KERBEROS_SHORT_NAME = "hive";
  protected static final String CLIENT_KERBEROS_NAME = CLIENT_KERBEROS_SHORT_NAME
      + "/" + SERVER_HOST + "@" + REALM;
  protected static final String ADMIN_USER = "admin_user";
  protected static final String ADMIN_GROUP = "admin_group";

  protected static SentryService server;
  protected SentryPolicyServiceClient client;
  protected static MiniKdc kdc;
  protected static File kdcWorkDir;
  protected static File dbDir;
  protected static File serverKeytab;
  protected static File httpKeytab;
  protected static File clientKeytab;
  protected static Subject clientSubject;
  protected static LoginContext clientLoginContext;
  protected static boolean kerberos;
  protected final static Configuration conf = new Configuration(false);
  protected PolicyFile policyFile;
  protected File policyFilePath;
  protected static Properties kdcConfOverlay = new Properties();

  protected static boolean haEnabled = false;
  protected static final String ZK_SERVER_PRINCIPAL = "zookeeper/" + SERVER_HOST;
  protected static TestingServer zkServer;

  private static File ZKKeytabFile;

  protected static boolean webServerEnabled = false;
  protected static int webServerPort = ServerConfig.SENTRY_WEB_PORT_DEFAULT;
  protected static boolean webSecurity = false;

  protected static boolean pooled = false;

  @BeforeClass
  public static void setup() throws Exception {
    kerberos = true;
    pooled = true;
    beforeSetup();
    setupConf();
    startSentryService();
    afterSetup();
  }

  private static void setupKdc() throws Exception {
    startMiniKdc(kdcConfOverlay);
  }

  public static void startSentryService() throws Exception {
    server.start();
    final long start = System.currentTimeMillis();
    while(!server.isRunning()) {
      Thread.sleep(1000);
      if(System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  public void stopSentryService() throws Exception {
    server.stop();
    Thread.sleep(30000);
  }

  public static void setupConf() throws Exception {
    if (kerberos) {
      setupKdc();
      kdc = getKdc();
      kdcWorkDir = getWorkDir();
      serverKeytab = new File(kdcWorkDir, "server.keytab");
      clientKeytab = new File(kdcWorkDir, "client.keytab");
      kdc.createPrincipal(serverKeytab, SERVER_PRINCIPAL);
      kdc.createPrincipal(clientKeytab, CLIENT_PRINCIPAL);
      conf.set(ServerConfig.PRINCIPAL, getServerKerberosName());
      conf.set(ServerConfig.KEY_TAB, serverKeytab.getPath());
      conf.set(ServerConfig.ALLOW_CONNECT, CLIENT_KERBEROS_SHORT_NAME);
      conf.set(ServerConfig.SERVER_HA_ZOOKEEPER_CLIENT_PRINCIPAL,
          getServerKerberosName());
      conf.set(ServerConfig.SERVER_HA_ZOOKEEPER_CLIENT_KEYTAB,
          serverKeytab.getPath());

      conf.set(ServerConfig.SECURITY_USE_UGI_TRANSPORT, "false");
      clientSubject = new Subject(false, Sets.newHashSet(
          new KerberosPrincipal(CLIENT_KERBEROS_NAME)), new HashSet<Object>(),
        new HashSet<Object>());
      clientLoginContext = new LoginContext("", clientSubject, null,
          KerberosConfiguration.createClientConfig(CLIENT_KERBEROS_NAME, clientKeytab));
      clientLoginContext.login();
      clientSubject = clientLoginContext.getSubject();
    } else {
      LOGGER.info("Stopped KDC");
      conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    }
    if (haEnabled) {
      zkServer = getZKServer();
      conf.set(ServerConfig.SENTRY_HA_ENABLED, "true");
      conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
      conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE, "sentry-test-case");
      if (kerberos) {
        conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_SECURITY, "true");
      }
    }
    if (webServerEnabled) {
      conf.set(ServerConfig.SENTRY_WEB_ENABLE, "true");
      conf.set(ServerConfig.SENTRY_WEB_PORT, String.valueOf(webServerPort));
      if (webSecurity) {
        httpKeytab = new File(kdcWorkDir, "http.keytab");
        kdc.createPrincipal(httpKeytab, HTTP_PRINCIPAL);
        conf.set(ServerConfig.SENTRY_WEB_SECURITY_TYPE,
            ServerConfig.SENTRY_WEB_SECURITY_TYPE_KERBEROS);
        conf.set(ServerConfig.SENTRY_WEB_SECURITY_PRINCIPAL, HTTP_PRINCIPAL);
        conf.set(ServerConfig.SENTRY_WEB_SECURITY_KEYTAB, httpKeytab.getPath());
      } else {
        conf.set(ServerConfig.SENTRY_WEB_SECURITY_TYPE,
            ServerConfig.SENTRY_WEB_SECURITY_TYPE_NONE);
      }
    } else {
      conf.set(ServerConfig.SENTRY_WEB_ENABLE, "false");
    }
    if (pooled) {
      conf.set(ClientConfig.SENTRY_POOL_ENABLED, "true");
    }
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP);
    conf.set(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    conf.set(ServerConfig.RPC_PORT, String.valueOf(0));
    dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    server = new SentryServiceFactory().create(conf);
    conf.set(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress().getHostName());
    conf.set(ClientConfig.SERVER_RPC_PORT, String.valueOf(server.getAddress().getPort()));
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
  }

  @Before
  public void before() throws Exception {
    policyFilePath = new File(dbDir, "local_policy_file.ini");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    policyFile = new PolicyFile();
    connectToSentryService();
  }

  @After
  public void after() throws SentryUserException {
    if (client != null) {
      Set<TSentryRole> tRoles = client.listRoles(ADMIN_USER);
      if (tRoles != null) {
        for (TSentryRole tRole : tRoles) {
          client.dropRole(ADMIN_USER, tRole.getRoleName());
        }
      }
      client.close();
    }
    policyFilePath.delete();
  }

  public void connectToSentryService() throws Exception {
    if (kerberos) {
      client = Subject.doAs(clientSubject, new PrivilegedExceptionAction<SentryPolicyServiceClient>() {
        @Override
        public SentryPolicyServiceClient run() throws Exception {
          return SentryServiceClientFactory.create(conf);
        }
      });
    } else {
      client = SentryServiceClientFactory.create(conf);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    beforeTeardown();

    if(clientLoginContext != null) {
      try {
        clientLoginContext.logout();
      } catch (Exception e) {
        LOGGER.warn("Error logging client out", e);
      }
    }
    if(server != null) {
      server.stop();
    }
    if (dbDir != null) {
      FileUtils.deleteQuietly(dbDir);
    }
    stopMiniKdc();
    afterTeardown();
  }

  public static String getServerKerberosName() {
    return SERVER_KERBEROS_NAME;
  }

  public static void beforeSetup() throws Exception {

  }
  public static void afterSetup() throws Exception {

  }
  public static void beforeTeardown() throws Exception {

  }
  public static void afterTeardown() throws Exception {

  }
  protected static void assertOK(TSentryResponseStatus resp) {
    assertStatus(Status.OK, resp);
  }

  protected static void assertStatus(Status status, TSentryResponseStatus resp) {
    if (resp.getValue() !=  status.getCode()) {
      String message = "Expected: " + status + ", Response: " + Status.fromCode(resp.getValue())
          + ", Code: " + resp.getValue() + ", Message: " + resp.getMessage();
      String stackTrace = Strings.nullToEmpty(resp.getStack()).trim();
      if (!stackTrace.isEmpty()) {
        message += ", StackTrace: " + stackTrace;
      }
      Assert.fail(message);
    }
  }

  protected void setLocalGroupMapping(String user, Set<String> groupSet) {
    for (String group : groupSet) {
      policyFile.addGroupsToUser(user, group);
    }
  }

  protected void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }

  protected static TestingServer getZKServer() throws Exception {
    if (!kerberos) {
      LOGGER.info("Creating a non-security ZooKeeper Server.");
      return new TestingServer();
    } else {
      LOGGER.info("Creating a security ZooKeeper Server.");
      // Not entirely sure exactly what "javax.security.auth.useSubjectCredsOnly=false" does, but it has something to do with
      // re-authenticating in cases where it otherwise wouldn't.  One of the sections on this page briefly mentions it:
      // http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

      // Setup KDC and principal
      kdc = getKdc();
      ZKKeytabFile = new File(kdcWorkDir, "test.keytab");
      kdc.createPrincipal(ZKKeytabFile, ZK_SERVER_PRINCIPAL);

      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
      System.setProperty("zookeeper.kerberos.removeHostFromPrincipal", "true");
      System.setProperty("zookeeper.kerberos.removeRealmFromPrincipal", "true");

      JaasConfiguration.addEntryForKeytab("Server", ZK_SERVER_PRINCIPAL, ZKKeytabFile.getAbsolutePath());
      // Here's where we add the "Client" to the jaas configuration, even though we'd like not to
      JaasConfiguration.addEntryForKeytab(HAContext.SENTRY_ZK_JAAS_NAME,
          SERVER_KERBEROS_NAME, serverKeytab.getAbsolutePath());
      javax.security.auth.login.Configuration.setConfiguration(JaasConfiguration.getInstance());

      System.setProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY, "Server");

      return new TestingServer();
    }

  }

  protected void runTestAsSubject(final TestOperation test) throws Exception {
    if (kerberos) {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          test.runTestAsSubject();
          return null;
        }});
    } else {
      test.runTestAsSubject();
    }
  }

  protected interface TestOperation {
    public void runTestAsSubject() throws Exception;
  }

}
