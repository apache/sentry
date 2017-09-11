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

/**
 * This class used to test the Sqoop integration with Sentry.
 * It will set up a miniSqoopCluster and Sentry service in a JVM process.
 */
package org.apache.sentry.tests.e2e.sqoop;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.sqoop.conf.SqoopAuthConf.AuthzConfVars;
import org.apache.sqoop.common.test.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

public class AbstractSqoopSentryTestBase {
  private static final String SERVER_HOST = NetUtils
      .createSocketAddr("localhost:80").getAddress().getCanonicalHostName();

  protected static final String COMPONENT = "sqoop";
  protected static final String ADMIN_USER = "sqoop";
  protected static final String ADMIN_GROUP = "sqoop";
  protected static final String ADMIN_ROLE  = "sqoop";
  protected static final String SQOOP_SERVER_NAME = "sqoopServer1";
  /** test users, groups and roles */
  protected static final String USER1 = StaticUserGroupRole.USER_1;
  protected static final String USER2 = StaticUserGroupRole.USER_2;
  protected static final String USER3 = StaticUserGroupRole.USER_3;
  protected static final String USER4 = StaticUserGroupRole.USER_4;
  protected static final String USER5 = StaticUserGroupRole.USER_5;

  protected static final String GROUP1 = StaticUserGroupRole.GROUP_1;
  protected static final String GROUP2 = StaticUserGroupRole.GROUP_2;
  protected static final String GROUP3 = StaticUserGroupRole.GROUP_3;
  protected static final String GROUP4 = StaticUserGroupRole.GROUP_4;
  protected static final String GROUP5 = StaticUserGroupRole.GROUP_5;

  protected static final String ROLE1 = StaticUserGroupRole.ROLE_1;
  protected static final String ROLE2 = StaticUserGroupRole.ROLE_2;
  protected static final String ROLE3 = StaticUserGroupRole.ROLE_3;
  protected static final String ROLE4 = StaticUserGroupRole.ROLE_4;
  protected static final String ROLE5 = StaticUserGroupRole.ROLE_5;

  protected static SentryService server;
  protected static TomcatSqoopRunner sqoopServerRunner;

  protected static File baseDir;
  protected static File sqoopDir;
  protected static File dbDir;
  protected static File policyFilePath;

  protected static PolicyFile policyFile;

  @BeforeClass
  public static void beforeTestEndToEnd() throws Exception {
    setupConf();
    startSentryService();
    setUserGroups();
    setAdminPrivilege();
    startSqoopWithSentryEnable();
  }

  @AfterClass
  public static void afterTestEndToEnd() throws Exception {
    if (server != null) {
      server.stop();
    }
    if (sqoopServerRunner != null) {
      sqoopServerRunner.stop();
    }

    FileUtils.deleteDirectory(baseDir);
  }

  public static void setupConf() throws Exception {
    baseDir = createTempDir();
    sqoopDir = new File(baseDir, "sqoop");
    dbDir = new File(baseDir, "sentry_policy_db");
    policyFilePath = new File(baseDir, "local_policy_file.ini");
    policyFile = new PolicyFile();

    /** set the configuratoion for Sentry Service */
    Configuration conf = new Configuration();

    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.ADMIN_GROUPS, Joiner.on(",").join(ADMIN_GROUP,
        UserGroupInformation.getLoginUser().getPrimaryGroupName()));
    conf.set(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    conf.set(ServerConfig.RPC_PORT, String.valueOf(NetworkUtils.findAvailablePort()));
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    server = SentryServiceFactory.create(conf);
  }

  public static File createTempDir() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    String baseName = "sqoop-e2e-";
    File tempDir = new File(baseDir, baseName + UUID.randomUUID().toString());
    if (tempDir.mkdir()) {
        return tempDir;
    }
    throw new IllegalStateException("Failed to create temp directory");
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

  public static void startSqoopWithSentryEnable() throws Exception {
    File sentrySitePath = new File(baseDir, "sentry-site.xml");
    getClientConfig().writeXml(new FileOutputStream(sentrySitePath));
    sqoopServerRunner = new TomcatSqoopRunner(sqoopDir.toString(), SQOOP_SERVER_NAME,
        sentrySitePath.toURI().toURL().toString());
    sqoopServerRunner.start();
  }

  private static Configuration getClientConfig() {
    Configuration conf = new Configuration();
    /** set the Sentry client configuration for Sqoop Service integration */
    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    conf.set(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress().getHostName());
    conf.set(ClientConfig.SERVER_RPC_PORT, String.valueOf(server.getAddress().getPort()));

    conf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        LocalGroupResourceAuthorizationProvider.class.getName());
    conf.set(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(),
        SentryGenericProviderBackend.class.getName());
    conf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), policyFilePath.getPath());
    conf.set(AuthzConfVars.AUTHZ_TESTING_MODE.getVar(), "true");
    return conf;
  }

  public static void setUserGroups() throws Exception {
    for (String user : StaticUserGroupRole.getUsers()) {
      Set<String> groups = StaticUserGroupRole.getGroups(user);
      policyFile.addGroupsToUser(user,
          groups.toArray(new String[groups.size()]));
    }
    policyFile.addGroupsToUser(ADMIN_USER, ADMIN_GROUP);
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    policyFile.addGroupsToUser(loginUser.getShortUserName(), loginUser.getGroupNames());
    policyFile.write(policyFilePath);
  }

  public static void setAdminPrivilege() throws Exception {
    try (SentryGenericServiceClient sentryClient =
                 SentryGenericServiceClientFactory.create(getClientConfig())){
      // grant all privilege to admin user
      sentryClient.createRoleIfNotExist(ADMIN_USER, ADMIN_ROLE, COMPONENT);
      sentryClient.addRoleToGroups(ADMIN_USER, ADMIN_ROLE, COMPONENT, Sets.newHashSet(ADMIN_GROUP));
      sentryClient.grantPrivilege(ADMIN_USER, ADMIN_ROLE, COMPONENT,
          new TSentryPrivilege(COMPONENT, SQOOP_SERVER_NAME, new ArrayList<TAuthorizable>(),
              SqoopActionConstant.ALL));
    }
  }

  public static void assertCausedMessage(Exception e, String message) {
    assertTrue(e.getCause().getMessage().contains(message));
  }
}
