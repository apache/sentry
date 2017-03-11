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

package org.apache.sentry.tests.e2e.kafka;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.model.kafka.Cluster;
import org.apache.sentry.core.model.kafka.KafkaActionConstant;
import org.apache.sentry.core.model.kafka.Host;
import org.apache.sentry.kafka.conf.KafkaAuthConf;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;


/**
 * This class used to test the Kafka integration with Sentry.
 */
public class AbstractKafkaSentryTestBase {

  protected static final String COMPONENT = "kafka";
  protected static final String ADMIN_USER = "kafka";
  protected static final String ADMIN_GROUP = "group_kafka";
  protected static final String ADMIN_ROLE  = "role_kafka";
  private static final long CACHE_TTL_MS = 1;
  private static final int SAFETY_FACTOR = 2; // Sleep for specified times of expected time for an operation to complete.

  protected static SentryService sentryServer;
  protected static File sentrySitePath;

  protected static File baseDir;
  protected static File dbDir;
  protected static File policyFilePath;

  protected static PolicyFile policyFile;

  protected static String bootstrapServers = null;
  protected static KafkaTestServer kafkaServer = null;

  @BeforeClass
  public static void beforeTestEndToEnd() throws Exception {
    setupConf();
    startSentryServer();
    setUserGroups();
    setAdminPrivilege();
    startKafkaServer();
  }

  @AfterClass
  public static void afterTestEndToEnd() throws Exception {
    stopSentryServer();
    stopKafkaServer();
  }

  private static void stopKafkaServer() {
    if (kafkaServer != null) {
      kafkaServer.shutdown();
      kafkaServer = null;
    }
  }

  private static void stopSentryServer() throws Exception {
    if (sentryServer != null) {
      sentryServer.stop();
      sentryServer = null;
    }

    FileUtils.deleteDirectory(baseDir);
  }

  public static void setupConf() throws Exception {
    baseDir = createTempDir();
    sentrySitePath = new File(baseDir, "sentry-site.xml");
    dbDir = new File(baseDir, "sentry_policy_db");
    policyFilePath = new File(baseDir, "local_policy_file.ini");
    policyFile = new PolicyFile();

    /** set the configuratoion for Sentry Service */
    Configuration conf = new Configuration();

    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.ADMIN_GROUPS, Joiner.on(",").join(ADMIN_GROUP,
        UserGroupInformation.getLoginUser().getPrimaryGroupName()));
    conf.set(ServerConfig.RPC_PORT, String.valueOf(TestUtils.getFreePort()));
    conf.set(ServerConfig.RPC_ADDRESS, NetUtils.createSocketAddr(
            InetAddress.getLocalHost().getHostAddress() + ":" + conf.get(ServerConfig.RPC_PORT))
            .getAddress().getCanonicalHostName());
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    sentryServer = new SentryServiceFactory().create(conf);
  }

  public static File createTempDir() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    String baseName = "kafka-e2e-";
    File tempDir = new File(baseDir, baseName + UUID.randomUUID().toString());
    if (tempDir.mkdir()) {
        return tempDir;
    }
    throw new IllegalStateException("Failed to create temp directory");
  }

  public static void startSentryServer() throws Exception {
    sentryServer.start();
    final long start = System.currentTimeMillis();
    while(!sentryServer.isRunning()) {
      Thread.sleep(1000);
      if(System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  public static void setUserGroups() throws Exception {
    for (String user : StaticUserGroupRole.getUsers()) {
      Set<String> groups = StaticUserGroupRole.getGroups(user);
      policyFile.addGroupsToUser(user,
          groups.toArray(new String[groups.size()]));
    }
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    policyFile.addGroupsToUser(loginUser.getShortUserName(), loginUser.getGroupNames());

    policyFile.write(policyFilePath);
  }

  public static void setAdminPrivilege() throws Exception {
    SentryGenericServiceClient sentryClient = null;
    try {
      /** grant all privilege to admin user */
      sentryClient = getSentryClient();
      sentryClient.createRoleIfNotExist(ADMIN_USER, ADMIN_ROLE, COMPONENT);
      sentryClient.addRoleToGroups(ADMIN_USER, ADMIN_ROLE, COMPONENT, Sets.newHashSet(ADMIN_GROUP));
      final ArrayList<TAuthorizable> authorizables = new ArrayList<TAuthorizable>();
      Host host = new Host(InetAddress.getLocalHost().getHostName());
      authorizables.add(new TAuthorizable(host.getTypeName(), host.getName()));
      Cluster cluster = new Cluster();
      authorizables.add(new TAuthorizable(cluster.getTypeName(), cluster.getName()));
      sentryClient.grantPrivilege(ADMIN_USER, ADMIN_ROLE, COMPONENT,
          new TSentryPrivilege(COMPONENT, "kafka", authorizables,
              KafkaActionConstant.ALL));
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  protected static SentryGenericServiceClient getSentryClient() throws Exception {
    return SentryGenericServiceClientFactory.create(getClientConfig());
  }

  public static void assertCausedMessage(Exception e, String message) {
    if (e.getCause() != null) {
      assertTrue("Expected message: " + message + ", but got: " + e.getCause().getMessage(), e.getCause().getMessage().contains(message));
    } else {
      assertTrue("Expected message: " + message + ", but got: " + e.getMessage(), e.getMessage().contains(message));
    }
  }

  private static Configuration getClientConfig() {
    Configuration conf = new Configuration();
    /** set the Sentry client configuration for Kafka Service integration */
    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    conf.set(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.getAddress().getHostName());
    conf.setInt(ClientConfig.SERVER_RPC_PORT, sentryServer.getAddress().getPort());

    conf.set(KafkaAuthConf.AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        LocalGroupResourceAuthorizationProvider.class.getName());
    conf.set(KafkaAuthConf.AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(),
        SentryGenericProviderBackend.class.getName());
    conf.set(KafkaAuthConf.AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), policyFilePath.getPath());
    conf.setBoolean(ClientConfig.ENABLE_CACHING, true);
    conf.setLong(ClientConfig.CACHE_TTL_MS, CACHE_TTL_MS);
    return conf;
  }

  private static void startKafkaServer() throws Exception {
    // Workaround for SentryKafkaAuthorizer to be added to classpath
    Class.forName("org.apache.sentry.kafka.authorizer.SentryKafkaAuthorizer");
    getClientConfig().writeXml(new FileOutputStream(sentrySitePath));

    kafkaServer = new KafkaTestServer(sentrySitePath);
    kafkaServer.start();
    bootstrapServers = kafkaServer.getBootstrapServers();
  }

  static void sleepIfCachingEnabled() throws InterruptedException {
    if (getClientConfig().getBoolean(ClientConfig.ENABLE_CACHING, false)) {
      Thread.sleep(CACHE_TTL_MS * SAFETY_FACTOR);
    }
  }
}
