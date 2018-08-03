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
package org.apache.sentry.tests.e2e.solr;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClient;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.api.common.ApiConstants;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.sentry.binding.solr.conf.SolrAuthzConf.AuthzConfVars;

public class TestSentryServer implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(TestSentryServer.class);

  protected static final String SERVER_HOST =
      NetUtils.createSocketAddr("localhost:80").getAddress().getCanonicalHostName();
  protected static final int PORT = 8038;
  protected static final String ADMIN_GROUP = "admin_group";
  static final String ADMIN_USER = "admin";

  private final Path dbDir;
  private final Path policyFilePath;
  private final Path sentrySitePath;
  private final Configuration clientConf;
  private final SentryService sentryService;

  public TestSentryServer(Path testDir, Map<String, Set<String>> groupsByUserName) throws Exception {
    this.dbDir = testDir.resolve("sentry_policy_db");
    this.policyFilePath = testDir.resolve("local_policy_file.ini");
    this.sentrySitePath = testDir.resolve("sentry-site.xml");
    this.sentryService = new SentryServiceFactory().create(getServerConfig());
    this.clientConf = getClientConfig();
    // Write sentry-site.xml
    this.clientConf.writeXml(new FileOutputStream(this.sentrySitePath.toFile()));
    // Write sentry policy file (for storing user-group mappings).
    PolicyFile policyFile = new PolicyFile();
    for (Map.Entry<String, Set<String>> userGroupMapping : groupsByUserName.entrySet()) {
      String userName = userGroupMapping.getKey();
      for (String groupName : userGroupMapping.getValue()) {
        log.info("Configuring user-group mapping with userName : {} group: {}", userName, groupName);
        policyFile.addGroupsToUser(userName, groupName);
      }
    }

    policyFile.write(this.policyFilePath.toFile());
  }

  public SentryService getSentryService() {
    return sentryService;
  }

  public Path getSentrySitePath() {
    return sentrySitePath;
  }

  public void startSentryService() throws Exception {
    sentryService.start();
    final long start = System.nanoTime();
    while(!sentryService.isRunning()) {
      Thread.sleep(1000);
      if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) > 60) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  public SentryGenericServiceClient connectToSentryService() throws Exception {
    return SentryGenericServiceClientFactory.create(this.clientConf);
  }

  @Override
  public void close() throws IOException {
    if (this.sentryService != null) {
      try {
        this.sentryService.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private Configuration getServerConfig () {
    Configuration conf = new Configuration(true);
    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP + ",solr");
    conf.set(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    conf.set(ServerConfig.RPC_PORT, String.valueOf(PORT));
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.toString());
    return conf;
  }

  private Configuration getClientConfig() {
    Configuration conf = new Configuration(true);
    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    conf.set(ApiConstants.ClientConfig.SERVER_RPC_ADDRESS, sentryService.getAddress().getHostName());
    conf.set(ApiConstants.ClientConfig.SERVER_RPC_PORT, String.valueOf(sentryService.getAddress().getPort()));
    conf.set(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(),
        SentryGenericProviderBackend.class.getName());
    conf.set("sentry.provider",
        LocalGroupResourceAuthorizationProvider.class.getName());
    conf.set("sentry.solr.provider.resource",
        policyFilePath.toString());
    return conf;
  }
}
