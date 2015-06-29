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

package org.apache.sentry.provider.db.service.persistent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.sentry.service.thrift.JaasConfiguration;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Stores the HA related context
 */
public class HAContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(HAContext.class);
  private static HAContext serverHAContext = null;
  private static boolean aclChecked = false;

  public final static String SENTRY_SERVICE_REGISTER_NAMESPACE = "sentry-service";
  public static final String SENTRY_ZK_JAAS_NAME = "SentryClient";
  private final String zookeeperQuorum;
  private final int retriesMaxCount;
  private final int sleepMsBetweenRetries;
  private final String namespace;

  private final boolean zkSecure;
  private List<ACL> saslACL;

  private final CuratorFramework curatorFramework;
  private final RetryPolicy retryPolicy;

  protected HAContext(Configuration conf) throws Exception {
    this.zookeeperQuorum = conf.get(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM,
        ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM_DEFAULT);
    this.retriesMaxCount = conf.getInt(ServerConfig.SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT,
        ServerConfig.SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT_DEFAULT);
    this.sleepMsBetweenRetries = conf.getInt(ServerConfig.SENTRY_HA_ZOOKEEPER_SLEEP_BETWEEN_RETRIES_MS,
        ServerConfig.SENTRY_HA_ZOOKEEPER_SLEEP_BETWEEN_RETRIES_MS_DEFAULT);
    this.namespace = conf.get(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE,
        ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT);
    this.zkSecure = conf.getBoolean(ServerConfig.SENTRY_HA_ZOOKEEPER_SECURITY,
        ServerConfig.SENTRY_HA_ZOOKEEPER_SECURITY_DEFAULT);
    ACLProvider aclProvider;
    validateConf();
    if (zkSecure) {
      LOGGER.info("Connecting to ZooKeeper with SASL/Kerberos and using 'sasl' ACLs");
      setJaasConfiguration(conf);
      System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY,
          SENTRY_ZK_JAAS_NAME);
      saslACL = Lists.newArrayList();
      saslACL.add(new ACL(Perms.ALL, new Id("sasl", getServicePrincipal(conf,
          ServerConfig.PRINCIPAL))));
      saslACL.add(new ACL(Perms.ALL, new Id("sasl", getServicePrincipal(conf,
              ServerConfig.SERVER_HA_ZOOKEEPER_CLIENT_PRINCIPAL))));
      aclProvider = new SASLOwnerACLProvider();
      String allowConnect = conf.get(ServerConfig.ALLOW_CONNECT);

      if (!Strings.isNullOrEmpty(allowConnect)) {
        for (String principal : Arrays.asList(allowConnect.split("\\s*,\\s*"))) {
          LOGGER.info("Adding acls for " + principal);
          saslACL.add(new ACL(Perms.ALL, new Id("sasl", principal)));
        }
      }
    } else {
      LOGGER.info("Connecting to ZooKeeper without authentication");
      aclProvider = new DefaultACLProvider();
    }

    retryPolicy = new RetryNTimes(retriesMaxCount, sleepMsBetweenRetries);
    this.curatorFramework = CuratorFrameworkFactory.builder()
        .namespace(this.namespace)
        .connectString(this.zookeeperQuorum)
        .retryPolicy(retryPolicy)
        .aclProvider(aclProvider)
        .build();
    startCuratorFramework();
  }

  /**
   * Use common HAContext (ie curator framework connection to ZK)
   *
   * @param conf
   * @throws Exception
   */
  public static HAContext getHAContext(Configuration conf) throws Exception {
    if (serverHAContext == null) {
      serverHAContext = new HAContext(conf);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          LOGGER.info("ShutdownHook closing curator framework");
          try {
            clearServerContext();
          } catch (Throwable t) {
            LOGGER.error("Error stopping SentryService", t);
          }
        }
      });

    }
    return serverHAContext;
  }

  // HA context for server which verifies the ZK ACLs on namespace
  public static HAContext getHAServerContext(Configuration conf) throws Exception {
    HAContext serverContext = getHAContext(conf);
    serverContext.checkAndSetACLs();
    return serverContext;
  }

  @VisibleForTesting
  public static synchronized void clearServerContext() {
    if (serverHAContext != null) {
      serverHAContext.getCuratorFramework().close();
      serverHAContext = null;
    }
  }

  public void startCuratorFramework() {
    if (curatorFramework.getState() != CuratorFrameworkState.STARTED) {
      curatorFramework.start();
    }
  }

  public CuratorFramework getCuratorFramework() {
    return this.curatorFramework;
  }

  public String getZookeeperQuorum() {
    return zookeeperQuorum;
  }

  public static boolean isHaEnabled(Configuration conf) {
    return conf.getBoolean(ServerConfig.SENTRY_HA_ENABLED, ServerConfig.SENTRY_HA_ENABLED_DEFAULT);
  }

  public String getNamespace() {
    return namespace;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  private void validateConf() {
    Preconditions.checkNotNull(zookeeperQuorum, "Zookeeper Quorum should not be null.");
    Preconditions.checkNotNull(namespace, "Zookeeper namespace should not be null.");
  }

  protected String getServicePrincipal(Configuration conf, String confProperty)
      throws IOException {
    String principal = conf.get(confProperty);
    Preconditions.checkNotNull(principal);
    Preconditions.checkArgument(principal.length() != 0, "Server principal is not right.");
    return principal.split("[/@]")[0];
  }

  private void checkAndSetACLs() throws Exception {
    if (zkSecure && !aclChecked) {
      // If znodes were previously created without security enabled, and now it is, we need to go through all existing znodes
      // and set the ACLs for them. This is done just once at the startup
      // We can't get the namespace znode through curator; have to go through zk client
      startCuratorFramework();
      String namespace = "/" + curatorFramework.getNamespace();
      if (curatorFramework.getZookeeperClient().getZooKeeper().exists(namespace, null) != null) {
        List<ACL> acls = curatorFramework.getZookeeperClient().getZooKeeper().getACL(namespace, new Stat());
        if (acls.isEmpty() || !acls.get(0).getId().getScheme().equals("sasl")) {
          LOGGER.info("'sasl' ACLs not set; setting...");
          List<String> children = curatorFramework.getZookeeperClient().getZooKeeper().getChildren(namespace, null);
          for (String child : children) {
            checkAndSetACLs("/" + child);
          }
          curatorFramework.getZookeeperClient().getZooKeeper().setACL(namespace, saslACL, -1);
        }
      }
      aclChecked = true;

    }
  }

  private void checkAndSetACLs(String path) throws Exception {
      LOGGER.info("Setting acls on " + path);
      List<String> children = curatorFramework.getChildren().forPath(path);
      for (String child : children) {
        checkAndSetACLs(path + "/" + child);
      }
      curatorFramework.setACL().withACL(saslACL).forPath(path);
  }

  // This gets ignored during most tests, see ZKXTestCaseWithSecurity#setupZKServer()
  private void setJaasConfiguration(Configuration conf) throws IOException {
    if ("false".equalsIgnoreCase(conf.get(
          ServerConfig.SERVER_HA_ZOOKEEPER_CLIENT_TICKET_CACHE,
          ServerConfig.SERVER_HA_ZOOKEEPER_CLIENT_TICKET_CACHE_DEFAULT))) {
      String keytabFile = conf.get(ServerConfig.SERVER_HA_ZOOKEEPER_CLIENT_KEYTAB);
      Preconditions.checkArgument(keytabFile.length() != 0, "Keytab File is not right.");
      String principal = conf.get(ServerConfig.SERVER_HA_ZOOKEEPER_CLIENT_PRINCIPAL);
      principal = SecurityUtil.getServerPrincipal(principal,
        conf.get(ServerConfig.RPC_ADDRESS, ServerConfig.RPC_ADDRESS_DEFAULT));
      Preconditions.checkArgument(principal.length() != 0, "Kerberos principal is not right.");

      // This is equivalent to writing a jaas.conf file and setting the system property, "java.security.auth.login.config", to
      // point to it (but this way we don't have to write a file, and it works better for the tests)
      JaasConfiguration.addEntryForKeytab(SENTRY_ZK_JAAS_NAME, principal, keytabFile);
    } else {
      // Create jaas conf for ticket cache
      JaasConfiguration.addEntryForTicketCache(SENTRY_ZK_JAAS_NAME);
    }
    javax.security.auth.login.Configuration.setConfiguration(JaasConfiguration.getInstance());
  }

  public class SASLOwnerACLProvider implements ACLProvider {
    @Override
    public List<ACL> getDefaultAcl() {
        return saslACL;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
        return saslACL;
    }
  }
}
