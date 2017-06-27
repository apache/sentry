/*
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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.sentry.service.thrift.JaasConfiguration;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.*;

/**
 * HAContext stores the global ZooKeeper related context.
 * <p>
 * This class is a singleton - only one ZooKeeper context is maintained.
 */
public final class HAContext implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HAContext.class);
  private static HAContext serverHAContext = null;
  private static boolean aclUnChecked = true;

  private static final String SENTRY_ZK_JAAS_NAME = "SentryClient";
  private final String zookeeperQuorum;
  private final String namespace;

  private final boolean zkSecure;
  private final List<ACL> saslACL;

  private final CuratorFramework curatorFramework;

  private HAContext(Configuration conf) throws IOException {
    this.zookeeperQuorum = conf.get(SENTRY_HA_ZOOKEEPER_QUORUM, "");
    int retriesMaxCount = conf.getInt(SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT,
            SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT_DEFAULT);
    int sleepMsBetweenRetries = conf.getInt(SENTRY_HA_ZOOKEEPER_SLEEP_BETWEEN_RETRIES_MS,
            SENTRY_HA_ZOOKEEPER_SLEEP_BETWEEN_RETRIES_MS_DEFAULT);
    String ns = conf.get(SENTRY_HA_ZOOKEEPER_NAMESPACE, SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT);
    // Namespace shouldn't start with slash.
    // If config namespace starts with slash, remove it first
    this.namespace = ns.startsWith("/") ? ns.substring(1) : ns;

    this.zkSecure = conf.getBoolean(SENTRY_HA_ZOOKEEPER_SECURITY,
        SENTRY_HA_ZOOKEEPER_SECURITY_DEFAULT);
    this.validateConf();
    ACLProvider aclProvider;
    if (zkSecure) {
      LOGGER.info("Connecting to ZooKeeper with SASL/Kerberos and using 'sasl' ACLs");
      this.setJaasConfiguration(conf);
      System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY,
          SENTRY_ZK_JAAS_NAME);
      saslACL = Lists.newArrayList();
      saslACL.add(new ACL(Perms.ALL, new Id("sasl", getServicePrincipal(conf,
          PRINCIPAL))));
      saslACL.add(new ACL(Perms.ALL, new Id("sasl", getServicePrincipal(conf,
              SERVER_HA_ZOOKEEPER_CLIENT_PRINCIPAL))));
      aclProvider = new SASLOwnerACLProvider();
      String allowConnect = conf.get(ALLOW_CONNECT);

      if (!Strings.isNullOrEmpty(allowConnect)) {
        for (String principal : allowConnect.split("\\s*,\\s*")) {
          LOGGER.info("Adding acls for " + principal);
          saslACL.add(new ACL(Perms.ALL, new Id("sasl", principal)));
        }
      }
    } else {
      saslACL = null;
      LOGGER.info("Connecting to ZooKeeper without authentication");
      aclProvider = new DefaultACLProvider();
    }

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(sleepMsBetweenRetries, retriesMaxCount);
    this.curatorFramework = CuratorFrameworkFactory.builder()
        .namespace(this.namespace)
        .connectString(this.zookeeperQuorum)
        .retryPolicy(retryPolicy)
        .aclProvider(aclProvider)
        .build();
  }

  private void start() {
    if (curatorFramework.getState() != CuratorFrameworkState.STARTED) {
      curatorFramework.start();
    }
  }

  /**
   * Create a singleton instance of ZooKeeper context (if needed) and return it.
   * The instance returned is already running.
   *
   * @param conf Configuration, The following keys are used:
   *             <ul>
   *             <li>SENTRY_HA_ZOOKEEPER_QUORUM</li>
   *             <li>SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT</li>
   *             <li>SENTRY_HA_ZOOKEEPER_SLEEP_BETWEEN_RETRIES_MS</li>
   *             <li>SENTRY_HA_ZOOKEEPER_NAMESPACE</li>
   *             <li>SENTRY_HA_ZOOKEEPER_SECURITY</li>
   *             <li>LOGIN_CONTEXT_NAME_KEY</li>
   *             <li>PRINCIPAL</li>
   *             <li>SERVER_HA_ZOOKEEPER_CLIENT_PRINCIPAL</li>
   *             <li>ALLOW_CONNECT</li>
   *             <li>SERVER_HA_ZOOKEEPER_CLIENT_TICKET_CACHE</li>
   *             <li>SERVER_HA_ZOOKEEPER_CLIENT_KEYTAB</li>
   *             <li>RPC_ADDRESS</li>
   *             </ul>
   * @return Global ZooKeeper context.
   * @throws Exception
   */
  static synchronized HAContext getHAContext(Configuration conf) throws IOException {
    if (serverHAContext != null) {
      return serverHAContext;
    }
    serverHAContext = new HAContext(conf);

    serverHAContext.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOGGER.info("ShutdownHook closing curator framework");
        try {
          if (serverHAContext != null) {
            serverHAContext.close();
          }
        } catch (Throwable t) {
          LOGGER.error("Error stopping curator framework", t);
        }
      }
    });
    return serverHAContext;
  }

  /**
   * HA context for server which verifies the ZK ACLs on namespace
   *
   * @param conf Configuration - see {@link #getHAContext(Configuration)}
   * @return Server ZK context
   * @throws Exception
   */
  public static HAContext getHAServerContext(Configuration conf) throws Exception {
    HAContext serverContext = getHAContext(conf);
    serverContext.checkAndSetACLs();
    return serverContext;
  }

  /**
   * Reset existing HA context.
   * Should be only used by tests to provide different configurations.
   */
  public static void resetHAContext() {
    HAContext oldContext = serverHAContext;
    if (oldContext != null) {
      try {
        oldContext.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close HACOntext", e);
      }
    }
    serverHAContext = null;
  }


  private void validateConf() {
    checkNotNull(zookeeperQuorum, "Zookeeper Quorum should not be null.");
    checkNotNull(namespace, "Zookeeper namespace should not be null.");
  }

  private static String getServicePrincipal(Configuration conf, String confProperty)
      throws IOException {
    String principal = checkNotNull(conf.get(confProperty));
    checkArgument(!principal.isEmpty(), "Server principal is empty.");
    return principal.split("[/@]")[0];
  }

  private void checkAndSetACLs() throws Exception {
    if (zkSecure && aclUnChecked) {
      // If znodes were previously created without security enabled, and now it is, we need to go
      // through all existing znodes and set the ACLs for them. This is done just once at the startup
      // We can't get the namespace znode through curator; have to go through zk client
      String newNamespace = "/" + curatorFramework.getNamespace();
      if (curatorFramework.getZookeeperClient().getZooKeeper().exists(newNamespace, null) != null) {
        List<ACL> acls = curatorFramework.getZookeeperClient().getZooKeeper().getACL(newNamespace, new Stat());
        if (acls.isEmpty() || !acls.get(0).getId().getScheme().equals("sasl")) {
          LOGGER.info("'sasl' ACLs not set; setting...");
          List<String> children = curatorFramework.getZookeeperClient().getZooKeeper().getChildren(newNamespace,
                  null);
          for (String child : children) {
            this.checkAndSetACLs("/" + child);
          }
          curatorFramework.getZookeeperClient().getZooKeeper().setACL(newNamespace, saslACL, -1);
        }
      }
      aclUnChecked = false;
    }
  }

  private void checkAndSetACLs(String path) throws Exception {
    LOGGER.info("Setting acls on " + path);
    List<String> children = curatorFramework.getChildren().forPath(path);
    for (String child : children) {
      this.checkAndSetACLs(path + "/" + child);
    }
    curatorFramework.setACL().withACL(saslACL).forPath(path);
  }

  // This gets ignored during most tests, see ZKXTestCaseWithSecurity#setupZKServer()
  private void setJaasConfiguration(Configuration conf) throws IOException {
    if ("false".equalsIgnoreCase(conf.get(
          SERVER_HA_ZOOKEEPER_CLIENT_TICKET_CACHE,
          SERVER_HA_ZOOKEEPER_CLIENT_TICKET_CACHE_DEFAULT))) {
      String keytabFile = conf.get(SERVER_HA_ZOOKEEPER_CLIENT_KEYTAB);
      checkArgument(!keytabFile.isEmpty(), "Keytab File is empty.");
      String principal = conf.get(SERVER_HA_ZOOKEEPER_CLIENT_PRINCIPAL);
      principal = SecurityUtil.getServerPrincipal(principal,
        conf.get(RPC_ADDRESS, RPC_ADDRESS_DEFAULT));
      checkArgument(!principal.isEmpty(), "Kerberos principal is empty.");

      // This is equivalent to writing a jaas.conf file and setting the system property,
      // "java.security.auth.login.config", to point to it (but this way we don't have to write
      // a file, and it works better for the tests)
      JaasConfiguration.addEntryForKeytab(SENTRY_ZK_JAAS_NAME, principal, keytabFile);
    } else {
      // Create jaas conf for ticket cache
      JaasConfiguration.addEntryForTicketCache(SENTRY_ZK_JAAS_NAME);
    }
    javax.security.auth.login.Configuration.setConfiguration(JaasConfiguration.getInstance());
  }

  /**
   * Create a new Curator leader szselector
   * @param path Zookeeper path
   * @param listener Curator listener for leader selection changes
   * @return an instance of leader selector associated with the running curator framework
   */
  public LeaderSelector newLeaderSelector(String path, LeaderSelectorListener listener) {
    return new LeaderSelector(this.curatorFramework, path, listener);
  }

  @Override
  public void close() throws Exception {
    this.curatorFramework.close();
  }

  private class SASLOwnerACLProvider implements ACLProvider {
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
