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
import java.util.Collections;
import java.util.List;

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

import com.google.common.base.Preconditions;

/**
 * Stores the HA related context
 */
public class HAContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(HAContext.class);

  public final static String SENTRY_SERVICE_REGISTER_NAMESPACE = "sentry-service";
  private final String zookeeperQuorum;
  private final int retriesMaxCount;
  private final int sleepMsBetweenRetries;
  private final String namespace;

  private final boolean zkSecure;
  private List<ACL> saslACL;

  private final CuratorFramework curatorFramework;
  private final RetryPolicy retryPolicy;

  public HAContext(Configuration conf) throws Exception {
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
      System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "Client");
      saslACL = Collections.singletonList(new ACL(Perms.ALL, new Id("sasl", getServicePrincipal(conf))));
      aclProvider = new SASLOwnerACLProvider();
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
    checkAndSetACLs();
  }

  public CuratorFramework getCuratorFramework() {
    return this.curatorFramework;
  }

  public String getZookeeperQuorum() {
    return zookeeperQuorum;
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

  private String getServicePrincipal(Configuration conf) throws IOException {
    String principal = conf.get(ServerConfig.PRINCIPAL);
    Preconditions.checkNotNull(principal);
    Preconditions.checkArgument(principal.length() != 0, "Server principal is not right.");
    return principal.split("[/@]")[0];
  }

  private void checkAndSetACLs() throws Exception {
    if (zkSecure) {
      // If znodes were previously created without security enabled, and now it is, we need to go through all existing znodes
      // and set the ACLs for them
      // We can't get the namespace znode through curator; have to go through zk client
      if (curatorFramework.getState() != CuratorFrameworkState.STARTED) {
        curatorFramework.start();
      }
      String namespace = "/" + curatorFramework.getNamespace();
      if (curatorFramework.getZookeeperClient().getZooKeeper().exists(namespace, null) != null) {
        List<ACL> acls = curatorFramework.getZookeeperClient().getZooKeeper().getACL(namespace, new Stat());
        if (!acls.get(0).getId().getScheme().equals("sasl")) {
          LOGGER.info("'sasl' ACLs not set; setting...");
          List<String> children = curatorFramework.getZookeeperClient().getZooKeeper().getChildren(namespace, null);
          for (String child : children) {
              checkAndSetACLs(child);
          }
          curatorFramework.getZookeeperClient().getZooKeeper().setACL(namespace, saslACL, -1);
        }
      }
    }
  }

  private void checkAndSetACLs(String path) throws Exception {
      List<String> children = curatorFramework.getChildren().forPath(path);
      for (String child : children) {
          checkAndSetACLs(path + "/" + child);
      }
      curatorFramework.setACL().withACL(saslACL).forPath(path);
  }

  // This gets ignored during most tests, see ZKXTestCaseWithSecurity#setupZKServer()
  private void setJaasConfiguration(Configuration conf) throws IOException {
      String keytabFile = conf.get(ServerConfig.KEY_TAB);
      Preconditions.checkArgument(keytabFile.length() != 0, "Keytab File is not right.");
      String principal = conf.get(ServerConfig.PRINCIPAL);
      principal = SecurityUtil.getServerPrincipal(principal, conf.get(ServerConfig.RPC_ADDRESS));
      Preconditions.checkArgument(principal.length() != 0, "Kerberos principal is not right.");

      // This is equivalent to writing a jaas.conf file and setting the system property, "java.security.auth.login.config", to
      // point to it (but this way we don't have to write a file, and it works better for the tests)
      JaasConfiguration.addEntry("Client", principal, keytabFile);
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
