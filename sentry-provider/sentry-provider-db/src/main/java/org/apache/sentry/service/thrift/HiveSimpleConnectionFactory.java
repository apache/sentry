/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.service.thrift;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory used to generate Hive connections.
 * Supports insecure and Kerberos connections.
 * For Kerberos connections starts the ticket renewal thread and sets
 * up Kerberos credentials.
 */
public final class HiveSimpleConnectionFactory implements HiveConnectionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveSimpleConnectionFactory.class);

  /** Sentty configuration */
  private final Configuration conf;
  /** Hive configuration */
  private final HiveConf hiveConf;
  private final boolean insecure;
  private SentryKerberosContext kerberosContext = null;

  public HiveSimpleConnectionFactory(Configuration sentryConf, HiveConf hiveConf) {
    this.conf = sentryConf;
    this.hiveConf = hiveConf;
    insecure = !ServerConfig.SECURITY_MODE_KERBEROS.equalsIgnoreCase(
        sentryConf.get(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE).trim());
  }

  /**
   * Initialize the Factory.
   * For insecure connections there is nothing to initialize.
   * For Kerberos connections sets up ticket renewal thread.
   * @throws IOException
   * @throws LoginException
   */
  public void init() throws IOException, LoginException {
    if (insecure) {
      LOGGER.info("Using insecure connection to HMS");
      return;
    }

    int port = conf.getInt(ServerConfig.RPC_PORT, ServerConfig.RPC_PORT_DEFAULT);
    String rawPrincipal = Preconditions.checkNotNull(conf.get(ServerConfig.PRINCIPAL),
        "%s is required", ServerConfig.PRINCIPAL);
    String principal = SecurityUtil.getServerPrincipal(rawPrincipal, NetUtils.createSocketAddr(
        conf.get(ServerConfig.RPC_ADDRESS, ServerConfig.RPC_ADDRESS_DEFAULT),
        port).getAddress());
    LOGGER.debug("Opening kerberos connection to HMS using kerberos principal {}", principal);
    String[] principalParts = SaslRpcServer.splitKerberosName(principal);
    Preconditions.checkArgument(principalParts.length == 3,
        "Kerberos principal %s should have 3 parts", principal);
    String keytab = Preconditions.checkNotNull(conf.get(ServerConfig.KEY_TAB),
        "Configuration is missing required %s paraeter", ServerConfig.KEY_TAB);
    File keytabFile = new File(keytab);
    Preconditions.checkState(keytabFile.isFile() && keytabFile.canRead(),
        "Keytab %s does not exist or is not readable", keytab);
    // Instantiating SentryKerberosContext in non-server mode handles the ticket renewal.
    kerberosContext = new SentryKerberosContext(principal, keytab, false);
    UserGroupInformation.setConfiguration(conf);
    LOGGER.info("Using secure connection to HMS");
  }

  /**
   * Connect to HMS in unsecure mode or in Kerberos mode according to config.
   *
   * @return HMS connection
   * @throws IOException          if could not establish connection
   * @throws InterruptedException if connection was interrupted
   * @throws MetaException        if other errors happened
   */
  public HMSClient connect() throws IOException, InterruptedException, MetaException {
    if (insecure) {
      return new HMSClient(new HiveMetaStoreClient(hiveConf));
    }
    UserGroupInformation clientUGI =
        UserGroupInformation.getUGIFromSubject(kerberosContext.getSubject());
    return new HMSClient(clientUGI.doAs(new PrivilegedExceptionAction<HiveMetaStoreClient>() {
      @Override
      public HiveMetaStoreClient run() throws MetaException {
        return new HiveMetaStoreClient(hiveConf);
      }
    }));
  }

  @Override
  public void close() throws Exception {
    if (kerberosContext != null) {
      kerberosContext.shutDown();
      kerberosContext = null;
    }
  }
}
