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
package org.apache.sentry.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService.Client;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateResponse;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.ServiceConstants.ClientConfig;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SentryHDFSServiceClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHDFSServiceClient.class);

  public static final String SENTRY_HDFS_SERVICE_NAME = "SentryHDFSService";

  public static class SentryAuthzUpdate {

    private final List<PermissionsUpdate> permUpdates;
    private final List<PathsUpdate> pathUpdates;

    public SentryAuthzUpdate(List<PermissionsUpdate> permUpdates, List<PathsUpdate> pathUpdates) {
      this.permUpdates = permUpdates;
      this.pathUpdates = pathUpdates;
    }

    public List<PermissionsUpdate> getPermUpdates() {
      return permUpdates;
    }

    public List<PathsUpdate> getPathUpdates() {
      return pathUpdates;
    }
  }
  
  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  public static class UgiSaslClientTransport extends TSaslClientTransport {
    protected UserGroupInformation ugi = null;

    public UgiSaslClientTransport(String mechanism, String authorizationId,
        String protocol, String serverName, Map<String, String> props,
        CallbackHandler cbh, TTransport transport, boolean wrapUgi)
        throws IOException {
      super(mechanism, authorizationId, protocol, serverName, props, cbh,
          transport);
      if (wrapUgi) {
        ugi = UserGroupInformation.getLoginUser();
      }
    }

    // open the SASL transport with using the current UserGroupInformation
    // This is needed to get the current login context stored
    @Override
    public void open() throws TTransportException {
      if (ugi == null) {
        baseOpen();
      } else {
        try {
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws TTransportException {
              baseOpen();
              return null;
            }
          });
        } catch (IOException e) {
          throw new TTransportException("Failed to open SASL transport", e);
        } catch (InterruptedException e) {
          throw new TTransportException(
              "Interrupted while opening underlying transport", e);
        }
      }
    }

    private void baseOpen() throws TTransportException {
      super.open();
    }
  }

  private final Configuration conf;
  private final InetSocketAddress serverAddress;
  private final int connectionTimeout;
  private boolean kerberos;
  private TTransport transport;

  private String[] serverPrincipalParts;
  private Client client;
  
  public SentryHDFSServiceClient(Configuration conf) throws IOException {
    this.conf = conf;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    this.serverAddress = NetUtils.createSocketAddr(Preconditions.checkNotNull(
                           conf.get(ClientConfig.SERVER_RPC_ADDRESS), "Config key "
                           + ClientConfig.SERVER_RPC_ADDRESS + " is required"), conf.getInt(
                           ClientConfig.SERVER_RPC_PORT, ClientConfig.SERVER_RPC_PORT_DEFAULT));
    this.connectionTimeout = conf.getInt(ClientConfig.SERVER_RPC_CONN_TIMEOUT,
                                         ClientConfig.SERVER_RPC_CONN_TIMEOUT_DEFAULT);
    kerberos = ClientConfig.SECURITY_MODE_KERBEROS.equalsIgnoreCase(
        conf.get(ClientConfig.SECURITY_MODE, ClientConfig.SECURITY_MODE_KERBEROS).trim());
    transport = new TSocket(serverAddress.getHostName(),
        serverAddress.getPort(), connectionTimeout);
    if (kerberos) {
      String serverPrincipal = Preconditions.checkNotNull(
          conf.get(ClientConfig.PRINCIPAL), ClientConfig.PRINCIPAL + " is required");

      // Resolve server host in the same way as we are doing on server side
      serverPrincipal = SecurityUtil.getServerPrincipal(serverPrincipal, serverAddress.getAddress());
      LOGGER.info("Using server kerberos principal: " + serverPrincipal);

      serverPrincipalParts = SaslRpcServer.splitKerberosName(serverPrincipal);
      Preconditions.checkArgument(serverPrincipalParts.length == 3,
           "Kerberos principal should have 3 parts: " + serverPrincipal);
      boolean wrapUgi = "true".equalsIgnoreCase(conf
          .get(ClientConfig.SECURITY_USE_UGI_TRANSPORT, "true"));
      transport = new UgiSaslClientTransport(AuthMethod.KERBEROS.getMechanismName(),
          null, serverPrincipalParts[0], serverPrincipalParts[1],
          ClientConfig.SASL_PROPERTIES, null, transport, wrapUgi);
    } else {
      serverPrincipalParts = null;
    }
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException("Transport exception while opening transport: " + e.getMessage(), e);
    }
    LOGGER.info("Successfully opened transport: " + transport + " to " + serverAddress);
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
      new TCompactProtocol(transport),
      SentryHDFSServiceClient.SENTRY_HDFS_SERVICE_NAME);
    client = new SentryHDFSService.Client(protocol);
    LOGGER.info("Successfully created client");
  }

  public synchronized void notifyHMSUpdate(PathsUpdate update)
      throws IOException {
    try {
      client.handle_hms_notification(update.toThrift());
    } catch (Exception e) {
      throw new IOException("Thrift Exception occurred !!", e);
    }
  }

  public synchronized long getLastSeenHMSPathSeqNum()
      throws IOException {
    try {
      return client.check_hms_seq_num(-1);
    } catch (Exception e) {
      throw new IOException("Thrift Exception occurred !!", e);
    }
  }

  public synchronized SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum)
      throws IOException {
    SentryAuthzUpdate retVal = new SentryAuthzUpdate(new LinkedList<PermissionsUpdate>(), new LinkedList<PathsUpdate>());
    try {
      TAuthzUpdateResponse sentryUpdates = client.get_all_authz_updates_from(permSeqNum, pathSeqNum);
      if (sentryUpdates.getAuthzPathUpdate() != null) {
        for (TPathsUpdate pathsUpdate : sentryUpdates.getAuthzPathUpdate()) {
          retVal.getPathUpdates().add(new PathsUpdate(pathsUpdate));
        }
      }
      if (sentryUpdates.getAuthzPermUpdate() != null) {
        for (TPermissionsUpdate permsUpdate : sentryUpdates.getAuthzPermUpdate()) {
          retVal.getPermUpdates().add(new PermissionsUpdate(permsUpdate));
        }
      }
    } catch (Exception e) {
      throw new IOException("Thrift Exception occurred !!", e);
    }
    return retVal;
  }

  public void close() {
    if (transport != null) {
      transport.close();
    }
  }
}
