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

package org.apache.sentry.provider.db.service.thrift;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SentryPolicyServiceClient {

  @SuppressWarnings("unused")
  private final Configuration conf;
  private final InetSocketAddress serverAddress;
  private final String[] serverPrincipalParts;
  private SentryPolicyService.Client client;
  private TTransport transport;
  private int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory
                                       .getLogger(SentryPolicyServiceClient.class);

  public SentryPolicyServiceClient(Configuration conf) throws Exception {
    this.conf = conf;
    this.serverAddress = NetUtils.createSocketAddr(Preconditions.checkNotNull(
                           conf.get(ClientConfig.SERVER_RPC_ADDRESS), "Config key "
                           + ClientConfig.SERVER_RPC_ADDRESS + " is required"), conf.getInt(
                           ClientConfig.SERVER_RPC_PORT, ClientConfig.SERVER_RPC_PORT_DEFAULT));
    this.connectionTimeout = conf.getInt(ClientConfig.SERVER_RPC_CONN_TIMEOUT,
                                         ClientConfig.SERVER_RPC_CONN_TIMEOUT_DEFAULT);
    String serverPrincipal = Preconditions.checkNotNull(
                               conf.get(ServerConfig.PRINCIPAL), ServerConfig.PRINCIPAL
                               + " is required");
    serverPrincipalParts = SaslRpcServer.splitKerberosName(serverPrincipal);
    Preconditions.checkArgument(serverPrincipalParts.length == 3,
                                "Kerberos principal should have 3 parts: " + serverPrincipal);
    transport = new TSocket(serverAddress.getHostString(),
                            serverAddress.getPort(), connectionTimeout);
    TTransport saslTransport = new TSaslClientTransport(
      AuthMethod.KERBEROS.getMechanismName(), null, serverPrincipalParts[0],
      serverPrincipalParts[1], ClientConfig.SASL_PROPERTIES, null, transport);
    saslTransport.open();
    LOGGER.info("Successfully opened transport");
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
      new TBinaryProtocol(saslTransport),
      SentryPolicyStoreProcessor.SENTRY_POLICY_SERVICE_NAME);
    client = new SentryPolicyService.Client(protocol);
    LOGGER.info("Successfully created client");
  }

  public TCreateSentryRoleResponse createRole(TCreateSentryRoleRequest req)
  throws TException {
    return client.create_sentry_role(req);
  }

  public TListSentryRolesResponse listRoleByName(TListSentryRolesRequest req)
  throws TException {
    return client.list_sentry_roles_by_role_name(req);
  }

  public TDropSentryRoleResponse dropRole(TDropSentryRoleRequest req)
  throws TException {
    return client.drop_sentry_role(req);
  }

  public TAlterSentryRoleGrantPrivilegeResponse grantPrivilege(TAlterSentryRoleGrantPrivilegeRequest req)
  throws TException {
    return client.alter_sentry_role_grant_privilege(req);
  }

  public TAlterSentryRoleRevokePrivilegeResponse revokePrivilege(TAlterSentryRoleRevokePrivilegeRequest req)
  throws TException {
    return client.alter_sentry_role_revoke_privilege(req);
  }

  public void close() {
    if (transport != null) {
      transport.close();
    }
  }
}