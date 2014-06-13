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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.callback.CallbackHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ThriftConstants;
import org.apache.sentry.service.thrift.Status;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class SentryPolicyServiceClient {

  private final Configuration conf;
  private final InetSocketAddress serverAddress;
  private final boolean kerberos;
  private final String[] serverPrincipalParts;
  private SentryPolicyService.Client client;
  private TTransport transport;
  private int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory
                                       .getLogger(SentryPolicyServiceClient.class);
  private static final String THRIFT_EXCEPTION_MESSAGE = "Thrift exception occured ";

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

  public SentryPolicyServiceClient(Configuration conf) throws IOException {
    this.conf = conf;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    this.serverAddress = NetUtils.createSocketAddr(Preconditions.checkNotNull(
                           conf.get(ClientConfig.SERVER_RPC_ADDRESS), "Config key "
                           + ClientConfig.SERVER_RPC_ADDRESS + " is required"), conf.getInt(
                           ClientConfig.SERVER_RPC_PORT, ClientConfig.SERVER_RPC_PORT_DEFAULT));
    this.connectionTimeout = conf.getInt(ClientConfig.SERVER_RPC_CONN_TIMEOUT,
                                         ClientConfig.SERVER_RPC_CONN_TIMEOUT_DEFAULT);
    kerberos = ServerConfig.SECURITY_MODE_KERBEROS.equalsIgnoreCase(
        conf.get(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_KERBEROS).trim());
    transport = new TSocket(serverAddress.getHostName(),
        serverAddress.getPort(), connectionTimeout);
    if (kerberos) {
      String serverPrincipal = Preconditions.checkNotNull(conf.get(ServerConfig.PRINCIPAL), ServerConfig.PRINCIPAL + " is required");

      // Resolve server host in the same way as we are doing on server side
      serverPrincipal = SecurityUtil.getServerPrincipal(serverPrincipal, serverAddress.getAddress());
      LOGGER.info("Using server kerberos principal: " + serverPrincipal);

      serverPrincipalParts = SaslRpcServer.splitKerberosName(serverPrincipal);
      Preconditions.checkArgument(serverPrincipalParts.length == 3,
           "Kerberos principal should have 3 parts: " + serverPrincipal);
      boolean wrapUgi = "true".equalsIgnoreCase(conf
          .get(ServerConfig.SECURITY_USE_UGI_TRANSPORT, "true"));
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
      new TBinaryProtocol(transport),
      SentryPolicyStoreProcessor.SENTRY_POLICY_SERVICE_NAME);
    client = new SentryPolicyService.Client(protocol);
    LOGGER.info("Successfully created client");
  }

  public synchronized void createRole(String requestorUserName, String roleName)
  throws SentryUserException {
    TCreateSentryRoleRequest request = new TCreateSentryRoleRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    try {
      TCreateSentryRoleResponse response = client.create_sentry_role(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public void dropRole(String requestorUserName,
      String roleName)
  throws SentryUserException {
    dropRole(requestorUserName, roleName, false);
  }

  public void dropRoleIfExists(String requestorUserName,
      String roleName)
  throws SentryUserException {
    dropRole(requestorUserName, roleName, true);
  }

  private void dropRole(String requestorUserName,
      String roleName, boolean ifExists)
  throws SentryUserException {
    TDropSentryRoleRequest request = new TDropSentryRoleRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    try {
      TDropSentryRoleResponse response = client.drop_sentry_role(request);
      Status status = Status.fromCode(response.getStatus().getValue());
      if (ifExists && status == Status.NO_SUCH_OBJECT) {
        return;
      }
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Gets sentry role objects for a given groupName using the Sentry service
   * @param requestorUserName : user on whose behalf the request is issued
   * @param requestorUserGroupNames :groups the requesting user belongs to
   * @param groupName : groupName to look up ( if null returns all roles for all groups)
   * @return Set of thrift sentry role objects
   * @throws SentryUserException
   */
  public synchronized Set<TSentryRole> listRolesByGroupName(
      String requestorUserName,
      String groupName)
  throws SentryUserException {
    TListSentryRolesRequest request = new TListSentryRolesRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setGroupName(groupName);
    TListSentryRolesResponse response;
    Set<String> roles = new HashSet<String>();
    try {
      response = client.list_sentry_roles_by_group(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getRoles();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public Set<TSentryPrivilege> listAllPrivilegesByRoleName(String requestorUserName, String roleName) 
		  throws SentryUserException {
    return listPrivilegesByRoleName(requestorUserName, roleName, null);
  }

  /**
   * Gets sentry privilege objects for a given roleName using the Sentry service
   * @param requestorUserName : user on whose behalf the request is issued
   * @param requestorUserGroupNames :groups the requesting user belongs to
   * @param roleName : roleName to look up
   * @param authorizable : authorizable Hierarchy (server->db->table etc)
   * @return Set of thrift sentry privilege objects
   * @throws SentryUserException
   */
  public Set<TSentryPrivilege> listPrivilegesByRoleName(String requestorUserName,
      String roleName, List<? extends Authorizable> authorizable)
  throws SentryUserException {
    TListSentryPrivilegesRequest request = new TListSentryPrivilegesRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    if (authorizable != null) {
      TSentryAuthorizable tSentryAuthorizable = new TSentryAuthorizable();
      // TODO : Needed to support SearchModelAuthorizable
      for (Authorizable authzble : authorizable) {
        if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.Server.toString())) {
          tSentryAuthorizable.setServer(authzble.getName());
        } else if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.URI.toString())) {
          tSentryAuthorizable.setUri(authzble.getName());
        } else if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.Db.toString())) {
          tSentryAuthorizable.setDb(authzble.getName());
        } else if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.Table.toString())) {
          tSentryAuthorizable.setTable(authzble.getName());
        }
      }
      request.setAuthorizableHierarchy(tSentryAuthorizable);
    }
    TListSentryPrivilegesResponse response;
    try {
      response = client.list_sentry_privileges_by_role(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getPrivileges();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public Set<TSentryRole> listRoles(String requestorUserName)
       throws SentryUserException {
    return listRolesByGroupName(requestorUserName, null);
  }

  public void grantURIPrivilege(String requestorUserName,
      String roleName, String server, String uri)
  throws SentryUserException {
    grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.URI, server, uri, null, null, AccessConstants.ALL);
  }

  public void grantServerPrivilege(String requestorUserName,
      String roleName, String server)
  throws SentryUserException {
    grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.SERVER, server, null, null, null, AccessConstants.ALL);
  }

  public void grantDatabasePrivilege(String requestorUserName,
      String roleName, String server, String db)
  throws SentryUserException {
    grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.DATABASE, server, null, db, null, AccessConstants.ALL);
  }

  public void grantTablePrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String action)
  throws SentryUserException {
    grantPrivilege(requestorUserName, roleName, PrivilegeScope.TABLE, server,
        null,
        db, table, action);
  }

  private void grantPrivilege(String requestorUserName,
      String roleName, PrivilegeScope scope, String serverName, String uri, String db, String table, String action)
  throws SentryUserException {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(scope.toString());
    privilege.setServerName(serverName);
    privilege.setURI(uri);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(action);
    privilege.setGrantorPrincipal(requestorUserName);
    privilege.setCreateTime(System.currentTimeMillis());
    request.setPrivilege(privilege);
    try {
      TAlterSentryRoleGrantPrivilegeResponse response = client.alter_sentry_role_grant_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public void revokeURIPrivilege(String requestorUserName,
      String roleName, String server, String uri)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.URI, server, uri, null, null, AccessConstants.ALL);
  }

  public void revokeServerPrivilege(String requestorUserName,
      String roleName, String server)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.SERVER, server, null, null, null, AccessConstants.ALL);
  }

  public void revokeDatabasePrivilege(String requestorUserName,
      String roleName, String server, String db)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.DATABASE, server, null, db, null, AccessConstants.ALL);
  }

  public void revokeTablePrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String action)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.TABLE, server, null,
        db, table, action);
  }

  private void revokePrivilege(String requestorUserName,
      String roleName, PrivilegeScope scope, String serverName, String uri, String db, String table, String action)
  throws SentryUserException {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(scope.toString());
    privilege.setServerName(serverName);
    privilege.setURI(uri);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(action);
    privilege.setGrantorPrincipal(requestorUserName);
    privilege.setCreateTime(System.currentTimeMillis());
    request.setPrivilege(privilege);
    try {
      TAlterSentryRoleRevokePrivilegeResponse response = client.alter_sentry_role_revoke_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public Set<String> listPrivilegesForProvider(Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizable)
  throws SentryUserException {
    TSentryActiveRoleSet thriftRoleSet = new TSentryActiveRoleSet(roleSet.isAll(), roleSet.getRoles());
    TListSentryPrivilegesForProviderRequest request =
        new TListSentryPrivilegesForProviderRequest(ThriftConstants.
            TSENTRY_SERVICE_VERSION_CURRENT, groups, thriftRoleSet);
    if ((authorizable != null)&&(authorizable.length > 0)) {
      TSentryAuthorizable tSentryAuthorizable = new TSentryAuthorizable();
      // TODO : Needed to support SearchModelAuthorizable
      for (Authorizable authzble : authorizable) {
        if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.Server.toString())) {
          tSentryAuthorizable.setServer(authzble.getName());
        } else if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.URI.toString())) {
          tSentryAuthorizable.setUri(authzble.getName());
        } else if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.Db.toString())) {
          tSentryAuthorizable.setDb(authzble.getName());
        } else if (authzble.getTypeName().equalsIgnoreCase(DBModelAuthorizable.AuthorizableType.Table.toString())) {
          tSentryAuthorizable.setTable(authzble.getName());
        }
      }
      request.setAuthorizableHierarchy(tSentryAuthorizable);
    }
    try {
      TListSentryPrivilegesForProviderResponse response = client.list_sentry_privileges_for_provider(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getPrivileges();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public synchronized void grantRoleToGroup(String requestorUserName,
      String groupName, String roleName)
  throws SentryUserException {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest(ThriftConstants.
TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName,
        roleName, Sets.newHashSet(new TSentryGroup(groupName)));
    try {
      TAlterSentryRoleAddGroupsResponse response = client.alter_sentry_role_add_groups(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public synchronized void revokeRoleFromGroup(String requestorUserName,
      String groupName, String roleName)
  throws SentryUserException {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest(ThriftConstants.
TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName,
        roleName, Sets.newHashSet(new TSentryGroup(groupName)));
    try {
      TAlterSentryRoleDeleteGroupsResponse response = client.alter_sentry_role_delete_groups(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public void close() {
    if (transport != null) {
      transport.close();
    }
  }
}
