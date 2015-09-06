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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SentryPolicyServiceClientDefaultImpl implements SentryPolicyServiceClient {

  private final Configuration conf;
  private final InetSocketAddress serverAddress;
  private final boolean kerberos;
  private final String[] serverPrincipalParts;
  private SentryPolicyService.Client client;
  private TTransport transport;
  private int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory
                                       .getLogger(SentryPolicyServiceClient.class);
  private static final String THRIFT_EXCEPTION_MESSAGE = "Thrift exception occurred ";

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
          if (ugi.isFromKeytab()) {
            ugi.checkTGTAndReloginFromKeytab();
          }
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

  public SentryPolicyServiceClientDefaultImpl(Configuration conf) throws IOException {
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
      LOGGER.debug("Using server kerberos principal: " + serverPrincipal);

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
    LOGGER.debug("Successfully opened transport: " + transport + " to " + serverAddress);
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
      new TBinaryProtocol(transport),
      SentryPolicyStoreProcessor.SENTRY_POLICY_SERVICE_NAME);
    client = new SentryPolicyService.Client(protocol);
    LOGGER.debug("Successfully created client");
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
    if (authorizable != null && !authorizable.isEmpty()) {
      TSentryAuthorizable tSentryAuthorizable = setupSentryAuthorizable(authorizable);
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

  public Set<TSentryRole> listUserRoles(String requestorUserName)
      throws SentryUserException {
    return listRolesByGroupName(requestorUserName, AccessConstants.ALL);
  }

  public TSentryPrivilege grantURIPrivilege(String requestorUserName,
      String roleName, String server, String uri)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL);
  }

  public TSentryPrivilege grantURIPrivilege(String requestorUserName,
      String roleName, String server, String uri, Boolean grantOption)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL, grantOption);
  }

  public void grantServerPrivilege(String requestorUserName,
      String roleName, String server, String action)
  throws SentryUserException {
    grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.SERVER, server, null, null, null, null, action);
  }

  @Deprecated
  /***
   * Should use grantServerPrivilege(String requestorUserName,
   *  String roleName, String server, String action, Boolean grantOption)
   */
  public TSentryPrivilege grantServerPrivilege(String requestorUserName,
      String roleName, String server, Boolean grantOption) throws SentryUserException {
    return grantServerPrivilege(requestorUserName, roleName, server,
        AccessConstants.ALL, grantOption);
  }

  public TSentryPrivilege grantServerPrivilege(String requestorUserName,
      String roleName, String server, String action, Boolean grantOption)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.SERVER, server, null, null, null, null, action, grantOption);
  }

  public TSentryPrivilege grantDatabasePrivilege(String requestorUserName,
      String roleName, String server, String db, String action)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.DATABASE, server, null, db, null, null, action);
  }

  public TSentryPrivilege grantDatabasePrivilege(String requestorUserName,
      String roleName, String server, String db, String action, Boolean grantOption)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
        PrivilegeScope.DATABASE, server, null, db, null, null, action, grantOption);
  }

  public TSentryPrivilege grantTablePrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String action)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.TABLE, server,
        null,
        db, table, null, action);
  }

  public TSentryPrivilege grantTablePrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String action, Boolean grantOption)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.TABLE, server,
        null, db, table, null, action, grantOption);
  }

  public TSentryPrivilege grantColumnPrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String columnName, String action)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.COLUMN, server,
          null,
          db, table, columnName, action);
  }

  public TSentryPrivilege grantColumnPrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String columnName, String action, Boolean grantOption)
  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.COLUMN, server,
          null, db, table, columnName, action, grantOption);
  }

  public Set<TSentryPrivilege> grantColumnsPrivileges(String requestorUserName,
      String roleName, String server, String db, String table, List<String> columnNames, String action)
  throws SentryUserException {
    return grantPrivileges(requestorUserName, roleName, PrivilegeScope.COLUMN, server,
            null,
            db, table, columnNames, action);
  }

  public Set<TSentryPrivilege> grantColumnsPrivileges(String requestorUserName,
      String roleName, String server, String db, String table, List<String> columnNames, String action, Boolean grantOption)
  throws SentryUserException {
    return grantPrivileges(requestorUserName, roleName, PrivilegeScope.COLUMN,
        server,
        null, db, table, columnNames, action, grantOption);
  }

  @VisibleForTesting
  public static TSentryAuthorizable setupSentryAuthorizable(
      List<? extends Authorizable> authorizable) {
    TSentryAuthorizable tSentryAuthorizable = new TSentryAuthorizable();

    for (Authorizable authzble : authorizable) {
      if (authzble.getTypeName().equalsIgnoreCase(
          DBModelAuthorizable.AuthorizableType.Server.toString())) {
        tSentryAuthorizable.setServer(authzble.getName());
      } else if (authzble.getTypeName().equalsIgnoreCase(
          DBModelAuthorizable.AuthorizableType.URI.toString())) {
        tSentryAuthorizable.setUri(authzble.getName());
      } else if (authzble.getTypeName().equalsIgnoreCase(
          DBModelAuthorizable.AuthorizableType.Db.toString())) {
        tSentryAuthorizable.setDb(authzble.getName());
      } else if (authzble.getTypeName().equalsIgnoreCase(
          DBModelAuthorizable.AuthorizableType.Table.toString())) {
        tSentryAuthorizable.setTable(authzble.getName());
      } else if (authzble.getTypeName().equalsIgnoreCase(
          DBModelAuthorizable.AuthorizableType.Column.toString())) {
        tSentryAuthorizable.setColumn(authzble.getName());
      }
    }
    return tSentryAuthorizable;
  }

  private TSentryPrivilege grantPrivilege(String requestorUserName,
      String roleName,
      PrivilegeScope scope, String serverName, String uri, String db,
      String table, String column, String action)  throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, scope, serverName, uri,
    db, table, column, action, false);
  }

  private TSentryPrivilege grantPrivilege(String requestorUserName,
      String roleName, PrivilegeScope scope, String serverName, String uri, String db, String table,
      String column, String action, Boolean grantOption)
  throws SentryUserException {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    Set<TSentryPrivilege> privileges = convertColumnPrivilege(requestorUserName, scope,
        serverName, uri, db, table, column, action, grantOption);
    request.setPrivileges(privileges);
    try {
      TAlterSentryRoleGrantPrivilegeResponse response = client.alter_sentry_role_grant_privilege(request);
      Status.throwIfNotOk(response.getStatus());
      if (response.isSetPrivileges()
          && response.getPrivilegesSize()>0 ) {
        return response.getPrivileges().iterator().next();
      } else {
        return new TSentryPrivilege();
      }
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  private Set<TSentryPrivilege> grantPrivileges(String requestorUserName,
      String roleName,
      PrivilegeScope scope, String serverName, String uri, String db,
      String table, List<String> columns, String action)  throws SentryUserException {
    return grantPrivileges(requestorUserName, roleName, scope, serverName, uri,
    db, table, columns, action, false);
  }

  private Set<TSentryPrivilege> grantPrivileges(String requestorUserName,
      String roleName, PrivilegeScope scope, String serverName, String uri, String db, String table,
      List<String> columns, String action, Boolean grantOption)
  throws SentryUserException {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    Set<TSentryPrivilege> privileges = convertColumnPrivileges(requestorUserName, scope,
        serverName, uri, db, table, columns, action, grantOption);
    request.setPrivileges(privileges);
    try {
      TAlterSentryRoleGrantPrivilegeResponse response = client.alter_sentry_role_grant_privilege(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getPrivileges();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public void revokeURIPrivilege(String requestorUserName,
      String roleName, String server, String uri)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL);
  }

  public void revokeURIPrivilege(String requestorUserName,
      String roleName, String server, String uri, Boolean grantOption)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL, grantOption);
  }

  public void revokeServerPrivilege(String requestorUserName,
      String roleName, String server, String action)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.SERVER, server, null, null, null, null, action);
  }

  public void revokeServerPrivilege(String requestorUserName,
      String roleName, String server, String action, Boolean grantOption)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.SERVER, server, null, null, null, null, action, grantOption);
  }

  public void revokeServerPrivilege(String requestorUserName,
      String roleName, String server, boolean grantOption)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.SERVER, server, null, null, null, null, AccessConstants.ALL, grantOption);
  }

  public void revokeDatabasePrivilege(String requestorUserName,
      String roleName, String server, String db, String action)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.DATABASE, server, null, db, null, null, action);
  }

  public void revokeDatabasePrivilege(String requestorUserName,
      String roleName, String server, String db, String action, Boolean grantOption)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.DATABASE, server, null, db, null, null, action, grantOption);
  }

  public void revokeTablePrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String action)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.TABLE, server, null,
        db, table, null, action);
  }

  public void revokeTablePrivilege(String requestorUserName,
      String roleName, String server, String db, String table, String action, Boolean grantOption)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.TABLE, server, null,
        db, table, null, action, grantOption);
  }

  public void revokeColumnPrivilege(String requestorUserName, String roleName,
      String server, String db, String table, String columnName, String action)
  throws SentryUserException {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    listBuilder.add(columnName);
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.COLUMN, server, null,
        db, table, listBuilder.build(), action);
  }

  public void revokeColumnPrivilege(String requestorUserName, String roleName,
      String server, String db, String table, String columnName, String action, Boolean grantOption)
  throws SentryUserException {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    listBuilder.add(columnName);
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.COLUMN, server, null,
        db, table, listBuilder.build(), action, grantOption);
  }

  public void revokeColumnsPrivilege(String requestorUserName, String roleName,
      String server, String db, String table, List<String> columns, String action)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.COLUMN, server, null,
        db, table, columns, action);
  }

  public void revokeColumnsPrivilege(String requestorUserName, String roleName,
      String server, String db, String table, List<String> columns, String action, Boolean grantOption)
  throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
        PrivilegeScope.COLUMN, server, null,
        db, table, columns, action, grantOption);
  }

  private void revokePrivilege(String requestorUserName,
      String roleName, PrivilegeScope scope, String serverName, String uri,
      String db, String table, List<String> columns, String action)
  throws SentryUserException {
    this.revokePrivilege(requestorUserName, roleName, scope, serverName, uri, db, table, columns, action, false);
  }

  private void revokePrivilege(String requestorUserName, String roleName,
      PrivilegeScope scope, String serverName, String uri, String db, String table, List<String> columns,
      String action, Boolean grantOption)
  throws SentryUserException {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    Set<TSentryPrivilege> privileges = convertColumnPrivileges(requestorUserName, scope,
        serverName, uri, db, table, columns, action, grantOption);
    request.setPrivileges(privileges);
    try {
      TAlterSentryRoleRevokePrivilegeResponse response = client.alter_sentry_role_revoke_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  private Set<TSentryPrivilege> convertColumnPrivileges(String requestorUserName,
      PrivilegeScope scope, String serverName, String uri, String db, String table, List<String> columns,
      String action, Boolean grantOption) {
    ImmutableSet.Builder<TSentryPrivilege> setBuilder = ImmutableSet.builder();
    if (columns == null || columns.isEmpty()) {
      TSentryPrivilege privilege = new TSentryPrivilege();
      privilege.setPrivilegeScope(scope.toString());
      privilege.setServerName(serverName);
      privilege.setURI(uri);
      privilege.setDbName(db);
      privilege.setTableName(table);
      privilege.setColumnName(null);
      privilege.setAction(action);
      privilege.setCreateTime(System.currentTimeMillis());
      privilege.setGrantOption(convertTSentryGrantOption(grantOption));
      setBuilder.add(privilege);
    } else {
      for (String column : columns) {
        TSentryPrivilege privilege = new TSentryPrivilege();
        privilege.setPrivilegeScope(scope.toString());
        privilege.setServerName(serverName);
        privilege.setURI(uri);
        privilege.setDbName(db);
        privilege.setTableName(table);
        privilege.setColumnName(column);
        privilege.setAction(action);
        privilege.setCreateTime(System.currentTimeMillis());
        privilege.setGrantOption(convertTSentryGrantOption(grantOption));
        setBuilder.add(privilege);
      }
    }
    return setBuilder.build();
  }

  private Set<TSentryPrivilege> convertColumnPrivilege(String requestorUserName,
      PrivilegeScope scope, String serverName, String uri, String db, String table, String column,
      String action, Boolean grantOption) {
    ImmutableSet.Builder<TSentryPrivilege> setBuilder = ImmutableSet.builder();
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(scope.toString());
    privilege.setServerName(serverName);
    privilege.setURI(uri);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setColumnName(column);
    privilege.setAction(action);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(convertTSentryGrantOption(grantOption));
    setBuilder.add(privilege);
    return setBuilder.build();
  }

  private TSentryGrantOption convertTSentryGrantOption(Boolean grantOption) {
    if (grantOption == null) {
      return TSentryGrantOption.UNSET;
    } else if (grantOption.equals(true)) {
      return TSentryGrantOption.TRUE;
    } else if (grantOption.equals(false)) {
      return TSentryGrantOption.FALSE;
    }
    return TSentryGrantOption.FALSE;
  }

  public Set<String> listPrivilegesForProvider(Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizable)
  throws SentryUserException {
    TSentryActiveRoleSet thriftRoleSet = new TSentryActiveRoleSet(roleSet.isAll(), roleSet.getRoles());
    TListSentryPrivilegesForProviderRequest request =
        new TListSentryPrivilegesForProviderRequest(ThriftConstants.
            TSENTRY_SERVICE_VERSION_CURRENT, groups, thriftRoleSet);
    if ((authorizable != null)&&(authorizable.length > 0)) {
      TSentryAuthorizable tSentryAuthorizable = setupSentryAuthorizable(Lists
          .newArrayList(authorizable));
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

  @Override
  public synchronized void grantRoleToGroup(String requestorUserName,
      String groupName, String roleName)
  throws SentryUserException {
    grantRoleToGroups(requestorUserName, roleName, Sets.newHashSet(groupName));
  }

  @Override
  public synchronized void revokeRoleFromGroup(String requestorUserName,
      String groupName, String roleName)
  throws SentryUserException {
    revokeRoleFromGroups(requestorUserName, roleName, Sets.newHashSet(groupName));
  }

  @Override
  public synchronized void grantRoleToGroups(String requestorUserName,
      String roleName, Set<String> groups)
  throws SentryUserException {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest(
        ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName,
        roleName, convert2TGroups(groups));
    try {
      TAlterSentryRoleAddGroupsResponse response = client.alter_sentry_role_add_groups(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public synchronized void revokeRoleFromGroups(String requestorUserName,
      String roleName, Set<String> groups)
  throws SentryUserException {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest(
        ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName,
        roleName, convert2TGroups(groups));
    try {
      TAlterSentryRoleDeleteGroupsResponse response = client.alter_sentry_role_delete_groups(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  private Set<TSentryGroup> convert2TGroups(Set<String> groups) {
    Set<TSentryGroup> tGroups = Sets.newHashSet();
    if (groups != null) {
      for (String groupName : groups) {
        tGroups.add(new TSentryGroup(groupName));
      }
    }
    return tGroups;
  }

  public synchronized void dropPrivileges(String requestorUserName,
      List<? extends Authorizable> authorizableObjects)
      throws SentryUserException {
    TSentryAuthorizable tSentryAuthorizable = setupSentryAuthorizable(authorizableObjects);

    TDropPrivilegesRequest request = new TDropPrivilegesRequest(
        ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName,
        tSentryAuthorizable);
    try {
      TDropPrivilegesResponse response = client.drop_sentry_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public synchronized void renamePrivileges(String requestorUserName,
      List<? extends Authorizable> oldAuthorizables,
      List<? extends Authorizable> newAuthorizables) throws SentryUserException {
    TSentryAuthorizable tOldSentryAuthorizable = setupSentryAuthorizable(oldAuthorizables);
    TSentryAuthorizable tNewSentryAuthorizable = setupSentryAuthorizable(newAuthorizables);

    TRenamePrivilegesRequest request = new TRenamePrivilegesRequest(
        ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName,
        tOldSentryAuthorizable, tNewSentryAuthorizable);
    try {
      TRenamePrivilegesResponse response = client
          .rename_sentry_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public synchronized Map<TSentryAuthorizable, TSentryPrivilegeMap> listPrivilegsbyAuthorizable(
      String requestorUserName,
      Set<List<? extends Authorizable>> authorizables, Set<String> groups,
      ActiveRoleSet roleSet) throws SentryUserException {
    Set<TSentryAuthorizable> authSet = Sets.newTreeSet();

    for (List<? extends Authorizable> authorizableHierarchy : authorizables) {
      authSet.add(setupSentryAuthorizable(authorizableHierarchy));
    }
    TListSentryPrivilegesByAuthRequest request = new TListSentryPrivilegesByAuthRequest(
        ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName,
        authSet);
    if (groups != null) {
      request.setGroups(groups);
    }
    if (roleSet != null) {
      request.setRoleSet(new TSentryActiveRoleSet(roleSet.isAll(), roleSet.getRoles()));
    }

    try {
      TListSentryPrivilegesByAuthResponse response = client
          .list_sentry_privileges_by_authorizable(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getPrivilegesMapByAuth();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Returns the configuration value in the sentry server associated with
   * propertyName, or if propertyName does not exist, the defaultValue.
   * There is no "requestorUserName" because this is regarded as an
   * internal interface.
   * @param propertyName Config attribute to search for
   * @param defaultValue String to return if not found
   * @return The value of the propertyName
   * @throws SentryUserException
   */
  public String getConfigValue(String propertyName, String defaultValue)
          throws SentryUserException {
    TSentryConfigValueRequest request = new TSentryConfigValueRequest(
            ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, propertyName);
    if (defaultValue != null) {
      request.setDefaultValue(defaultValue);
    }
    try {
      TSentryConfigValueResponse response = client.get_sentry_config_value(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getValue();
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