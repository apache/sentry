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
package org.apache.sentry.provider.db.generic.service.thrift;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.*;

import javax.security.auth.callback.CallbackHandler;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.sentry_common_serviceConstants;
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
import com.google.common.collect.Lists;

public class SentryGenericServiceClientDefaultImpl implements SentryGenericServiceClient {
  private final Configuration conf;
  private final InetSocketAddress serverAddress;
  private final boolean kerberos;
  private final String[] serverPrincipalParts;
  private SentryGenericPolicyService.Client client;
  private TTransport transport;
  private int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory
                                       .getLogger(SentryGenericServiceClientDefaultImpl.class);
  private static final String THRIFT_EXCEPTION_MESSAGE = "Thrift exception occured ";

  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  public static class UgiSaslClientTransport extends TSaslClientTransport {
    protected UserGroupInformation ugi = null;

    public UgiSaslClientTransport(String mechanism, String authorizationId,
        String protocol, String serverName, Map<String, String> props,
        CallbackHandler cbh, TTransport transport, boolean wrapUgi, Configuration conf)
        throws IOException {
      super(mechanism, authorizationId, protocol, serverName, props, cbh,
          transport);
      if (wrapUgi) {
       // If we don't set the configuration, the UGI will be created based on
       // what's on the classpath, which may lack the kerberos changes we require
        UserGroupInformation.setConfiguration(conf);
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

  public SentryGenericServiceClientDefaultImpl(Configuration conf) throws IOException {
    // copy the configuration because we may make modifications to it.
    this.conf = new Configuration(conf);
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
      // since the client uses hadoop-auth, we need to set kerberos in
      // hadoop-auth if we plan to use kerberos
      conf.set(HADOOP_SECURITY_AUTHENTICATION, ServerConfig.SECURITY_MODE_KERBEROS);

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
          ClientConfig.SASL_PROPERTIES, null, transport, wrapUgi, conf);
    } else {
      serverPrincipalParts = null;
    }
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException("Transport exception while opening transport: " + e.getMessage(), e);
    }
    LOGGER.debug("Successfully opened transport: " + transport + " to " + serverAddress);
    long maxMessageSize = conf.getLong(ServiceConstants.ClientConfig.SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE,
        ServiceConstants.ClientConfig.SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE_DEFAULT);
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
        new TBinaryProtocol(transport, maxMessageSize, maxMessageSize, true, true),
        SentryGenericPolicyProcessor.SENTRY_GENERIC_SERVICE_NAME);
    client = new SentryGenericPolicyService.Client(protocol);
    LOGGER.debug("Successfully created client");
  }



  /**
   * Create a sentry role
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @throws SentryUserException
   */
  public synchronized void createRole(String requestorUserName, String roleName, String component)
  throws SentryUserException {
    TCreateSentryRoleRequest request = new TCreateSentryRoleRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setComponent(component);
    try {
      TCreateSentryRoleResponse response = client.create_sentry_role(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public void createRoleIfNotExist(String requestorUserName, String roleName, String component) throws SentryUserException {
    TCreateSentryRoleRequest request = new TCreateSentryRoleRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setComponent(component);
    try {
      TCreateSentryRoleResponse response = client.create_sentry_role(request);
      Status status = Status.fromCode(response.getStatus().getValue());
      if (status == Status.ALREADY_EXISTS) {
        return;
      }
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Drop a sentry role
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @throws SentryUserException
   */
  public void dropRole(String requestorUserName,
      String roleName, String component)
  throws SentryUserException {
    dropRole(requestorUserName, roleName, component, false);
  }

  public void dropRoleIfExists(String requestorUserName,
      String roleName, String component)
  throws SentryUserException {
    dropRole(requestorUserName, roleName, component, true);
  }

  private void dropRole(String requestorUserName,
      String roleName, String component , boolean ifExists)
  throws SentryUserException {
    TDropSentryRoleRequest request = new TDropSentryRoleRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setComponent(component);
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
   * add a sentry role to groups.
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param groups: The name of groups
   * @throws SentryUserException
   */
  public void addRoleToGroups(String requestorUserName, String roleName,
      String component, Set<String> groups) throws SentryUserException {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setGroups(groups);
    request.setComponent(component);

    try {
      TAlterSentryRoleAddGroupsResponse response = client.alter_sentry_role_add_groups(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * delete a sentry role from groups.
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param groups: The name of groups
   * @throws SentryUserException
   */
  public void deleteRoleToGroups(String requestorUserName, String roleName,
      String component, Set<String> groups) throws SentryUserException {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setGroups(groups);
    request.setComponent(component);

    try {
      TAlterSentryRoleDeleteGroupsResponse response = client.alter_sentry_role_delete_groups(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * grant privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  public void grantPrivilege(String requestorUserName, String roleName,
      String component, TSentryPrivilege privilege) throws SentryUserException {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setComponent(component);
    request.setRoleName(roleName);
    request.setRequestorUserName(requestorUserName);
    request.setPrivilege(privilege);

    try {
      TAlterSentryRoleGrantPrivilegeResponse response = client.alter_sentry_role_grant_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * revoke privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  public void revokePrivilege(String requestorUserName, String roleName,
      String component, TSentryPrivilege privilege) throws SentryUserException {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setComponent(component);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setPrivilege(privilege);

    try {
      TAlterSentryRoleRevokePrivilegeResponse response = client.alter_sentry_role_revoke_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * drop privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param component: The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  public void dropPrivilege(String requestorUserName,String component,
      TSentryPrivilege privilege) throws SentryUserException {
    TDropPrivilegesRequest request = new TDropPrivilegesRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setComponent(component);
    request.setRequestorUserName(requestorUserName);
    request.setPrivilege(privilege);

    try {
      TDropPrivilegesResponse response = client.drop_sentry_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * rename privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param component: The request is issued to which component
   * @param serviceName: The Authorizable belongs to which service
   * @param oldAuthorizables
   * @param newAuthorizables
   * @throws SentryUserException
   */
  public void renamePrivilege(String requestorUserName, String component,
      String serviceName, List<? extends Authorizable> oldAuthorizables,
      List<? extends Authorizable> newAuthorizables) throws SentryUserException {
    if ((oldAuthorizables == null) || (oldAuthorizables.size() == 0)
        || (newAuthorizables == null) || (newAuthorizables.size() == 0)) {
      throw new SentryUserException("oldAuthorizables and newAuthorizables can't be null or empty");
    }

    TRenamePrivilegesRequest request = new TRenamePrivilegesRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setComponent(component);
    request.setRequestorUserName(requestorUserName);
    request.setServiceName(serviceName);

    List<TAuthorizable> oldTAuthorizables = Lists.newArrayList();
    List<TAuthorizable> newTAuthorizables = Lists.newArrayList();
    for (Authorizable authorizable : oldAuthorizables) {
      oldTAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
      request.setOldAuthorizables(oldTAuthorizables);
    }
    for (Authorizable authorizable : newAuthorizables) {
      newTAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
      request.setNewAuthorizables(newTAuthorizables);
    }

    try {
      TRenamePrivilegesResponse response = client.rename_sentry_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Gets sentry role objects for a given groupName using the Sentry service
   * @param requestorUserName : user on whose behalf the request is issued
   * @param groupName : groupName to look up ( if null returns all roles for groups related to requestorUserName)
   * @param component: The request is issued to which component
   * @return Set of thrift sentry role objects
   * @throws SentryUserException
   */
  public synchronized Set<TSentryRole> listRolesByGroupName(
      String requestorUserName,
      String groupName,
      String component)
  throws SentryUserException {
    TListSentryRolesRequest request = new TListSentryRolesRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setRequestorUserName(requestorUserName);
    request.setGroupName(groupName);
    request.setComponent(component);
    TListSentryRolesResponse response;
    try {
      response = client.list_sentry_roles_by_group(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getRoles();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public Set<TSentryRole> listUserRoles(String requestorUserName, String component)
      throws SentryUserException {
    return listRolesByGroupName(requestorUserName, AccessConstants.ALL, component);
  }

  public Set<TSentryRole> listAllRoles(String requestorUserName, String component)
      throws SentryUserException {
    return listRolesByGroupName(requestorUserName, null, component);
  }

  /**
   * Gets sentry privileges for a given roleName and Authorizable Hirerchys using the Sentry service
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:
   * @param component: The request is issued to which component
   * @param serviceName
   * @param authorizables
   * @return
   * @throws SentryUserException
   */
  public Set<TSentryPrivilege> listPrivilegesByRoleName(
      String requestorUserName, String roleName, String component,
      String serviceName, List<? extends Authorizable> authorizables)
      throws SentryUserException {
    TListSentryPrivilegesRequest request = new TListSentryPrivilegesRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setComponent(component);
    request.setServiceName(serviceName);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    if ((authorizables != null) && (authorizables.size() > 0)) {
      List<TAuthorizable> tAuthorizables = Lists.newArrayList();
      for (Authorizable authorizable : authorizables) {
        tAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
      }
      request.setAuthorizables(tAuthorizables);
    }

    TListSentryPrivilegesResponse response;
    try {
      response = client.list_sentry_privileges_by_role(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
    return response.getPrivileges();
  }

  public Set<TSentryPrivilege> listPrivilegesByRoleName(
      String requestorUserName, String roleName, String component,
      String serviceName) throws SentryUserException {
    return listPrivilegesByRoleName(requestorUserName, roleName, component, serviceName, null);
  }

  /**
   * get sentry permissions from provider as followings:
   * @param: component: The request is issued to which component
   * @param: serviceName: The privilege belongs to which service
   * @param: roleSet
   * @param: groupNames
   * @param: the authorizables
   * @returns the set of permissions
   * @throws SentryUserException
   */
  public Set<String> listPrivilegesForProvider(String component,
      String serviceName, ActiveRoleSet roleSet, Set<String> groups,
      List<? extends Authorizable> authorizables) throws SentryUserException {
    TSentryActiveRoleSet thriftRoleSet = new TSentryActiveRoleSet(roleSet.isAll(), roleSet.getRoles());
    TListSentryPrivilegesForProviderRequest request = new TListSentryPrivilegesForProviderRequest();
    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setComponent(component);
    request.setServiceName(serviceName);
    request.setRoleSet(thriftRoleSet);
    if (groups == null) {
      request.setGroups(new HashSet<String>());
    } else {
      request.setGroups(groups);
    }
    List<TAuthorizable> tAuthoriables = Lists.newArrayList();
    if ((authorizables != null) && (authorizables.size() > 0)) {
      for (Authorizable authorizable : authorizables) {
        tAuthoriables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
      }
      request.setAuthorizables(tAuthoriables);
    }

    try {
      TListSentryPrivilegesForProviderResponse response = client.list_sentry_privileges_for_provider(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getPrivileges();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Get sentry privileges based on valid active roles and the authorize objects. Note that
   * it is client responsibility to ensure the requestor username, etc. is not impersonated.
   *
   * @param component: The request respond to which component.
   * @param serviceName: The name of service.
   * @param requestorUserName: The requestor user name.
   * @param authorizablesSet: The set of authorize objects. One authorize object is represented
   *     as a string. e.g resourceType1=resourceName1->resourceType2=resourceName2->resourceType3=resourceName3.
   * @param groups: The requested groups.
   * @param roleSet: The active roles set.
   *
   * @returns The mapping of authorize objects and TSentryPrivilegeMap(<role, set<privileges>).
   * @throws SentryUserException
   */
  public Map<String, TSentryPrivilegeMap> listPrivilegsbyAuthorizable(String component,
      String serviceName, String requestorUserName, Set<String> authorizablesSet,
      Set<String> groups, ActiveRoleSet roleSet) throws SentryUserException {

    TListSentryPrivilegesByAuthRequest request = new TListSentryPrivilegesByAuthRequest();

    request.setProtocol_version(sentry_common_serviceConstants.TSENTRY_SERVICE_V2);
    request.setComponent(component);
    request.setServiceName(serviceName);
    request.setRequestorUserName(requestorUserName);
    request.setAuthorizablesSet(authorizablesSet);

    if (groups == null) {
      request.setGroups(new HashSet<String>());
    } else {
      request.setGroups(groups);
    }

    if (roleSet != null) {
      request.setRoleSet(new TSentryActiveRoleSet(roleSet.isAll(), roleSet.getRoles()));
    }

    try {
      TListSentryPrivilegesByAuthResponse response = client.list_sentry_privileges_by_authorizable(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getPrivilegesMapByAuth();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public void close() {
    if (transport != null) {
      transport.close();
    }
  }

}
