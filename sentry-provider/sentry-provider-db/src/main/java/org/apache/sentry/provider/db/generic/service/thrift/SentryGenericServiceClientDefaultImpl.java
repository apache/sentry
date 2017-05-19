/**
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
package org.apache.sentry.provider.db.generic.service.thrift;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.transport.SentryPolicyClientTransportConfig;
import org.apache.sentry.core.common.transport.SentryServiceClient;
import org.apache.sentry.core.common.transport.SentryTransportFactory;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.sentry_common_serviceConstants;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;

import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Sentry Generic Service Client
 * <p>
 * The public implementation of SentryGenericServiceClient.
 * TODO(kalyan) A Sentry Client in which all the operations are synchronized for thread safety
 * Note: When using this client, if there is an exception in RPC, socket can get into an inconsistent state.
 * So it is important to close and re-open the transportFactory so that new socket is used.
 */

public class SentryGenericServiceClientDefaultImpl implements SentryGenericServiceClient, SentryServiceClient {
  private SentryGenericPolicyService.Client client;
  private SentryTransportFactory transportFactory;
  private TTransport transport;
  private Configuration conf;
  private static final Logger LOGGER = LoggerFactory
    .getLogger(SentryGenericServiceClientDefaultImpl.class);
  private static final String THRIFT_EXCEPTION_MESSAGE = "Thrift exception occured ";

  public SentryGenericServiceClientDefaultImpl(Configuration conf, SentryPolicyClientTransportConfig transportConfig) throws IOException {
    transportFactory = new SentryTransportFactory(conf, transportConfig);
    this.conf = conf;
  }

  /**
   * Connect to the specified server configured
   *
   * @throws IOException
   */
  @Override
  public synchronized void connect() throws IOException {
    if (transport != null && transport.isOpen()) {
      return;
    }

    transport = transportFactory.getTransport();
    TMultiplexedProtocol protocol = null;
    long maxMessageSize = conf.getLong(ServiceConstants.ClientConfig.SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE,
      ServiceConstants.ClientConfig.SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE_DEFAULT);
    protocol = new TMultiplexedProtocol(
      new TBinaryProtocol(transport, maxMessageSize, maxMessageSize, true, true),
      SentryGenericPolicyProcessor.SENTRY_GENERIC_SERVICE_NAME);
    client = new SentryGenericPolicyService.Client(protocol);
    LOGGER.debug("Successfully created client");
  }

  /**
   * Create a sentry role
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:          Name of the role
   * @param component:         The request is issued to which component
   * @throws SentryUserException
   */
  @Override
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

  @Override
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
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:          Name of the role
   * @param component:         The request is issued to which component
   * @throws SentryUserException
   */
  @Override
  public void dropRole(String requestorUserName,
                       String roleName, String component)
    throws SentryUserException {
    dropRole(requestorUserName, roleName, component, false);
  }

  @Override
  public void dropRoleIfExists(String requestorUserName,
                               String roleName, String component)
    throws SentryUserException {
    dropRole(requestorUserName, roleName, component, true);
  }

  private void dropRole(String requestorUserName,
                        String roleName, String component, boolean ifExists)
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
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:          Name of the role
   * @param component:         The request is issued to which component
   * @param groups:            The name of groups
   * @throws SentryUserException
   */
  @Override
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
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:          Name of the role
   * @param component:         The request is issued to which component
   * @param groups:            The name of groups
   * @throws SentryUserException
   */
  @Override
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
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:          Name of the role
   * @param component:         The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  @Override
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
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:          Name of the role
   * @param component:         The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  @Override
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
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param component:         The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  @Override
  public void dropPrivilege(String requestorUserName, String component,
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
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param component:         The request is issued to which component
   * @param serviceName:       The Authorizable belongs to which service
   * @param oldAuthorizables
   * @param newAuthorizables
   * @throws SentryUserException
   */
  @Override
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
   *
   * @param requestorUserName : user on whose behalf the request is issued
   * @param groupName         : groupName to look up ( if null returns all roles for groups related to requestorUserName)
   * @param component:        The request is issued to which component
   * @return Set of thrift sentry role objects
   * @throws SentryUserException
   */
  @Override
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

  @Override
  public Set<TSentryRole> listUserRoles(String requestorUserName, String component)
    throws SentryUserException {
    return listRolesByGroupName(requestorUserName, AccessConstants.ALL, component);
  }

  @Override
  public Set<TSentryRole> listAllRoles(String requestorUserName, String component)
    throws SentryUserException {
    return listRolesByGroupName(requestorUserName, null, component);
  }

  /**
   * Gets sentry privileges for a given roleName and Authorizable Hirerchys using the Sentry service
   *
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:
   * @param component:         The request is issued to which component
   * @param serviceName
   * @param authorizables
   * @return
   * @throws SentryUserException
   */
  @Override
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

  @Override
  public Set<TSentryPrivilege> listPrivilegesByRoleName(
    String requestorUserName, String roleName, String component,
    String serviceName) throws SentryUserException {
    return listPrivilegesByRoleName(requestorUserName, roleName, component, serviceName, null);
  }

  /**
   * get sentry permissions from provider as followings:
   *
   * @throws SentryUserException
   * @param: component: The request is issued to which component
   * @param: serviceName: The privilege belongs to which service
   * @param: roleSet
   * @param: groupNames
   * @param: the authorizables
   * @returns the set of permissions
   */
  @Override
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
   * @param component:         The request respond to which component.
   * @param serviceName:       The name of service.
   * @param requestorUserName: The requestor user name.
   * @param authorizablesSet:  The set of authorize objects. One authorize object is represented
   *                           as a string. e.g resourceType1=resourceName1->resourceType2=resourceName2->resourceType3=resourceName3.
   * @param groups:            The requested groups.
   * @param roleSet:           The active roles set.
   * @throws SentryUserException
   * @returns The mapping of authorize objects and TSentryPrivilegeMap(<role, set<privileges>).
   */
  @Override
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
  public synchronized void close() {
    transportFactory.close();
  }

  @Override
  public void disconnect() {
    transportFactory.releaseTransport();
  }
}
