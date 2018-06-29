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

package org.apache.sentry.api.service.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.common.ThriftConstants;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.transport.SentryConnection;
import org.apache.sentry.core.common.transport.SentryTransportPool;
import org.apache.sentry.core.common.transport.TTransportWrapper;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.api.common.ApiConstants.ClientConfig;
import org.apache.sentry.api.common.ApiConstants.SentryPolicyServiceConstants;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.SentryPolicyService.Client;
import org.apache.sentry.api.common.SentryServiceUtil;
import org.apache.sentry.api.common.Status;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client implementation for Policy (HMS) clients.
 * <p>
 * The class is not thread-safe - it is up to the callers to ensure thread safety
 */
public class SentryPolicyServiceClientDefaultImpl implements SentryPolicyServiceClient, SentryConnection {

  private Client client;
  private final SentryTransportPool transportPool;
  private TTransportWrapper transport;
  private final long maxMessageSize;

  private static final String THRIFT_EXCEPTION_MESSAGE = "Thrift exception occurred ";

  /**
   * Initialize the sentry configurations.
   */
  public SentryPolicyServiceClientDefaultImpl(Configuration conf,
                                              SentryTransportPool transportPool)
    throws IOException {
    maxMessageSize = conf.getLong(ClientConfig.SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE,
            ClientConfig.SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE_DEFAULT);
    this.transportPool = transportPool;
  }

  /**
   * Sets the Client object which is usually a mock object of the Client class used for testing.
   * @param client
   */
  @VisibleForTesting
  void setClient(Client client) {
    this.client = client;
  }

  /**
   * Connect to the sentry server
   *
   * @throws IOException
   */
  @Override
  public void connect() throws Exception {
    if ((transport != null) && transport.isOpen()) {
      return;
    }

    transport = transportPool.getTransport();
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
      new TBinaryProtocol(transport.getTTransport(), maxMessageSize, maxMessageSize,
              true, true),
      SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME);
    client = new Client(protocol);
  }

  @Override
  public void createRole(String requestorUserName, String roleName)
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

  @Override
  public void dropRole(String requestorUserName,
                                    String roleName)
    throws SentryUserException {
    dropRole(requestorUserName, roleName, false);
  }

  @Override
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
   *
   * @param requestorUserName : user on whose behalf the request is issued
   * @param groupName         : groupName to look up ( if null returns all roles for all groups)
   * @return Set of thrift sentry role objects
   * @throws SentryUserException
   */
  @Override
  public Set<TSentryRole> listRolesByGroupName(
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
      Status status = Status.fromCode(response.getStatus().getValue());
      if (status == Status.NO_SUCH_OBJECT) {
        return Collections.emptySet();
      }
      Status.throwIfNotOk(response.getStatus());
      return response.getRoles();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Gets sentry role objects for a given userName using the Sentry service
   *
   * @param requestorUserName : user on whose behalf the request is issued
   * @param userName          : userName to look up (can't be empty)
   * @return Set of thrift sentry role objects
   * @throws SentryUserException
   */
  @Override
  public Set<TSentryRole> listRolesByUserName(String requestorUserName, String userName)
    throws SentryUserException {
    TListSentryRolesForUserRequest request = new TListSentryRolesForUserRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setUserName(userName);
    TListSentryRolesResponse response;
    try {
      response = client.list_sentry_roles_by_user(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getRoles();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public Set<TSentryPrivilege> listAllPrivilegesByRoleName(String requestorUserName,
                                                                        String roleName)
    throws SentryUserException {
    return listPrivilegesByRoleName(requestorUserName, roleName, null);
  }

  /**
   * Gets sentry privilege objects for a given roleName using the Sentry service
   *
   * @param requestorUserName : user on whose behalf the request is issued
   * @param roleName          : roleName to look up
   * @param authorizable      : authorizable Hierarchy (server->db->table etc)
   * @return Set of thrift sentry privilege objects
   * @throws SentryUserException
   */
  @Override
  public Set<TSentryPrivilege> listPrivilegesByRoleName(String requestorUserName,
                                                                     String roleName, List<? extends Authorizable> authorizable)
    throws SentryUserException {
    TListSentryPrivilegesRequest request = new TListSentryPrivilegesRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);

    // TODO: Switch from setRoleName() to setEntityName()
    // The 'roleName' parameter is deprecated in Sentry 2.x, but it is still required by older
    // versions of Sentry 2.0. To keep compatibility when connecting to older versions of Sentry 2.x,
    // then we'll use this parameter, but it will be switched for setEntityName once the roleName
    // is removed.
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

  @Override
  public Set<TSentryPrivilege> listAllPrivilegesByUserName(String requestorUserName,
                                                                    String userName)
    throws SentryUserException {
    return listPrivilegesByUserName(requestorUserName, userName, null);
  }

  @Override
  public Set<TSentryPrivilege> listPrivilegesByUserName(String requestorUserName, String userName,
                                                                  List<? extends Authorizable> authorizable)
    throws SentryUserException {
    TListSentryPrivilegesRequest request = new TListSentryPrivilegesRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    // TODO: Remove setRoleName() once the required field is removed
    request.setRoleName(""); // roleName is unused by it is required by Thrift
    request.setEntityName(userName);
    if (authorizable != null && !authorizable.isEmpty()) {
      TSentryAuthorizable tSentryAuthorizable = setupSentryAuthorizable(authorizable);
      request.setAuthorizableHierarchy(tSentryAuthorizable);
    }
    TListSentryPrivilegesResponse response;
    try {
      response = client.list_sentry_privileges_by_user(request);
      if (response == null) {
        throw new SentryUserException("The Sentry server has returned a NULL response. "
          + "See the Sentry server logs for more information about the error.");
      }

      Status.throwIfNotOk(response.getStatus());
      return response.getPrivileges();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public Set<TSentryRole> listAllRoles(String requestorUserName)
    throws SentryUserException {
    return listRolesByGroupName(requestorUserName, null);
  }

  @Override
  public Set<TSentryRole> listUserRoles(String requestorUserName)
    throws SentryUserException {
    Set<TSentryRole> tSentryRoles = Sets.newHashSet();
    tSentryRoles.addAll(listRolesByGroupName(requestorUserName, AccessConstants.ALL));
    tSentryRoles.addAll(listRolesByUserName(requestorUserName, requestorUserName));
    return tSentryRoles;
  }

  @Override
  public TSentryPrivilege grantURIPrivilege(String requestorUserName,
                                                         String roleName, String server, String uri)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
      PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL);
  }

  @Override
  public TSentryPrivilege grantURIPrivilege(String requestorUserName,
                                                         String roleName, String server, String uri, Boolean grantOption)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
      PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL, grantOption);
  }

  @Override
  public void grantServerPrivilege(String requestorUserName,
                                                String roleName, String server, String action)
    throws SentryUserException {

    // "ALL" and "*" should be synonyms for action and need to be unified with grantServerPrivilege without
    // action explicitly specified.
    if (AccessConstants.ACTION_ALL.equalsIgnoreCase(action) || AccessConstants.ALL.equals(action)) {
      action = AccessConstants.ALL;
    }

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

  @Override
  public TSentryPrivilege grantServerPrivilege(String requestorUserName,
                                                            String roleName, String server, String action, Boolean grantOption)
    throws SentryUserException {

    // "ALL" and "*" should be synonyms for action and need to be unified with grantServerPrivilege without
    // action explicitly specified.
    if (AccessConstants.ACTION_ALL.equalsIgnoreCase(action) || AccessConstants.ALL.equals(action)) {
      action = AccessConstants.ALL;
    }

    return grantPrivilege(requestorUserName, roleName,
      PrivilegeScope.SERVER, server, null, null, null, null, action, grantOption);
  }

  @Override
  public TSentryPrivilege grantDatabasePrivilege(String requestorUserName,
                                                              String roleName, String server, String db, String action)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
      PrivilegeScope.DATABASE, server, null, db, null, null, action);
  }

  @Override
  public TSentryPrivilege grantDatabasePrivilege(String requestorUserName,
                                                              String roleName, String server, String db, String action, Boolean grantOption)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName,
      PrivilegeScope.DATABASE, server, null, db, null, null, action, grantOption);
  }

  @Override
  public TSentryPrivilege grantTablePrivilege(String requestorUserName,
                                                           String roleName, String server, String db, String table, String action)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.TABLE, server,
      null,
      db, table, null, action);
  }

  @Override
  public TSentryPrivilege grantTablePrivilege(String requestorUserName,
                                                           String roleName, String server, String db, String table, String action, Boolean grantOption)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.TABLE, server,
      null, db, table, null, action, grantOption);
  }

  @Override
  public TSentryPrivilege grantColumnPrivilege(String requestorUserName,
                                                            String roleName, String server, String db, String table, String columnName, String action)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.COLUMN, server,
      null,
      db, table, columnName, action);
  }

  @Override
  public TSentryPrivilege grantColumnPrivilege(String requestorUserName,
                                                            String roleName, String server, String db, String table, String columnName, String action, Boolean grantOption)
    throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, PrivilegeScope.COLUMN, server,
      null, db, table, columnName, action, grantOption);
  }

  @Override
  public Set<TSentryPrivilege> grantColumnsPrivileges(String requestorUserName,
                                                                   String roleName, String server, String db, String table, List<String> columnNames, String action)
    throws SentryUserException {
    return grantPrivileges(requestorUserName, roleName, PrivilegeScope.COLUMN, server,
      null,
      db, table, columnNames, action);
  }

  @Override
  public Set<TSentryPrivilege> grantColumnsPrivileges(String requestorUserName,
                                                                   String roleName, String server, String db, String table, List<String> columnNames, String action, Boolean grantOption)
    throws SentryUserException {
    return grantPrivileges(requestorUserName, roleName, PrivilegeScope.COLUMN,
      server,
      null, db, table, columnNames, action, grantOption);
  }

  @Override
  public Set<TSentryPrivilege> grantPrivileges(
    String requestorUserName, String roleName,
    Set<TSentryPrivilege> privileges) throws SentryUserException {
    return grantPrivilegesCore(requestorUserName, roleName, privileges);
  }

  @Override
  public TSentryPrivilege grantPrivilege(String requestorUserName, String roleName,
                                                      TSentryPrivilege privilege) throws SentryUserException {
    return grantPrivilegeCore(requestorUserName, roleName, privilege);
  }

  private TSentryPrivilege grantPrivilegeCore(String requestorUserName, String roleName,
                                              TSentryPrivilege privilege) throws SentryUserException {
    Set<TSentryPrivilege> results =
      grantPrivilegesCore(requestorUserName, roleName, ImmutableSet.of(privilege));
    if (results != null && results.size() > 0) {
      return results.iterator().next();
    } else {
      return new TSentryPrivilege();
    }
  }

  private Set<TSentryPrivilege> grantPrivilegesCore(String requestorUserName, String roleName,
                                                    Set<TSentryPrivilege> privileges) throws SentryUserException {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setPrivileges(privileges);
    try {
      TAlterSentryRoleGrantPrivilegeResponse response =
        client.alter_sentry_role_grant_privilege(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getPrivileges();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
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
                                          String table, String column, String action) throws SentryUserException {
    return grantPrivilege(requestorUserName, roleName, scope, serverName, uri,
      db, table, column, action, false);
  }

  private TSentryPrivilege grantPrivilege(String requestorUserName,

                                          String roleName, PrivilegeScope scope, String serverName, String uri, String db, String table,
                                          String column, String action, Boolean grantOption)
    throws SentryUserException {
    TSentryPrivilege privilege =
      convertToTSentryPrivilege(scope, serverName, uri, db, table, column, action, grantOption);
    return grantPrivilegeCore(requestorUserName, roleName, privilege);
  }

  private Set<TSentryPrivilege> grantPrivileges(String requestorUserName,
                                                String roleName,
                                                PrivilegeScope scope, String serverName, String uri, String db,
                                                String table, List<String> columns, String action) throws SentryUserException {
    return grantPrivileges(requestorUserName, roleName, scope, serverName, uri,
      db, table, columns, action, false);
  }

  private Set<TSentryPrivilege> grantPrivileges(String requestorUserName,
                                                String roleName, PrivilegeScope scope, String serverName, String uri, String db, String
                                                  table,
                                                List<String> columns, String action, Boolean grantOption)
    throws SentryUserException {
    Set<TSentryPrivilege> privileges = convertColumnPrivileges(scope,
      serverName, uri, db, table, columns, action, grantOption);
    return grantPrivilegesCore(requestorUserName, roleName, privileges);
  }

  @Override
  public void revokePrivileges(String requestorUserName, String roleName, Set<TSentryPrivilege> privileges) throws SentryUserException {
    this.revokePrivilegesCore(requestorUserName, roleName, privileges);
  }

  @Override
  public void revokePrivilege(String requestorUserName, String roleName, TSentryPrivilege privilege) throws SentryUserException {
    this.revokePrivilegeCore(requestorUserName, roleName, privilege);

  }

  private void revokePrivilegeCore(String requestorUserName, String roleName, TSentryPrivilege privilege) throws SentryUserException {
    this.revokePrivilegesCore(requestorUserName, roleName, ImmutableSet.of(privilege));
  }

  private void revokePrivilegesCore(String requestorUserName, String roleName, Set<TSentryPrivilege> privileges) throws SentryUserException {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    request.setPrivileges(privileges);
    try {
      TAlterSentryRoleRevokePrivilegeResponse response = client.alter_sentry_role_revoke_privilege(
        request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public void revokeURIPrivilege(String requestorUserName,
                                              String roleName, String server, String uri)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL);
  }

  @Override
  public void revokeURIPrivilege(String requestorUserName,
                                              String roleName, String server, String uri, Boolean grantOption)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.URI, server, uri, null, null, null, AccessConstants.ALL, grantOption);
  }

  @Override
  public void revokeServerPrivilege(String requestorUserName,
                                                 String roleName, String server, String action)
    throws SentryUserException {

    // "ALL" and "*" should be synonyms for action and need to be unified with revokeServerPrivilege without
    // action explicitly specified.
    if (AccessConstants.ACTION_ALL.equalsIgnoreCase(action) || AccessConstants.ALL.equals(action)) {
      action = AccessConstants.ALL;
    }

    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.SERVER, server, null, null, null, null, action);
  }

  public void revokeServerPrivilege(String requestorUserName,
                                                 String roleName, String server, String action, Boolean grantOption)
    throws SentryUserException {

    // "ALL" and "*" should be synonyms for action and need to be unified with revokeServerPrivilege without
    // action explicitly specified.
    if (AccessConstants.ACTION_ALL.equalsIgnoreCase(action) || AccessConstants.ALL.equals(action)) {
      action = AccessConstants.ALL;
    }

    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.SERVER, server, null, null, null, null, action, grantOption);
  }

  @Deprecated
  /***
   * Should use revokeServerPrivilege(String requestorUserName,
   *  String roleName, String server, String action, Boolean grantOption)
   */
  @Override
  public void revokeServerPrivilege(String requestorUserName,
                                                 String roleName, String server, boolean grantOption)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.SERVER, server, null, null, null, null, AccessConstants.ALL, grantOption);
  }

  @Override
  public void revokeDatabasePrivilege(String requestorUserName,
                                                   String roleName, String server, String db, String action)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.DATABASE, server, null, db, null, null, action);
  }

  @Override
  public void revokeDatabasePrivilege(String requestorUserName,
                                                   String roleName, String server, String db, String action, Boolean grantOption)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.DATABASE, server, null, db, null, null, action, grantOption);
  }

  @Override
  public void revokeTablePrivilege(String requestorUserName,
                                                String roleName, String server, String db, String table, String action)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.TABLE, server, null,
      db, table, null, action);
  }

  @Override
  public void revokeTablePrivilege(String requestorUserName,
                                                String roleName, String server, String db, String table, String action, Boolean grantOption)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.TABLE, server, null,
      db, table, null, action, grantOption);
  }

  @Override
  public void revokeColumnPrivilege(String requestorUserName, String roleName,
                                                 String server, String db, String table, String columnName, String action)
    throws SentryUserException {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    listBuilder.add(columnName);
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.COLUMN, server, null,
      db, table, listBuilder.build(), action);
  }

  @Override
  public void revokeColumnPrivilege(String requestorUserName, String roleName,
                                                 String server, String db, String table, String columnName, String action, Boolean grantOption)
    throws SentryUserException {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    listBuilder.add(columnName);
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.COLUMN, server, null,
      db, table, listBuilder.build(), action, grantOption);
  }

  @Override
  public void revokeColumnsPrivilege(String requestorUserName, String roleName,
                                                  String server, String db, String table, List<String> columns, String action)
    throws SentryUserException {
    revokePrivilege(requestorUserName, roleName,
      PrivilegeScope.COLUMN, server, null,
      db, table, columns, action);
  }

  @Override
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
    Set<TSentryPrivilege> privileges = convertColumnPrivileges(scope,
      serverName, uri, db, table, columns, action, grantOption);
    this.revokePrivilegesCore(requestorUserName, roleName, privileges);
  }

  private Set<TSentryPrivilege> convertColumnPrivileges(
    PrivilegeScope scope, String serverName, String uri, String db, String
    table, List<String> columns,
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

  private TSentryPrivilege convertToTSentryPrivilege(
    PrivilegeScope scope, String serverName, String uri, String db, String table, String
    column,
    String action, Boolean grantOption) {
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
    return privilege;
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

  @Override
  public Set<String> listPrivilegesForProvider
    (Set<String> groups, Set<String> users,
     ActiveRoleSet roleSet, Authorizable... authorizable) throws SentryUserException {
    TSentryActiveRoleSet thriftRoleSet = new TSentryActiveRoleSet(roleSet.isAll(), roleSet.getRoles());
    TListSentryPrivilegesForProviderRequest request =
      new TListSentryPrivilegesForProviderRequest(ThriftConstants.
        TSENTRY_SERVICE_VERSION_CURRENT, groups, thriftRoleSet);
    if (authorizable != null && authorizable.length > 0) {
      TSentryAuthorizable tSentryAuthorizable = setupSentryAuthorizable(Lists
        .newArrayList(authorizable));
      request.setAuthorizableHierarchy(tSentryAuthorizable);
    }
    if (users != null) {
      request.setUsers(users);
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
  public void grantRoleToGroup(String requestorUserName,
                                            String groupName, String roleName)
    throws SentryUserException {
    grantRoleToGroups(requestorUserName, roleName, Sets.newHashSet(groupName));
  }

  @Override
  public void revokeRoleFromGroup(String requestorUserName,
                                               String groupName, String roleName)
    throws SentryUserException {
    revokeRoleFromGroups(requestorUserName, roleName, Sets.newHashSet(groupName));
  }

  @Override
  public void grantRoleToGroups(String requestorUserName,
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
  public void revokeRoleFromGroups(String requestorUserName,
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

  @Override
  public void grantRoleToUser(String requestorUserName, String userName,
                                           String roleName) throws SentryUserException {
    grantRoleToUsers(requestorUserName, roleName, Sets.newHashSet(userName));
  }

  @Override
  public void revokeRoleFromUser(String requestorUserName, String userName,
                                              String roleName) throws SentryUserException {
    revokeRoleFromUsers(requestorUserName, roleName, Sets.newHashSet(userName));
  }

  @Override
  public void grantRoleToUsers(String requestorUserName, String roleName,
                                            Set<String> users) throws SentryUserException {
    TAlterSentryRoleAddUsersRequest request = new TAlterSentryRoleAddUsersRequest(
      ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName, roleName, users);
    try {
      TAlterSentryRoleAddUsersResponse response = client.alter_sentry_role_add_users(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public void revokeRoleFromUsers(String requestorUserName, String roleName,
                                               Set<String> users) throws SentryUserException {
    TAlterSentryRoleDeleteUsersRequest request = new TAlterSentryRoleDeleteUsersRequest(
      ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName, roleName, users);
    try {
      TAlterSentryRoleDeleteUsersResponse response = client.alter_sentry_role_delete_users(request);
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

  @Override
  public void dropPrivileges(String requestorUserName,
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

  @Override
  public void renamePrivileges(String requestorUserName,
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

  @Override
  public Map<TSentryAuthorizable, TSentryPrivilegeMap> listPrivilegsbyAuthorizable(
    String requestorUserName, Set<List<? extends Authorizable>> authorizables, Set<String> groups,
    ActiveRoleSet roleSet) throws SentryUserException {
    return listPrivilegsbyAuthorizable(requestorUserName, authorizables, groups, null, roleSet);
  }

  @Override
  public Map<TSentryAuthorizable, TSentryPrivilegeMap> listPrivilegsbyAuthorizable
    (
      String requestorUserName,
      Set<List<? extends Authorizable>> authorizables, Set<String> groups,
      Set<String> users, ActiveRoleSet roleSet) throws SentryUserException {

    TListSentryPrivilegesByAuthResponse response =
        getSentryPrivilegeByAuthResponse(requestorUserName, authorizables, groups, users, roleSet);

    return response.getPrivilegesMapByAuth();
  }

  @Override
  public SentryObjectPrivileges getAllPrivilegsbyAuthorizable
  (
    String requestorUserName,
    Set<List<? extends Authorizable>> authorizables, Set<String> groups,
    Set<String> users, ActiveRoleSet roleSet) throws SentryUserException {

    TListSentryPrivilegesByAuthResponse response =
        getSentryPrivilegeByAuthResponse(requestorUserName, authorizables, groups, users, roleSet);

    return new SentryObjectPrivileges(response);
  }

  public TListSentryPrivilegesByAuthResponse getSentryPrivilegeByAuthResponse(String requestorUserName,
      Set<List<? extends Authorizable>> authorizables, Set<String> groups, Set<String> users,
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
    if (users != null) {
      request.setUsers(users);
    }

    try {
      TListSentryPrivilegesByAuthResponse response = client
        .list_sentry_privileges_by_authorizable(request);
      Status.throwIfNotOk(response.getStatus());

      if(response == null) {
        throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE + ": received a NULL response while requesting for sentry privileges by authorizable");
      }
      return response;
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Returns the configuration value in the sentry server associated with
   * propertyName, or if propertyName does not exist, the defaultValue.
   * There is no "requestorUserName" because this is regarded as an
   * internal interface.
   *
   * @param propertyName Config attribute to search for
   * @param defaultValue String to return if not found
   * @return The value of the propertyName
   * @throws SentryUserException
   */

  @Override
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

  /**
   * Import the sentry mapping data, convert the mapping data from map structure to
   * TSentryMappingData, and call the import API.
   *
   * @param policyFileMappingData Include 2 maps to save the mapping data, the following is the example of the data
   *                              structure:
   *                              for the following mapping data:
   *                              group1=role1,role2
   *                              group2=role2,role3
   *                              role1=server=server1->db=db1
   *                              role2=server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2
   *                              role3=server=server1->url=hdfs://localhost/path
   *                              <p>
   *                              The policyFileMappingData will be inputed as:
   *                              {
   *                              groups={[group1={role1, role2}], group2=[role2, role3]},
   *                              roles={role1=[server=server1->db=db1],
   *                              role2=[server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2],
   *                              role3=[server=server1->url=hdfs://localhost/path]
   *                              }
   *                              }
   * @param requestorUserName     The name of the request user
   */
  @Override
  public void importPolicy
  (Map<String, Map<String, Set<String>>> policyFileMappingData,
   String requestorUserName, boolean isOverwriteRole)
    throws SentryUserException {
    try {
      TSentryMappingData tSentryMappingData = new TSentryMappingData();
      // convert the mapping data for [group,role] from map structure to
      // TSentryMappingData.GroupRolesMap
      tSentryMappingData.setGroupRolesMap(policyFileMappingData.get(PolicyFileConstants.GROUPS));
      tSentryMappingData.setUserRolesMap(policyFileMappingData.get(PolicyFileConstants.USER_ROLES));
      // convert the mapping data for [role,privilege] from map structure to
      // TSentryMappingData.RolePrivilegesMap
      tSentryMappingData
        .setRolePrivilegesMap(convertRolePrivilegesMapForSentryDB(policyFileMappingData
          .get(PolicyFileConstants.ROLES)));
      TSentryImportMappingDataRequest request = new TSentryImportMappingDataRequest(
        ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName, isOverwriteRole,
        tSentryMappingData);
      TSentryImportMappingDataResponse response = client.import_sentry_mapping_data(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  // convert the mapping data for [role,privilege] from map structure to
  // TSentryMappingData.RolePrivilegesMap
  private Map<String, Set<TSentryPrivilege>> convertRolePrivilegesMapForSentryDB(
    Map<String, Set<String>> rolePrivilegesMap) {
    Map<String, Set<TSentryPrivilege>> rolePrivilegesMapResult = Maps.newHashMap();
    if (rolePrivilegesMap != null) {
      for (Map.Entry<String, Set<String>> entry : rolePrivilegesMap.entrySet()) {
        Set<TSentryPrivilege> tempTSentryPrivileges = Sets.newHashSet();
        Set<String> tempPrivileges = entry.getValue();
        for (String tempPrivilege : tempPrivileges) {
          tempTSentryPrivileges.add(SentryServiceUtil.convertToTSentryPrivilege(tempPrivilege));
        }
        rolePrivilegesMapResult.put(entry.getKey(), tempTSentryPrivileges);
      }
    }
    return rolePrivilegesMapResult;
  }

  // export the sentry mapping data with map structure
  @Override
  public Map<String, Map<String, Set<String>>> exportPolicy(String
                                                                           requestorUserName,
                                                                         String objectPath) throws SentryUserException {
    TSentryExportMappingDataRequest request = new TSentryExportMappingDataRequest(
      ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, requestorUserName);
    request.setObjectPath(objectPath);
    try {
      TSentryExportMappingDataResponse response = client.export_sentry_mapping_data(request);
      Status.throwIfNotOk(response.getStatus());
      TSentryMappingData tSentryMappingData = response.getMappingData();
      Map<String, Map<String, Set<String>>> resultMap = Maps.newHashMap();
      resultMap.put(PolicyFileConstants.USER_ROLES, tSentryMappingData.getUserRolesMap());
      resultMap.put(PolicyFileConstants.GROUPS, tSentryMappingData.getGroupRolesMap());
      resultMap.put(PolicyFileConstants.ROLES, convertRolePrivilegesMapForPolicyFile(tSentryMappingData.getRolePrivilegesMap()));
      return resultMap;
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  // convert the mapping data for [roleName,privilege] from TSentryMappingData.RolePrivilegesMap to
  // map structure
  private Map<String, Set<String>> convertRolePrivilegesMapForPolicyFile(
    Map<String, Set<TSentryPrivilege>> rolePrivilegesMap) {
    Map<String, Set<String>> rolePrivilegesMapForFile = Maps.newHashMap();
    if (rolePrivilegesMap != null) {
      for (Map.Entry<String, Set<TSentryPrivilege>> entry : rolePrivilegesMap.entrySet()) {
        Set<TSentryPrivilege> tempSentryPrivileges = entry.getValue();
        Set<String> tempStrPrivileges = Sets.newHashSet();
        for (TSentryPrivilege tSentryPrivilege : tempSentryPrivileges) {
          // convert TSentryPrivilege to privilege in string
          String privilegeStr = SentryServiceUtil.convertTSentryPrivilegeToStr(tSentryPrivilege);
          if (!StringUtils.isEmpty(privilegeStr)) {
            tempStrPrivileges.add(privilegeStr);
          }
        }
        rolePrivilegesMapForFile.put(entry.getKey(), tempStrPrivileges);
      }
    }
    return rolePrivilegesMapForFile;
  }

  @Override
  public void close() {
    done();
  }

  @Override
  public void done() {
    if (transport != null) {
      transportPool.returnTransport(transport);
      transport = null;
    }
  }

  @Override
  public void invalidate() {
    if (transport != null) {
      transportPool.invalidateTransport(transport);
      transport = null;
    }
  }

  public long syncNotifications(long id) throws SentryUserException {
    TSentrySyncIDRequest request =
        new TSentrySyncIDRequest(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, id);

    try {
      TSentrySyncIDResponse response = client.sentry_sync_notifications(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getId();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  public long notifyHmsEvent(String requestorUserName, long eventId, String eventType,
    TSentryObjectOwnerType ownerType, String ownerName, TSentryAuthorizable authorizable)
          throws SentryUserException {
    TSentryHmsEventNotification request = new TSentryHmsEventNotification();

    try {
      request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
      request.setRequestorUserName(requestorUserName);
      request.setId(eventId);
      request.setEventType(eventType);
      request.setOwnerType(ownerType);
      request.setOwnerName(ownerName);
      request.setAuthorizable(authorizable);

      TSentryHmsEventNotificationResponse response = client.sentry_notify_hms_event(request);
      Status.throwIfNotOk(response.getStatus());
      return response.getId();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public Map<String, Set<TSentryPrivilege>> listAllRolesPrivileges(String requestorUserName)
    throws SentryUserException {
    TSentryPrivilegesRequest request = new TSentryPrivilegesRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);

    try {
      TSentryPrivilegesResponse response = client.list_roles_privileges(request);
      Status.throwIfNotOk(response.getStatus());

      return response.getPrivilegesMap();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  @Override
  public Map<String, Set<TSentryPrivilege>> listAllUsersPrivileges(String requestorUserName)
    throws SentryUserException {
    TSentryPrivilegesRequest request = new TSentryPrivilegesRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);

    try {
      TSentryPrivilegesResponse response = client.list_users_privileges(request);
      Status.throwIfNotOk(response.getStatus());

      return response.getPrivilegesMap();
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }
}