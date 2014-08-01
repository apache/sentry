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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.log.entity.JsonLogEntityFactory;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.provider.db.service.persistent.CommitContext;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.PolicyStoreConstants.PolicyStoreServerConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@SuppressWarnings("unused")
public class SentryPolicyStoreProcessor implements SentryPolicyService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryPolicyStoreProcessor.class);
  private static final Logger AUDIT_LOGGER = LoggerFactory.getLogger(Constants.AUDIT_LOGGER_NAME);

  public static final String SENTRY_POLICY_SERVICE_NAME = "SentryPolicyService";

  private final String name;
  private final Configuration conf;
  private final SentryStore sentryStore;
  private final NotificationHandlerInvoker notificationHandlerInvoker;
  private final ImmutableSet<String> adminGroups;
  private boolean isReady;

  public SentryPolicyStoreProcessor(String name, Configuration conf) throws Exception {
    super();
    this.name = name;
    this.conf = conf;
    this.notificationHandlerInvoker = new NotificationHandlerInvoker(conf,
        createHandlers(conf));
    isReady = false;
    sentryStore = new SentryStore(conf);
    isReady = true;
    adminGroups = ImmutableSet.copyOf(toTrimedLower(Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
  }

  public void stop() {
    if (isReady) {
      sentryStore.stop();
    }
  }

  @VisibleForTesting
  static List<NotificationHandler> createHandlers(Configuration conf)
  throws SentryConfigurationException {
    List<NotificationHandler> handlers = Lists.newArrayList();
    Iterable<String> notificationHandlers = Splitter.onPattern("[\\s,]").trimResults()
                                            .omitEmptyStrings().split(conf.get(PolicyStoreServerConfig.NOTIFICATION_HANDLERS, ""));
    for (String notificationHandler : notificationHandlers) {
      Class<?> clazz = null;
      try {
        clazz = Class.forName(notificationHandler);
        if (!NotificationHandler.class.isAssignableFrom(clazz)) {
          throw new SentryConfigurationException("Class " + notificationHandler + " is not a " +
                                                 NotificationHandler.class.getName());
        }
      } catch (ClassNotFoundException e) {
        throw new SentryConfigurationException("Value " + notificationHandler +
                                               " is not a class", e);
      }
      Preconditions.checkNotNull(clazz, "Error class cannot be null");
      try {
        Constructor<?> constructor = clazz.getConstructor(Configuration.class);
        handlers.add((NotificationHandler)constructor.newInstance(conf));
      } catch (Exception e) {
        throw new SentryConfigurationException("Error attempting to create " + notificationHandler, e);
      }
    }
    return handlers;
  }

  @VisibleForTesting
  public Configuration getSentryStoreConf() {
    return conf;
  }

  private static Set<String> toTrimedLower(Set<String> s) {
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }

  private void authorize(String requestorUser, Set<String> requestorGroups)
  throws SentryAccessDeniedException {
    requestorGroups = toTrimedLower(requestorGroups);
    if (Sets.intersection(adminGroups, requestorGroups).isEmpty()) {
      String msg = "User: " + requestorUser + " is part of " + requestorGroups +
          " which does not, intersect admin groups " + adminGroups;
      LOGGER.warn(msg);
      throw new SentryAccessDeniedException("Access denied to " + requestorUser);
    }
  }

  @Override
  public TCreateSentryRoleResponse create_sentry_role(
    TCreateSentryRoleRequest request) throws TException {
    TCreateSentryRoleResponse response = new TCreateSentryRoleResponse();
    try {
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      CommitContext commitContext = sentryStore.createSentryRole(request.getRoleName(),
          request.getRequestorUserName());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.create_sentry_role(commitContext,
          request, response);
    } catch (SentryAlreadyExistsException e) {
      String msg = "Role: " + request + " already exists.";
      LOGGER.error(msg, e);
      response.setStatus(Status.AlreadyExists(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }

    AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance().createJsonLogEntity(
    		request, response, conf).toJsonFormatLog());
    return response;
  }

  @Override
  public TAlterSentryRoleGrantPrivilegeResponse alter_sentry_role_grant_privilege
  (TAlterSentryRoleGrantPrivilegeRequest request) throws TException {

    TAlterSentryRoleGrantPrivilegeResponse response = new TAlterSentryRoleGrantPrivilegeResponse();
    try {
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      CommitContext commitContext = sentryStore.alterSentryRoleGrantPrivilege(request.getRoleName(),
                                    request.getPrivilege());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_grant_privilege(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request.getRoleName() + " doesn't exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryInvalidInputException e) {
      String msg = "Invalid input privilege object";
      LOGGER.error(msg, e);
      response.setStatus(Status.InvalidInput(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }

    AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance().createJsonLogEntity(
    		request, response, conf).toJsonFormatLog());
    return response;
  }

  @Override
  public TAlterSentryRoleRevokePrivilegeResponse alter_sentry_role_revoke_privilege
  (TAlterSentryRoleRevokePrivilegeRequest request) throws TException {
    TAlterSentryRoleRevokePrivilegeResponse response = new TAlterSentryRoleRevokePrivilegeResponse();
    try {
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      CommitContext commitContext = sentryStore.alterSentryRoleRevokePrivilege(request.getRoleName(),
                                    request.getPrivilege());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_revoke_privilege(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Privilege: [server=" + request.getPrivilege().getServerName() + 
    		  ",db=" + request.getPrivilege().getDbName() + 
    		  ",table=" + request.getPrivilege().getTableName() + 
    		  ",URI=" + request.getPrivilege().getURI() + 
    		  ",action=" + request.getPrivilege().getAction() + "] doesn't exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryInvalidInputException e) {
      String msg = "Invalid input privilege object";
      LOGGER.error(msg, e);
      response.setStatus(Status.InvalidInput(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }

    AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance().createJsonLogEntity(
    		request, response, conf).toJsonFormatLog());
    return response;
  }

  @Override
  public TDropSentryRoleResponse drop_sentry_role(
    TDropSentryRoleRequest request)  throws TException {
    TDropSentryRoleResponse response = new TDropSentryRoleResponse();
    TSentryResponseStatus status;
    try {
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      CommitContext commitContext = sentryStore.dropSentryRole(request.getRoleName());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.drop_sentry_role(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role :" + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }

    AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance().createJsonLogEntity(
    		request, response, conf).toJsonFormatLog());
    return response;
  }

  @Override
  public TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(
    TAlterSentryRoleAddGroupsRequest request) throws TException {
    TAlterSentryRoleAddGroupsResponse response = new TAlterSentryRoleAddGroupsResponse();
    try {
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      CommitContext commitContext = sentryStore.alterSentryRoleAddGroups(request.getRequestorUserName(),
                                    request.getRoleName(), request.getGroups());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_add_groups(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }

    AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance().createJsonLogEntity(
    		request, response, conf).toJsonFormatLog());
    return response;
  }

  @Override
  public TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(
    TAlterSentryRoleDeleteGroupsRequest request) throws TException {
    TAlterSentryRoleDeleteGroupsResponse response = new TAlterSentryRoleDeleteGroupsResponse();
    try {
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      CommitContext commitContext = sentryStore.alterSentryRoleDeleteGroups(request.getRoleName(),
          request.getGroups());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_delete_groups(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error adding groups to role: " + request;
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }

    AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance().createJsonLogEntity(
    		request, response, conf).toJsonFormatLog());
    return response;
  }

  @Override
  public TListSentryRolesResponse list_sentry_roles_by_group(
    TListSentryRolesRequest request) throws TException {
    TListSentryRolesResponse response = new TListSentryRolesResponse();
    TSentryResponseStatus status;
    Set<TSentryRole> roleSet = new HashSet<TSentryRole>();
    Set<String> groups = new HashSet<String>();
    boolean checkAllGroups = false;
    try {
      // Don't check admin permissions for listing requestor's own roles
      if (AccessConstants.ALL.equalsIgnoreCase(request.getGroupName())) {
        groups = getRequestorGroups(request.getRequestorUserName());
        checkAllGroups = true;
      } else {
        authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
        groups.add(request.getGroupName());
      }
      roleSet = sentryStore.getTSentryRolesByGroupName(groups, checkAllGroups);
      response.setRoles(roleSet);
      response.setStatus(Status.OK());
    } catch (SentryNoSuchObjectException e) {
      response.setRoles(roleSet);
      String msg = "Role: " + request + " couldn't be retrieved.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  @Override
  public TListSentryPrivilegesResponse list_sentry_privileges_by_role(
      TListSentryPrivilegesRequest request) throws TException {
    TListSentryPrivilegesResponse response = new TListSentryPrivilegesResponse();
    TSentryResponseStatus status;
    Set<TSentryPrivilege> privilegeSet = new HashSet<TSentryPrivilege>();
    try {
      //TODO: Handle authorization for metadata queries
      // Shall we allow only admin users to list privileges
      // or allow all users as long as user is granted this role?
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      if (request.isSetAuthorizableHierarchy()) {
        TSentryAuthorizable authorizableHierarchy = request.getAuthorizableHierarchy();
        privilegeSet = sentryStore.getTSentryPrivileges(Sets.newHashSet(request.getRoleName()), authorizableHierarchy);
      } else {
        privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(request.getRoleName());
      }
      response.setPrivileges(privilegeSet);
      response.setStatus(Status.OK());
    } catch (SentryNoSuchObjectException e) {
      response.setPrivileges(privilegeSet);
      String msg = "Privilege: " + request + " couldn't be retrieved.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  /**
   * This method was created specifically for ProviderBackend.getPrivileges() and is not meant
   * to be used for general privilege retrieval. More details in the .thrift file.
   */
  @Override
  public TListSentryPrivilegesForProviderResponse list_sentry_privileges_for_provider(
      TListSentryPrivilegesForProviderRequest request) throws TException {
    TListSentryPrivilegesForProviderResponse response = new TListSentryPrivilegesForProviderResponse();
    response.setPrivileges(new HashSet<String>());
    try {
      Set<String> privilegesForProvider = sentryStore.listSentryPrivilegesForProvider(
          request.getGroups(), request.getRoleSet(), request.getAuthorizableHierarchy());
      response.setPrivileges(privilegesForProvider);
      if (((privilegesForProvider == null)||(privilegesForProvider.size() == 0))&&(request.getAuthorizableHierarchy() != null)) {
        if (sentryStore.hasAnyServerPrivileges(
            request.getGroups(), request.getRoleSet(), request.getAuthorizableHierarchy().getServer())) {

          // REQUIRED for ensuring 'default' Db is accessible by any user
          // with privileges to atleast 1 object with the specific server as root

          // Need some way to specify that even though user has no privilege
          // For the specific AuthorizableHierarchy.. he has privilege on
          // atleast 1 object in the server hierarchy
          HashSet<String> serverPriv = Sets.newHashSet("server=+");
          response.setPrivileges(serverPriv);
        }
      }
      response.setStatus(Status.OK());
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  // retrieve the group mapping for the given user name
  private Set<String> getRequestorGroups(String userName)
      throws SentryUserException {
      String groupMapping = conf.get(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
          ServerConfig.SENTRY_STORE_GROUP_MAPPING_DEFAULT);
      String authResoruce = conf
          .get(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE);

      // load the group mapping provider class
      GroupMappingService groupMappingService;
      try {
        Constructor<?> constrctor = Class.forName(groupMapping).getDeclaredConstructor(
            Configuration.class, String.class);
        constrctor.setAccessible(true);
        groupMappingService = (GroupMappingService) constrctor.newInstance(new Object[] { conf,
            authResoruce });
      } catch (NoSuchMethodException e) {
        throw new SentryUserException("Unable to instantiate group mapping", e);
      } catch (SecurityException e) {
        throw new SentryUserException("Unable to instantiate group mapping", e);
      } catch (ClassNotFoundException e) {
        throw new SentryUserException("Unable to instantiate group mapping", e);
      } catch (InstantiationException e) {
        throw new SentryUserException("Unable to instantiate group mapping", e);
      } catch (IllegalAccessException e) {
        throw new SentryUserException("Unable to instantiate group mapping", e);
      } catch (IllegalArgumentException e) {
        throw new SentryUserException("Unable to instantiate group mapping", e);
      } catch (InvocationTargetException e) {
        throw new SentryUserException("Unable to instantiate group mapping", e);
      }
      return groupMappingService.getGroups(userName);
  }

  @Override
  public TDropPrivilegesResponse drop_sentry_privilege(
      TDropPrivilegesRequest request) throws TException {
    TDropPrivilegesResponse response = new TDropPrivilegesResponse();
    try {
      authorize(request.getRequestorUserName(), adminGroups);
      sentryStore.dropPrivilege(request.getAuthorizable());
      response.setStatus(Status.OK());
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: "
          + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  @Override
  public TRenamePrivilegesResponse rename_sentry_privilege(
      TRenamePrivilegesRequest request) throws TException {
    TRenamePrivilegesResponse response = new TRenamePrivilegesResponse();
    try {
      authorize(request.getRequestorUserName(), adminGroups);
      sentryStore.renamePrivilege(request.getOldAuthorizable(),
          request.getNewAuthorizable(), request.getRequestorUserName());
      response.setStatus(Status.OK());
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: "
          + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

}
