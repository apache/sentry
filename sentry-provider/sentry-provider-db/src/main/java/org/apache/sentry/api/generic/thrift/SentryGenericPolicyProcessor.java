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
package org.apache.sentry.api.generic.thrift;

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.core.common.utils.SentryConstants.KV_JOINER;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.common.ThriftConstants;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.core.common.exception.SentryThriftAPIMismatchException;
import org.apache.sentry.provider.db.generic.service.persistent.DelegateSentryStore;
import org.apache.sentry.provider.db.generic.service.persistent.PrivilegeObject;
import org.apache.sentry.provider.db.generic.service.persistent.PrivilegeObject.Builder;
import org.apache.sentry.provider.db.generic.service.persistent.SentryStoreLayer;
import org.apache.sentry.provider.db.log.entity.JsonLogEntityFactory;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.core.common.utils.PolicyStoreConstants;
import org.apache.sentry.api.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.sentry.api.common.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SentryGenericPolicyProcessor implements SentryGenericPolicyService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryGenericPolicyProcessor.class);
  private static final Logger AUDIT_LOGGER = LoggerFactory
      .getLogger(Constants.AUDIT_LOGGER_NAME_GENERIC);
  private final Configuration conf;
  private final ImmutableSet<String> adminGroups;
  private final SentryStoreLayer store;
  private final NotificationHandlerInvoker handerInvoker;

  private static final String ACCESS_DENIAL_MESSAGE = "Access denied to ";

  SentryGenericPolicyProcessor(Configuration conf) throws Exception {
    this.store = new DelegateSentryStore(conf);
    this.handerInvoker = new NotificationHandlerInvoker(createHandlers(conf));
    this.conf = conf;
    adminGroups = ImmutableSet.copyOf((Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
  }

  @VisibleForTesting
  SentryGenericPolicyProcessor(Configuration conf, SentryStoreLayer store) throws Exception {
    this.store = store;
    this.handerInvoker = new NotificationHandlerInvoker(createHandlers(conf));
    this.conf = conf;
    adminGroups = ImmutableSet.copyOf(toTrimmed(Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
  }

  private void authorize(String requestorUser, Set<String> requestorGroups)
  throws SentryAccessDeniedException {
    if (!inAdminGroups(requestorGroups)) {
      String msg = "User: " + requestorUser + " is part of " + requestorGroups +
          " which does not, intersect admin groups " + adminGroups;
      LOGGER.warn(msg);
      throw new SentryAccessDeniedException(ACCESS_DENIAL_MESSAGE + requestorUser);
    }
  }

  private Set<String> toTrimmedLower(Set<String> s) {
    if (s == null) {
      return Collections.emptySet();
    }
    Set<String> result = new HashSet<>(s.size());
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }

  private Set<String> toTrimmed(Set<String> s) {
    if (s == null) {
      return Collections.emptySet();
    }
    Set<String> result = new HashSet<>(s.size());
    for (String v : s) {
      result.add(v.trim());
    }
    return result;
  }

  private String toTrimmedLower(String s) {
    if (Strings.isNullOrEmpty(s)){
      return "";
    }
    return s.trim().toLowerCase();
  }

  private static Set<String> getRequestorGroups(Configuration conf, String userName) throws SentryUserException {
    return SentryPolicyStoreProcessor.getGroupsFromUserName(conf, userName);
  }

  private boolean inAdminGroups(Set<String> requestorGroups) {
    return !Sets.intersection(adminGroups, requestorGroups).isEmpty();
  }

  static List<NotificationHandler> createHandlers(Configuration conf) throws SentrySiteConfigurationException {

    List<NotificationHandler> handlers = Lists.newArrayList();
    Iterable<String> notificationHandlers = Splitter.onPattern("[\\s,]").trimResults()
        .omitEmptyStrings().split(conf.get(PolicyStoreConstants.SENTRY_GENERIC_POLICY_NOTIFICATION, ""));
    try {
      for (String notificationHandler : notificationHandlers) {
        handlers.add(createInstance(notificationHandler, conf, NotificationHandler.class));
      }
    } catch (Exception e) {
      throw new SentrySiteConfigurationException("Create notificationHandlers error: " + e.getMessage(), e);
    }
    return handlers;
  }

  @SuppressWarnings("unchecked")
  private static <T> T createInstance(String className, Configuration conf, Class<T> iface) throws Exception {
    T result;
    try {
      Class<?> clazz = Class.forName(className);
      if (!iface.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("Class " + clazz + " is not a " +
                                                 iface.getName());
      }
      Constructor<T> meth = (Constructor<T>)clazz.getDeclaredConstructor(Configuration.class);
      meth.setAccessible(true);
      result = meth.newInstance(new Object[]{conf});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  private <T> Response<T> requestHandle(RequestHandler<T> handler) {
    Response<T> response = new Response<T>();
    try {
      response = handler.handle();
    } catch (SentryAccessDeniedException e) {
      String msg = "Sentry access denied: " + e.getMessage();
      LOGGER.error(msg, e);
      response.status = Status.AccessDenied(e.getMessage(), e);
    } catch (SentryAlreadyExistsException e) {
      String msg = "Sentry object already exists: " + e.getMessage();
      LOGGER.error(msg, e);
      response.status = Status.AlreadyExists(e.getMessage(), e);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Sentry object doesn't exist: " + e.getMessage();
      LOGGER.error(msg, e);
      response.status = Status.NoSuchObject(e.getMessage(), e);
    } catch (SentryInvalidInputException e) {
      String msg = "Invalid input privilege object: " + e.getMessage();
      LOGGER.error(msg, e);
      response.status = Status.InvalidInput(msg, e);
    } catch (SentryThriftAPIMismatchException e) {
      String msg = "Sentry thrift API mismatch error: " + e.getMessage();
      LOGGER.error(msg, e);
      response.status = Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e);
    } catch (Exception e) {
      String msg = "Unknown error:" + e.getMessage();
      LOGGER.error(msg, e);
      response.status = Status.RuntimeError(msg, e);
    }
    return response;
  }

  private PrivilegeObject toPrivilegeObject(TSentryPrivilege tSentryPrivilege) {
    Boolean grantOption;
    if (tSentryPrivilege.getGrantOption().equals(TSentryGrantOption.TRUE)) {
      grantOption = true;
    } else if (tSentryPrivilege.getGrantOption().equals(TSentryGrantOption.FALSE)) {
      grantOption = false;
    } else {
      grantOption = null;
    }
    return new Builder().setComponent(tSentryPrivilege.getComponent())
                                             .setService(tSentryPrivilege.getServiceName())
                                             .setAuthorizables(toAuthorizables(tSentryPrivilege.getAuthorizables()))
                                             .setAction(tSentryPrivilege.getAction())
                                             .withGrantOption(grantOption)
                                             .build();
  }

  private TSentryPrivilege fromPrivilegeObject(PrivilegeObject privilege) {

    TSentryPrivilege tPrivilege = new TSentryPrivilege(privilege.getComponent(), privilege.getService(),
                                                       fromAuthorizable(privilege.getAuthorizables()),
                                                       privilege.getAction());
    if (privilege.getGrantOption() == null) {
      tPrivilege.setGrantOption(TSentryGrantOption.UNSET);
    } else if (privilege.getGrantOption()) {
      tPrivilege.setGrantOption(TSentryGrantOption.TRUE);
    } else {
      tPrivilege.setGrantOption(TSentryGrantOption.FALSE);
    }
    return tPrivilege;
  }

  private List<TAuthorizable> fromAuthorizable(List<? extends Authorizable> authorizables) {
    List<TAuthorizable> tAuthorizables = Lists.newArrayList();
    for (Authorizable authorizable : authorizables) {
      tAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
    }
    return tAuthorizables;
  }

  private String fromAuthorizableToStr(List<? extends Authorizable> authorizables) {
    if (authorizables != null && !authorizables.isEmpty()) {
      List<String> privileges = Lists.newArrayList();

      for (Authorizable authorizable : authorizables) {

        privileges.add(SentryConstants.KV_JOINER.join(authorizable.getTypeName(),
            authorizable.getName()));
      }

      return SentryConstants.AUTHORIZABLE_JOINER.join(privileges);
    } else {
      return "";
    }
  }

  private List<? extends Authorizable> toAuthorizables(List<TAuthorizable> tAuthorizables) {
    List<Authorizable> authorizables = Lists.newArrayList();
    if (tAuthorizables == null) {
      return authorizables;
    }
    for (final TAuthorizable tAuthorizable : tAuthorizables) {
      authorizables.add(new Authorizable() {
        @Override
        public String getTypeName() {
          return tAuthorizable.getType();
        }
        @Override
        public String getName() {
          return tAuthorizable.getName();
        }
      });
    }
    return authorizables;
  }

  private List<? extends Authorizable> toAuthorizables(String privilegeStr) {
    List<Authorizable> authorizables = Lists.newArrayList();
    if (privilegeStr == null) {
      return authorizables;
    }

    for (String authorizable : SentryConstants.AUTHORIZABLE_SPLITTER.split(privilegeStr)) {
      KeyValue tempKV = new KeyValue(authorizable);
      final String key = tempKV.getKey();
      final String value = tempKV.getValue();

      authorizables.add(new Authorizable() {
        @Override
        public String getTypeName() {
          return key;
        }

        @Override
        public String getName() {
          return value;
        }
      });
    }

    return authorizables;
  }

  // Construct the role to set of privileges mapping based on the
  // MSentryGMPrivilege information.
  private TSentryPrivilegeMap toTSentryPrivilegeMap(Set<MSentryGMPrivilege> mPrivileges) {

    // Mapping of <Role, Set<Privilege>>.
    Map<String, Set<TSentryPrivilege>> tPrivilegeMap = Maps.newTreeMap();

    for (MSentryGMPrivilege mPrivilege : mPrivileges) {
      for (MSentryRole role : mPrivilege.getRoles()) {

        TSentryPrivilege tPrivilege = toTSentryPrivilege(mPrivilege);

        if (tPrivilegeMap.containsKey(role.getRoleName())) {
          tPrivilegeMap.get(role.getRoleName()).add(tPrivilege);
        } else {
          Set<TSentryPrivilege> tPrivilegeSet = Sets.newTreeSet();
          tPrivilegeSet.add(tPrivilege);
          tPrivilegeMap.put(role.getRoleName(), tPrivilegeSet);
        }
      }
    }

    return new TSentryPrivilegeMap(tPrivilegeMap);
  }

  // Construct TSentryPrivilege based on MSentryGMPrivilege information.
  private TSentryPrivilege toTSentryPrivilege(MSentryGMPrivilege mPrivilege) {

    TSentryPrivilege tPrivilege = new TSentryPrivilege(mPrivilege.getComponentName(),
    mPrivilege.getServiceName(), fromAuthorizable(mPrivilege.getAuthorizables()), mPrivilege.getAction());

    if (mPrivilege.getGrantOption() == null) {
      tPrivilege.setGrantOption(TSentryGrantOption.UNSET);
    } else if (mPrivilege.getGrantOption()) {
      tPrivilege.setGrantOption(TSentryGrantOption.TRUE);
    } else {
      tPrivilege.setGrantOption(TSentryGrantOption.FALSE);
    }

    return tPrivilege;
  }

  private Set<String> buildPermissions(Set<PrivilegeObject> privileges) {
    Set<String> permissions = Sets.newHashSet();
    for (PrivilegeObject privilege : privileges) {
      List<String> hierarchy = Lists.newArrayList();
      if (hasComponentServerPrivilege(privilege.getComponent())) {
        hierarchy.add(KV_JOINER.join("server", privilege.getService()));
      }
      for (Authorizable authorizable : privilege.getAuthorizables()) {
        hierarchy.add(KV_JOINER.join(authorizable.getTypeName(),authorizable.getName()));
      }
      hierarchy.add(KV_JOINER.join("action", privilege.getAction()));
      permissions.add(AUTHORIZABLE_JOINER.join(hierarchy));
    }
    return permissions;
  }

  private boolean hasComponentServerPrivilege(String component) {
    //judge the component whether has the server privilege, for example: sqoop has the privilege on the server
    return AuthorizationComponent.SQOOP.equalsIgnoreCase(component);
  }

  @Override
  public TCreateSentryRoleResponse create_sentry_role(
      final TCreateSentryRoleRequest request) throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        authorize(request.getRequestorUserName(),
            getRequestorGroups(conf, request.getRequestorUserName()));
        store.createRole(request.getComponent(), request.getRoleName(),
                request.getRequestorUserName());
        return new Response<Void>(Status.OK());
      }
    });

    TCreateSentryRoleResponse tResponse = new TCreateSentryRoleResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.create_sentry_role(request, tResponse);
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, tResponse, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error in creating audit log for create role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return tResponse;
  }

  @Override
  public TDropSentryRoleResponse drop_sentry_role(final TDropSentryRoleRequest request)
      throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        authorize(request.getRequestorUserName(),
            getRequestorGroups(conf, request.getRequestorUserName()));
        store.dropRole(request.getComponent(), request.getRoleName(),
                request.getRequestorUserName());
        return new Response<Void>(Status.OK());
      }
    });

    TDropSentryRoleResponse tResponse = new TDropSentryRoleResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.drop_sentry_role(request, tResponse);
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, tResponse, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error in creating audit log for drop role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return tResponse;
  }

  @Override
  public TAlterSentryRoleGrantPrivilegeResponse alter_sentry_role_grant_privilege(
      final TAlterSentryRoleGrantPrivilegeRequest request) throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        store.alterRoleGrantPrivilege(request.getComponent(),
                request.getRoleName(),
                toPrivilegeObject(request.getPrivilege()),
                request.getRequestorUserName());
       return new Response<Void>(Status.OK());
      }
    });

    TAlterSentryRoleGrantPrivilegeResponse tResponse = new TAlterSentryRoleGrantPrivilegeResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.alter_sentry_role_grant_privilege(request, tResponse);
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, tResponse, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error in creating audit log for grant privilege to role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return tResponse;
  }

  @Override
  public TAlterSentryRoleRevokePrivilegeResponse alter_sentry_role_revoke_privilege(
      final TAlterSentryRoleRevokePrivilegeRequest request) throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        store.alterRoleRevokePrivilege(request.getComponent(),
                request.getRoleName(),
                toPrivilegeObject(request.getPrivilege()),
                request.getRequestorUserName());
       return new Response<Void>(Status.OK());
      }
    });

    TAlterSentryRoleRevokePrivilegeResponse tResponse =
            new TAlterSentryRoleRevokePrivilegeResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.alter_sentry_role_revoke_privilege(request, tResponse);
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, tResponse, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error in creating audit log for revoke privilege from role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return tResponse;
  }

  @Override
  public TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(
      final TAlterSentryRoleAddGroupsRequest request) throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        authorize(request.getRequestorUserName(),
            getRequestorGroups(conf, request.getRequestorUserName()));
        store.alterRoleAddGroups(request.getComponent(),
                request.getRoleName(),
                request.getGroups(),
                request.getRequestorUserName());
        return new Response<Void>(Status.OK());
      }
    });

    TAlterSentryRoleAddGroupsResponse tResponse =
            new TAlterSentryRoleAddGroupsResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.alter_sentry_role_add_groups(request, tResponse);
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, tResponse, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error in creating audit log for add role to group: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return tResponse;
  }

  @Override
  public TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(
      final TAlterSentryRoleDeleteGroupsRequest request) throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        authorize(request.getRequestorUserName(),
            getRequestorGroups(conf, request.getRequestorUserName()));
        store.alterRoleDeleteGroups(request.getComponent(),
                request.getRoleName(),
                request.getGroups(),
                request.getRequestorUserName());
        return new Response<Void>(Status.OK());
      }
    });

    TAlterSentryRoleDeleteGroupsResponse tResponse =
            new TAlterSentryRoleDeleteGroupsResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.alter_sentry_role_delete_groups(request, tResponse);
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, tResponse, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error in creating audit log for delete role from group: " +
              e.getMessage();
      LOGGER.error(msg, e);
    }
    return tResponse;
  }

  @Override
  public TListSentryRolesResponse list_sentry_roles_by_group(
      final TListSentryRolesRequest request) throws TException {
    Response<Set<TSentryRole>> respose = requestHandle(new RequestHandler<Set<TSentryRole>>() {
      @Override
      public Response<Set<TSentryRole>> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        Set<String> groups = getRequestorGroups(conf, request.getRequestorUserName());
        if (!AccessConstants.ALL.equalsIgnoreCase(request.getGroupName())) {
          boolean admin = inAdminGroups(groups);
          //Only admin users can list all roles in the system ( groupname = null)
          //Non admin users are only allowed to list only groups which they belong to
          if(!admin && (request.getGroupName() == null || !groups.contains(request.getGroupName()))) {
            throw new SentryAccessDeniedException(ACCESS_DENIAL_MESSAGE + request.getRequestorUserName());
          }
          groups.clear();
          groups.add(request.getGroupName());
        }

        Set<String> roleNames = store.getRolesByGroups(request.getComponent(), groups);
        Set<TSentryRole> tSentryRoles = Sets.newHashSet();
        for (String roleName : roleNames) {
          Set<String> groupsForRoleName = store.getGroupsByRoles(request.getComponent(), Sets.newHashSet(roleName));
          tSentryRoles.add(new TSentryRole(roleName, groupsForRoleName));
        }
        return new Response<Set<TSentryRole>>(Status.OK(), tSentryRoles);
      }
    });
    TListSentryRolesResponse tResponse = new TListSentryRolesResponse();
    tResponse.setStatus(respose.status);
    tResponse.setRoles(respose.content);
    return tResponse;
  }

  @Override
  public TListSentryPrivilegesResponse list_sentry_privileges_by_role(
      final TListSentryPrivilegesRequest request) throws TException {
    Response<Set<TSentryPrivilege>> respose = requestHandle(new RequestHandler<Set<TSentryPrivilege>>() {
      @Override
      public Response<Set<TSentryPrivilege>> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        Set<String> groups = getRequestorGroups(conf, request.getRequestorUserName());
        if (!inAdminGroups(groups)) {
          Set<String> roleNamesForGroups = toTrimmedLower(store.getRolesByGroups(request.getComponent(), groups));
          if (!roleNamesForGroups.contains(toTrimmedLower(request.getRoleName()))) {
            throw new SentryAccessDeniedException(ACCESS_DENIAL_MESSAGE + request.getRequestorUserName());
          }
        }
        Set<PrivilegeObject> privileges = store.getPrivilegesByProvider(request.getComponent(),
                                                                        request.getServiceName(),
                                                                        Sets.newHashSet(request.getRoleName()),
                                                                        null, toAuthorizables(request.getAuthorizables()));
        Set<TSentryPrivilege> tSentryPrivileges = Sets.newHashSet();
        for (PrivilegeObject privilege : privileges) {
          tSentryPrivileges.add(fromPrivilegeObject(privilege));
        }
        return new Response<Set<TSentryPrivilege>>(Status.OK(), tSentryPrivileges);
      }
    });
    TListSentryPrivilegesResponse tResponse = new TListSentryPrivilegesResponse();
    tResponse.setStatus(respose.status);
    tResponse.setPrivileges(respose.content);
    return tResponse;
  }

  @Override
  public TListSentryPrivilegesForProviderResponse list_sentry_privileges_for_provider(
      final TListSentryPrivilegesForProviderRequest request) throws TException {
    Response<Set<String>> respose = requestHandle(new RequestHandler<Set<String>>() {
      @Override
      public Response<Set<String>> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        Set<String> activeRoleNames = toTrimmedLower(request.getRoleSet().getRoles());
        Set<String> roleNamesForGroups = store.getRolesByGroups(request.getComponent(), request.getGroups());
        Set<String> rolesToQuery = request.getRoleSet().isAll() ? roleNamesForGroups : Sets.intersection(activeRoleNames, roleNamesForGroups);
        Set<PrivilegeObject> privileges = store.getPrivilegesByProvider(request.getComponent(),
                                                                        request.getServiceName(),
                                                                        rolesToQuery, null,
                                                                        toAuthorizables(request.getAuthorizables()));
        return new Response<Set<String>>(Status.OK(), buildPermissions(privileges));
      }
    });
    TListSentryPrivilegesForProviderResponse tResponse = new TListSentryPrivilegesForProviderResponse();
    tResponse.setStatus(respose.status);
    tResponse.setPrivileges(respose.content);
    return tResponse;
  }

  @Override
  public TListSentryPrivilegesByAuthResponse list_sentry_privileges_by_authorizable(TListSentryPrivilegesByAuthRequest request) throws TException {

    TListSentryPrivilegesByAuthResponse response = new TListSentryPrivilegesByAuthResponse();
    Map<String, TSentryPrivilegeMap> authRoleMap = Maps.newHashMap();

    // Group names are case sensitive.
    Set<String> requestedGroups = request.getGroups();
    String subject = request.getRequestorUserName();
    TSentryActiveRoleSet activeRoleSet = request.getRoleSet();
    Set<String> validActiveRoles = Sets.newHashSet();

    try {
      validateClientVersion(request.getProtocol_version());
      Set<String> memberGroups = getRequestorGroups(conf, subject);

      // Disallow non-admin users to lookup groups that
      // they are not part of.
      if(!inAdminGroups(memberGroups)) {

        if (requestedGroups != null && !requestedGroups.isEmpty()) {
          for (String requestedGroup : requestedGroups) {

            // If user doesn't belong to one of the requested groups,
            // then raise security exception.
            if (!memberGroups.contains(requestedGroup)) {
              throw new SentryAccessDeniedException(ACCESS_DENIAL_MESSAGE + subject);
            }
          }
        } else {
          // Non-admin's search is limited to its own groups.
          requestedGroups = memberGroups;
        }

        Set<String> grantedRoles = toTrimmedLower(store.getRolesByGroups(request.getComponent(), requestedGroups));

        // If activeRoleSet is not null, disallow non-admin to lookup roles that they are not part of.
        if (activeRoleSet != null && !activeRoleSet.isAll()) {

          Set<String> activeRoleNames = toTrimmedLower(activeRoleSet.getRoles());
          for (String activeRole : activeRoleNames) {
            if (!grantedRoles.contains(activeRole)) {
              throw new SentryAccessDeniedException(ACCESS_DENIAL_MESSAGE
              + subject);
            }
          }

          // For non-admin, valid active roles are intersection of active roles and granted roles.
          validActiveRoles.addAll(activeRoleSet.isAll() ? grantedRoles : Sets.intersection(activeRoleNames, grantedRoles));
        } else {
          // For non-admin, if activeRoleSet is null, valid active roles would be the granted roles.
          validActiveRoles.addAll(grantedRoles);
        }
      } else {
        // For admin, if requestedGroups are empty, requested roles will be all roles.
        Set<String> requestedRoles = toTrimmedLower(store.getAllRoleNames());
        if (requestedGroups != null && !requestedGroups.isEmpty())  {
          requestedRoles = toTrimmedLower(store.getRolesByGroups(request.getComponent(), requestedGroups));
        }

        // If activeRoleSet (which is optional) is not null, valid active role will be intersection
        // of active roles and requested roles. Otherwise, valid active roles are the requested roles.
        if (activeRoleSet != null && !activeRoleSet.isAll()) {
          validActiveRoles.addAll(Sets.intersection(toTrimmedLower(activeRoleSet.getRoles()), requestedRoles));
        } else {
          validActiveRoles.addAll(requestedRoles);
        }
      }

      // If user is not part of any group.. return empty response
      if (request.getAuthorizablesSet() != null) {
        for (String authorizablesStr : request.getAuthorizablesSet()) {

          List<? extends Authorizable> authorizables = toAuthorizables(authorizablesStr);
          Set<MSentryGMPrivilege> sentryPrivileges = store.getPrivilegesByAuthorizable(request.getComponent(), request.getServiceName(), validActiveRoles, authorizables);
          authRoleMap.put(fromAuthorizableToStr(authorizables), toTSentryPrivilegeMap(sentryPrivileges));
        }
      }

      response.setPrivilegesMapByAuth(authRoleMap);
      response.setStatus(Status.OK());
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: "
      + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }

    return response;
  }

  @Override
  public TDropPrivilegesResponse drop_sentry_privilege(
      final TDropPrivilegesRequest request) throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        authorize(request.getRequestorUserName(),
            getRequestorGroups(conf, request.getRequestorUserName()));
        store.dropPrivilege(request.getComponent(),
            toPrivilegeObject(request.getPrivilege()),
            request.getRequestorUserName());
        return new Response<Void>(Status.OK());
      }
    });

    TDropPrivilegesResponse tResponse = new TDropPrivilegesResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.drop_sentry_privilege(request, tResponse);
    }
    return tResponse;
  }

  @Override
  public TRenamePrivilegesResponse rename_sentry_privilege(
      final TRenamePrivilegesRequest request) throws TException {
    Response<Void> respose = requestHandle(new RequestHandler<Void>() {
      @Override
      public Response<Void> handle() throws Exception {
        validateClientVersion(request.getProtocol_version());
        authorize(request.getRequestorUserName(),
            getRequestorGroups(conf, request.getRequestorUserName()));
        store.renamePrivilege(request.getComponent(), request.getServiceName(),
                              toAuthorizables(request.getOldAuthorizables()),
                              toAuthorizables(request.getNewAuthorizables()),
                              request.getRequestorUserName());
        return new Response<Void>(Status.OK());
      }
    });

    TRenamePrivilegesResponse tResponse = new TRenamePrivilegesResponse(respose.status);
    if (Status.OK.getCode() == respose.status.getValue()) {
      handerInvoker.rename_sentry_privilege(request, tResponse);
    }
    return tResponse;
  }

  private static class Response<T> {
    private TSentryResponseStatus status;
    private T content;

    Response() {
    }

    Response(TSentryResponseStatus status) {
      this(status, null);
    }

    Response(TSentryResponseStatus status, T content) {
      this.status = status;
      this.content = content;
    }
  }
  private interface RequestHandler <T>{
    Response<T> handle() throws Exception ;
  }

  private static void validateClientVersion(int protocolVersion) throws SentryThriftAPIMismatchException {
    if (ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT != protocolVersion) {
      String msg = "Sentry thrift API protocol version mismatch: Client thrift version " +
          "is: " + protocolVersion + " , server thrift version " +
              "is " + ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT;
      throw new SentryThriftAPIMismatchException(msg);
    }
  }
}
