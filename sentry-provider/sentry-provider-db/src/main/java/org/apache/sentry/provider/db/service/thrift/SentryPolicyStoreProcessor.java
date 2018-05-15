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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin.SentryPluginException;
import org.apache.sentry.core.common.exception.SentryThriftAPIMismatchException;
import org.apache.sentry.provider.db.log.entity.JsonLogEntity;
import org.apache.sentry.provider.db.log.entity.JsonLogEntityFactory;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.core.common.utils.PolicyStoreConstants.PolicyStoreServerConfig;
import org.apache.sentry.provider.db.service.thrift.validator.GrantPrivilegeRequestValidator;
import org.apache.sentry.provider.db.service.thrift.validator.RevokePrivilegeRequestValidator;
import org.apache.sentry.service.thrift.SentryServiceUtil;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.sentry.service.thrift.ServiceConstants.ConfUtilties;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ThriftConstants;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;
import org.apache.thrift.TException;
import org.apache.log4j.Logger;

import com.codahale.metrics.Timer;
import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.sentry.hdfs.Updateable.Update;

@SuppressWarnings("unused")
public class SentryPolicyStoreProcessor implements SentryPolicyService.Iface {
  private static final Logger LOGGER = Logger.getLogger(SentryPolicyStoreProcessor.class);
  private static final Logger AUDIT_LOGGER = Logger.getLogger(Constants.AUDIT_LOGGER_NAME);

  static final String SENTRY_POLICY_SERVICE_NAME = "SentryPolicyService";

  private final String name;
  private final Configuration conf;
  private final SentryStore sentryStore;
  private final NotificationHandlerInvoker notificationHandlerInvoker;
  private final ImmutableSet<String> adminGroups;
  private SentryMetrics sentryMetrics;
  private final Timer hmsWaitTimer =
          SentryMetrics.getInstance().
                  getTimer(name(SentryPolicyStoreProcessor.class, "hms", "wait"));

  private List<SentryPolicyStorePlugin> sentryPlugins = new LinkedList<SentryPolicyStorePlugin>();

  SentryPolicyStoreProcessor(String name,
        Configuration conf, SentryStore store) throws Exception {
    super();
    this.name = name;
    this.conf = conf;
    this.sentryStore = store;
    this.notificationHandlerInvoker = new NotificationHandlerInvoker(conf,
        createHandlers(conf));
    adminGroups = ImmutableSet.copyOf(toTrimedLower(Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
    Iterable<String> pluginClasses = ConfUtilties.CLASS_SPLITTER
        .split(conf.get(ServerConfig.SENTRY_POLICY_STORE_PLUGINS,
            ServerConfig.SENTRY_POLICY_STORE_PLUGINS_DEFAULT).trim());
    for (String pluginClassStr : pluginClasses) {
      Class<?> clazz = conf.getClassByName(pluginClassStr);
      if (!SentryPolicyStorePlugin.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("Sentry Plugin ["
            + pluginClassStr + "] is not a "
            + SentryPolicyStorePlugin.class.getName());
      }
      SentryPolicyStorePlugin plugin = (SentryPolicyStorePlugin)clazz.newInstance();
      plugin.initialize(conf, sentryStore);
      sentryPlugins.add(plugin);
    }
    initMetrics();
  }

  private void initMetrics() {
    sentryMetrics = SentryMetrics.getInstance();
    sentryMetrics.addSentryStoreGauges(sentryStore);
    sentryMetrics.initReporting(conf);
  }

  public void stop() {
    sentryStore.stop();
  }

  public void registerPlugin(SentryPolicyStorePlugin plugin) throws SentryPluginException {
    plugin.initialize(conf, sentryStore);
    sentryPlugins.add(plugin);
  }

  @VisibleForTesting
  static List<NotificationHandler> createHandlers(Configuration conf)
  throws SentrySiteConfigurationException {
    List<NotificationHandler> handlers = Lists.newArrayList();
    Iterable<String> notificationHandlers = Splitter.onPattern("[\\s,]").trimResults()
                                            .omitEmptyStrings().split(conf.get(PolicyStoreServerConfig.NOTIFICATION_HANDLERS, ""));
    for (String notificationHandler : notificationHandlers) {
      Class<?> clazz = null;
      try {
        clazz = Class.forName(notificationHandler);
        if (!NotificationHandler.class.isAssignableFrom(clazz)) {
          throw new SentrySiteConfigurationException("Class " + notificationHandler + " is not a " +
                                                 NotificationHandler.class.getName());
        }
      } catch (ClassNotFoundException e) {
        throw new SentrySiteConfigurationException("Value " + notificationHandler +
                                               " is not a class", e);
      }
      Preconditions.checkNotNull(clazz, "Error class cannot be null");
      try {
        Constructor<?> constructor = clazz.getConstructor(Configuration.class);
        handlers.add((NotificationHandler)constructor.newInstance(conf));
      } catch (Exception e) {
        throw new SentrySiteConfigurationException("Error attempting to create " + notificationHandler, e);
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

  private boolean inAdminGroups(Set<String> requestorGroups) {
    Set<String> trimmedRequestorGroups = toTrimedLower(requestorGroups);
    return !Sets.intersection(adminGroups, trimmedRequestorGroups).isEmpty();
  }
  
  private void authorize(String requestorUser, Set<String> requestorGroups)
  throws SentryAccessDeniedException {
    if (!inAdminGroups(requestorGroups)) {
      String msg = "User: " + requestorUser + " is part of " + requestorGroups +
          " which does not, intersect admin groups " + adminGroups;
      LOGGER.warn(msg);
      throw new SentryAccessDeniedException("Access denied to " + requestorUser);
    }
  }

  @Override
  public TCreateSentryRoleResponse create_sentry_role(
    TCreateSentryRoleRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.createRoleTimer.time();
    TCreateSentryRoleResponse response = new TCreateSentryRoleResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));
      sentryStore.createSentryRole(request.getRoleName());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.create_sentry_role(request, response);
    } catch (SentryAlreadyExistsException e) {
      String msg = "Role: " + request + " already exists.";
      LOGGER.error(msg, e);
      response.setStatus(Status.AlreadyExists(e.getMessage(), e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
          .createJsonLogEntity(request, response, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for create role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TAlterSentryRoleGrantPrivilegeResponse alter_sentry_role_grant_privilege
  (TAlterSentryRoleGrantPrivilegeRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.grantTimer.time();
    TAlterSentryRoleGrantPrivilegeResponse response = new TAlterSentryRoleGrantPrivilegeResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      // There should only one field be set
      if ( !(request.isSetPrivileges()^request.isSetPrivilege()) ) {
        throw new SentryUserException("SENTRY API version is not right!");
      }
      // Maintain compatibility for old API: Set privilege field to privileges field
      if (request.isSetPrivilege()) {
        request.setPrivileges(Sets.newHashSet(request.getPrivilege()));
      }
      // TODO: now only has SentryPlugin. Once add more SentryPolicyStorePlugins,
      // TODO: need to differentiate the updates for different Plugins.
      Preconditions.checkState(sentryPlugins.size() <= 1);
      Map<TSentryPrivilege, Update> privilegesUpdateMap = new HashMap<>();
      for (SentryPolicyStorePlugin plugin : sentryPlugins) {
        plugin.onAlterSentryRoleGrantPrivilege(request, privilegesUpdateMap);
      }

      if (!privilegesUpdateMap.isEmpty()) {
        sentryStore.alterSentryRoleGrantPrivileges(request.getRequestorUserName(),
            request.getRoleName(), request.getPrivileges(), privilegesUpdateMap);
      } else {
        sentryStore.alterSentryRoleGrantPrivileges(request.getRequestorUserName(),
            request.getRoleName(), request.getPrivileges());
      }
      GrantPrivilegeRequestValidator.validate(request);
      response.setStatus(Status.OK());
      response.setPrivileges(request.getPrivileges());
      // Maintain compatibility for old API: Set privilege field to response
      if (response.isSetPrivileges() && response.getPrivileges().size() == 1) {
        response.setPrivilege(response.getPrivileges().iterator().next());
      }
      notificationHandlerInvoker.alter_sentry_role_grant_privilege(request,
              response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request.getRoleName() + " doesn't exist";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryInvalidInputException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.InvalidInput(e.getMessage(), e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      Set<JsonLogEntity> jsonLogEntitys = JsonLogEntityFactory.getInstance().createJsonLogEntitys(
          request, response, conf);
      for (JsonLogEntity jsonLogEntity : jsonLogEntitys) {
        AUDIT_LOGGER.info(jsonLogEntity.toJsonFormatLog());
      }
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for grant privilege to role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TAlterSentryRoleRevokePrivilegeResponse alter_sentry_role_revoke_privilege
  (TAlterSentryRoleRevokePrivilegeRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.revokeTimer.time();
    TAlterSentryRoleRevokePrivilegeResponse response = new TAlterSentryRoleRevokePrivilegeResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      // There should only one field be set
      if ( !(request.isSetPrivileges()^request.isSetPrivilege()) ) {
        throw new SentryUserException("SENTRY API version is not right!");
      }
      // Maintain compatibility for old API: Set privilege field to privileges field
      if (request.isSetPrivilege()) {
        request.setPrivileges(Sets.newHashSet(request.getPrivilege()));
      }

      // TODO: now only has SentryPlugin. Once add more SentryPolicyStorePlugins,
      // TODO: need to differentiate the updates for different Plugins.
      Preconditions.checkState(sentryPlugins.size() <= 1);
      Map<TSentryPrivilege, Update> privilegesUpdateMap = new HashMap<>();
      for (SentryPolicyStorePlugin plugin : sentryPlugins) {
        plugin.onAlterSentryRoleRevokePrivilege(request, privilegesUpdateMap);
      }

      if (!privilegesUpdateMap.isEmpty()) {
        sentryStore.alterSentryRoleRevokePrivileges(request.getRequestorUserName(),
            request.getRoleName(), request.getPrivileges(), privilegesUpdateMap);
      } else {
        sentryStore.alterSentryRoleRevokePrivileges(request.getRequestorUserName(),
            request.getRoleName(), request.getPrivileges());
      }
      RevokePrivilegeRequestValidator.validate(request);
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_revoke_privilege(request,
              response);
    } catch (SentryNoSuchObjectException e) {
      StringBuilder msg = new StringBuilder();
      if (request.getPrivileges().size() > 0) {
        for (TSentryPrivilege privilege : request.getPrivileges()) {
          msg.append("Privilege: [server=");
          msg.append(privilege.getServerName());
          msg.append(",db=");
          msg.append(privilege.getDbName());
          msg.append(",table=");
          msg.append(privilege.getTableName());
          msg.append(",URI=");
          msg.append(privilege.getURI());
          msg.append(",action=");
          msg.append(privilege.getAction());
          msg.append("] ");
        }
        msg.append("doesn't exist.");
      }
      LOGGER.error(msg.toString(), e);
      response.setStatus(Status.NoSuchObject(msg.toString(), e));
    } catch (SentryInvalidInputException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.InvalidInput(e.getMessage(), e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      Set<JsonLogEntity> jsonLogEntitys = JsonLogEntityFactory.getInstance().createJsonLogEntitys(
          request, response, conf);
      for (JsonLogEntity jsonLogEntity : jsonLogEntitys) {
        AUDIT_LOGGER.info(jsonLogEntity.toJsonFormatLog());
      }
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for revoke privilege from role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TDropSentryRoleResponse drop_sentry_role(
    TDropSentryRoleRequest request)  throws TException {
    final Timer.Context timerContext = sentryMetrics.dropRoleTimer.time();
    TDropSentryRoleResponse response = new TDropSentryRoleResponse();
    TSentryResponseStatus status;
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));

      // TODO: now only has SentryPlugin. Once add more SentryPolicyStorePlugins,
      // TODO: need to differentiate the updates for different Plugins.
      Preconditions.checkState(sentryPlugins.size() <= 1);
      Update update = null;
      for (SentryPolicyStorePlugin plugin : sentryPlugins) {
        update = plugin.onDropSentryRole(request);
      }

      if (update != null) {
        sentryStore.dropSentryRole(request.getRoleName(), update);
      } else {
        sentryStore.dropSentryRole(request.getRoleName());
      }
      response.setStatus(Status.OK());
      notificationHandlerInvoker.drop_sentry_role(request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role :" + request + " doesn't exist";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
          .createJsonLogEntity(request, response, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for drop role: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(
    TAlterSentryRoleAddGroupsRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.grantRoleTimer.time();
    TAlterSentryRoleAddGroupsResponse response = new TAlterSentryRoleAddGroupsResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));

      // TODO: now only has SentryPlugin. Once add more SentryPolicyStorePlugins,
      // TODO: need to differentiate the updates for different Plugins.
      Preconditions.checkState(sentryPlugins.size() <= 1);
      Update update = null;
      for (SentryPolicyStorePlugin plugin : sentryPlugins) {
        update = plugin.onAlterSentryRoleAddGroups(request);
      }
      if (update != null) {
        sentryStore.alterSentryRoleAddGroups(request.getRequestorUserName(),
            request.getRoleName(), request.getGroups(), update);
      } else {
        sentryStore.alterSentryRoleAddGroups(request.getRequestorUserName(),
            request.getRoleName(), request.getGroups());
      }
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_add_groups(request,
          response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " doesn't exist";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
          .createJsonLogEntity(request, response, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for add role to group: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TAlterSentryRoleAddUsersResponse alter_sentry_role_add_users(
      TAlterSentryRoleAddUsersRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.grantRoleTimer.time();
    TAlterSentryRoleAddUsersResponse response = new TAlterSentryRoleAddUsersResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(), getRequestorGroups(request.getRequestorUserName()));
      sentryStore.alterSentryRoleAddUsers(request.getRoleName(), request.getUsers());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_add_users(request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
          .createJsonLogEntity(request, response, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for add role to user: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TAlterSentryRoleDeleteUsersResponse alter_sentry_role_delete_users(
      TAlterSentryRoleDeleteUsersRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.grantRoleTimer.time();
    TAlterSentryRoleDeleteUsersResponse response = new TAlterSentryRoleDeleteUsersResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(), getRequestorGroups(request.getRequestorUserName()));
      sentryStore.alterSentryRoleDeleteUsers(request.getRoleName(),
              request.getUsers());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_delete_users(request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
          .createJsonLogEntity(request, response, conf).toJsonFormatLog());
   } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for delete role from user: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(
    TAlterSentryRoleDeleteGroupsRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.revokeRoleTimer.time();
    TAlterSentryRoleDeleteGroupsResponse response = new TAlterSentryRoleDeleteGroupsResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(),
          getRequestorGroups(request.getRequestorUserName()));

      // TODO: now only has SentryPlugin. Once add more SentryPolicyStorePlugins,
      // TODO: need to differentiate the updates for different Plugins.
      Preconditions.checkState(sentryPlugins.size() <= 1);
      Update update = null;
      for (SentryPolicyStorePlugin plugin : sentryPlugins) {
        update = plugin.onAlterSentryRoleDeleteGroups(request);
      }

      if (update != null) {
        sentryStore.alterSentryRoleDeleteGroups(request.getRoleName(),
          request.getGroups(), update);
      } else {
        sentryStore.alterSentryRoleDeleteGroups(request.getRoleName(),
          request.getGroups());
      }
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_delete_groups(request,
          response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error adding groups to role: " + request;
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }

    try {
      AUDIT_LOGGER.info(JsonLogEntityFactory.getInstance()
          .createJsonLogEntity(request, response, conf).toJsonFormatLog());
    } catch (Exception e) {
      // if any exception, log the exception.
      String msg = "Error creating audit log for delete role from group: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return response;
  }

  @Override
  public TListSentryRolesResponse list_sentry_roles_by_group(
    TListSentryRolesRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.listRolesByGroupTimer.time();
    TListSentryRolesResponse response = new TListSentryRolesResponse();
    TSentryResponseStatus status;
    Set<TSentryRole> roleSet = new HashSet<TSentryRole>();
    String subject = request.getRequestorUserName();
    boolean checkAllGroups = false;
    try {
      validateClientVersion(request.getProtocol_version());
      Set<String> groups = getRequestorGroups(subject);
      // Don't check admin permissions for listing requestor's own roles
      if (AccessConstants.ALL.equalsIgnoreCase(request.getGroupName())) {
        checkAllGroups = true;
      } else {
        boolean admin = inAdminGroups(groups);
        //Only admin users can list all roles in the system ( groupname = null)
        //Non admin users are only allowed to list only groups which they belong to
        if(!admin && (request.getGroupName() == null || !groups.contains(request.getGroupName()))) {
          throw new SentryAccessDeniedException("Access denied to " + subject);
        } else {
          groups.clear();
          groups.add(request.getGroupName());
        }
      }
      roleSet = sentryStore.getTSentryRolesByGroupName(groups, checkAllGroups);
      response.setRoles(roleSet);
      response.setStatus(Status.OK());
    } catch (SentryNoSuchObjectException e) {
      response.setRoles(roleSet);
      String msg = "Request: " + request + " couldn't be completed, message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }
    return response;
  }

  public TListSentryRolesResponse list_sentry_roles_by_user(TListSentryRolesForUserRequest request)
      throws TException {
    final Timer.Context timerContext = sentryMetrics.listRolesByGroupTimer.time();
    TListSentryRolesResponse response = new TListSentryRolesResponse();
    TSentryResponseStatus status;
    Set<TSentryRole> roleSet = new HashSet<TSentryRole>();
    String requestor = request.getRequestorUserName();
    String userName = request.getUserName();
    boolean checkAllGroups = false;
    try {
      validateClientVersion(request.getProtocol_version());
      // userName can't be empty
      if (StringUtils.isEmpty(userName)) {
        throw new SentryAccessDeniedException("The user name can't be empty.");
      }

      Set<String> requestorGroups;
      try {
        requestorGroups = getRequestorGroups(requestor);
      } catch (SentryGroupNotFoundException e) {
        LOGGER.error(e.getMessage(), e);
        response.setStatus(Status.AccessDenied(e.getMessage(), e));
        return response;
      }

      Set<String> userGroups;
      try {
        userGroups = getRequestorGroups(userName);
      } catch (SentryGroupNotFoundException e) {
        LOGGER.error(e.getMessage(), e);
        String msg = "Groups for user " + userName + " do not exist: " + e.getMessage();
        response.setStatus(Status.AccessDenied(msg, e));
        return response;
      }
      boolean isAdmin = inAdminGroups(requestorGroups);

      // Only admin users can list other user's roles in the system
      // Non admin users are only allowed to list only their own roles related user and group
      if (!isAdmin && !userName.equals(requestor)) {
        throw new SentryAccessDeniedException("Access denied to list the roles for " + userName);
      }
      roleSet = sentryStore.getTSentryRolesByUserNames(Sets.newHashSet(userName));
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
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }
    return response;
  }

  @Override
  public TListSentryPrivilegesResponse list_sentry_privileges_by_role(
      TListSentryPrivilegesRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.listPrivilegesByRoleTimer.time();
    TListSentryPrivilegesResponse response = new TListSentryPrivilegesResponse();
    TSentryResponseStatus status;
    Set<TSentryPrivilege> privilegeSet = new HashSet<TSentryPrivilege>();
    String subject = request.getRequestorUserName();
    try {
      validateClientVersion(request.getProtocol_version());
      Set<String> groups = getRequestorGroups(subject);
      Boolean admin = inAdminGroups(groups);
      if(!admin) {
        Set<String> roleNamesForGroups = toTrimedLower(sentryStore.getRoleNamesForGroups(groups));
        if(!roleNamesForGroups.contains(request.getRoleName().trim().toLowerCase())) {
          throw new SentryAccessDeniedException("Access denied to " + subject);
        }
      }
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
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
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
    final Timer.Context timerContext = sentryMetrics.listPrivilegesForProviderTimer.time();
    TListSentryPrivilegesForProviderResponse response = new TListSentryPrivilegesForProviderResponse();
    response.setPrivileges(new HashSet<String>());
    try {
      validateClientVersion(request.getProtocol_version());
      Set<String> privilegesForProvider =
          sentryStore.listSentryPrivilegesForProvider(request.getGroups(), request.getUsers(),
              request.getRoleSet(), request.getAuthorizableHierarchy());
      response.setPrivileges(privilegesForProvider);
      if (privilegesForProvider == null
          || privilegesForProvider.size() == 0
          && request.getAuthorizableHierarchy() != null
          && sentryStore.hasAnyServerPrivileges(request.getGroups(), request.getUsers(),
              request.getRoleSet(), request.getAuthorizableHierarchy().getServer())) {

        // REQUIRED for ensuring 'default' Db is accessible by any user
        // with privileges to atleast 1 object with the specific server as root

        // Need some way to specify that even though user has no privilege
        // For the specific AuthorizableHierarchy.. he has privilege on
        // atleast 1 object in the server hierarchy
        HashSet<String> serverPriv = Sets.newHashSet("server=+");
        response.setPrivileges(serverPriv);
      }
      response.setStatus(Status.OK());
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.stop();
    }
    return response;
  }

  // retrieve the group mapping for the given user name
  private Set<String> getRequestorGroups(String userName)
      throws SentryUserException {
    return getGroupsFromUserName(this.conf, userName);
  }

  public static Set<String> getGroupsFromUserName(Configuration conf,
      String userName) throws SentryUserException {
    String groupMapping = conf.get(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_GROUP_MAPPING_DEFAULT);
    String authResoruce = conf
        .get(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE);

    // load the group mapping provider class
    GroupMappingService groupMappingService;
    try {
      Constructor<?> constrctor = Class.forName(groupMapping)
          .getDeclaredConstructor(Configuration.class, String.class);
      constrctor.setAccessible(true);
      groupMappingService = (GroupMappingService) constrctor
          .newInstance(new Object[] { conf, authResoruce });
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
    final Timer.Context timerContext = sentryMetrics.dropPrivilegeTimer.time();
    TDropPrivilegesResponse response = new TDropPrivilegesResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(), adminGroups);

      // TODO: now only has SentryPlugin. Once add more SentryPolicyStorePlugins,
      // TODO: need to differentiate the updates for different Plugins.
      Preconditions.checkState(sentryPlugins.size() <= 1);
      Update update = null;
      for (SentryPolicyStorePlugin plugin : sentryPlugins) {
        update = plugin.onDropSentryPrivilege(request);
      }
      if (update != null) {
        sentryStore.dropPrivilege(request.getAuthorizable(), update);
      } else {
        sentryStore.dropPrivilege(request.getAuthorizable());
      }
      response.setStatus(Status.OK());
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
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
    } finally {
      timerContext.stop();
    }
    return response;
  }

  @Override
  public TRenamePrivilegesResponse rename_sentry_privilege(
      TRenamePrivilegesRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.renamePrivilegeTimer.time();
    TRenamePrivilegesResponse response = new TRenamePrivilegesResponse();
    try {
      validateClientVersion(request.getProtocol_version());
      authorize(request.getRequestorUserName(), adminGroups);

      // TODO: now only has SentryPlugin. Once add more SentryPolicyStorePlugins,
      // TODO: need to differentiate the updates for different Plugins.
      Preconditions.checkState(sentryPlugins.size() <= 1);
      Update update = null;
      for (SentryPolicyStorePlugin plugin : sentryPlugins) {
        update = plugin.onRenameSentryPrivilege(request);
      }
      if (update != null) {
        sentryStore.renamePrivilege(request.getOldAuthorizable(),
            request.getNewAuthorizable(), update);
      } else {
        sentryStore.renamePrivilege(request.getOldAuthorizable(),
            request.getNewAuthorizable());
      }
      response.setStatus(Status.OK());
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    } catch (SentryInvalidInputException e) {
      response.setStatus(Status.InvalidInput(e.getMessage(), e));
    }
    catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: "
          + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    } finally {
      timerContext.close();
    }
    return response;
  }

  @Override
  public TListSentryPrivilegesByAuthResponse list_sentry_privileges_by_authorizable(
      TListSentryPrivilegesByAuthRequest request) throws TException {
    final Timer.Context timerContext = sentryMetrics.listPrivilegesByAuthorizableTimer.time();
    TListSentryPrivilegesByAuthResponse response = new TListSentryPrivilegesByAuthResponse();
    Map<TSentryAuthorizable, TSentryPrivilegeMap> authRoleMap = Maps.newHashMap();
    String subject = request.getRequestorUserName();
    Set<String> requestedGroups = request.getGroups();
    TSentryActiveRoleSet requestedRoleSet = request.getRoleSet();
    try {
      validateClientVersion(request.getProtocol_version());
      Set<String> memberGroups = getRequestorGroups(subject);
      if(!inAdminGroups(memberGroups)) {
        // disallow non-admin to lookup groups that they are not part of
        if (requestedGroups != null && !requestedGroups.isEmpty()) {
          for (String requestedGroup : requestedGroups) {
            if (!memberGroups.contains(requestedGroup)) {
              // if user doesn't belong to one of the requested group then raise error
              throw new SentryAccessDeniedException("Access denied to " + subject);
            }
          }
        } else {
          // non-admin's search is limited to it's own groups
          requestedGroups = memberGroups;
        }

        // disallow non-admin to lookup roles that they are not part of
        if (requestedRoleSet != null && !requestedRoleSet.isAll()) {
          Set<String> roles = toTrimedLower(sentryStore
              .getRoleNamesForGroups(memberGroups));
          for (String role : toTrimedLower(requestedRoleSet.getRoles())) {
            if (!roles.contains(role)) {
              throw new SentryAccessDeniedException("Access denied to "
                  + subject);
            }
          }
        }
      }

      // If user is not part of any group.. return empty response
      for (TSentryAuthorizable authorizable : request.getAuthorizableSet()) {
        authRoleMap.put(authorizable, sentryStore
            .listSentryPrivilegesByAuthorizable(requestedGroups,
                request.getRoleSet(), authorizable, inAdminGroups(memberGroups)));
      }
      response.setPrivilegesMapByAuth(authRoleMap);
      response.setStatus(Status.OK());
      // TODO : Sentry - HDFS : Have to handle this
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
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
    } finally {
      timerContext.stop();
    }
    return response;
  }

  /**
   * Respond to a request for a config value in the sentry server.  The client
   * can request any config value that starts with "sentry." and doesn't contain
   * "keytab".
   * @param request Contains config parameter sought and default if not found
   * @return The response, containing the value and status
   * @throws TException
   */
  @Override
  public TSentryConfigValueResponse get_sentry_config_value(
          TSentryConfigValueRequest request) throws TException {

    final String requirePattern = "^sentry\\..*";
    final String excludePattern = ".*keytab.*|.*\\.jdbc\\..*|.*password.*";

    TSentryConfigValueResponse response = new TSentryConfigValueResponse();
    String attr = request.getPropertyName();

    try {
      validateClientVersion(request.getProtocol_version());
    } catch (SentryThriftAPIMismatchException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.THRIFT_VERSION_MISMATCH(e.getMessage(), e));
    }
    // Only allow config parameters like...
    if (!Pattern.matches(requirePattern, attr) ||
        Pattern.matches(excludePattern, attr)) {
      String msg = "Attempted access of the configuration property " + attr +
              " was denied";
      LOGGER.error(msg);
      response.setStatus(Status.AccessDenied(msg,
              new SentryAccessDeniedException(msg)));
      return response;
    }

    response.setValue(conf.get(attr,request.getDefaultValue()));
    response.setStatus(Status.OK());
    return response;
  }

  @VisibleForTesting
  static void validateClientVersion(int protocolVersion) throws SentryThriftAPIMismatchException {
    if (ServiceConstants.ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT != protocolVersion) {
      String msg = "Sentry thrift API protocol version mismatch: Client thrift version " +
          "is: " + protocolVersion + " , server thrift verion " +
              "is " + ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT;
      throw new SentryThriftAPIMismatchException(msg);
    }
  }

  // get the sentry mapping data and return the data with map structure
  @Override
  public TSentryExportMappingDataResponse export_sentry_mapping_data(
      TSentryExportMappingDataRequest request) throws TException {
    TSentryExportMappingDataResponse response = new TSentryExportMappingDataResponse();
    try {
      String requestor = request.getRequestorUserName();
      Set<String> memberGroups = getRequestorGroups(requestor);
      String objectPath = request.getObjectPath();
      String databaseName = null;
      String tableName = null;

      Map<String, String> objectMap =
          SentryServiceUtil.parseObjectPath(objectPath);
      databaseName = objectMap.get(PolicyFileConstants.PRIVILEGE_DATABASE_NAME);
      tableName = objectMap.get(PolicyFileConstants.PRIVILEGE_TABLE_NAME);

      if (!inAdminGroups(memberGroups)) {
        // disallow non-admin to import the metadata of sentry
        throw new SentryAccessDeniedException("Access denied to " + requestor
            + " for export the metadata of sentry.");
      }
      TSentryMappingData tSentryMappingData = new TSentryMappingData();
      Map<String, Set<TSentryPrivilege>> rolePrivileges =
          sentryStore.getRoleNameTPrivilegesMap(databaseName, tableName);
      tSentryMappingData.setRolePrivilegesMap(rolePrivileges);
      Set<String> roleNames = rolePrivileges.keySet();
      // roleNames should be null if databaseName == null and tableName == null
      if (databaseName == null && tableName == null) {
        roleNames = null;
      }
      List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(
          roleNames);
      tSentryMappingData.setGroupRolesMap(mapList.get(
          SentryStore.INDEX_GROUP_ROLES_MAP));
      tSentryMappingData.setUserRolesMap(mapList.get(SentryStore.INDEX_USER_ROLES_MAP));

      response.setMappingData(tSentryMappingData);
      response.setStatus(Status.OK());
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setMappingData(new TSentryMappingData());
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  // import the sentry mapping data
  @Override
  public TSentryImportMappingDataResponse import_sentry_mapping_data(
      TSentryImportMappingDataRequest request) throws TException {
    TSentryImportMappingDataResponse response = new TSentryImportMappingDataResponse();
    try {
      String requestor = request.getRequestorUserName();
      Set<String> memberGroups = getRequestorGroups(requestor);
      if (!inAdminGroups(memberGroups)) {
        // disallow non-admin to import the metadata of sentry
        throw new SentryAccessDeniedException("Access denied to " + requestor
            + " for import the metadata of sentry.");
      }
      sentryStore.importSentryMetaData(request.getMappingData(), request.isOverwriteRole());
      response.setStatus(Status.OK());
    } catch (SentryAccessDeniedException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryGroupNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      response.setStatus(Status.AccessDenied(e.getMessage(), e));
    } catch (SentryInvalidInputException e) {
      String msg = "Invalid input privilege object";
      LOGGER.error(msg, e);
      response.setStatus(Status.InvalidInput(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  @Override
  public TSentrySyncIDResponse sentry_sync_notifications(TSentrySyncIDRequest request)
          throws TException {
    TSentrySyncIDResponse response = new TSentrySyncIDResponse();
    try (Timer.Context timerContext = hmsWaitTimer.time()) {
      // Wait until Sentry Server processes specified HMS Notification ID.
      response.setId(sentryStore.getCounterWait().waitFor(request.getId()));
      response.setStatus(Status.OK());
    } catch (InterruptedException e) {
      String msg = String.format("wait request for id %d is interrupted",
              request.getId());
      LOGGER.error(msg, e);
      response.setId(0);
      response.setStatus(Status.RuntimeError(msg, e));
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      String msg = String.format("timed out wait request for id %d", request.getId());
      LOGGER.warn(msg, e);
      response.setId(0);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }
}
