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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.CommitContext;
import org.apache.sentry.provider.db.service.persistent.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.service.persistent.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.PolicyStoreConstants.PolicyStoreServerConfig;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

@SuppressWarnings("unused")
public class SentryPolicyStoreProcessor implements SentryPolicyService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryPolicyStoreProcessor.class);

  public static final String SENTRY_POLICY_SERVICE_NAME = "SentryPolicyService";

  private final String name;
  private final Configuration conf;
  private final SentryStore sentryStore;
  private final NotificationHandlerInvoker notificationHandlerInvoker;
  private boolean isReady;

  public SentryPolicyStoreProcessor(String name, Configuration conf) throws Exception {
    super();
    this.name = name;
    this.conf = conf;
    this.notificationHandlerInvoker = new NotificationHandlerInvoker(conf,
        createHandlers(conf));
    isReady = false;
    sentryStore = new SentryStore();
    isReady = true;
  }

  public void stop() {
    if (isReady) {
      sentryStore.stop();
    }
  }

  @VisibleForTesting
  static List<NotificationHandler> createHandlers(Configuration conf)
      throws Exception {
    List<NotificationHandler> handlers = Lists.newArrayList();
    Iterable<String> notificationHandlers = Splitter.onPattern("[\\s,]").trimResults()
        .omitEmptyStrings().split(conf.get(PolicyStoreServerConfig.NOTIFICATION_HANDLERS, ""));
    for (String notificationHandler : notificationHandlers) {
      Class<?> clazz = null;
      try {
        clazz = Class.forName(notificationHandler);
        if (!NotificationHandler.class.isAssignableFrom(clazz)) {
          throw new IllegalArgumentException("Class " + notificationHandler + " is not a " +
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

  @Override
  public TCreateSentryRoleResponse create_sentry_role(
    TCreateSentryRoleRequest request) throws TException {
    TCreateSentryRoleResponse response = new TCreateSentryRoleResponse();
    try {
      CommitContext commitContext = sentryStore.createSentryRole(request.getRole());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.create_sentry_role(commitContext,
          request, response);
    } catch (SentryAlreadyExistsException e) {
      String msg = "Role: " + request + " already exists.";
      LOGGER.error(msg, e);
      response.setStatus(Status.AlreadyExists(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }
  @Override
  public TCreateSentryPrivilegeResponse create_sentry_privilege(
    TCreateSentryPrivilegeRequest request) throws TException {
    TCreateSentryPrivilegeResponse response = new TCreateSentryPrivilegeResponse();
    try {
      CommitContext commitContext = sentryStore.createSentryPrivilege(request.getPrivilege());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.create_sentry_privilege(commitContext,
          request, response);
    } catch (SentryAlreadyExistsException e) {
      String msg = "Privilege: " + request + " already exists.";
      LOGGER.error(msg, e);
      response.setStatus(Status.AlreadyExists(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  public TDropSentryRoleResponse drop_sentry_role(
    TDropSentryRoleRequest request)  throws TException {
    TDropSentryRoleResponse response = new TDropSentryRoleResponse();
    TSentryResponseStatus status;
    try {
      CommitContext commitContext = sentryStore.dropSentryRole(request.getRoleName());
      response.setStatus(Status.OK());
      notificationHandlerInvoker.drop_sentry_role(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role :" + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  @Override
  public TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(
    TAlterSentryRoleAddGroupsRequest request) throws TException {
    TAlterSentryRoleAddGroupsResponse response = new TAlterSentryRoleAddGroupsResponse();
    try {
      CommitContext commitContext = sentryStore.alterSentryRoleAddGroups();
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_add_groups(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }
  @Override
  public TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(
      TAlterSentryRoleDeleteGroupsRequest request) throws TException {
    // TODO implement
    TAlterSentryRoleDeleteGroupsResponse response = new TAlterSentryRoleDeleteGroupsResponse();
    try {
      CommitContext commitContext = sentryStore.alterSentryRoleDeleteGroups();
      response.setStatus(Status.OK());
      notificationHandlerInvoker.alter_sentry_role_delete_groups(commitContext,
          request, response);
    } catch (SentryNoSuchObjectException e) {
      String msg = "Role: " + request + " does not exist.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error adding groups to role: " + request;
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  @Override
  public TListSentryRolesResponse list_sentry_roles_by_group(
    TListSentryRolesRequest request) throws TException {
    TListSentryRolesResponse response = new TListSentryRolesResponse();
    TSentryResponseStatus status;
    TSentryRole role = null;
    Set<TSentryRole> roleSet = new HashSet<TSentryRole>();
    try {
      // TODO implement
      role = sentryStore.getSentryRoleByName(request.getRoleName());
      roleSet.add(role);
      response.setRoles(roleSet);
      response.setStatus(Status.OK());
    } catch (SentryNoSuchObjectException e) {
      response.setRoles(roleSet);
      String msg = "Role: " + request + " couldn't be retrieved.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }

  @Override
  public TListSentryRolesResponse list_sentry_roles_by_role_name(
    TListSentryRolesRequest request) throws TException {
    TListSentryRolesResponse response = new TListSentryRolesResponse();
    TSentryResponseStatus status;
    TSentryRole role = null;
    Set<TSentryRole> roleSet = new HashSet<TSentryRole>();
    try {
      role = sentryStore.getSentryRoleByName(request.getRoleName());
      roleSet.add(role);
      response.setRoles(roleSet);
      response.setStatus(Status.OK());
    } catch (SentryNoSuchObjectException e) {
      response.setRoles(roleSet);
      String msg = "Role: " + request + " couldn't be retrieved.";
      LOGGER.error(msg, e);
      response.setStatus(Status.NoSuchObject(msg, e));
    } catch (Exception e) {
      String msg = "Unknown error for request: " + request + ", message: " + e.getMessage();
      LOGGER.error(msg, e);
      response.setStatus(Status.RuntimeError(msg, e));
    }
    return response;
  }
}