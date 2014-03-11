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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.CommitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Invokes configured instances of NotificationHandler. Importantly
 * NotificationHandler's each receive a copy of the request and
 * response thrift objects from each successful request.
 */
public class NotificationHandlerInvoker extends NotificationHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationHandlerInvoker.class);

  private final ImmutableList<NotificationHandler> handlers;

  public NotificationHandlerInvoker(Configuration conf, NotificationHandler handler)
  throws Exception {
    this(conf, ImmutableList.of(handler));
  }

  public NotificationHandlerInvoker(Configuration conf, List<NotificationHandler> handlers)
  throws Exception {
    super(conf);
    this.handlers = ImmutableList.copyOf(handlers);
  }

  @Override
  public void create_sentry_role(CommitContext context,
                                 TCreateSentryRoleRequest request, TCreateSentryRoleResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.create_sentry_role(context,  new TCreateSentryRoleRequest(request),
                                   new TCreateSentryRoleResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void drop_sentry_role(CommitContext context, TDropSentryRoleRequest request,
                               TDropSentryRoleResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.drop_sentry_role(context,  new TDropSentryRoleRequest(request),
                                 new TDropSentryRoleResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_grant_privilege(CommitContext context,
      TAlterSentryRoleGrantPrivilegeRequest request,
      TAlterSentryRoleGrantPrivilegeResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_grant_privilege(context,
            new TAlterSentryRoleGrantPrivilegeRequest(request),
            new TAlterSentryRoleGrantPrivilegeResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_revoke_privilege(CommitContext context,
      TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_revoke_privilege(context,
            new TAlterSentryRoleRevokePrivilegeRequest(request),
            new TAlterSentryRoleRevokePrivilegeResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_add_groups(CommitContext context,
      TAlterSentryRoleAddGroupsRequest request,
      TAlterSentryRoleAddGroupsResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_add_groups(context, new TAlterSentryRoleAddGroupsRequest(request),
                                             new TAlterSentryRoleAddGroupsResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_delete_groups(
    CommitContext context, TAlterSentryRoleDeleteGroupsRequest request,
    TAlterSentryRoleDeleteGroupsResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_delete_groups(context, new TAlterSentryRoleDeleteGroupsRequest(request),
                                                new TAlterSentryRoleDeleteGroupsResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }
}