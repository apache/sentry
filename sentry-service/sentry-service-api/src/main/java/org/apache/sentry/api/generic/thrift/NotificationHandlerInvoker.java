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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Invokes configured instances of NotificationHandler. Importantly
 * NotificationHandler's each receive a copy of the request and
 * response thrift objects from each successful request.
 */
public class NotificationHandlerInvoker implements NotificationHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationHandlerInvoker.class);
  private List<? extends NotificationHandler> handlers = Lists.newArrayList();

  public NotificationHandlerInvoker(List<? extends NotificationHandler> handlers) {
    this.handlers = handlers;
  }
  @Override
  public void create_sentry_role(TCreateSentryRoleRequest request,
                                 TCreateSentryRoleResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.create_sentry_role(new TCreateSentryRoleRequest(request),
                                   new TCreateSentryRoleResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void drop_sentry_role(TDropSentryRoleRequest request,
                               TDropSentryRoleResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.drop_sentry_role(new TDropSentryRoleRequest(request),
                                 new TDropSentryRoleResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_grant_privilege(
          TAlterSentryRoleGrantPrivilegeRequest request,
          TAlterSentryRoleGrantPrivilegeResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_grant_privilege(
            new TAlterSentryRoleGrantPrivilegeRequest(request),
            new TAlterSentryRoleGrantPrivilegeResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_revoke_privilege(
      TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_revoke_privilege(
            new TAlterSentryRoleRevokePrivilegeRequest(request),
            new TAlterSentryRoleRevokePrivilegeResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_add_groups(
      TAlterSentryRoleAddGroupsRequest request,
      TAlterSentryRoleAddGroupsResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_add_groups(new TAlterSentryRoleAddGroupsRequest(request),
                                             new TAlterSentryRoleAddGroupsResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

  @Override
  public void alter_sentry_role_delete_groups(
      TAlterSentryRoleDeleteGroupsRequest request,
      TAlterSentryRoleDeleteGroupsResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.alter_sentry_role_delete_groups(new TAlterSentryRoleDeleteGroupsRequest(request),
                                                new TAlterSentryRoleDeleteGroupsResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }
  @Override
  public void drop_sentry_privilege(
      TDropPrivilegesRequest request, TDropPrivilegesResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.drop_sentry_privilege(new TDropPrivilegesRequest(request),
                                                new TDropPrivilegesResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }
  @Override
  public void rename_sentry_privilege(TRenamePrivilegesRequest request,
                                      TRenamePrivilegesResponse response) {
    for (NotificationHandler handler : handlers) {
      try {
        LOGGER.debug("Calling " + handler);
        handler.rename_sentry_privilege(new TRenamePrivilegesRequest(request),
                                        new TRenamePrivilegesResponse(response));
      } catch (Exception ex) {
        LOGGER.error("Unexpected error in " + handler + ". Request: "
                     + request + ", Response: " + response, ex);
      }
    }
  }

}
