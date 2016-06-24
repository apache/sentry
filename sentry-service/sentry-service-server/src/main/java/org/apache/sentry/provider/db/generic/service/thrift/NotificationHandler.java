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

import org.apache.sentry.provider.db.service.persistent.CommitContext;

public interface NotificationHandler {

  void create_sentry_role(CommitContext context,
      TCreateSentryRoleRequest request, TCreateSentryRoleResponse response);

  void drop_sentry_role(CommitContext context, TDropSentryRoleRequest request,
      TDropSentryRoleResponse response);

  void alter_sentry_role_grant_privilege(CommitContext context, TAlterSentryRoleGrantPrivilegeRequest request,
      TAlterSentryRoleGrantPrivilegeResponse response);

  void alter_sentry_role_revoke_privilege(CommitContext context, TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response);

  void alter_sentry_role_add_groups(CommitContext context,TAlterSentryRoleAddGroupsRequest request,
      TAlterSentryRoleAddGroupsResponse response);

  void alter_sentry_role_delete_groups(CommitContext context, TAlterSentryRoleDeleteGroupsRequest request,
      TAlterSentryRoleDeleteGroupsResponse response);

  void drop_sentry_privilege(CommitContext context, TDropPrivilegesRequest request,
      TDropPrivilegesResponse response);

  void rename_sentry_privilege(CommitContext context, TRenamePrivilegesRequest request,
      TRenamePrivilegesResponse response);
}
