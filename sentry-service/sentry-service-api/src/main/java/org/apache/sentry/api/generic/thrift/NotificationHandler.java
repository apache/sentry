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

public interface NotificationHandler {

  void create_sentry_role(TCreateSentryRoleRequest request,
                          TCreateSentryRoleResponse response);

  void drop_sentry_role(TDropSentryRoleRequest request,
      TDropSentryRoleResponse response);

  void alter_sentry_role_grant_privilege(TAlterSentryRoleGrantPrivilegeRequest request,
      TAlterSentryRoleGrantPrivilegeResponse response);

  void alter_sentry_role_revoke_privilege(TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response);

  void alter_sentry_role_add_groups(TAlterSentryRoleAddGroupsRequest request,
      TAlterSentryRoleAddGroupsResponse response);

  void alter_sentry_role_delete_groups(TAlterSentryRoleDeleteGroupsRequest request,
      TAlterSentryRoleDeleteGroupsResponse response);

  void drop_sentry_privilege(TDropPrivilegesRequest request,
      TDropPrivilegesResponse response);

  void rename_sentry_privilege(TRenamePrivilegesRequest request,
      TRenamePrivilegesResponse response);
}
