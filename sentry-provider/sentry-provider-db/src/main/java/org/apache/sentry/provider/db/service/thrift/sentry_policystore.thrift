#!/usr/local/bin/thrift -java

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

#
# Thrift Service that the MetaStore is built on
#

include "share/fb303/if/fb303.thrift"

namespace java org.apache.sentry.policystore.api
namespace php sentrypolicystore
namespace cpp Apache.Sentry

enum TSentryPolicyServiceVersion {
V1
}

struct TSentryPrivilege {
1: required string privilegeScope,
2: required string privilegeName,
3: required string serverName,
4: optional string dbName,
5: optional string tableName,
6: optional string URI,
7: required string action,
8: required i64 createTime,
9: required string grantorPrincipal
}

struct TSentryRole {
1: required string roleName,
2: required set<TSentryPrivilege> privileges,
3: required i64 createTime,
4: required string grantorPrincipal
}
// TODO fill out
struct TSentryGroup {
1: required string groupName
}

struct TCreateSentryRoleRequest {
1: required TSentryPolicyServiceVersion protocol_version = TSentryPolicyServiceVersion.V1,
2: required string userName,
3: required TSentryRole role
}
struct TCreateSentryRoleResponse {
1: required bool success
}

struct TCreateSentryPrivilegeRequest {
1: required TSentryPolicyServiceVersion protocol_version = TSentryPolicyServiceVersion.V1,
2: required string userName,
3: required TSentryPrivilege privilege
}
struct TCreateSentryPrivilegeResponse {
1: required bool success
}

struct TCreateSentryPrivilegeRequest {
1: required TSentryPolicyServiceVersion protocol_version = TSentryPolicyServiceVersion.V1,
2: required string userName,
3: required TSentryPrivilege privilege
}
struct TCreateSentryPrivilegeResponse {
1: required bool success
}

struct TAlterSentryRoleAddGroupsRequest {
1: required TSentryPolicyServiceVersion protocol_version = TSentryPolicyServiceVersion.V1,
2: required string userName,
3: required string roleName,
4: required set<TSentryGroup> groups
}
struct TAlterSentryRoleAddGroupsResponse {
1: required bool success
}

struct TAlterSentryRoleDeleteGroupsRequest {
1: required TSentryPolicyServiceVersion protocol_version = TSentryPolicyServiceVersion.V1,
2: required string userName,
}
struct TAlterSentryRoleDeleteGroupsResponse {
1: required bool success
}

struct TListSentryRolesRequest {
1: required TSentryPolicyServiceVersion protocol_version = TSentryPolicyServiceVersion.V1,
2: required string userName,
3: optional string groupName,
4: optional string roleName
}
struct TListSentryRolesResponse {
1: required bool success,
2: required set<TSentryRole> roles
}

exception TSentryAlreadyExistsException {
  1: string message
}

exception TSentryNoSuchObjectException {
  1: string message
}

service SentryThriftPolicyService
{
  TCreateSentryRoleResponse create_sentry_role(1:TCreateSentryRoleRequest request) throws (1:TSentryAlreadyExistsException o1)
  //TDropSentryRoleResponse drop_sentry_role(1:TDropSentryRoleRequest request) throws (1:TSentryNoSuchObjectException o1)

  TCreateSentryPrivilegeResponse create_sentry_privilege(1:TCreateSentryPrivilegeRequest request) throws (1:TSentryAlreadyExistsException o1)
  //TDropSentryPrivilegeResponse drop_sentry_privilege(1:TDropSentryPrivilegeRequest request) throws (1:TSentryNoSuchObjectException o1)

  TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(1:TAlterSentryRoleAddGroupsRequest request) throws (1:TSentryNoSuchObjectException o1)
  TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(1:TAlterSentryRoleDeleteGroupsRequest request) throws (1:TSentryNoSuchObjectException o1)

  TListSentryRolesResponse list_sentry_roles(1:TListSentryRolesRequest request) throws (1:TSentryNoSuchObjectException o1)
}
