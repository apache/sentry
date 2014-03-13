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
include "sentry_common_service.thrift"

namespace java org.apache.sentry.provider.db.service.thrift
namespace php sentry.provider.db.service.thrift
namespace cpp Apache.Sentry.Provider.Db.Service.Thrift

struct TSentryPrivilege {
1: required string privilegeScope, # Valid values are SERVER, DATABASE, TABLE
2: optional string privilegeName, # Generated on server side
3: required string serverName,
4: optional string dbName,
5: optional string tableName,
6: optional string URI,
7: required string action,
8: optional i64 createTime, # Set on server side
9: optional string grantorPrincipal # Set on server side
}

struct TSentryRole {
1: required string roleName,
# TODO privs should not be part of Sentry role as
# they are created when a grant is executed
# They need to be returned as part of the list role API, else
# there would be another round trip
2: required set<TSentryPrivilege> privileges,
3: required i64 createTime,
4: required string grantorPrincipal
}

// TODO fill out
struct TSentryGroup {
1: required string groupName
}

struct TCreateSentryRoleRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName,
3: required TSentryRole role,
4: required set<string> requestorGroupName
}
struct TCreateSentryRoleResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

struct TListSentryRolesRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName, # user on whose behalf the request is issued
3: optional string rolerequestorGroupName, # list roles for this group
4: required string roleName,
5: required set<string> requestorGroupName # groups the requesting user belongs to
}
struct TListSentryRolesResponse {
1: required sentry_common_service.TSentryResponseStatus status
2: required set<TSentryRole> roles
}

struct TDropSentryRoleRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName,
3: required string roleName,
4: required set<string> requestorGroupName
}
struct TDropSentryRoleResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

struct TAlterSentryRoleAddGroupsRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName,
3: required string roleName,
4: required set<string> requestorGroupName,
5: required set<TSentryGroup> groups
}

struct TAlterSentryRoleAddGroupsResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

struct TAlterSentryRoleDeleteGroupsRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName,
3: required set<string> requestorGroupName
}
struct TAlterSentryRoleDeleteGroupsResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

struct TAlterSentryRoleGrantPrivilegeRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName,
3: required string roleName,
4: required set<string> requestorGroupName,
5: required TSentryPrivilege privilege
}

struct TAlterSentryRoleGrantPrivilegeResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

struct TAlterSentryRoleRevokePrivilegeRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName,
3: required string roleName,
4: required set<string> requestorGroupName,
5: required TSentryPrivilege privilege
}

struct TAlterSentryRoleRevokePrivilegeResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

service SentryPolicyService
{
  TCreateSentryRoleResponse create_sentry_role(1:TCreateSentryRoleRequest request)
  TDropSentryRoleResponse drop_sentry_role(1:TDropSentryRoleRequest request)
  
  TAlterSentryRoleGrantPrivilegeResponse alter_sentry_role_grant_privilege(1:TAlterSentryRoleGrantPrivilegeRequest request)
  TAlterSentryRoleRevokePrivilegeResponse alter_sentry_role_revoke_privilege(1:TAlterSentryRoleRevokePrivilegeRequest request)
  
  TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(1:TAlterSentryRoleAddGroupsRequest request)
  TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(1:TAlterSentryRoleDeleteGroupsRequest request)

  TListSentryRolesResponse list_sentry_roles_by_group(1:TListSentryRolesRequest request)
  TListSentryRolesResponse list_sentry_roles_by_role_name(1:TListSentryRolesRequest request) 
}
