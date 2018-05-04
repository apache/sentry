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

namespace java org.apache.sentry.hdfs.service.thrift
namespace php sentry.hdfs.thrift
namespace cpp Apache.Sentry.HDFS.Thrift

enum TPrivilegeEntityType {
  ROLE,
  USER,
  AUTHZ_OBJ
}

struct TPrivilegeEntity {

# Type of the privilege entity
1: required TPrivilegeEntityType type;

# Value of entity
2: required string value;
}

struct TPathChanges {

# The authorizable object that needs to be updated.
1: required string authzObj;

# The path (splits into string segments) that needs to be
# added to the authorizable object.
2: required list<list<string>> addPaths;

# The path (splits into string segments) that needs to be
# deleted to the authorizable object.
3: required list<list<string>> delPaths;
}

struct TPathEntry {

# The type of the Path Entry.
1: required byte type;

# The path element in string.
2: required string pathElement;

# The child tuple id of the Path Entry.
4: required list<i32> children;

# A set of authzObjs associated with the Path Entry.
5: optional list<string> authzObjs;
}

struct TPathsDump {
1: required i32 rootId;
2: required map<i32,TPathEntry> nodeMap;
3: optional list<string> dupStringValues;
}

# Value used to specify that a path image number is not used on a request or response
const i64 UNUSED_PATH_UPDATE_IMG_NUM = -1;

struct TPathsUpdate {
1: required bool hasFullImage;
2: optional TPathsDump pathsDump;
3: required i64 seqNum;
4: required list<TPathChanges> pathChanges;
5: optional i64 imgNum = UNUSED_PATH_UPDATE_IMG_NUM;
}

struct TPrivilegeChanges {

# The authorizable object that needs to be updated.
1: required string authzObj;

# The privileges that needs to be added to
# the authorizable object.
2: required map<TPrivilegeEntity, string> addPrivileges;

# The privileges that needs to be deleted to
# the authorizable object.
3: required map<TPrivilegeEntity, string> delPrivileges;
}

struct TRoleChanges {

# The role that needs to be updated.
1: required string role;

# The groups that needs to be added.
2: required list<string> addGroups;

# The groups that needs to be deleted.
3: required list<string> delGroups;
}

struct TPermissionsUpdate {
1: required bool hasfullImage;
2: required i64 seqNum;
3: required map<string, TPrivilegeChanges> privilegeChanges;
4: required map<string, TRoleChanges> roleChanges;
}

struct TAuthzUpdateResponse {
1: optional list<TPathsUpdate> authzPathUpdate,
2: optional list<TPermissionsUpdate> authzPermUpdate,
}

struct TAuthzUpdateRequest {
1: required i64 permSeqNum;
2: required i64 pathSeqNum;
3: required i64 pathImgNum;
}

service SentryHDFSService
{
  # HMS Path cache
  void handle_hms_notification(1:TPathsUpdate pathsUpdate);
  i64 check_hms_seq_num(1:i64 pathSeqNum);
  TAuthzUpdateResponse get_all_authz_updates_from(1:i64 permSeqNum, 2:i64 pathSeqNum);
  TAuthzUpdateResponse get_authz_updates(1:TAuthzUpdateRequest request);
  map<string, list<string>> get_all_related_paths(1:string path, 2:bool exactMatch);
}
