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

namespace java org.apache.sentry.hdfs.service.thrift
namespace php sentry.hdfs.thrift
namespace cpp Apache.Sentry.HDFS.Thrift

struct TPathChanges {
1: required string authzObj;
2: required list<list<string>> addPaths;
3: required list<list<string>> delPaths;
}

struct TPathEntry {
1: required byte type;
2: required string pathElement;
3: optional string authzObj;
4: required set<i32> children;
}

struct TPathsDump {
1: required i32 rootId;
2: required map<i32,TPathEntry> nodeMap;
}

struct TPathsUpdate {
1: required bool hasFullImage;
2: optional TPathsDump pathsDump;
3: required i64 seqNum;
4: required list<TPathChanges> pathChanges;
}

struct TPrivilegeChanges {
1: required string authzObj;
2: required map<string, string> addPrivileges;
3: required map<string, string> delPrivileges;
}

struct TRoleChanges {
1: required string role;
2: required list<string> addGroups;
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

service SentryHDFSService
{
  # HMS Path cache
  void handle_hms_notification(1:TPathsUpdate pathsUpdate);

  TAuthzUpdateResponse get_all_authz_updates_from(1:i64 permSeqNum, 2:i64 pathSeqNum);
  map<string, list<string>> get_all_related_paths(1:string path, 2:bool exactMatch);
}
