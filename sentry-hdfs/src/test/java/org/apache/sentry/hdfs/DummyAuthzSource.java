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
package org.apache.sentry.hdfs;

//import org.apache.sentry.hdfs.old.AuthzPermCache.AuthzSource;
//import org.apache.sentry.hdfs.old.AuthzPermCache.PrivilegeInfo;
//import org.apache.sentry.hdfs.old.AuthzPermCache.RoleInfo;

public class DummyAuthzSource {
//public class DummyAuthzSource implements AuthzSource{
//
//  public Map<String, PrivilegeInfo> privs = new HashMap<String, PrivilegeInfo>();
//  public Map<String, RoleInfo> roles = new HashMap<String, RoleInfo>();
//
//  @Override
//  public PrivilegeInfo loadPrivilege(String authzObj) throws Exception {
//    return privs.get(authzObj);
//  }
//
//  @Override
//  public RoleInfo loadGroupsForRole(String group) throws Exception {
//    return roles.get(group);
//  }
//
//  @Override
//  public PermissionsUpdate createFullImage(long seqNum) {
//    PermissionsUpdate retVal = new PermissionsUpdate(seqNum, true);
//    for (Map.Entry<String, PrivilegeInfo> pE : privs.entrySet()) {
//      PrivilegeChanges pUpdate = retVal.addPrivilegeUpdate(pE.getKey());
//      PrivilegeInfo pInfo = pE.getValue();
//      for (Map.Entry<String, FsAction> ent : pInfo.roleToPermission.entrySet()) {
//        pUpdate.addPrivilege(ent.getKey(), ent.getValue().SYMBOL);
//      }
//    }
//    for (Map.Entry<String, RoleInfo> rE : roles.entrySet()) {
//      RoleChanges rUpdate = retVal.addRoleUpdate(rE.getKey());
//      RoleInfo rInfo = rE.getValue();
//      for (String role : rInfo.groups) {
//        rUpdate.addGroup(role);
//      }
//    }
//    return retVal;
//  }

}
