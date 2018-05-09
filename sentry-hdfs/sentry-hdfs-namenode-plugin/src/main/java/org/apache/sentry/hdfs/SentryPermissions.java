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

import java.util.*;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeEntity;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeEntityType;

public class SentryPermissions implements AuthzPermissions {

  public static class PrivilegeInfo {
    private final String authzObj;
    // It is safe to use TPrivilegeEntity as key as it implements the hashCode and equals API's.
    // Equals() API would help in handling hash collisions.
    private final Map<TPrivilegeEntity, FsAction> privilegeEntityFsActionMap = new HashMap<TPrivilegeEntity, FsAction>();
    public PrivilegeInfo(String authzObj) {
      this.authzObj = authzObj;
    }
    public PrivilegeInfo setPermission(TPrivilegeEntity privilegeEntity, FsAction perm) {
      privilegeEntityFsActionMap.put(privilegeEntity, perm);
      return this;
    }
    public PrivilegeInfo removePermission(TPrivilegeEntity privilegeEntity) {
      privilegeEntityFsActionMap.remove(privilegeEntity);
      return this;
    }
    public FsAction getPermission(TPrivilegeEntity privilegeEntity) {
      return privilegeEntityFsActionMap.get(privilegeEntity);
    }
    public Map<TPrivilegeEntity, FsAction> getAllPermissions() {
      return privilegeEntityFsActionMap;
    }
    public String getAuthzObj() {
      return authzObj;
    }
    @Override
    public String toString() {
      return "PrivilegeInfo(" + authzObj + " --> " + privilegeEntityFsActionMap + ")";
    }
  }

  public static class RoleInfo {
    private final String role;
    private final Set<String> groups = new HashSet<String>();
    public RoleInfo(String role) {
      this.role = role;
    }
    public RoleInfo addGroup(String group) {
      groups.add(group);
      return this;
    }
    public RoleInfo delGroup(String group) {
      groups.remove(group);
      return this;
    }
    public String getRole() {
      return role;
    }
    public Set<String> getAllGroups() {
      return groups;
    }
    @Override
    public String toString() {
      return "RoleInfo(" + role + " --> " + groups + ")";
    }
  }

  // Comparison of authorizable object should be case insensitive.
  private final Map<String, PrivilegeInfo> privileges = new TreeMap<String, PrivilegeInfo>(String.CASE_INSENSITIVE_ORDER);
  private Map<String, Set<String>> authzObjChildren = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);

  // RoleInfo should be case insensitive.
  private final Map<String, RoleInfo> roles = new TreeMap<String, RoleInfo>(String.CASE_INSENSITIVE_ORDER);

  String getParentAuthzObject(String authzObject) {
    if (authzObject != null) {
      int dot = authzObject.indexOf('.');
      if (dot > 0) {
        return authzObject.substring(0, dot);
      }
    }
    return authzObject;
  }

  void addParentChildMappings(String authzObject) {
    String parent = getParentAuthzObject(authzObject);
    if (parent != null) {
      Set<String> children = authzObjChildren.get(parent);
      if (children == null) {
        children = new HashSet<String>();
        authzObjChildren.put(parent, children);
      }
      children.add(authzObject);
    }
  }

  void removeParentChildMappings(String authzObject) {
    String parent = getParentAuthzObject(authzObject);
    if (parent != null) {
      Set<String> children = authzObjChildren.get(parent);
      if (children != null) {
        children.remove(authzObject);
      }
    } else {
      // is parent
      authzObjChildren.remove(authzObject);
    }
  }

  private Map<String, FsAction> getGroupPerms(String authzObj) {
    Map<String, FsAction> groupPerms;
    String parent = getParentAuthzObject(authzObj);
    if (parent == null || parent.equals(authzObj)) {
      groupPerms = new HashMap<String, FsAction>();
    } else {
      groupPerms = getGroupPerms(parent);
    }

    PrivilegeInfo privilegeInfo = privileges.get(authzObj);
    if (privilegeInfo != null) {
      for (Map.Entry<TPrivilegeEntity, FsAction> privs : privilegeInfo
          .getAllPermissions().entrySet()) {
        if(privs.getKey().getType() == TPrivilegeEntityType.ROLE) {
          constructAclEntry(privs.getKey().getValue(), privs.getValue(), groupPerms);
        }
      }
    }
    return groupPerms;
  }

  @Override
  public List<AclEntry> getAcls(String authzObj) {
    Map<String, FsAction> groupPerms = getGroupPerms(authzObj);
    List<AclEntry> retList = new LinkedList<AclEntry>();
    for (Map.Entry<String, FsAction> groupPerm : groupPerms.entrySet()) {
      AclEntry.Builder builder = new AclEntry.Builder();
      builder.setName(groupPerm.getKey());
      builder.setType(AclEntryType.GROUP);
      builder.setScope(AclEntryScope.ACCESS);
      FsAction action = groupPerm.getValue();
      if (action == FsAction.READ || action == FsAction.WRITE
          || action == FsAction.READ_WRITE) {
        action = action.or(FsAction.EXECUTE);
      }
      builder.setPermission(action);
      retList.add(builder.build());
    }
    return retList;
  }

  private void constructAclEntry(String role, FsAction permission,
      Map<String, FsAction> groupPerms) {
    RoleInfo roleInfo = roles.get(role);
    if (roleInfo != null) {
      for (String group : roleInfo.groups) {
        FsAction fsAction = groupPerms.get(group);
        if (fsAction == null) {
          fsAction = FsAction.NONE;
        }
        groupPerms.put(group, fsAction.or(permission));
      }
    }
  }

  public PrivilegeInfo getPrivilegeInfo(String authzObj) {
    return privileges.get(authzObj);
  }

  Collection<PrivilegeInfo> getAllPrivileges() {
    return privileges.values();
  }

  Collection<RoleInfo> getAllRoles() {
    return roles.values();
  }

  public void delPrivilegeInfo(String authzObj) {
    privileges.remove(authzObj);
  }

  public void addPrivilegeInfo(PrivilegeInfo privilegeInfo) {
    privileges.put(privilegeInfo.authzObj, privilegeInfo);
  }

  public Set<String> getChildren(String authzObj) {
    return authzObjChildren.get(authzObj);
  }

  public RoleInfo getRoleInfo(String role) {
    return roles.get(role);
  }

  public void delRoleInfo(String role) {
    roles.remove(role);
  }

  public void addRoleInfo(RoleInfo roleInfo) {
    roles.put(roleInfo.role, roleInfo);
  }

  public String dumpContent() {
    return new StringBuffer(getClass().getSimpleName())
      .append(": Privileges: ").append(privileges)
      .append(", Roles: ").append(roles)
      .append(", AuthzObjChildren: ").append(authzObjChildren)
      .toString();
  }

  @Override
  public String toString() {
    return new StringBuffer(getClass().getSimpleName())
      .append(": Privileges: ").append(privileges.size())
      .append(", Roles: ").append(roles.size())
      .append(", AuthzObjChildren: ").append(authzObjChildren.size())
      .toString();
  }
}
