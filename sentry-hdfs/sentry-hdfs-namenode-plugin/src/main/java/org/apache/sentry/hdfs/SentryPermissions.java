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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /**
   * Defines HDFS ACL entity to which ACL's are assigned.
   */
  public static class HdfsAclEntity {
    private final AclEntryType type;
    private final String value;

    private HdfsAclEntity(AclEntryType type, String value) throws IllegalArgumentException {
      if(type == AclEntryType.USER || type == AclEntryType.GROUP) {
        this.type = type;
        this.value = value;
      } else {
        throw new IllegalArgumentException("Invalid AclEntryType");
      }
    }
    public static HdfsAclEntity constructAclEntityForUser(String user) {
      return new HdfsAclEntity(AclEntryType.USER, user);
    }

    public static HdfsAclEntity constructAclEntityForGroup(String group) {
      return new HdfsAclEntity(AclEntryType.GROUP, group);
    }

    public AclEntryType getType() {
      return type;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }

      HdfsAclEntity other = (HdfsAclEntity) obj;
      if (type == null) {
        if (other.type != null) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }

      if (value == null) {
        if (other.value != null) {
          return false;
        }
      } else if (!value.equals(other.value)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      result = prime * result + ((value == null) ? 0 : value.hashCode());

      return result;
    }
  }

  // Comparison of authorizable object should be case insensitive.
  private final Map<String, PrivilegeInfo> privileges = new TreeMap<String, PrivilegeInfo>(String.CASE_INSENSITIVE_ORDER);
  private Map<String, Set<String>> authzObjChildren = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);

  // RoleInfo should be case insensitive.
  private final Map<String, RoleInfo> roles = new TreeMap<String, RoleInfo>(String.CASE_INSENSITIVE_ORDER);
  private static Logger LOG =
          LoggerFactory.getLogger(SentryINodeAttributesProvider.class);


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

  /**
   * Retrieves all the permissions granted to the object directly and inherited from
   * the parents.
   * @param authzObj Object name for which permissions are needed.
   * @return Sentry Permissions
   */
  private Map<HdfsAclEntity, FsAction> getPerms(String authzObj) {
    Map<HdfsAclEntity, FsAction> perms;
    String parent = getParentAuthzObject(authzObj);
    if (parent == null || parent.equals(authzObj)) {
      perms = new HashMap<HdfsAclEntity, FsAction>();
    } else {
      perms = getPerms(parent);
    }

    PrivilegeInfo privilegeInfo = privileges.get(authzObj);
    if (privilegeInfo != null) {
      for (Map.Entry<TPrivilegeEntity, FsAction> privs : privilegeInfo
          .getAllPermissions().entrySet()) {
        constructHdfsPermissions(privs.getKey(), privs.getValue(), perms);
      }
    }
    return perms;
  }

  /**
   * Constructs HDFS ACL's based on the permissions granted to the object directly
   * and inherited from the parents.
   * @param authzObj Object name for which ACL are needed
   * @return HDFS ACL's
   */
  @Override
  public List<AclEntry> getAcls(String authzObj) {
    Map<HdfsAclEntity, FsAction> permissions = getPerms(authzObj);

    List<AclEntry> retList = new LinkedList<AclEntry>();
    for (Map.Entry<HdfsAclEntity, FsAction> permission : permissions.entrySet()) {
      AclEntry.Builder builder = new AclEntry.Builder();
      if(permission.getKey().getType() == AclEntryType.GROUP) {
        builder.setName(permission.getKey().getValue());
        builder.setType(AclEntryType.GROUP);
      } else if (permission.getKey().getType() == AclEntryType.USER){
        builder.setName(permission.getKey().getValue());
        builder.setType(AclEntryType.USER);
      } else {
        LOG.warn("Permissions for Invalid AclEntryType: %s", permission.getKey().getType());
        continue;
      }
      builder.setScope(AclEntryScope.ACCESS);
      FsAction action = permission.getValue();
      if (action == FsAction.READ || action == FsAction.WRITE
          || action == FsAction.READ_WRITE) {
        action = action.or(FsAction.EXECUTE);
      }
      builder.setPermission(action);
      retList.add(builder.build());
    }
    return retList;
  }

  /**
   * Constructs HDFS Permissions entry based on the privileges granted.
   * @param privilegeEntity Privilege Entity
   * @param permission Permission granted
   * @param perms
   */
  private void constructHdfsPermissions(TPrivilegeEntity privilegeEntity, FsAction permission,
    Map<HdfsAclEntity, FsAction> perms) {
    HdfsAclEntity aclEntry;
    FsAction fsAction;
    if(privilegeEntity.getType() == TPrivilegeEntityType.ROLE) {
      RoleInfo roleInfo = roles.get(privilegeEntity.getValue());
      if (roleInfo != null) {
        for (String group : roleInfo.groups) {
          aclEntry = HdfsAclEntity.constructAclEntityForGroup(group);
          // fsAction is an aggregate of permissions granted to
          // the group on the object and it's parents.
          fsAction = perms.get(aclEntry);
          if (fsAction == null) {
            fsAction = FsAction.NONE;
          }
          perms.put(aclEntry, fsAction.or(permission));
        }
      }
    } else if(privilegeEntity.getType() == TPrivilegeEntityType.USER) {
      aclEntry = HdfsAclEntity.constructAclEntityForUser(privilegeEntity.getValue());
      // fsAction is an aggregate of permissions granted to
      // the user on the object and it's parents.
      fsAction = perms.get(aclEntry);
      if (fsAction == null) {
        fsAction = FsAction.NONE;
      }
      perms.put(aclEntry, fsAction.or(permission));
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
