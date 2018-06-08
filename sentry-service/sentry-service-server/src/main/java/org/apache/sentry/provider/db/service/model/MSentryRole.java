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

package org.apache.sentry.provider.db.service.model;

import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.PersistenceCapable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.sentry.service.common.ServiceConstants;
import org.apache.sentry.provider.db.service.persistent.PrivilegeEntity;

/**
 * Database backed Sentry Role. Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryRole implements PrivilegeEntity {

  private String roleName;
  // set of privileges granted to this role
  private Set<MSentryPrivilege> privileges;
  // set of generic model privileges grant ro this role
  private Set<MSentryGMPrivilege> gmPrivileges;

  // set of groups this role belongs to
  private Set<MSentryGroup> groups;
  // set of users this role belongs to
  private Set<MSentryUser> users;
  private long createTime;

  public MSentryRole(String roleName, long createTime) {
    this.roleName = MSentryUtil.safeIntern(roleName);
    this.createTime = createTime;
    privileges = new HashSet<>();
    gmPrivileges = new HashSet<>();
    groups = new HashSet<>();
    users = new HashSet<>();
  }

  public MSentryRole(String roleName) {
    this(roleName, System.currentTimeMillis());
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  /**
   * Get the Name of the Role.
   * @return roleName
   */
  public String getEntityName() {
    return roleName;
  }

  public ServiceConstants.SentryEntityType getType() {
    return ServiceConstants.SentryEntityType.ROLE;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public void setPrivileges(Set<MSentryPrivilege> privileges) {
    this.privileges = privileges;
  }

  public Set<MSentryPrivilege> getPrivileges() {
    return privileges;
  }

  public Set<MSentryGMPrivilege> getGmPrivileges() {
    return gmPrivileges;
  }

  public void setGmPrivileges(Set<MSentryGMPrivilege> gmPrivileges) {
    this.gmPrivileges = gmPrivileges;
  }

  public void setGroups(Set<MSentryGroup> groups) {
    this.groups = groups;
  }

  public Set<MSentryGroup> getGroups() {
    return groups;
  }

  public Set<MSentryUser> getUsers() {
    return users;
  }

  public void setUsers(Set<MSentryUser> users) {
    this.users = users;
  }

  public void removePrivilege(MSentryPrivilege privilege) {
    if (privileges.remove(privilege)) {
      privilege.removeEntity(this);
    }
  }

  public void appendPrivileges(Set<MSentryPrivilege> privileges) {
    this.privileges.addAll(privileges);
  }

  public void appendPrivilege(MSentryPrivilege privilege) {
    if (privileges.add(privilege)) {
      privilege.appendEntity(this);
    }
  }

  public void removeGMPrivilege(MSentryGMPrivilege gmPrivilege) {
    if (gmPrivileges.remove(gmPrivilege)) {
      gmPrivilege.removeRole(this);
    }
  }

  public void appendGMPrivilege(MSentryGMPrivilege gmPrivilege) {
    if (gmPrivileges.add(gmPrivilege)) {
      gmPrivilege.appendRole(this);
    }
  }

  public void removeGMPrivileges() {
    for (MSentryGMPrivilege privilege : ImmutableSet.copyOf(gmPrivileges)) {
      privilege.removeRole(this);
    }
    Preconditions.checkState(gmPrivileges.isEmpty(), "gmPrivileges should be empty: " + gmPrivileges);
  }

  public void appendGroups(Set<MSentryGroup> groups) {
    this.groups.addAll(groups);
  }

  public void appendGroup(MSentryGroup group) {
    if (groups.add(group)) {
      group.appendRole(this);
    }
  }

  public void removeGroup(MSentryGroup group) {
    if (groups.remove(group)) {
      group.removeRole(this);
    }
  }

  public void appendUsers(Set<MSentryUser> users) {
    this.users.addAll(users);
  }

  public void appendUser(MSentryUser user) {
    if (users.add(user)) {
      user.appendRole(this);
    }
  }

  public void removeUser(MSentryUser user) {
    if (users.remove(user)) {
      user.removeRole(this);
    }
  }

  public void removePrivileges() {
    // As we iterate through the loop below Method removeRole will modify the privileges set
    // will be updated.
    // Copy of the <code>privileges<code> is taken at the beginning of the loop to avoid using
    // the actual privilege set in MSentryRole instance.

    for (MSentryPrivilege privilege : ImmutableSet.copyOf(privileges)) {
      privilege.removeEntity(this);
    }
    Preconditions.checkState(privileges.isEmpty(), "Privileges should be empty: " + privileges);
  }

  @Override
  public String toString() {
    return "MSentryRole [roleName=" + roleName + ", privileges=[..]" + ", gmPrivileges=[..]"
        + ", groups=[...]" + ", users=[...]" + ", createTime=" + createTime + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((roleName == null) ? 0 : roleName.hashCode());
    return result;
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
    MSentryRole other = (MSentryRole) obj;
    if (roleName == null) {
      if (other.roleName != null) {
        return false;
      }
    } else if (!roleName.equals(other.roleName)) {
      return false;
    }
    return true;
  }

}
