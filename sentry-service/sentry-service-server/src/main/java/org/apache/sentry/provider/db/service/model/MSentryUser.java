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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.PersistenceCapable;

import org.apache.sentry.service.common.ServiceConstants;
import org.apache.sentry.provider.db.service.persistent.PrivilegeEntity;

/**
 * Database backed Sentry User. Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryUser implements PrivilegeEntity {

  /**
   * User name is unique
   */
  private String userName;
  // set of roles granted to this user
  private Set<MSentryRole> roles;
  // set of privileges granted to this user
  private Set<MSentryPrivilege> privileges;
  private long createTime;

  public MSentryUser(String userName, long createTime, Set<MSentryRole> roles) {
    this.userName = MSentryUtil.safeIntern(userName);
    this.createTime = createTime;
    this.roles = roles;
    this.privileges = new HashSet<>();
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  /**
   * Gets the User name
   * @return username
   */
  public String getEntityName() {
    return userName;
  }

  public ServiceConstants.SentryEntityType getType() {
    return ServiceConstants.SentryEntityType.USER;
  }

  public Set<MSentryRole> getRoles() {
    return roles;
  }

  public String getUserName() {
    return userName;
  }

  public void appendRole(MSentryRole role) {
    if (roles.add(role)) {
      role.appendUser(this);
    }
  }

  public void removeRole(MSentryRole role) {
    if (roles.remove(role)) {
      role.removeUser(this);
    }
  }

  public void setPrivileges(Set<MSentryPrivilege> privileges) {
    this.privileges = privileges;
  }

  public Set<MSentryPrivilege> getPrivileges() {
    return privileges;
  }

  public void removePrivilege(MSentryPrivilege privilege) {
    if (privileges.remove(privilege)) {
      privilege.removeUser(this);
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

  public void removePrivileges() {
    // As we iterate through the loop below Method removeRole will modify the privileges set
    // will be updated.
    // Copy of the <code>privileges<code> is taken at the beginning of the loop to avoid using
    // the actual privilege set in MSentryUser instance.

    for (MSentryPrivilege privilege : ImmutableSet.copyOf(privileges)) {
      privilege.removeUser(this);
    }
    Preconditions.checkState(privileges.isEmpty(), "Privileges should be empty: " + privileges);
  }

  @Override
  public String toString() {
    return "MSentryUser [userName=" + userName + ", roles=[...]" + ", privileges=[...]" + ", createTime=" + createTime
        + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((userName == null) ? 0 : userName.hashCode());
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
    MSentryUser other = (MSentryUser) obj;
    if (createTime != other.createTime) {
      return false;
    }
    if (userName == null) {
      if (other.userName != null) {
        return false;
      }
    } else if (!userName.equals(other.userName)) {
      return false;
    }
    return true;
  }
}