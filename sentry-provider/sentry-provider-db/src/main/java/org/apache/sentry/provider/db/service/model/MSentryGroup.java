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

import java.util.Set;

import javax.jdo.annotations.PersistenceCapable;

/**
 * Database backed Sentry Group. Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryGroup {

  /**
   * Group name is unique
   */
  private String groupName;
  // set of roles granted to this group
  private Set<MSentryRole> roles;
  private long createTime;
  private String grantorPrincipal;

  public MSentryGroup(String groupName, long createTime, String grantorPrincipal,
      Set<MSentryRole> roles) {
    this.setGroupName(groupName);
    this.createTime = createTime;
    this.grantorPrincipal = grantorPrincipal;
    this.roles = roles;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public String getGrantorPrincipal() {
    return grantorPrincipal;
  }

  public void setGrantorPrincipal(String grantorPrincipal) {
    this.grantorPrincipal = grantorPrincipal;
  }

  public Set<MSentryRole> getRoles() {
    return roles;
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public void appendRole(MSentryRole role) {
    if (roles.add(role)) {
      role.appendGroup(this);
    }
  }

  public void removeRole(MSentryRole role) {
    if (roles.remove(role)) {
      role.removeGroup(this);
    }
  }

  @Override
  public String toString() {
    return "MSentryGroup [groupName=" + groupName + ", roles=[...]"
        + ", createTime=" + createTime + ", grantorPrincipal="
        + grantorPrincipal + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MSentryGroup other = (MSentryGroup) obj;
    if (createTime != other.createTime)
      return false;
    if (groupName == null) {
      if (other.groupName != null)
        return false;
    } else if (!groupName.equals(other.groupName))
      return false;
    return true;
  }
}