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
 * Database backed Sentry User. Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryUser {

  /**
   * User name is unique
   */
  private String userName;
  // set of roles granted to this user
  private Set<MSentryRole> roles;
  private long createTime;

  public MSentryUser(String userName, long createTime, Set<MSentryRole> roles) {
    this.setUserName(userName);
    this.createTime = createTime;
    this.roles = roles;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public Set<MSentryRole> getRoles() {
    return roles;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
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

  @Override
  public String toString() {
    return "MSentryUser [userName=" + userName + ", roles=[...]" + ", createTime=" + createTime
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