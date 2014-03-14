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

/**
 * Database backed Sentry Privilege. Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryPrivilege {

  private String privilegeScope;
  /**
   * Privilege name is unique
   */
  private String privilegeName;
  private String serverName;
  private String dbName;
  private String tableName;
  private String URI;
  private String action;
  // roles this privilege is a part of
  private Set<MSentryRole> roles;
  private long createTime;
  private String grantorPrincipal;

  public MSentryPrivilege() {
    this.roles = new HashSet<MSentryRole>();
  }

  public MSentryPrivilege(String privilegeName, String privilegeScope,
      String serverName, String dbName, String tableName, String URI,
      String action) {
    this.privilegeName = privilegeName;
    this.privilegeScope = privilegeScope;
    this.serverName = serverName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.URI = URI;
    this.action = action;
    this.roles = new HashSet<MSentryRole>();
  }

  public String getServerName() {
    return serverName;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getURI() {
    return URI;
  }

  public void setURI(String uRI) {
    URI = uRI;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
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

  public String getPrivilegeScope() {
    return privilegeScope;
  }

  public void setPrivilegeScope(String privilegeScope) {
    this.privilegeScope = privilegeScope;
  }

  public String getPrivilegeName() {
    return privilegeName;
  }

  public void setPrivilegeName(String privilegeName) {
    this.privilegeName = privilegeName;
  }

  public void appendRole(MSentryRole role) {
    if (!roles.contains(role)) {
      roles.add(role);
      role.appendPrivilege(this);
    }
  }

  public void removeRole(MSentryRole role) {
    if (roles.remove(role)) {
      role.removePrivilege(this);
    }
  }

  @Override
  public String toString() {
    return "MSentryPrivilege [privilegeScope=" + privilegeScope
        + ", privilegeName=" + privilegeName + ", serverName=" + serverName
        + ", dbName=" + dbName + ", tableName=" + tableName + ", URI=" + URI
        + ", action=" + action + ", roles=[...]" + ", createTime="
        + createTime + ", grantorPrincipal=" + grantorPrincipal + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((privilegeName == null) ? 0 : privilegeName.hashCode());
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
    MSentryPrivilege other = (MSentryPrivilege) obj;
    if (privilegeName == null) {
      if (other.privilegeName != null)
        return false;
    } else if (!privilegeName.equals(other.privilegeName))
      return false;
    return true;
  }
}
