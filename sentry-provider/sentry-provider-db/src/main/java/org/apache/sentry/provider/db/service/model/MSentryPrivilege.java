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
import java.util.Iterator;
import java.util.Set;

import javax.jdo.annotations.PersistenceCapable;

/**
 * Database backed Sentry Privilege. Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryPrivilege {

  private String privilegeScope;
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

  public void appendRoles(Set<MSentryRole> roles) {
    this.roles.addAll(roles);
  }

  public void appendRole(MSentryRole role) {
    if (!roles.contains(role)) {
      roles.add(role);
      role.appendPrivilege(this);
    }
  }

  public void removeRole(MSentryRole role) {
    for (Iterator<MSentryRole> iter = roles.iterator(); iter.hasNext();) {
      if (iter.next().getRoleName().equalsIgnoreCase(role.getRoleName())) {
        iter.remove();
        role.removePrivilege(this);
        return;
      }
    }
  }

  public void removeRole(String roleName) {
    for (MSentryRole role: roles) {
      if (role.getRoleName().equalsIgnoreCase(roleName)) {
        roles.remove(role);
        return;
      }
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
    result = prime * result + ((URI == null) ? 0 : URI.hashCode());
    result = prime * result + ((action == null) ? 0 : action.hashCode());
    result = prime * result + (int) (createTime ^ (createTime >>> 32));
    result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
    result = prime * result
        + ((grantorPrincipal == null) ? 0 : grantorPrincipal.hashCode());
    result = prime * result
        + ((privilegeName == null) ? 0 : privilegeName.hashCode());
    result = prime * result
        + ((privilegeScope == null) ? 0 : privilegeScope.hashCode());
    result = prime * result
        + ((serverName == null) ? 0 : serverName.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
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
    if (URI == null) {
      if (other.URI != null)
        return false;
    } else if (!URI.equals(other.URI))
      return false;
    if (action == null) {
      if (other.action != null)
        return false;
    } else if (!action.equals(other.action))
      return false;
    if (createTime != other.createTime)
      return false;
    if (dbName == null) {
      if (other.dbName != null)
        return false;
    } else if (!dbName.equals(other.dbName))
      return false;
    if (grantorPrincipal == null) {
      if (other.grantorPrincipal != null)
        return false;
    } else if (!grantorPrincipal.equals(other.grantorPrincipal))
      return false;
    if (privilegeName == null) {
      if (other.privilegeName != null)
        return false;
    } else if (!privilegeName.equals(other.privilegeName))
      return false;
    if (privilegeScope == null) {
      if (other.privilegeScope != null)
        return false;
    } else if (!privilegeScope.equals(other.privilegeScope))
      return false;
    if (serverName == null) {
      if (other.serverName != null)
        return false;
    } else if (!serverName.equals(other.serverName))
      return false;
    if (tableName == null) {
      if (other.tableName != null)
        return false;
    } else if (!tableName.equals(other.tableName))
      return false;
    return true;
  }
}