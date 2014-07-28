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

import org.apache.sentry.provider.db.service.persistent.SentryStore;

import com.google.common.base.Strings;

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
  private String serverName = "";
  private String dbName = "";
  private String tableName = "";
  private String URI = "";
  private String action = "";
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
    this.privilegeScope = privilegeScope;
    this.serverName = serverName;
    this.dbName = SentryStore.toNULLCol(dbName);
    this.tableName = SentryStore.toNULLCol(tableName);
    this.URI = SentryStore.toNULLCol(URI);
    this.action = SentryStore.toNULLCol(action);
    this.roles = new HashSet<MSentryRole>();
  }

  public String getServerName() {
    return serverName;
  }

  public void setServerName(String serverName) {
    this.serverName = (serverName == null) ? "" : serverName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = (dbName == null) ? "" : dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = (tableName == null) ? "" : tableName;
  }

  public String getURI() {
    return URI;
  }

  public void setURI(String uRI) {
    URI = (uRI == null) ? "" : uRI;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = (action == null) ? "" : action;
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

  public void appendRole(MSentryRole role) {
    roles.add(role);
  }

  public Set<MSentryRole> getRoles() {
    return roles;
  }

  public void removeRole(MSentryRole role) {
    roles.remove(role);
    role.removePrivilege(this);
  }

  @Override
  public String toString() {
    return "MSentryPrivilege [privilegeScope=" + privilegeScope
        + ", serverName=" + serverName + ", dbName=" + dbName 
        + ", tableName=" + tableName + ", URI=" + URI
        + ", action=" + action + ", roles=[...]" + ", createTime="
        + createTime + ", grantorPrincipal=" + grantorPrincipal + "]";
  }

@Override
public int hashCode() {
  final int prime = 31;
  int result = 1;
  result = prime * result + ((URI == null) ? 0 : URI.hashCode());
  result = prime * result + ((action == null) ? 0 : action.hashCode());
  result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
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
	if (dbName == null) {
		if (other.dbName != null)
			return false;
	} else if (!dbName.equals(other.dbName))
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
