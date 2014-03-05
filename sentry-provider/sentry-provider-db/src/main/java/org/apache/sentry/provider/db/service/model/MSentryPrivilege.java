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

@PersistenceCapable
public class MSentryPrivilege {

  String privilegeScope;
  String privilegeName;
  String serverName;
  String dbName;
  String tableName;
  String URI;
  String action;
  // roles this privilege is a part of
  Set<MSentryRole> roles;
  long createTime;
  String grantorPrincipal;

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
    this.roles.add(role);
  }
}