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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;

public class PermissionsUpdate implements Updateable.Update {

  public static String RENAME_PRIVS = "__RENAME_PRIV__";
  public static String ALL_AUTHZ_OBJ = "__ALL_AUTHZ_OBJ__";
  public static String ALL_PRIVS = "__ALL_PRIVS__";
  public static String ALL_ROLES = "__ALL_ROLES__";
  public static String ALL_GROUPS = "__ALL_GROUPS__";

  private final TPermissionsUpdate tPermUpdate;

  public PermissionsUpdate(TPermissionsUpdate tPermUpdate) {
    this.tPermUpdate = tPermUpdate;
  }

  public PermissionsUpdate(long seqNum, boolean hasFullImage) {
    this.tPermUpdate = new TPermissionsUpdate(hasFullImage, seqNum,
        new HashMap<String, TPrivilegeChanges>(),
        new HashMap<String, TRoleChanges>());
  }

  @Override
  public long getSeqNum() {
    return tPermUpdate.getSeqNum();
  }

  @Override
  public void setSeqNum(long seqNum) {
    tPermUpdate.setSeqNum(seqNum);
  }

  @Override
  public boolean hasFullImage() {
    return tPermUpdate.isHasfullImage();
  }

  public TPrivilegeChanges addPrivilegeUpdate(String authzObj) {
    if (tPermUpdate.getPrivilegeChanges().containsKey(authzObj)) {
      return tPermUpdate.getPrivilegeChanges().get(authzObj);
    }
    TPrivilegeChanges privUpdate = new TPrivilegeChanges(authzObj,
        new HashMap<String, String>(), new HashMap<String, String>());
    tPermUpdate.getPrivilegeChanges().put(authzObj, privUpdate);
    return privUpdate;
  }

  public TRoleChanges addRoleUpdate(String role) {
    if (tPermUpdate.getRoleChanges().containsKey(role)) {
      return tPermUpdate.getRoleChanges().get(role);
    }
    TRoleChanges roleUpdate = new TRoleChanges(role, new LinkedList<String>(),
        new LinkedList<String>());
    tPermUpdate.getRoleChanges().put(role, roleUpdate);
    return roleUpdate;
  }

  public Collection<TRoleChanges> getRoleUpdates() {
    return tPermUpdate.getRoleChanges().values();
  }

  public Collection<TPrivilegeChanges> getPrivilegeUpdates() {
    return tPermUpdate.getPrivilegeChanges().values();
  }

  public TPermissionsUpdate toThrift() {
    return tPermUpdate;
  }
}
