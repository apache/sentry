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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipal;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.hdfs.service.thrift.sentry_hdfs_serviceConstants;
import org.apache.thrift.TException;

public class PermissionsUpdate implements Updateable.Update {

  public static final String RENAME_PRIVS = "__RENAME_PRIV__";
  public static final String ALL_AUTHZ_OBJ = "__ALL_AUTHZ_OBJ__";
  public static final String ALL_PRIVS = "__ALL_PRIVS__";
  public static final String ALL_ROLES = "__ALL_ROLES__";
  public static final String ALL_GROUPS = "__ALL_GROUPS__";

  private final TPermissionsUpdate tPermUpdate;

  public PermissionsUpdate() {
    this(0, false);
  }

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
  public long getImgNum() {
    return sentry_hdfs_serviceConstants.UNUSED_PATH_UPDATE_IMG_NUM;
  }

  @Override
  public void setImgNum(long imgNum) {
    throw new UnsupportedOperationException("setImgNum not used");
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
        new HashMap<TPrivilegePrincipal, String>(), new HashMap<TPrivilegePrincipal, String>());
    tPermUpdate.getPrivilegeChanges().put(authzObj, privUpdate);
    return privUpdate;
  }

  public TRoleChanges addRoleUpdate(String role) {
    if (tPermUpdate.getRoleChanges().containsKey(role)) {
      return tPermUpdate.getRoleChanges().get(role);
    }
    TRoleChanges roleUpdate = new TRoleChanges(role, new ArrayList<String>(),
        new ArrayList<String>());
    tPermUpdate.getRoleChanges().put(role, roleUpdate);
    return roleUpdate;
  }

  Collection<TRoleChanges> getRoleUpdates() {
    return tPermUpdate.getRoleChanges().values();
  }

  Collection<TPrivilegeChanges> getPrivilegeUpdates() {
    return tPermUpdate.getPrivilegeChanges().values();
  }

  TPermissionsUpdate toThrift() {
    return tPermUpdate;
  }

  @Override
  public byte[] serialize() throws IOException {
    return ThriftSerializer.serialize(tPermUpdate);
  }

  @Override
  public void deserialize(byte[] data) throws IOException {
    ThriftSerializer.deserialize(tPermUpdate, data);
  }

  @Override
  public void JSONDeserialize(String update) throws TException {
    ThriftSerializer.deserializeFromJSON(tPermUpdate, update);
  }

  @Override
  public String JSONSerialize() throws TException {
    return ThriftSerializer.serializeToJSON(tPermUpdate);
  }

  @Override
  public int hashCode() {
    return (tPermUpdate == null) ? 0 : tPermUpdate.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (this == obj) {
      return true;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    PermissionsUpdate other = (PermissionsUpdate) obj;
    if (tPermUpdate == null) {
      return other.tPermUpdate == null;
    }
    return tPermUpdate.equals(other.tPermUpdate);
  }

  @Override
  public String toString() {
    // TPermissionsUpdate implements toString() perfectly; null tPermUpdate is ok
    return getClass().getSimpleName() + "(" + tPermUpdate + ")";
  }

}
