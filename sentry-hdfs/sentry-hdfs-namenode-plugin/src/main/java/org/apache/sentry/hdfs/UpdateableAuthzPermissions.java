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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.hdfs.SentryPermissions.PrivilegeInfo;
import org.apache.sentry.hdfs.SentryPermissions.RoleInfo;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateableAuthzPermissions implements AuthzPermissions, Updateable<PermissionsUpdate> {
  private static final int MAX_UPDATES_PER_LOCK_USE = 99;
  private volatile SentryPermissions perms = new SentryPermissions();
  private final AtomicLong seqNum = new AtomicLong(0);
  
  private static Logger LOG = LoggerFactory.getLogger(UpdateableAuthzPermissions.class);

  public static Map<String, FsAction> ACTION_MAPPING = new HashMap<String, FsAction>();
  static {
    ACTION_MAPPING.put("ALL", FsAction.ALL);
    ACTION_MAPPING.put("*", FsAction.ALL);
    ACTION_MAPPING.put(AccessConstants.SELECT.toUpperCase(), FsAction.READ_EXECUTE);
    ACTION_MAPPING.put(AccessConstants.INSERT.toUpperCase(), FsAction.WRITE_EXECUTE);
  }

  @Override
  public List<AclEntry> getAcls(String authzObj) {
    return perms.getAcls(authzObj);
  }

  @Override
  public UpdateableAuthzPermissions updateFull(PermissionsUpdate update) {
    UpdateableAuthzPermissions other = new UpdateableAuthzPermissions();
    other.applyPartialUpdate(update);
    other.seqNum.set(update.getSeqNum());
    return other;
  }

  @Override
  public void updatePartial(Iterable<PermissionsUpdate> updates, ReadWriteLock lock) {
    lock.writeLock().lock();
    try {
      int counter = 0;
      for (PermissionsUpdate update : updates) {
        applyPartialUpdate(update);
        if (++counter > MAX_UPDATES_PER_LOCK_USE) {
          counter = 0;
          lock.writeLock().unlock();
          lock.writeLock().lock();
        }
        seqNum.set(update.getSeqNum());
        LOG.warn("##### Updated perms seq Num [" + seqNum.get() + "]");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
  

  private void applyPartialUpdate(PermissionsUpdate update) {
    applyPrivilegeUpdates(update);
    applyRoleUpdates(update);
  }

  private void applyRoleUpdates(PermissionsUpdate update) {
    for (TRoleChanges rUpdate : update.getRoleUpdates()) {
      if (rUpdate.getRole().equals(PermissionsUpdate.ALL_ROLES)) {
        // Request to remove group from all roles
        String groupToRemove = rUpdate.getDelGroups().iterator().next();
        for (RoleInfo rInfo : perms.getAllRoles()) {
          rInfo.delGroup(groupToRemove);
        }
      }
      RoleInfo rInfo = perms.getRoleInfo(rUpdate.getRole());
      for (String group : rUpdate.getAddGroups()) {
        if (rInfo == null) {
          rInfo = new RoleInfo(rUpdate.getRole());
        }
        rInfo.addGroup(group);
      }
      if (rInfo != null) {
        perms.addRoleInfo(rInfo);
        for (String group : rUpdate.getDelGroups()) {
          if (group.equals(PermissionsUpdate.ALL_GROUPS)) {
            perms.delRoleInfo(rInfo.getRole());
            break;
          }
          // If there are no groups to remove, rUpdate.getDelGroups() will
          // return empty list and this code will not be reached
          rInfo.delGroup(group);
        }
      }
    }
  }

  private void applyPrivilegeUpdates(PermissionsUpdate update) {
    for (TPrivilegeChanges pUpdate : update.getPrivilegeUpdates()) {
      if (pUpdate.getAuthzObj().equals(PermissionsUpdate.RENAME_PRIVS)) {
        String newAuthzObj = pUpdate.getAddPrivileges().keySet().iterator().next();
        String oldAuthzObj = pUpdate.getDelPrivileges().keySet().iterator().next();
        PrivilegeInfo privilegeInfo = perms.getPrivilegeInfo(oldAuthzObj);
        Map<String, FsAction> allPermissions = privilegeInfo.getAllPermissions();
        perms.delPrivilegeInfo(oldAuthzObj);
        PrivilegeInfo newPrivilegeInfo = new PrivilegeInfo(newAuthzObj);
        for (Map.Entry<String, FsAction> e : allPermissions.entrySet()) {
          newPrivilegeInfo.setPermission(e.getKey(), e.getValue());
        }
        perms.addPrivilegeInfo(newPrivilegeInfo);
        return;
      }
      if (pUpdate.getAuthzObj().equals(PermissionsUpdate.ALL_PRIVS)) {
        // Request to remove role from all Privileges
        String roleToRemove = pUpdate.getDelPrivileges().keySet().iterator()
            .next();
        for (PrivilegeInfo pInfo : perms.getAllPrivileges()) {
          pInfo.removePermission(roleToRemove);
        }
      }
      PrivilegeInfo pInfo = perms.getPrivilegeInfo(pUpdate.getAuthzObj());
      for (Map.Entry<String, String> aMap : pUpdate.getAddPrivileges().entrySet()) {
        if (pInfo == null) {
          pInfo = new PrivilegeInfo(pUpdate.getAuthzObj());
        }
        FsAction fsAction = pInfo.getPermission(aMap.getKey());
        if (fsAction == null) {
          fsAction = getFAction(aMap.getValue());
        } else {
          fsAction = fsAction.or(getFAction(aMap.getValue()));
        }
        pInfo.setPermission(aMap.getKey(), fsAction);
      }
      if (pInfo != null) {
        perms.addPrivilegeInfo(pInfo);
        for (Map.Entry<String, String> dMap : pUpdate.getDelPrivileges().entrySet()) {
          if (dMap.getKey().equals(PermissionsUpdate.ALL_ROLES)) {
            // Remove all privileges
            perms.delPrivilegeInfo(pUpdate.getAuthzObj());
            break;
          }
          FsAction fsAction = pInfo.getPermission(dMap.getKey());
          if (fsAction != null) {
            fsAction = fsAction.and(getFAction(dMap.getValue()).not());
            if (FsAction.NONE == fsAction) {
              pInfo.removePermission(dMap.getKey());
            } else {
              pInfo.setPermission(dMap.getKey(), fsAction);
            }
          }
        }
      }
    }
  }

  static FsAction getFAction(String sentryPriv) {
    String[] strPrivs = sentryPriv.trim().split(",");
    FsAction retVal = FsAction.NONE;
    for (String strPriv : strPrivs) {
      retVal = retVal.or(ACTION_MAPPING.get(strPriv.toUpperCase()));
    }
    return retVal;
  }

  @Override
  public long getLastUpdatedSeqNum() {
    return seqNum.get();
  }

  @Override
  public PermissionsUpdate createFullImageUpdate(long currSeqNum) {
    PermissionsUpdate retVal = new PermissionsUpdate(currSeqNum, true);
    for (PrivilegeInfo pInfo : perms.getAllPrivileges()) {
      TPrivilegeChanges pUpdate = retVal.addPrivilegeUpdate(pInfo.getAuthzObj());
      for (Map.Entry<String, FsAction> ent : pInfo.getAllPermissions().entrySet()) {
        pUpdate.putToAddPrivileges(ent.getKey(), ent.getValue().SYMBOL);
      }
    }
    for (RoleInfo rInfo : perms.getAllRoles()) {
      TRoleChanges rUpdate = retVal.addRoleUpdate(rInfo.getRole());
      for (String group : rInfo.getAllGroups()) {
        rUpdate.addToAddGroups(group);
      }
    }
    return retVal;
  }

  
}
