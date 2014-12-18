package org.apache.sentry.provider.db.service.persistent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilegeMap;
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

import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.service.thrift.TStoreSnapshot;

/**
 * A Decorator SentryStore that delegates all calls to a provided SentryStore
 * within a LockContext. The LockContext encapsulates the Locking policy. This
 * is determined by a subclass. The Subclass is responsible for creating the
 * LockContext before a method is called and after the method, unlock() is
 * called on the Context.
 * 
 * @param <T> A {@link LockContext} which exposes a single unlock() method
 * a subclass has to implement.
 */
public abstract class LockingSentryStore<T extends LockingSentryStore.LockContext>
    implements SentryStore {

  public static interface LockContext {
    public void unlock();
  }

  private final SentryStore sentryStore;

  public LockingSentryStore(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  protected abstract T writeLock();
  protected abstract T readLock();

  
  @Override
  public Configuration getConfiguration() {
    return sentryStore.getConfiguration();
  }

  @Override
  public CommitContext createSentryRole(String roleName)
      throws SentryAlreadyExistsException {
    T context = writeLock();
    try {
      return sentryStore.createSentryRole(roleName);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleGrantPrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege privilege) throws SentryUserException {
    T context = writeLock();
    try {
      return sentryStore.alterSentryRoleGrantPrivilege(
          grantorPrincipal,roleName, privilege);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleGrantPrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> privileges)
      throws SentryUserException {
    T context = writeLock();
    try {
      return sentryStore.alterSentryRoleGrantPrivileges(
          grantorPrincipal, roleName, privileges);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleRevokePrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege tPrivilege) throws SentryUserException {
    T context = writeLock();
    try {
      return sentryStore.alterSentryRoleRevokePrivilege(
          grantorPrincipal, roleName, tPrivilege);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleRevokePrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> tPrivileges)
      throws SentryUserException {
    T context = writeLock();
    try {
      return sentryStore.alterSentryRoleRevokePrivileges(
          grantorPrincipal, roleName, tPrivileges);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public CommitContext dropSentryRole(String roleName)
      throws SentryNoSuchObjectException {
    T context = writeLock();
    try {
      return sentryStore.dropSentryRole(roleName);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleAddGroups(String grantorPrincipal,
      String roleName, Set<TSentryGroup> groupNames)
      throws SentryNoSuchObjectException {
    T context = writeLock();
    try {
      return sentryStore.alterSentryRoleAddGroups(
          grantorPrincipal, roleName, groupNames);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleDeleteGroups(String roleName,
      Set<TSentryGroup> groupNames) throws SentryNoSuchObjectException {
    T context = writeLock();
    try {
      return sentryStore.alterSentryRoleDeleteGroups(roleName, groupNames);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public void dropPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    T context = writeLock();
    try {
      sentryStore.dropPrivilege(tAuthorizable);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public void renamePrivilege(TSentryAuthorizable tAuthorizable,
      TSentryAuthorizable newTAuthorizable) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    T context = writeLock();
    try {
      sentryStore.renamePrivilege(tAuthorizable, newTAuthorizable);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public void setSentryVersion(String newVersion, String verComment)
      throws SentryNoSuchObjectException, SentryAccessDeniedException {
    T context = writeLock();
    try {
      sentryStore.setSentryVersion(newVersion, verComment);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(
      Set<String> groups, TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable authHierarchy, boolean isAdmin)
      throws SentryInvalidInputException {
    T context = readLock();
    try {
      return sentryStore.listSentryPrivilegesByAuthorizable(groups, activeRoles,
          authHierarchy, isAdmin);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Set<TSentryPrivilege>
      getAllTSentryPrivilegesByRoleName(String roleName)
          throws SentryNoSuchObjectException {
    T context = readLock();
    try {
      return sentryStore.getAllTSentryPrivilegesByRoleName(roleName);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Set<TSentryPrivilege> getTSentryPrivileges(Set<String> roleNames,
      TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    T context = readLock();
    try {
      return sentryStore.getTSentryPrivileges(roleNames, authHierarchy);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Set<TSentryRole> getTSentryRolesByGroupName(Set<String> groupNames,
      boolean checkAllGroups) throws SentryNoSuchObjectException {
    T context = readLock();
    try {
      return sentryStore.getTSentryRolesByGroupName(groupNames, checkAllGroups);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Set<String> getRoleNamesForGroups(Set<String> groups) {
    T context = readLock();
    try {
      return sentryStore.getRoleNamesForGroups(groups);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet) throws SentryInvalidInputException {
    T context = readLock();
    try {
      return sentryStore.listAllSentryPrivilegesForProvider(groups, roleSet);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Set<String> listSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy)
      throws SentryInvalidInputException {
    T context = readLock();
    try {
      return sentryStore.listSentryPrivilegesForProvider(groups, roleSet,
          authHierarchy);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public boolean hasAnyServerPrivileges(Set<String> groups,
      TSentryActiveRoleSet roleSet, String server) {
    LockContext context = readLock();
    try {
      return sentryStore.hasAnyServerPrivileges(groups, roleSet, server);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public String getSentryVersion() throws SentryNoSuchObjectException,
      SentryAccessDeniedException {
    T context = readLock();
    try {
      return sentryStore.getSentryVersion();
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Map<String, HashMap<String, String>> retrieveFullPrivilegeImage() {
    T context = readLock();
    try {
      return sentryStore.retrieveFullPrivilegeImage();
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Map<String, LinkedList<String>> retrieveFullRoleImage() {
    T context = readLock();
    try {
      return sentryStore.retrieveFullRoleImage();
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public Set<String> getGroupsForRole(String roleName) {
    T context = readLock();
    try {
      return sentryStore.getGroupsForRole(roleName);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public long getRoleCount() {
    T context = readLock();
    try {
      return sentryStore.getRoleCount();
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public long getPrivilegeCount() {
    T context = readLock();
    try {
      return sentryStore.getPrivilegeCount();
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public long getGroupCount() {
    T context = readLock();
    try {
      return sentryStore.getGroupCount();
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public void stop() {
    sentryStore.stop();
  }

  @Override
  public TStoreSnapshot toSnapshot() {
    T context = readLock();
    try {
      return sentryStore.toSnapshot();
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }

  @Override
  public void fromSnapshot(TStoreSnapshot snapshot) {
    T context = writeLock();
    try {
      sentryStore.fromSnapshot(snapshot);
    } finally {
      if (context != null) {
        context.unlock();
      }
    }
  }
}
