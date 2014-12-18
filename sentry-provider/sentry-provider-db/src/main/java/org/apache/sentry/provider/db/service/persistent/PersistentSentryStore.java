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

package org.apache.sentry.provider.db.service.persistent;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreOp;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreRecord;
import org.apache.sentry.provider.db.service.thrift.TStoreSnapshot;

import com.google.common.collect.Sets;

/**
 * A Decorator SentryStore that delegates all calls to a provided SentryStore
 * after wrapping all writes with a PersistenceContext. The PersistenceContext
 * encapsulates the Persistence strategy. This is determined by a subclass.
 * The Subclass is responsible for creating the PersistenceContext before a
 * method is called and after the method, the {@link #onSuccess(PersistentContext)} or
 * {@link #onFailure(PersistentContext)} will be called with the context
 * based on if the operation was successful or not on the underlying
 * SentryStore
 * 
 * @param <T> A PersistentContext implementation
 */
public abstract class PersistentSentryStore
<T extends PersistentSentryStore.PersistentContext> implements SentryStore {

  /**
   * Marker interface to be implemented by a subclass. The Context is passed
   * back to the subclass in the onSuccess or onFailure calls after the
   * operation is complete.
   */
  public static interface PersistentContext { 

  }

  private final SentryStore sentryStore;

  public PersistentSentryStore(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  /**
   * This Method is called before the write operations. Subclasses would ideally
   * have to return an implementation of a PersistenceContext
   * @param record A TSentryStoreRecord that encapsulates the operation and
   *        all arguments
   * @return A PersistenceContext
   * @throws IOException
   */
  protected abstract T createRecord(TSentryStoreRecord record) throws IOException;

  /**
   * This method is called if the operation has been successfully applied on
   * the underlying SentryStore
   * @param context A PersistenceContext
   */
  protected abstract void onSuccess(T context);

  /**
   * This method is called if the underlying SentryStore rejected the write
   * operation. The default implementation is to persist a NO-OP record.
   * (This is so that implementing classes that rely on a monotonically
   * increment by 1 sequence Ids do not have to deal with gaps in the
   * sequence Ids)
   * @param context
   */
  protected abstract void onFailure(T context);

  protected SentryStore getStore() {
    return sentryStore;
  }

  @Override
  public Configuration getConfiguration() {
    return sentryStore.getConfiguration();
  }

  protected void applyRecord(TSentryStoreRecord record) 
      throws SentryUserException {
    if ((record.getStoreOp() == TSentryStoreOp.SNAPSHOT) && (record.getSnapshot() != null)) {
      sentryStore.fromSnapshot(record.getSnapshot());
    } else if (record.getStoreOp() == TSentryStoreOp.CREATE_ROLE) {
      sentryStore.createSentryRole(record.getRoleName());
    } else if (record.getStoreOp() == TSentryStoreOp.DROP_ROLE) {
      sentryStore.dropSentryRole(record.getRoleName());
    } else if (record.getStoreOp() == TSentryStoreOp.GRANT_PRIVILEGES) {
      sentryStore.alterSentryRoleGrantPrivileges(
          record.getGrantorPrincipal(), record.getRoleName(),
          record.getPrivileges());
    } else if (record.getStoreOp() == TSentryStoreOp.REVOKE_PRVILEGES) {
      sentryStore.alterSentryRoleRevokePrivileges(
          record.getGrantorPrincipal(), record.getRoleName(),
          record.getPrivileges());
    } else if (record.getStoreOp() == TSentryStoreOp.ADD_GROUPS) {
      Set<TSentryGroup> groups = new HashSet<TSentryGroup>();
      for (String group : record.getGroups()) {
        groups.add(new TSentryGroup(group));
      }
      sentryStore.alterSentryRoleAddGroups(
          record.getGrantorPrincipal(), record.getRoleName(),
          groups);
    } else if (record.getStoreOp() == TSentryStoreOp.DEL_GROUPS) {
      Set<TSentryGroup> groups = new HashSet<TSentryGroup>();
      for (String group : record.getGroups()) {
        groups.add(new TSentryGroup(group));
      }
      sentryStore.alterSentryRoleDeleteGroups(record.getRoleName(), groups);
    } else if (record.getStoreOp() == TSentryStoreOp.DROP_PRIVILEGE) {
      sentryStore.dropPrivilege(record.getAuthorizable());
    } else if (record.getStoreOp() == TSentryStoreOp.RENAME_PRIVILEGE) {
      sentryStore.renamePrivilege(record.getAuthorizable(),
          record.getNewAuthorizable());
    } else if (record.getStoreOp() == TSentryStoreOp.SET_VERSION) {
      sentryStore.setSentryVersion(record.getVersion(),
          record.getVersionComment());
    } else if (record.getStoreOp() == TSentryStoreOp.NO_OP) {
      // NO-OP
    } else {
      throw new RuntimeException("Unknown Sentry Store OP [" + record.getStoreOp() + "]");
    }
  }

  @Override
  public CommitContext createSentryRole(String roleName)
      throws SentryAlreadyExistsException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.CREATE_ROLE);
    record.setRoleName(roleName);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      CommitContext retVal = sentryStore.createSentryRole(roleName);
      opSuccess = true;
      onSuccess(pContext);
      return retVal;
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryAlreadyExistsException) {
        throw (SentryAlreadyExistsException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public CommitContext dropSentryRole(String roleName)
      throws SentryNoSuchObjectException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.DROP_ROLE);
    record.setRoleName(roleName);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      CommitContext retVal = sentryStore.dropSentryRole(roleName);
      opSuccess = true;
      onSuccess(pContext);
      return retVal;
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryNoSuchObjectException) {
        throw (SentryNoSuchObjectException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleGrantPrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege privilege) throws SentryUserException {
    return alterSentryRoleGrantPrivileges(grantorPrincipal, roleName,
        Sets.newHashSet(privilege));
  }

  @Override
  public CommitContext alterSentryRoleGrantPrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> privileges)
      throws SentryUserException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.GRANT_PRIVILEGES);
    record.setGrantorPrincipal(grantorPrincipal);
    record.setRoleName(roleName);
    record.setPrivileges(privileges);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      CommitContext retVal =
          sentryStore.alterSentryRoleGrantPrivileges(grantorPrincipal,
          roleName, privileges);
      opSuccess = true;
      onSuccess(pContext);
      return retVal;
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryUserException) {
        throw (SentryUserException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleRevokePrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege tPrivilege) throws SentryUserException {
    return alterSentryRoleRevokePrivileges(grantorPrincipal, roleName,
        Sets.newHashSet(tPrivilege));
  }

  @Override
  public CommitContext alterSentryRoleRevokePrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> tPrivileges)
      throws SentryUserException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.REVOKE_PRVILEGES);
    record.setGrantorPrincipal(grantorPrincipal);
    record.setRoleName(roleName);
    record.setPrivileges(tPrivileges);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      CommitContext retVal =
          sentryStore.alterSentryRoleRevokePrivileges(grantorPrincipal,
          roleName, tPrivileges);
      opSuccess = true;
      onSuccess(pContext);
      return retVal;
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryUserException) {
        throw (SentryUserException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleAddGroups(String grantorPrincipal,
      String roleName, Set<TSentryGroup> groupNames)
      throws SentryNoSuchObjectException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.ADD_GROUPS);
    record.setGrantorPrincipal(grantorPrincipal);
    record.setRoleName(roleName);
    Set<String> groups = new HashSet<String>();
    for (TSentryGroup gr : groupNames) {
      groups.add(gr.getGroupName());
    }
    record.setGroups(groups);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      CommitContext retVal =
          sentryStore.alterSentryRoleAddGroups(grantorPrincipal,
          roleName, groupNames);
      opSuccess = true;
      onSuccess(pContext);
      return retVal;
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryNoSuchObjectException) {
        throw (SentryNoSuchObjectException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleDeleteGroups(String roleName,
      Set<TSentryGroup> groupNames) throws SentryNoSuchObjectException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.DEL_GROUPS);
    record.setRoleName(roleName);
    Set<String> groups = new HashSet<String>();
    for (TSentryGroup gr : groupNames) {
      groups.add(gr.getGroupName());
    }
    record.setGroups(groups);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      CommitContext retVal =
          sentryStore.alterSentryRoleDeleteGroups(roleName, groupNames);
      opSuccess = true;
      onSuccess(pContext);
      return retVal;
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryNoSuchObjectException) {
        throw (SentryNoSuchObjectException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void dropPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.DROP_PRIVILEGE);
    record.setAuthorizable(tAuthorizable);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      sentryStore.dropPrivilege(tAuthorizable);
      opSuccess = true;
      onSuccess(pContext);
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryNoSuchObjectException) {
        throw (SentryNoSuchObjectException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void renamePrivilege(TSentryAuthorizable tAuthorizable,
      TSentryAuthorizable newTAuthorizable) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.RENAME_PRIVILEGE);
    record.setAuthorizable(tAuthorizable);
    record.setNewAuthorizable(newTAuthorizable);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      sentryStore.renamePrivilege(tAuthorizable, newTAuthorizable);
      opSuccess = true;
      onSuccess(pContext);
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryNoSuchObjectException) {
        throw (SentryNoSuchObjectException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void setSentryVersion(String newVersion, String verComment)
      throws SentryNoSuchObjectException, SentryAccessDeniedException {
    TSentryStoreRecord record =
        new TSentryStoreRecord(TSentryStoreOp.SET_VERSION);
    record.setVersion(newVersion);
    record.setVersionComment(verComment);
    T pContext = null;
    try {
      pContext = createRecord(record);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write record to Persistent Store [" + record + "]");
    }
    boolean opSuccess = false;
    try {
      sentryStore.setSentryVersion(newVersion, verComment);
      opSuccess = true;
      onSuccess(pContext);
    } catch (Exception e) {
      if (!opSuccess) {
        onFailure(pContext);
      }
      if (e instanceof SentryNoSuchObjectException) {
        throw (SentryNoSuchObjectException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(
      Set<String> groups, TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable authHierarchy, boolean isAdmin)
      throws SentryInvalidInputException {
    return sentryStore.listSentryPrivilegesByAuthorizable(groups, activeRoles,
        authHierarchy, isAdmin);
  }

  @Override
  public Set<TSentryPrivilege> getAllTSentryPrivilegesByRoleName(String roleName)
          throws SentryNoSuchObjectException {
    return sentryStore.getAllTSentryPrivilegesByRoleName(roleName);
  }

  @Override
  public Set<TSentryPrivilege> getTSentryPrivileges(Set<String> roleNames,
      TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    return sentryStore.getTSentryPrivileges(roleNames, authHierarchy);
  }

  @Override
  public Set<TSentryRole> getTSentryRolesByGroupName(Set<String> groupNames,
      boolean checkAllGroups) throws SentryNoSuchObjectException {
    return sentryStore.getTSentryRolesByGroupName(groupNames, checkAllGroups);
  }

  @Override
  public Set<String> getRoleNamesForGroups(Set<String> groups) {
    return sentryStore.getRoleNamesForGroups(groups);
  }

  @Override
  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet) throws SentryInvalidInputException {
    return sentryStore.listAllSentryPrivilegesForProvider(groups, roleSet);
  }

  @Override
  public Set<String> listSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy)
      throws SentryInvalidInputException {
    return sentryStore.listSentryPrivilegesForProvider(groups, roleSet, authHierarchy);
  }

  @Override
  public boolean hasAnyServerPrivileges(Set<String> groups,
      TSentryActiveRoleSet roleSet, String server) {
    return sentryStore.hasAnyServerPrivileges(groups, roleSet, server);
  }

  @Override
  public String getSentryVersion() throws SentryNoSuchObjectException,
      SentryAccessDeniedException {
    return sentryStore.getSentryVersion();
  }

  @Override
  public Map<String, HashMap<String, String>> retrieveFullPrivilegeImage() {
    return sentryStore.retrieveFullPrivilegeImage();
  }

  @Override
  public Map<String, LinkedList<String>> retrieveFullRoleImage() {
    return sentryStore.retrieveFullRoleImage();
  }

  @Override
  public long getRoleCount() {
    return sentryStore.getRoleCount();
  }

  @Override
  public long getPrivilegeCount() {
    return sentryStore.getPrivilegeCount();
  }

  @Override
  public long getGroupCount() {
    return sentryStore.getGroupCount();
  }

  @Override
  public Set<String> getGroupsForRole(String roleName) {
    return sentryStore.getGroupsForRole(roleName);
  }

  @Override
  public void stop() {
    sentryStore.stop();
  }

  @Override
  public TStoreSnapshot toSnapshot() {
    return sentryStore.toSnapshot();
  }

  @Override
  public void fromSnapshot(TStoreSnapshot snapshot) {
    sentryStore.fromSnapshot(snapshot);
  }

}
