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

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;

import com.google.common.base.Preconditions;

public class SentryStore {
  private static final UUID SERVER_UUID = UUID.randomUUID();
  /**
   * Commit order sequence id. This is used by notification handlers
   * to know the order in which events where committed to the database.
   * This instance variable is incremented in incrementGetSequenceId
   * and read in commitUpdateTransaction. Synchronization on this
   * is required to read commitSequenceId.
   */
  private long commitSequenceId;
  private final Properties prop;
  private final PersistenceManagerFactory pmf;

  public SentryStore () {
    commitSequenceId = 0;
    prop = getDataSourceProperties();
    pmf = JDOHelper.getPersistenceManagerFactory(prop);
  }

  public synchronized void stop() {
    if (pmf != null) {
      pmf.close();
    }
  }

  public CommitContext createSentryRole(TSentryRole role)
      throws SentryAlreadyExistsException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("roleName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      MSentryRole sentryRole = (MSentryRole) query.execute(role.getRoleName());
      if (sentryRole == null) {
        MSentryRole mRole = convertToMSentryRole(role);
        pm.makePersistent(mRole);
        CommitContext commit = commitUpdateTransaction(pm);
        rollbackTransaction = false;
        return commit;
      } else {
        throw new SentryAlreadyExistsException("Role: " + role.getRoleName());
      }
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  public CommitContext createSentryPrivilege(TSentryPrivilege privilege)
      throws SentryAlreadyExistsException {
    // TODO implement
    throw new RuntimeException("TODO");
  }

  public CommitContext alterSentryRoleAddGroups()
      throws SentryNoSuchObjectException {
    // TODO implement
    throw new RuntimeException("TODO");
  }

  public CommitContext alterSentryRoleDeleteGroups()
      throws SentryNoSuchObjectException {
    // TODO implement
    throw new RuntimeException("TODO");
  }


  public CommitContext dropSentryRole(String roleName)
      throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = roleName.trim();
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("roleName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      MSentryRole sentryRole = (MSentryRole) query.execute(roleName);
      if (sentryRole == null) {
        throw new SentryNoSuchObjectException("Role " + roleName);
      } else {
        pm.retrieve(sentryRole);
        sentryRole.removePrivileges();
        pm.deletePersistent(sentryRole);
      }
      CommitContext commit = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commit;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  public TSentryRole getSentryRoleByName(String roleName)
      throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = roleName.trim();
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("roleName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      MSentryRole sentryRole = (MSentryRole) query.execute(roleName);
      if (sentryRole == null) {
        throw new SentryNoSuchObjectException("Role " + roleName);
      } else {
        pm.retrieve(sentryRole);
      }
      rollbackTransaction = false;
      commitTransaction(pm);
      return convertToSentryRole(sentryRole);
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  private Properties getDataSourceProperties() {
    Properties prop = new Properties();
    // FIXME: Read from configuration, override the default
    //prop.setProperty("datanucleus.connectionPoolingType", "BONECP");
    prop.setProperty("datanucleus.validateTables", "false");
    prop.setProperty("datanucleus.validateColumns", "false");
    prop.setProperty("datanucleus.validateConstraints", "false");
    prop.setProperty("datanucleus.storeManagerType", "rdbms");
    prop.setProperty("datanucleus.autoCreateSchema", "true");
    prop.setProperty("datanucleus.fixedDatastore", "false");
    prop.setProperty("datanucleus.autoStartMechanismMode", "checked");
    prop.setProperty("datanucleus.transactionIsolation", "read-committed");
    prop.setProperty("datanucleus.cache.level2", "false");
    prop.setProperty("datanucleus.cache.level2.type", "none");
    prop.setProperty("datanucleus.identifierFactory", "datanucleus1");
    prop.setProperty("datanucleus.rdbms.useLegacyNativeValueStrategy", "true");
    prop.setProperty("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");
    prop.setProperty("javax.jdo.option.ConnectionDriverName",
        "org.apache.derby.jdbc.EmbeddedDriver");
    prop.setProperty("javax.jdo.PersistenceManagerFactoryClass",
        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
    prop.setProperty("javax.jdo.option.DetachAllOnCommit", "true");
    prop.setProperty("javax.jdo.option.NonTransactionalRead", "false");
    prop.setProperty("javax.jdo.option.NonTransactionalWrite", "false");
    prop.setProperty("javax.jdo.option.ConnectionUserName", "Sentry");
    prop.setProperty("javax.jdo.option.ConnectionPassword", "Sentry");
    prop.setProperty("javax.jdo.option.Multithreaded", "true");
    prop.setProperty("javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=sentry_policy_db;create=true");
    return prop;
  }

  /**
   * PersistenceManager object and Transaction object have a one to one
   * correspondence. Each PersistenceManager object is associated with a
   * transaction object and vice versa. Hence we create a persistence manager
   * instance when we create a new transaction. We create a new transaction
   * for every store API since we want that unit of work to behave as a
   * transaction.
   *
   * Note that there's only one instance of PersistenceManagerFactory object
   * for the service.
   */
  private synchronized PersistenceManager openTransaction() {
    PersistenceManager pm = pmf.getPersistenceManager();
    Transaction currentTransaction = pm.currentTransaction();
    currentTransaction.begin();
    return pm;
  }

  /**
   * Synchronized due to sequence id generation
   */
  private synchronized CommitContext commitUpdateTransaction(PersistenceManager pm) {
    commitTransaction(pm);
    return new CommitContext(SERVER_UUID, incrementGetSequenceId());
  }

  /**
   * Increments commitSequenceId which should not be modified outside
   * this method.
   *
   * @return sequence id
   */
  private synchronized long incrementGetSequenceId() {
    return ++commitSequenceId;
  }
  
  private void commitTransaction(PersistenceManager pm) {
    Transaction currentTransaction = pm.currentTransaction();
    try {
      Preconditions.checkState(currentTransaction.isActive(), "Transaction is not active");
      currentTransaction.commit();
    } finally {
      pm.close();
    }
  }

  private void rollbackTransaction(PersistenceManager pm) {
    if (pm == null) {
      return;
    }
    Transaction currentTransaction = pm.currentTransaction();
    if (currentTransaction.isActive()) {
      try {
        currentTransaction.rollback();
      } finally {
        pm.close();
      }
    }
  }

  private MSentryRole convertToMSentryRole(TSentryRole role) {
    MSentryRole mRole = new MSentryRole();
    mRole.setCreateTime(role.getCreateTime());
    mRole.setRoleName(role.getRoleName());
    mRole.setGrantorPrincipal(role.getGrantorPrincipal());
    return mRole;
  }

  private TSentryRole convertToSentryRole(MSentryRole mSentryRole) {
    TSentryRole role = new TSentryRole();
    role.setCreateTime(mSentryRole.getCreateTime());
    role.setRoleName(mSentryRole.getRoleName());
    role.setGrantorPrincipal(mSentryRole.getGrantorPrincipal());

    Set<TSentryPrivilege> sentryPrivileges = new HashSet<TSentryPrivilege>();
    for(MSentryPrivilege mSentryPrivilege:mSentryRole.getPrivileges()) {
      TSentryPrivilege privilege = convertToSentryPrivilege(mSentryPrivilege);
      sentryPrivileges.add(privilege);
    }

    role.setPrivileges(sentryPrivileges);
    return role;
  }

  private TSentryPrivilege convertToSentryPrivilege(MSentryPrivilege mSentryPrivilege) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setCreateTime(mSentryPrivilege.getCreateTime());
    privilege.setPrivilegeName(mSentryPrivilege.getPrivilegeName());
    privilege.setAction(mSentryPrivilege.getAction());
    privilege.setPrivilegeScope(mSentryPrivilege.getPrivilegeScope());
    privilege.setServerName(mSentryPrivilege.getServerName());
    privilege.setDbName(mSentryPrivilege.getDbName());
    privilege.setTableName(mSentryPrivilege.getTableName());
    privilege.setURI(mSentryPrivilege.getURI());
    privilege.setGrantorPrincipal(mSentryPrivilege.getGrantorPrincipal());

    return privilege;
  }

  @SuppressWarnings("unused")
  private MSentryPrivilege convertToMSentryPrivilege(TSentryPrivilege privilege) {
    MSentryPrivilege mSentryPrivilege = new MSentryPrivilege();
    mSentryPrivilege.setServerName(privilege.getServerName());
    mSentryPrivilege.setDbName(privilege.getDbName());
    mSentryPrivilege.setTableName(privilege.getTableName());
    mSentryPrivilege.setPrivilegeScope(privilege.getPrivilegeScope());
    mSentryPrivilege.setAction(privilege.getAction());
    mSentryPrivilege.setCreateTime(privilege.getCreateTime());
    mSentryPrivilege.setGrantorPrincipal(privilege.getGrantorPrincipal());
    mSentryPrivilege.setURI(privilege.getURI());
    mSentryPrivilege.setPrivilegeName(privilege.getPrivilegeName());
    //MSentryRole mSentryRole = convertToMSentryRole(role);
    return mSentryPrivilege;
  }
}