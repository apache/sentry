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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.DataStoreCache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;

public class SentryStore {
  private static Properties prop;
  private static PersistenceManagerFactory pmf = null;
  private static final Log LOG = LogFactory.getLog(SentryStore.class.getName());
  private boolean isReady;
  private final AtomicBoolean isSchemaVerified = new AtomicBoolean(false);

  private static enum TXN_STATUS {
    NO_STATE, OPEN, COMMITED, ROLLBACK
  }

  public SentryStore () {
    prop = getDataSourceProperties();
    pmf = getPMF(prop);
    isReady = true;
  }

  public synchronized void stop() {
    pmf.close();
    isReady = false;
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

  private synchronized PersistenceManagerFactory getPMF(Properties prop) {
    if (pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if (dsc == null) {
        LOG.warn("PersistenceManagerFactory returned null DataStoreCache object. Unable to initialize object pin types defined by hive.metastore.cache.pinobjtypes");
      }
    }
    return pmf;
  }

  /* PersistenceManager object and Transaction object have a one to one
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

  private boolean commitTransaction(PersistenceManager pm) {
    Transaction currentTransaction = pm.currentTransaction();
    if (currentTransaction.isActive()) {
      currentTransaction.commit();
      pm.close();
      return true;
    } else {
      pm.close();
      return false;
    }
  }

  private boolean rollbackTransaction(PersistenceManager pm) {
    Transaction currentTransaction = pm.currentTransaction();
    if (currentTransaction.isActive()) {
      currentTransaction.rollback();
      pm.close();
      return true;
    } else {
      pm.close();
      return false;
    }
  }

  private MSentryRole convertToMSentryRole(TSentryRole role) {
    MSentryRole mRole = new MSentryRole();
    mRole.setCreateTime(role.getCreateTime());
    mRole.setRoleName(role.getRoleName());
    mRole.setGrantorPrincipal(role.getGrantorPrincipal());
    return mRole;
  }

  public boolean createSentryRole(TSentryRole role) {
    boolean commit = false;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      MSentryRole mRole = convertToMSentryRole(role);
      pm.makePersistent(mRole);
      commit = commitTransaction(pm);
    } finally {
      if (!commit) {
        commit = rollbackTransaction(pm);
      }
    }
    return commit;
  }

  public TSentryRole getSentryRoleByName(String roleName) {
    TSentryRole role;
    MSentryRole mSentryRole = getMSentryRoleByName(roleName);
    role = convertToSentryRole(mSentryRole);
    return role;
  }

  private MSentryRole getMSentryRoleByName (String roleName) {
    boolean commit = false;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("roleName == t");
      query
      .declareParameters("java.lang.String t");
      query.setUnique(true);

      MSentryRole mSentryRole = (MSentryRole) query.execute(roleName.trim());
      pm.retrieve(mSentryRole);
      commit = commitTransaction(pm);
      return mSentryRole;
    } finally {
      if (!commit) {
        rollbackTransaction(pm);
        return null;
      }
    }
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

  public boolean dropSentryRole(String roleName) {
    boolean commit = false;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      MSentryRole mSentryRole;
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("roleName == t");
      query
      .declareParameters("java.lang.String t");
      query.setUnique(true);

      mSentryRole = (MSentryRole) query.execute(roleName.trim());
      pm.retrieve(mSentryRole);

      if (mSentryRole != null) {
        mSentryRole.removePrivileges();
        pm.deletePersistent(mSentryRole);
      }
      commit = commitTransaction(pm);
    } finally {
      if (!commit) {
        commit = rollbackTransaction(pm);
      }
    }
    return commit;
  }
}