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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.sentry.service.api.TSentryPrivilege;
import org.apache.sentry.service.api.TSentryRole;
import org.apache.sentry.provider.db.service.model.*;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.DataStoreCache;
import javax.jdo.identity.IntIdentity;
import org.apache.hadoop.hive.conf.HiveConf;

public class SentryStore {

	  private static Properties prop = null;
	  private static PersistenceManagerFactory pmf = null;

	  private static Lock pmfPropLock = new ReentrantLock();
	  private static final Log LOG = LogFactory.getLog(SentryStore.class.getName());

	  private boolean isInitialized = false;
	  private PersistenceManager pm = null;
	  int openTrasactionCalls = 0;
	  private Transaction currentTransaction = null;
	  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;
	  private final AtomicBoolean isSchemaVerified = new AtomicBoolean(false);

	  private static enum TXN_STATUS {
	    NO_STATE, OPEN, COMMITED, ROLLBACK
    }


	public SentryStore () {

	}

	//FIXME: Cleanup this mess i.e., creating a new PM and PMF.
	@SuppressWarnings("nls")
  public void setConf() {

  	pmfPropLock.lock();
    try {
      isInitialized = false;
      Properties propsFromConf = getDataSourceProps();
      assert(!isActiveTransaction());
      shutdown();
      // Always want to re-create pm as we don't know if it were created by the
      // most recent instance of the pmf
      pm = null;
      openTrasactionCalls = 0;
      currentTransaction = null;
      transactionStatus = TXN_STATUS.NO_STATE;

      initialize(propsFromConf);

      if (!isInitialized) {
        throw new RuntimeException(
        "Unable to create persistence manager. Check dss.log for details");
      } else {
        LOG.info("Initialized ObjectStore");
      }
    } finally {
      pmfPropLock.unlock();
    }
  }

  private ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ObjectStore.class.getClassLoader();
    }
  }

	@SuppressWarnings("nls")
  private void initialize(Properties dsProps) {
    LOG.info("ObjectStore, initialize called");
    prop = dsProps;
    pm = getPersistenceManager();
    isInitialized = (pm != null);  
  }

	public PersistenceManager getPersistenceManager() {
    return getPMF().getPersistenceManager();
  }
	
  private static synchronized PersistenceManagerFactory getPMF() {
    if (pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if (dsc == null) {
       LOG.warn("PersistenceManagerFactory returned null DataStoreCache object. Unable to initialize object pin types defined by hive.metastore.cache.pinobjtypes");
      }
    }
    return pmf;
  }

  public void shutdown() {
    if (pm != null) {
      pm.close();
    }
  }
	
  //FIXME: Cleanup this logic
  public boolean openTransaction() {
	    openTrasactionCalls++;
	    if (openTrasactionCalls == 1) {
	      currentTransaction = pm.currentTransaction();
	      currentTransaction.begin();
	      transactionStatus = TXN_STATUS.OPEN;
	    } else {
	      // something is wrong since openTransactionCalls is greater than 1 but
	      // currentTransaction is not active
	      assert ((currentTransaction != null) && (currentTransaction.isActive()));
	    }
	    return currentTransaction.isActive();
	  }

	  @SuppressWarnings("nls")
	  public boolean commitTransaction() {
	    if (TXN_STATUS.ROLLBACK == transactionStatus) {
	      return false;
	    }
	    if (openTrasactionCalls <= 0) {
	      throw new RuntimeException("commitTransaction was called but openTransactionCalls = "
	          + openTrasactionCalls + ". This probably indicates that there are unbalanced " +
	              "calls to openTransaction/commitTransaction");
	    }
	    if (!currentTransaction.isActive()) {
	      throw new RuntimeException(
	          "Commit is called, but transaction is not active. Either there are"
	              + " mismatching open and close calls or rollback was called in the same trasaction");
	    }
	    openTrasactionCalls--;
	    if ((openTrasactionCalls == 0) && currentTransaction.isActive()) {
	      transactionStatus = TXN_STATUS.COMMITED;
	      currentTransaction.commit();
	    }
	    return true;
	  }

	  public boolean isActiveTransaction() {
	    if (currentTransaction == null) {
	      return false;
	    }
	    return currentTransaction.isActive();
	  }

	  public void rollbackTransaction() {
	    if (openTrasactionCalls < 1) {
	      return;
	    }
	    openTrasactionCalls = 0;
	    if (currentTransaction.isActive()
	        && transactionStatus != TXN_STATUS.ROLLBACK) {
	      transactionStatus = TXN_STATUS.ROLLBACK;
	      // could already be rolled back
	      currentTransaction.rollback();
	      // remove all detached objects from the cache, since the transaction is
	      // being rolled back they are no longer relevant, and this prevents them
	      // from reattaching in future transactions
	      pm.evictAll();
	    }
	  }

	private static Properties getDataSourceProps() {
	    Properties prop = new Properties();
	    // FIXME: Read from configuration, don't hard-code everything
	    prop.setProperty("datanucleus.connectionPoolingType", "BONECP");
	    prop.setProperty("datanucleus.validateTables", "false");
	    prop.setProperty("datanucleus.validateColumns", "false");
	    prop.setProperty("datanucleus.validateConstraints", "false");
	    prop.setProperty("datanucleus.storeManagerType", "rdbms");
	    prop.setProperty("datanucleus.autoCreateSchema", "true");
	    prop.setProperty("datanucleus.fixedDatastore", "false");
	    prop.setProperty("hive.metastore.schema.verification", "false");
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
	   prop.setProperty("javax.jdo.option.NonTransactionalRead", "true");
	   prop.setProperty("javax.jdo.option.ConnectionUserName", "APP");

       prop.setProperty("javax.jdo.option.ConnectionPassword", "mine");
	   prop.setProperty("javax.jdo.option.Multithreaded", "true");
	   prop.setProperty("javax.jdo.option.ConnectionURL",
	        "jdbc:derby:;databaseName=sentry_policy_db;create=true");
	    return prop;
	 }


	private MSentryRole convertToMSentryRole(TSentryRole role) {
	    MSentryRole mRole = new MSentryRole();
	    mRole.setCreateTime(role.getCreateTime());
	    mRole.setRoleName(role.getRoleName());
	    mRole.setGrantorPrincipal(role.getGrantorPrincipal());

	    return mRole;

	  }


	  private void writeSentryRole(MSentryRole role) {

	    // TODO: verify if the role exists, if it does throw an exception
	      pm.makePersistent(role);

	  }


	  public boolean createSentryRole(TSentryRole role) {

	    // TODO: add some logging

	    boolean committed = false;

	    try {
	      openTransaction();
	      MSentryRole mRole = convertToMSentryRole(role);
	      writeSentryRole(mRole);
	      committed = commitTransaction();
	    } finally {
	      if (!committed) {
	        rollbackTransaction();
	      }
	    }

	    return committed;
	  }

	  private MSentryRole getMSentryRole (String roleName) {

	    boolean committed = false;

	    try {
	    openTransaction();
	    Query query = pm.newQuery(MSentryRole.class);
	    query.setFilter("roleName == t");
	    query
	    .declareParameters("java.lang.String t");
	    query.setUnique(true);

	    MSentryRole mSentryRole = (MSentryRole) query.execute(roleName.trim());
	    pm.retrieve(mSentryRole);
	    committed = commitTransaction();
	    return mSentryRole;
	  } finally {
	      if (!committed) {
	      rollbackTransaction();
	      return null;
	     }
	   }
	 }

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

	  public boolean alterSentryRole(String roleName, TSentryPrivilege privilege) {

	    boolean committed = false;

	    try {
	      openTransaction();
	      MSentryRole mSentryRole = getMSentryRole(roleName);
	      MSentryPrivilege mSentryPrivilege = convertToMSentryPrivilege(privilege);
	      mSentryRole.appendPrivilege(mSentryPrivilege);
	      mSentryPrivilege.appendRole(mSentryRole);
	      pm.makePersistent(mSentryPrivilege);
	      //pm.makePersistent(mSentryRole);
	      committed = commitTransaction();
	    } finally {
	      if (!committed) {
	        rollbackTransaction();
	      }
	    }

	    return committed;
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

	  public TSentryRole getSentryRole(String roleName) {
	    TSentryRole role;
	    MSentryRole mSentryRole = getMSentryRole(roleName);
	    role = convertToSentryRole(mSentryRole);
	    return role;

	  }

	  public boolean dropSentryRole(String roleName) {

	    boolean committed = false;
	    try {
	      MSentryRole mSentryRole;

	      openTransaction();
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
	      committed = commitTransaction();
	    } finally {
	      if (!committed) {
	        rollbackTransaction();
	      }
	    }

	    return committed;

	  }
}
