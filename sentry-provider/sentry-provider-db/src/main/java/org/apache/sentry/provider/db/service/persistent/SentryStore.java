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

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_JOINER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jdo.FetchGroup;
import javax.jdo.JDODataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.provider.common.ProviderConstants;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MAuthzPathsMapping;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.model.MSentryVersion;
import org.apache.sentry.provider.db.service.thrift.SentryConfigurationException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilegeMap;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.sentry.provider.db.service.persistent.QueryParamBuilder.newQueryParamBuilder;
/**
 * SentryStore is the data access object for Sentry data. Strings
 * such as role and group names will be normalized to lowercase
 * in addition to starting and ending whitespace.
 */
public class SentryStore {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentryStore.class);

  public static final String NULL_COL = "__NULL__";
  public static int INDEX_GROUP_ROLES_MAP = 0;
  public static int INDEX_USER_ROLES_MAP = 1;

  // String constants for field names
  public static final String SERVER_NAME = "serverName";
  public static final String DB_NAME = "dbName";
  public static final String TABLE_NAME = "tableName";
  public static final String COLUMN_NAME = "columnName";
  public static final String ACTION = "action";
  public static final String URI = "URI";
  public static final String GRANT_OPTION = "grantOption";
  public static final String ROLE_NAME = "roleName";

  // For counters, representation of the "unknown value"
  private static final long COUNT_VALUE_UNKNOWN = -1;

  private static final Set<String> ALL_ACTIONS = Sets.newHashSet(AccessConstants.ALL,
      AccessConstants.SELECT, AccessConstants.INSERT, AccessConstants.ALTER,
      AccessConstants.CREATE, AccessConstants.DROP, AccessConstants.INDEX,
      AccessConstants.LOCK);

  // Now partial revoke just support action with SELECT,INSERT and ALL.
  // e.g. If we REVOKE SELECT from a privilege with action ALL, it will leads to INSERT
  // Otherwise, if we revoke other privilege(e.g. ALTER,DROP...), we will remove it from a role directly.
  private static final Set<String> PARTIAL_REVOKE_ACTIONS = Sets.newHashSet(AccessConstants.ALL,
      AccessConstants.ACTION_ALL.toLowerCase(), AccessConstants.SELECT, AccessConstants.INSERT);

  private final PersistenceManagerFactory pmf;
  private Configuration conf;
  private PrivCleaner privCleaner = null;
  private Thread privCleanerThread = null;
  private final TransactionManager tm;

  public static Properties getDataNucleusProperties(Configuration conf)
      throws SentryConfigurationException, IOException {
    Properties prop = new Properties();
    prop.putAll(ServerConfig.SENTRY_STORE_DEFAULTS);
    String jdbcUrl = conf.get(ServerConfig.SENTRY_STORE_JDBC_URL, "").trim();
    Preconditions.checkArgument(!jdbcUrl.isEmpty(), "Required parameter " +
        ServerConfig.SENTRY_STORE_JDBC_URL + " missing");
    String user = conf.get(ServerConfig.SENTRY_STORE_JDBC_USER, ServerConfig.
        SENTRY_STORE_JDBC_USER_DEFAULT).trim();
    //Password will be read from Credential provider specified using property
    // CREDENTIAL_PROVIDER_PATH("hadoop.security.credential.provider.path" in sentry-site.xml
    // it falls back to reading directly from sentry-site.xml
    char[] passTmp = conf.getPassword(ServerConfig.SENTRY_STORE_JDBC_PASS);
    if (passTmp == null) {
      throw new SentryConfigurationException("Error reading " +
              ServerConfig.SENTRY_STORE_JDBC_PASS);
    }
    String pass = new String(passTmp);

    String driverName = conf.get(ServerConfig.SENTRY_STORE_JDBC_DRIVER,
        ServerConfig.SENTRY_STORE_JDBC_DRIVER_DEFAULT);
    prop.setProperty(ServerConfig.JAVAX_JDO_URL, jdbcUrl);
    prop.setProperty(ServerConfig.JAVAX_JDO_USER, user);
    prop.setProperty(ServerConfig.JAVAX_JDO_PASS, pass);
    prop.setProperty(ServerConfig.JAVAX_JDO_DRIVER_NAME, driverName);

    /*
     * Oracle doesn't support "repeatable-read" isolation level, so we use
     * "serializable" instead. This should be handled by Datanucleus, but it
     * incorrectly states that "repeatable-read" is supported and Oracle barks
     * at run-time. This code is a hack, but until it is fixed in Datanucleus
     * we can't do much.
     *
     * JDBC URL always looks like jdbc:oracle:<drivertype>:@<database>
     *  we look at the second component.
     *
     * The isolation property can be overwritten via configuration property.
     */
    final String oracleDb = "oracle";
    if (prop.getProperty(ServerConfig.DATANUCLEUS_ISOLATION_LEVEL, "").
            equals(ServerConfig.DATANUCLEUS_REPEATABLE_READ) &&
                    jdbcUrl.contains(oracleDb)) {
      String parts[] = jdbcUrl.split(":");
      if (parts.length > 1 && parts[1].equals(oracleDb)) {
        // For Oracle JDBC driver, replace "repeatable-read" with "serializable"
        prop.setProperty(ServerConfig.DATANUCLEUS_ISOLATION_LEVEL,
                "serializable");
      }
    }

    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (key.startsWith(ServerConfig.SENTRY_JAVAX_JDO_PROPERTY_PREFIX) ||
          key.startsWith(ServerConfig.SENTRY_DATANUCLEUS_PROPERTY_PREFIX)) {
        key = StringUtils.removeStart(key, ServerConfig.SENTRY_DB_PROPERTY_PREFIX);
        prop.setProperty(key, entry.getValue());
      }
    }
    // Disallow operations outside of transactions
    prop.setProperty("datanucleus.NontransactionalRead", "false");
    prop.setProperty("datanucleus.NontransactionalWrite", "false");
    return prop;
  }

  public SentryStore(Configuration conf) throws Exception {
    this.conf = conf;
    Properties prop = getDataNucleusProperties(conf);
    boolean checkSchemaVersion = conf.get(
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION,
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION_DEFAULT).equalsIgnoreCase(
        "true");
    if (!checkSchemaVersion) {
      prop.setProperty("datanucleus.autoCreateSchema", "true");
      prop.setProperty("datanucleus.fixedDatastore", "false");
    }
    pmf = JDOHelper.getPersistenceManagerFactory(prop);
    tm = new TransactionManager(pmf, conf);
    verifySentryStoreSchema(checkSchemaVersion);

    // Kick off the thread that cleans orphaned privileges (unless told not to)
    privCleaner = this.new PrivCleaner();
    if (conf.get(ServerConfig.SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL,
        ServerConfig.SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL_DEFAULT)
        .equalsIgnoreCase("true")) {
      privCleanerThread = new Thread(privCleaner);
      privCleanerThread.start();
    }
  }

  public TransactionManager getTransactionManager() {
    return tm;
  }

  // ensure that the backend DB schema is set
  void verifySentryStoreSchema(boolean checkVersion) throws Exception {
    if (!checkVersion) {
      setSentryVersion(SentryStoreSchemaInfo.getSentryVersion(),
          "Schema version set implicitly");
    } else {
      String currentVersion = getSentryVersion();
      if (!SentryStoreSchemaInfo.getSentryVersion().equals(currentVersion)) {
        throw new SentryAccessDeniedException(
            "The Sentry store schema version " + currentVersion
                + " is different from distribution version "
                + SentryStoreSchemaInfo.getSentryVersion());
      }
    }
  }

  public synchronized void stop() {
    if (privCleanerThread != null) {
      privCleaner.exit();
      try {
        privCleanerThread.join();
      } catch (InterruptedException e) {
        // Ignore...
      }
    }
    if (pmf != null) {
      pmf.close();
    }
  }

  private void rollbackTransaction(PersistenceManager pm) {
    if (pm == null || pm.isClosed()) {
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

  /**
   * Get a single role with the given name inside a transaction
   * @param pm Persistence Manager instance
   * @param roleName Role name (should not be null)
   * @return single role with the given name
   */
  public MSentryRole getRole(PersistenceManager pm, String roleName) {
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == :roleName");
    query.setUnique(true);
    return (MSentryRole) query.execute(roleName);
  }

  /**
   * Get list of all roles. Should be called inside transaction.
   * @param pm Persistence manager instance
   * @return List of all roles
   */
  @SuppressWarnings("unchecked")
  private List<MSentryRole> getAllRoles(PersistenceManager pm) {
    Query query = pm.newQuery(MSentryRole.class);
    return (List<MSentryRole>) query.execute();
  }

  /**
   * Get a group with the given name. Should be called inside transaction.
   * @param pm Persistence Manager instance
   * @param groupName - group name
   * @return Single group with the given name
     */
  private MSentryGroup getGroup(PersistenceManager pm, String groupName) {
    Query query = pm.newQuery(MSentryGroup.class);
    query.setFilter("this.groupName == :groupName");
    query.setUnique(true);
    return (MSentryGroup) query.execute(groupName);
  }

  /**
   * Normalize the string values - remove leading and trailing whitespaces and
   * convert to lower case
   * @return normalized input
   */
  private String trimAndLower(String input) {
    return input.trim().toLowerCase();
  }

  /**
   * Create a sentry role and persist it. Role name is the primary key for the
   * role, so an attempt to create a role which exists fails with JDO exception.
   *
   * @param roleName: Name of the role being persisted.
   *    The name is normalized.
   * @throws Exception
   */
  public void createSentryRole(final String roleName) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            if (getRole(pm, trimmedRoleName) != null) {
              throw new SentryAlreadyExistsException("Role: " + trimmedRoleName);
            }
            pm.makePersistent(new MSentryRole(trimmedRoleName));
            return null;
            }
        });
  }

  /**
   * Get count of object of the given class
   * @param tClass Class to count
   * @param <T> Class type
   * @return count of objects or -1 in case of error
     */
  private <T> Long getCount(final Class<T> tClass) {
    try {
      return tm.executeTransaction(
          new TransactionBlock<Long>() {
            public Long execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery();
              query.setClass(tClass);
              query.setResult("count(this)");
              return (Long) query.execute();
            }
          });
    } catch (Exception e) {
       return COUNT_VALUE_UNKNOWN;
    }
  }

  /**
   * @return number of roles
   */
  public Gauge<Long> getRoleCountGauge() {
    return new Gauge< Long >() {
      @Override
      public Long getValue() {
        return getCount(MSentryRole.class);
      }
    };
  }

  /**
   * @return Number of privileges
   */
  public Gauge<Long> getPrivilegeCountGauge() {
    return new Gauge< Long >() {
      @Override
      public Long getValue() {
        return getCount(MSentryPrivilege.class);
      }
    };
  }

  /**
   * @return number of groups
   */
  public Gauge<Long> getGroupCountGauge() {
    return new Gauge< Long >() {
      @Override
      public Long getValue() {
        return getCount(MSentryGroup.class);
      }
    };
  }

  /**
   * Lets the test code know how many privs are in the db, so that we know
   * if they are in fact being cleaned up when not being referenced any more.
   * @return The number of rows in the db priv table.
   */
  @VisibleForTesting
  long countMSentryPrivileges() {
    return getCount(MSentryPrivilege.class);
  }

  @VisibleForTesting
  void clearAllTables() {
    try {
      tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              pm.newQuery(MSentryRole.class).deletePersistentAll();
              pm.newQuery(MSentryGroup.class).deletePersistentAll();
              pm.newQuery(MSentryPrivilege.class).deletePersistentAll();
              return null;
            }
          });
    } catch (Exception e) {
      // the method only for test, log the error and ignore the exception
      LOGGER.error(e.getMessage(), e);
    }
  }

  /**
   * Grant privilege for a role
   * @param grantorPrincipal User name
   * @param roleName Role name
   * @param privilege Privilege to grant
   * @throws Exception
   */
  public void alterSentryRoleGrantPrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege privilege) throws Exception {
    alterSentryRoleGrantPrivileges(grantorPrincipal, roleName,
            Sets.newHashSet(privilege));
  }

  /**
   * Grant multiple privileges
   * @param grantorPrincipal User name
   * @param roleName Role name
   * @param privileges Set of privileges
   * @throws Exception
   */
  public void alterSentryRoleGrantPrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> privileges) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            for (TSentryPrivilege privilege : privileges) {
              // first do grant check
              grantOptionCheck(pm, grantorPrincipal, privilege);
              MSentryPrivilege mPrivilege = alterSentryRoleGrantPrivilegeCore(
                  pm, trimmedRoleName, privilege);
              if (mPrivilege != null) {
                convertToTSentryPrivilege(mPrivilege, privilege);
              }
            }
            return null;
          }
        });
  }

  private MSentryPrivilege alterSentryRoleGrantPrivilegeCore(PersistenceManager pm,
      String roleName, TSentryPrivilege privilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    MSentryPrivilege mPrivilege = null;
    MSentryRole mRole = getRole(pm, roleName);
    if (mRole == null) {
      throw noSuchRole(roleName);
    } else {
      if (!isNULL(privilege.getColumnName()) || !isNULL(privilege.getTableName())
          || !isNULL(privilege.getDbName())) {
        // If Grant is for ALL and Either INSERT/SELECT already exists..
        // need to remove it and GRANT ALL..
        if (privilege.getAction().equalsIgnoreCase("*")) {
          TSentryPrivilege tNotAll = new TSentryPrivilege(privilege);
          tNotAll.setAction(AccessConstants.SELECT);
          MSentryPrivilege mSelect = getMSentryPrivilege(tNotAll, pm);
          tNotAll.setAction(AccessConstants.INSERT);
          MSentryPrivilege mInsert = getMSentryPrivilege(tNotAll, pm);
          if ((mSelect != null) && (mRole.getPrivileges().contains(mSelect))) {
            mSelect.removeRole(mRole);
            privCleaner.incPrivRemoval();
            pm.makePersistent(mSelect);
          }
          if ((mInsert != null) && (mRole.getPrivileges().contains(mInsert))) {
            mInsert.removeRole(mRole);
            privCleaner.incPrivRemoval();
            pm.makePersistent(mInsert);
          }
        } else {
          // If Grant is for Either INSERT/SELECT and ALL already exists..
          // do nothing..
          TSentryPrivilege tAll = new TSentryPrivilege(privilege);
          tAll.setAction(AccessConstants.ALL);
          MSentryPrivilege mAll = getMSentryPrivilege(tAll, pm);
          if ((mAll != null) && (mRole.getPrivileges().contains(mAll))) {
            return null;
          }
        }
      }

      mPrivilege = getMSentryPrivilege(privilege, pm);
      if (mPrivilege == null) {
        mPrivilege = convertToMSentryPrivilege(privilege);
      }
      mPrivilege.appendRole(mRole);
      pm.makePersistent(mRole);
      pm.makePersistent(mPrivilege);
    }
    return mPrivilege;
  }

  public void alterSentryRoleRevokePrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege tPrivilege) throws Exception {
    alterSentryRoleRevokePrivileges(grantorPrincipal, roleName,
            Sets.newHashSet(tPrivilege));
  }

  public void alterSentryRoleRevokePrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> tPrivileges) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = safeTrimLower(roleName);
            for (TSentryPrivilege tPrivilege : tPrivileges) {
              // first do revoke check
              grantOptionCheck(pm, grantorPrincipal, tPrivilege);

              alterSentryRoleRevokePrivilegeCore(pm, trimmedRoleName, tPrivilege);
            }
            return null;
          }
        });
  }

  private void alterSentryRoleRevokePrivilegeCore(PersistenceManager pm,
      String roleName, TSentryPrivilege tPrivilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    MSentryRole mRole = getRole(pm, roleName);
    if (mRole == null) {
      throw noSuchRole(roleName);
    }
    MSentryPrivilege mPrivilege = getMSentryPrivilege(tPrivilege, pm);
    if (mPrivilege == null) {
      mPrivilege = convertToMSentryPrivilege(tPrivilege);
    } else {
      mPrivilege = pm.detachCopy(mPrivilege);
    }

    Set<MSentryPrivilege> privilegeGraph = new HashSet<>();
    if (mPrivilege.getGrantOption() != null) {
      privilegeGraph.add(mPrivilege);
    } else {
      MSentryPrivilege mTure = new MSentryPrivilege(mPrivilege);
      mTure.setGrantOption(true);
      privilegeGraph.add(mTure);
      MSentryPrivilege mFalse = new MSentryPrivilege(mPrivilege);
      mFalse.setGrantOption(false);
      privilegeGraph.add(mFalse);
    }
    // Get the privilege graph
    populateChildren(pm, Sets.newHashSet(roleName), mPrivilege, privilegeGraph);
    for (MSentryPrivilege childPriv : privilegeGraph) {
      revokePrivilegeFromRole(pm, tPrivilege, mRole, childPriv);
    }
    pm.makePersistent(mRole);
  }

  /**
   * Roles can be granted ALL, SELECT, and INSERT on tables. When
   * a role has ALL and SELECT or INSERT are revoked, we need to remove the ALL
   * privilege and add SELECT (INSERT was revoked) or INSERT (SELECT was revoked).
   */
  private void revokePartial(PersistenceManager pm,
      TSentryPrivilege requestedPrivToRevoke, MSentryRole mRole,
      MSentryPrivilege currentPrivilege) throws SentryInvalidInputException {
    MSentryPrivilege persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
    if (persistedPriv == null) {
      persistedPriv = convertToMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege));
    }

    if (requestedPrivToRevoke.getAction().equalsIgnoreCase("ALL") || requestedPrivToRevoke.getAction().equalsIgnoreCase("*")) {
      persistedPriv.removeRole(mRole);
      privCleaner.incPrivRemoval();
      pm.makePersistent(persistedPriv);
    } else if (requestedPrivToRevoke.getAction().equalsIgnoreCase(AccessConstants.SELECT)
        && (!currentPrivilege.getAction().equalsIgnoreCase(AccessConstants.INSERT))) {
      revokeRolePartial(pm, mRole, currentPrivilege, persistedPriv, AccessConstants.INSERT);
    } else if (requestedPrivToRevoke.getAction().equalsIgnoreCase(AccessConstants.INSERT)
        && (!currentPrivilege.getAction().equalsIgnoreCase(AccessConstants.SELECT))) {
      revokeRolePartial(pm, mRole, currentPrivilege, persistedPriv, AccessConstants.SELECT);
    }
  }

  private void revokeRolePartial(PersistenceManager pm, MSentryRole mRole,
      MSentryPrivilege currentPrivilege, MSentryPrivilege persistedPriv, String addAction)
      throws SentryInvalidInputException {
    // If table / URI, remove ALL
    persistedPriv.removeRole(mRole);
    privCleaner.incPrivRemoval();
    pm.makePersistent(persistedPriv);

    currentPrivilege.setAction(AccessConstants.ALL);
    persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
    if ((persistedPriv != null)&&(mRole.getPrivileges().contains(persistedPriv))) {
      persistedPriv.removeRole(mRole);
      privCleaner.incPrivRemoval();
      pm.makePersistent(persistedPriv);

      currentPrivilege.setAction(addAction);
      persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
      if (persistedPriv == null) {
        persistedPriv = convertToMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege));
        mRole.appendPrivilege(persistedPriv);
      }
      persistedPriv.appendRole(mRole);
      pm.makePersistent(persistedPriv);
    }
  }

  /**
   * Revoke privilege from role
   */
  private void revokePrivilegeFromRole(PersistenceManager pm, TSentryPrivilege tPrivilege,
      MSentryRole mRole, MSentryPrivilege mPrivilege) throws SentryInvalidInputException {
    if (PARTIAL_REVOKE_ACTIONS.contains(mPrivilege.getAction())) {
      // if this privilege is in {ALL,SELECT,INSERT}
      // we will do partial revoke
      revokePartial(pm, tPrivilege, mRole, mPrivilege);
    } else {
      // if this privilege is not ALL, SELECT nor INSERT,
      // we will revoke it from role directly
      MSentryPrivilege persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(mPrivilege), pm);
      if (persistedPriv != null) {
        mPrivilege.removeRole(mRole);
        privCleaner.incPrivRemoval();
        pm.makePersistent(mPrivilege);
      }
    }
  }

  /**
   * Explore Privilege graph and collect child privileges.
   * The responsibility to commit/rollback the transaction should be handled by the caller.
   */
  private void populateChildren(PersistenceManager pm, Set<String> roleNames, MSentryPrivilege priv,
      Set<MSentryPrivilege> children) throws SentryInvalidInputException {
    Preconditions.checkNotNull(pm);
    if ((!isNULL(priv.getServerName())) || (!isNULL(priv.getDbName()))
        || (!isNULL(priv.getTableName()))) {
      // Get all TableLevel Privs
      Set<MSentryPrivilege> childPrivs = getChildPrivileges(pm, roleNames, priv);
      for (MSentryPrivilege childPriv : childPrivs) {
        // Only recurse for table level privs..
        if ((!isNULL(childPriv.getDbName())) && (!isNULL(childPriv.getTableName()))
            && (!isNULL(childPriv.getColumnName()))) {
          populateChildren(pm, roleNames, childPriv, children);
        }
        // The method getChildPrivileges() didn't do filter on "action",
        // if the action is not "All", it should judge the action of children privilege.
        // For example: a user has a privilege “All on Col1”,
        // if the operation is “REVOKE INSERT on table”
        // the privilege should be the child of table level privilege.
        // but the privilege may still have other meaning, likes "SELECT on Col1".
        // and the privileges like "SELECT on Col1" should not be revoke.
        if (!priv.isActionALL()) {
          if (childPriv.isActionALL()) {
            // If the child privilege is All, we should convert it to the same
            // privilege with parent
            childPriv.setAction(priv.getAction());
          }
          // Only include privilege that imply the parent privilege.
          if (!priv.implies(childPriv)) {
            continue;
          }
        }
        children.add(childPriv);
      }
    }
  }

  private Set<MSentryPrivilege> getChildPrivileges(PersistenceManager pm, Set<String> roleNames,
      MSentryPrivilege parent) throws SentryInvalidInputException {
    // Column and URI do not have children
    if (!isNULL(parent.getColumnName()) || !isNULL(parent.getURI())) {
      return Collections.emptySet();
    }

    Query query = pm.newQuery(MSentryPrivilege.class);
    QueryParamBuilder paramBuilder = QueryParamBuilder.addRolesFilter(query, null, roleNames)
            .add(SERVER_NAME, parent.getServerName());

    if (!isNULL(parent.getDbName())) {
      paramBuilder.add(DB_NAME, parent.getDbName());
      if (!isNULL(parent.getTableName())) {
        paramBuilder.add(TABLE_NAME, parent.getTableName())
                .addNotNull(COLUMN_NAME);
      } else {
        paramBuilder.addNotNull(TABLE_NAME);
      }
    } else {
      // Andd condition dbName != NULL || URI != NULL
      paramBuilder.newChild()
              .addNotNull(DB_NAME)
              .addNotNull(URI);
    }

    query.setFilter(paramBuilder.toString());
    query.setResult("privilegeScope, serverName, dbName, tableName, columnName," +
        " URI, action, grantOption");
    List<Object[]> privObjects =
            (List<Object[]>) query.executeWithMap(paramBuilder.getArguments());
    Set<MSentryPrivilege> privileges = new HashSet<>(privObjects.size());
    for (Object[] privObj : privObjects) {
      String scope        = (String)privObj[0];
      String serverName   = (String)privObj[1];
      String dbName       = (String)privObj[2];
      String tableName    = (String) privObj[3];
      String columnName   = (String) privObj[4];
      String URI          = (String) privObj[5];
      String action       = (String) privObj[6];
      Boolean grantOption = (Boolean) privObj[7];
      MSentryPrivilege priv =
              new MSentryPrivilege("", scope, serverName, dbName, tableName,
                      columnName, URI, action, grantOption);
      privileges.add(priv);
    }
    return privileges;
  }

  @SuppressWarnings("unchecked")
  private List<MSentryPrivilege> getMSentryPrivileges(TSentryPrivilege tPriv, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryPrivilege.class);
    QueryParamBuilder paramBuilder = newQueryParamBuilder();
    paramBuilder
            .add(SERVER_NAME, tPriv.getServerName())
            .add("action", tPriv.getAction());

    if (!isNULL(tPriv.getDbName())) {
      paramBuilder.add(DB_NAME, tPriv.getDbName());
      if (!isNULL(tPriv.getTableName())) {
        paramBuilder.add(TABLE_NAME, tPriv.getTableName());
        if (!isNULL(tPriv.getColumnName())) {
          paramBuilder.add(COLUMN_NAME, tPriv.getColumnName());
        }
      }
    } else if (!isNULL(tPriv.getURI())) {
      // if db is null, uri is not null
      paramBuilder.add(URI, tPriv.getURI(), true);
    }

    query.setFilter(paramBuilder.toString());
    return (List<MSentryPrivilege>) query.executeWithMap(paramBuilder.getArguments());
  }

  private MSentryPrivilege getMSentryPrivilege(TSentryPrivilege tPriv, PersistenceManager pm) {
    Boolean grantOption = null;
    if (tPriv.getGrantOption().equals(TSentryGrantOption.TRUE)) {
      grantOption = true;
    } else if (tPriv.getGrantOption().equals(TSentryGrantOption.FALSE)) {
      grantOption = false;
    }

    QueryParamBuilder paramBuilder = newQueryParamBuilder();
    paramBuilder.add(SERVER_NAME, tPriv.getServerName())
            .add(DB_NAME, tPriv.getDbName())
            .add(TABLE_NAME, tPriv.getTableName())
            .add(COLUMN_NAME, tPriv.getColumnName())
            .add(URI, tPriv.getURI(), true)
            .addObject(GRANT_OPTION, grantOption)
            .add(ACTION, tPriv.getAction());

    Query query = pm.newQuery(MSentryPrivilege.class);
    query.setUnique(true);
    query.setFilter(paramBuilder.toString());
    return (MSentryPrivilege)query.executeWithMap(paramBuilder.getArguments());
  }

  /**
   * Drop a given sentry role.
   *
   * @param roleName the given role name
   * @throws Exception
   */
  public void dropSentryRole(final String roleName) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            dropSentryRoleCore(pm, roleName);
            return null;
          }
        });
  }

  private void dropSentryRoleCore(PersistenceManager pm, String roleName)
      throws SentryNoSuchObjectException {
    String lRoleName = trimAndLower(roleName);
    MSentryRole sentryRole = getRole(pm, lRoleName);
    if (sentryRole == null) {
      throw noSuchRole(lRoleName);
    }
    int numPrivs = sentryRole.getPrivileges().size();
    sentryRole.removePrivileges();
    // with SENTRY-398 generic model
    sentryRole.removeGMPrivileges();
    privCleaner.incPrivRemoval(numPrivs);
    pm.deletePersistent(sentryRole);
  }

  public void alterSentryRoleAddGroups(final String grantorPrincipal,
      final String roleName, final Set<TSentryGroup> groupNames) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            alterSentryRoleAddGroupsCore(pm, roleName, groupNames);
            return null;
          }
        });
  }

  private void alterSentryRoleAddGroupsCore(PersistenceManager pm, String roleName,
      Set<TSentryGroup> groupNames) throws SentryNoSuchObjectException {
    String lRoleName = trimAndLower(roleName);
    MSentryRole role = getRole(pm, lRoleName);
    if (role == null) {
      throw noSuchRole(lRoleName);
    }
    Query query = pm.newQuery(MSentryGroup.class);
    query.setFilter("this.groupName == :groupName");
    query.setUnique(true);
    List<MSentryGroup> groups = Lists.newArrayList();
    for (TSentryGroup tGroup : groupNames) {
      String groupName = tGroup.getGroupName().trim();
      MSentryGroup group = (MSentryGroup) query.execute(groupName);
      if (group == null) {
        group = new MSentryGroup(groupName, System.currentTimeMillis(), Sets.newHashSet(role));
      }
      group.appendRole(role);
      groups.add(group);
    }
    pm.makePersistentAll(groups);
  }

  public void alterSentryRoleDeleteGroups(final String roleName,
      final Set<TSentryGroup> groupNames) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            MSentryRole role = getRole(pm, trimmedRoleName);
            if (role == null) {
              throw noSuchRole(trimmedRoleName);
            }
            Query query = pm.newQuery(MSentryGroup.class);
            query.setFilter("this.groupName == :groupName");
            query.setUnique(true);
            List<MSentryGroup> groups = Lists.newArrayList();
            for (TSentryGroup tGroup : groupNames) {
              String groupName = tGroup.getGroupName().trim();
              MSentryGroup group = (MSentryGroup) query.execute(groupName);
              if (group != null) {
                group.removeRole(role);
                groups.add(group);
              }
            }
            pm.makePersistentAll(groups);
            return null;
          }
        });
  }

  @VisibleForTesting
  public MSentryRole getMSentryRoleByName(final String roleName) throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<MSentryRole>() {
          public MSentryRole execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            MSentryRole sentryRole = getRole(pm, trimmedRoleName);
            if (sentryRole == null) {
              throw noSuchRole(trimmedRoleName);
            }
            pm.retrieve(sentryRole);
            return sentryRole;
          }
        });
  }

  private boolean hasAnyServerPrivileges(final Set<String> roleNames, final String serverName) throws Exception {
    if (roleNames == null || roleNames.isEmpty()) {
      return false;
    }
    return tm.executeTransaction(
      new TransactionBlock<Boolean>() {
        public Boolean execute(PersistenceManager pm) throws Exception {
          Query query = pm.newQuery(MSentryPrivilege.class);
          QueryParamBuilder paramBuilder = QueryParamBuilder.addRolesFilter(query,null, roleNames);
          paramBuilder.add(SERVER_NAME, serverName);
          query.setFilter(paramBuilder.toString());
          query.setResult("count(this)");
          Long numPrivs = (Long) query.executeWithMap(paramBuilder.getArguments());
          return numPrivs > 0;
        }
      });
  }

  private List<MSentryPrivilege> getMSentryPrivileges(final Set<String> roleNames,
                                                      final TSentryAuthorizable authHierarchy) throws Exception {
    if (roleNames == null || roleNames.isEmpty()) {
      return Collections.emptyList();
    }

    return tm.executeTransaction(
      new TransactionBlock<List<MSentryPrivilege>>() {
        public List<MSentryPrivilege> execute(PersistenceManager pm)
                throws Exception {
          Query query = pm.newQuery(MSentryPrivilege.class);
          QueryParamBuilder paramBuilder = QueryParamBuilder.addRolesFilter(query, null, roleNames);

            if (authHierarchy != null && authHierarchy.getServer() != null) {
              paramBuilder.add(SERVER_NAME, authHierarchy.getServer());
              if (authHierarchy.getDb() != null) {
                paramBuilder.addNull(URI)
                        .newChild()
                          .add(DB_NAME, authHierarchy.getDb())
                          .addNull(DB_NAME);
                if (authHierarchy.getTable() != null
                    && !AccessConstants.ALL.equalsIgnoreCase(authHierarchy.getTable())) {
                  if (!AccessConstants.SOME.equalsIgnoreCase(authHierarchy.getTable())) {
                    paramBuilder.addNull(URI)
                            .newChild()
                              .add(TABLE_NAME, authHierarchy.getTable())
                              .addNull(TABLE_NAME);
                  }
                  if (authHierarchy.getColumn() != null
                      && !AccessConstants.ALL.equalsIgnoreCase(authHierarchy.getColumn())
                      && !AccessConstants.SOME.equalsIgnoreCase(authHierarchy.getColumn())) {
                    paramBuilder.addNull(URI)
                            .newChild()
                              .add(COLUMN_NAME, authHierarchy.getColumn())
                              .addNull(COLUMN_NAME);
                  }
                }
              }
              if (authHierarchy.getUri() != null) {
                paramBuilder.addNull(DB_NAME)
                        .newChild()
                          .addNull(URI)
                          .newChild()
                            .addNotNull(URI)
                            .addCustomParam("\"" + authHierarchy.getUri() +
                                    "\".startsWith(:URI)", URI, authHierarchy.getUri());
              }
            }
            LOGGER.debug("getMSentryPrivileges1() Query: " + paramBuilder.toString());
            query.setFilter(paramBuilder.toString());
            return (List<MSentryPrivilege>)query.executeWithMap(paramBuilder.getArguments());
          }
        });
  }

  List<MSentryPrivilege> getMSentryPrivilegesByAuth(final Set<String> roleNames,
      final TSentryAuthorizable authHierarchy) throws Exception {
      return tm.executeTransaction(
          new TransactionBlock<List<MSentryPrivilege>>() {
            public List<MSentryPrivilege> execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery(MSentryPrivilege.class);
              QueryParamBuilder paramBuilder = newQueryParamBuilder();
              if (roleNames == null || roleNames.isEmpty()) {
                paramBuilder.addString("!roles.isEmpty()");
              } else {
                QueryParamBuilder.addRolesFilter(query, paramBuilder, roleNames);
              }
              if (authHierarchy.getServer() != null) {
                paramBuilder.add(SERVER_NAME, authHierarchy.getServer());
                if (authHierarchy.getDb() != null) {
                  paramBuilder.add(DB_NAME, authHierarchy.getDb()).addNull(URI);
                  if (authHierarchy.getTable() != null) {
                    paramBuilder.add(TABLE_NAME, authHierarchy.getTable());
                  } else {
                    paramBuilder.addNull(TABLE_NAME);
                  }
                } else if (authHierarchy.getUri() != null) {
                  paramBuilder.addNotNull(URI)
                          .addNull(DB_NAME)
                          .addCustomParam("(:authURI.startsWith(URI))", "authURI", authHierarchy.getUri());
                } else {
                  paramBuilder.addNull(DB_NAME)
                        .addNull(URI);
                }
              } else {
                // if no server, then return empty resultset
                return new ArrayList<MSentryPrivilege>();
              }
              FetchGroup grp = pm.getFetchGroup(MSentryPrivilege.class, "fetchRole");
              grp.addMember("roles");
              pm.getFetchPlan().addGroup("fetchRole");
              // LOGGER.debug("XXX: " + paramBuilder.toString());
              query.setFilter(paramBuilder.toString());
              return (List<MSentryPrivilege>) query.executeWithMap(paramBuilder.getArguments());
            }
          });
  }

  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(
      Set<String> groups, TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable authHierarchy, boolean isAdmin)
          throws Exception {
    Map<String, Set<TSentryPrivilege>> resultPrivilegeMap = Maps.newTreeMap();
    Set<String> roles = Sets.newHashSet();
    if (groups != null && !groups.isEmpty()) {
      roles = getRolesToQuery(groups, new TSentryActiveRoleSet(true, null));
    }
    if (activeRoles != null && !activeRoles.isAll()) {
      // need to check/convert to lowercase here since this is from user input
      for (String aRole : activeRoles.getRoles()) {
        roles.add(aRole.toLowerCase());
      }
    }

    // An empty 'roles' is a treated as a wildcard (in case of admin role)..
    // so if not admin, don't return anything if 'roles' is empty..
    if (isAdmin || !roles.isEmpty()) {
      List<MSentryPrivilege> mSentryPrivileges = getMSentryPrivilegesByAuth(roles,
          authHierarchy);
      for (MSentryPrivilege priv : mSentryPrivileges) {
        for (MSentryRole role : priv.getRoles()) {
          TSentryPrivilege tPriv = convertToTSentryPrivilege(priv);
          if (resultPrivilegeMap.containsKey(role.getRoleName())) {
            resultPrivilegeMap.get(role.getRoleName()).add(tPriv);
          } else {
            Set<TSentryPrivilege> tPrivSet = Sets.newTreeSet();
            tPrivSet.add(tPriv);
            resultPrivilegeMap.put(role.getRoleName(), tPrivSet);
          }
        }
      }
    }
    return new TSentryPrivilegeMap(resultPrivilegeMap);
  }

  private Set<MSentryPrivilege> getMSentryPrivilegesByRoleName(String roleName)
      throws Exception {
    MSentryRole mSentryRole = getMSentryRoleByName(roleName);
    return mSentryRole.getPrivileges();
  }

  /**
   * Gets sentry privilege objects for a given roleName from the persistence layer
   * @param roleName : roleName to look up
   * @return : Set of thrift sentry privilege objects
   * @throws Exception
   */

  public Set<TSentryPrivilege> getAllTSentryPrivilegesByRoleName(String roleName)
      throws Exception {
    return convertToTSentryPrivileges(getMSentryPrivilegesByRoleName(roleName));
  }


  /**
   * Gets sentry privilege objects for criteria from the persistence layer
   * @param roleNames : roleNames to look up (required)
   * @param authHierarchy : filter push down based on auth hierarchy (optional)
   * @return : Set of thrift sentry privilege objects
   * @throws SentryInvalidInputException
   */

  public Set<TSentryPrivilege> getTSentryPrivileges(Set<String> roleNames,
                                                    TSentryAuthorizable authHierarchy)
          throws Exception {
    if (authHierarchy.getServer() == null) {
      throw new SentryInvalidInputException("serverName cannot be null !!");
    }
    if ((authHierarchy.getTable() != null) && (authHierarchy.getDb() == null)) {
      throw new SentryInvalidInputException("dbName cannot be null when tableName is present !!");
    }
    if ((authHierarchy.getColumn() != null) && (authHierarchy.getTable() == null)) {
      throw new SentryInvalidInputException("tableName cannot be null when columnName is present !!");
    }
    if ((authHierarchy.getUri() == null) && (authHierarchy.getDb() == null)) {
      throw new SentryInvalidInputException("One of uri or dbName must not be null !!");
    }
    return convertToTSentryPrivileges(getMSentryPrivileges(roleNames, authHierarchy));
  }

  private Set<MSentryRole> getMSentryRolesByGroupName(final String groupName)
      throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<Set<MSentryRole>>() {
          public Set<MSentryRole> execute(PersistenceManager pm) throws Exception {
            Set<MSentryRole> roles;

            //If no group name was specified, return all roles
            if (groupName == null) {
              roles = new HashSet<>(getAllRoles(pm));
            } else {
              String trimmedGroupName = groupName.trim();
              MSentryGroup sentryGroup = getGroup(pm, trimmedGroupName);
              if (sentryGroup == null) {
                throw noSuchGroup(trimmedGroupName);
              }
              roles = sentryGroup.getRoles();
            }
            for (MSentryRole role: roles) {
              pm.retrieve(role);
            }
            return roles;
          }
        });
  }

  /**
   * Gets sentry role objects for a given groupName from the persistence layer
   * @param groupNames : set of groupNames to look up (if null returns all
   *                   roles for all groups)
   * @return : Set of thrift sentry role objects
   * @throws Exception
   */
  public Set<TSentryRole> getTSentryRolesByGroupName(Set<String> groupNames,
      boolean checkAllGroups) throws Exception {
    Set<MSentryRole> roleSet = Sets.newHashSet();
    for (String groupName : groupNames) {
      try {
        roleSet.addAll(getMSentryRolesByGroupName(groupName));
      } catch (SentryNoSuchObjectException e) {
        // if we are checking for all the given groups, then continue searching
        if (!checkAllGroups) {
          throw e;
        }
      }
    }
    return convertToTSentryRoles(roleSet);
  }

  public Set<String> getRoleNamesForGroups(final Set<String> groups) throws Exception {
    if (groups == null || groups.isEmpty()) {
      return ImmutableSet.of();
    }

    return tm.executeTransaction(
        new TransactionBlock<Set<String>>() {
          public Set<String>execute(PersistenceManager pm) throws Exception {
            return getRoleNamesForGroupsCore(pm, groups);
          }
        });
  }

  private Set<String> getRoleNamesForGroupsCore(PersistenceManager pm, Set<String> groups) {
    Query query = pm.newQuery(MSentryGroup.class);
    query.setFilter(":p1.contains(this.groupName)");
    List<MSentryGroup> sentryGroups = (List) query.execute(groups.toArray());
    if (groups.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> result = new HashSet<>();
    if (sentryGroups != null) {
      for (MSentryGroup sentryGroup : sentryGroups) {
        for (MSentryRole role : sentryGroup.getRoles()) {
          result.add(role.getRoleName());
        }
      }
    }
    return result;
  }

  public Set<MSentryRole> getRolesForGroups(PersistenceManager pm, Set<String> groups) {
    Set<MSentryRole> result = new HashSet<>();
    Query query = pm.newQuery(MSentryGroup.class);
    query.setFilter(":p1.contains(this.groupName)");
    List<MSentryGroup> sentryGroups = (List) query.execute(groups.toArray());
    if (sentryGroups != null) {
      for (MSentryGroup sentryGroup : sentryGroups) {
        result.addAll(sentryGroup.getRoles());
      }
    }
    return result;
  }

  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups, TSentryActiveRoleSet roleSet) throws Exception {
    return listSentryPrivilegesForProvider(groups, roleSet, null);
  }


  public Set<String> listSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy) throws Exception {
    Set<String> result = Sets.newHashSet();
    Set<String> rolesToQuery = getRolesToQuery(groups, roleSet);
    List<MSentryPrivilege> mSentryPrivileges = getMSentryPrivileges(rolesToQuery, authHierarchy);

    for (MSentryPrivilege priv : mSentryPrivileges) {
      result.add(toAuthorizable(priv));
    }

    return result;
  }


  public boolean hasAnyServerPrivileges(Set<String> groups, TSentryActiveRoleSet roleSet, String server) throws Exception {
    Set<String> rolesToQuery = getRolesToQuery(groups, roleSet);
    return hasAnyServerPrivileges(rolesToQuery, server);
  }



  private Set<String> getRolesToQuery(Set<String> groups,
      TSentryActiveRoleSet roleSet) throws Exception {
    Set<String> activeRoleNames = toTrimedLower(roleSet.getRoles());

    Set<String> roleNamesForGroups = toTrimedLower(getRoleNamesForGroups(groups));
    Set<String> rolesToQuery = roleSet.isAll() ? roleNamesForGroups : Sets.intersection(activeRoleNames, roleNamesForGroups);
    return rolesToQuery;
  }

  @VisibleForTesting
  static String toAuthorizable(MSentryPrivilege privilege) {
    List<String> authorizable = new ArrayList<>(4);
    authorizable.add(KV_JOINER.join(AuthorizableType.Server.name().toLowerCase(),
        privilege.getServerName()));
    if (isNULL(privilege.getURI())) {
      if (!isNULL(privilege.getDbName())) {
        authorizable.add(KV_JOINER.join(AuthorizableType.Db.name().toLowerCase(),
            privilege.getDbName()));
        if (!isNULL(privilege.getTableName())) {
          authorizable.add(KV_JOINER.join(AuthorizableType.Table.name().toLowerCase(),
              privilege.getTableName()));
          if (!isNULL(privilege.getColumnName())) {
            authorizable.add(KV_JOINER.join(AuthorizableType.Column.name().toLowerCase(),
                privilege.getColumnName()));
          }
        }
      }
    } else {
      authorizable.add(KV_JOINER.join(AuthorizableType.URI.name().toLowerCase(),
          privilege.getURI()));
    }
    if (!isNULL(privilege.getAction())
        && !privilege.getAction().equalsIgnoreCase(AccessConstants.ALL)) {
      authorizable
      .add(KV_JOINER.join(ProviderConstants.PRIVILEGE_NAME.toLowerCase(),
          privilege.getAction()));
    }
    return AUTHORIZABLE_JOINER.join(authorizable);
  }

  @VisibleForTesting
  public static Set<String> toTrimedLower(Set<String> s) {
    if (s == null || s.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }


  /**
   * Converts model object(s) to thrift object(s).
   * Additionally does normalization
   * such as trimming whitespace and setting appropriate case. Also sets the create
   * time.
   */

  private Set<TSentryPrivilege> convertToTSentryPrivileges(Collection<MSentryPrivilege> mSentryPrivileges) {
    if (mSentryPrivileges.isEmpty()) {
      return Collections.emptySet();
    }
    Set<TSentryPrivilege> privileges = new HashSet<>(mSentryPrivileges.size());
    for(MSentryPrivilege mSentryPrivilege:mSentryPrivileges) {
      privileges.add(convertToTSentryPrivilege(mSentryPrivilege));
    }
    return privileges;
  }

  private Set<TSentryRole> convertToTSentryRoles(Set<MSentryRole> mSentryRoles) {
    if (mSentryRoles.isEmpty()) {
      return Collections.emptySet();
    }
    Set<TSentryRole> roles = new HashSet<>(mSentryRoles.size());
    for(MSentryRole mSentryRole:mSentryRoles) {
      roles.add(convertToTSentryRole(mSentryRole));
    }
    return roles;
  }

  private TSentryRole convertToTSentryRole(MSentryRole mSentryRole) {
    TSentryRole role = new TSentryRole();
    role.setRoleName(mSentryRole.getRoleName());
    role.setGrantorPrincipal("--");
    Set<MSentryGroup> groups = mSentryRole.getGroups();
    Set<TSentryGroup> sentryGroups = new HashSet<>(groups.size());
    for(MSentryGroup mSentryGroup: groups) {
      TSentryGroup group = convertToTSentryGroup(mSentryGroup);
      sentryGroups.add(group);
    }

    role.setGroups(sentryGroups);
    return role;
  }

  private TSentryGroup convertToTSentryGroup(MSentryGroup mSentryGroup) {
    TSentryGroup group = new TSentryGroup();
    group.setGroupName(mSentryGroup.getGroupName());
    return group;
  }

  TSentryPrivilege convertToTSentryPrivilege(MSentryPrivilege mSentryPrivilege) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    convertToTSentryPrivilege(mSentryPrivilege, privilege);
    return privilege;
  }

  private void convertToTSentryPrivilege(MSentryPrivilege mSentryPrivilege,
      TSentryPrivilege privilege) {
    privilege.setCreateTime(mSentryPrivilege.getCreateTime());
    privilege.setAction(fromNULLCol(mSentryPrivilege.getAction()));
    privilege.setPrivilegeScope(mSentryPrivilege.getPrivilegeScope());
    privilege.setServerName(fromNULLCol(mSentryPrivilege.getServerName()));
    privilege.setDbName(fromNULLCol(mSentryPrivilege.getDbName()));
    privilege.setTableName(fromNULLCol(mSentryPrivilege.getTableName()));
    privilege.setColumnName(fromNULLCol(mSentryPrivilege.getColumnName()));
    privilege.setURI(fromNULLCol(mSentryPrivilege.getURI()));
    if (mSentryPrivilege.getGrantOption() != null) {
      privilege.setGrantOption(TSentryGrantOption.valueOf(mSentryPrivilege.getGrantOption().toString().toUpperCase()));
    } else {
      privilege.setGrantOption(TSentryGrantOption.UNSET);
    }
  }

  /**
   * Converts thrift object to model object. Additionally does normalization
   * such as trimming whitespace and setting appropriate case.
   * @throws SentryInvalidInputException
   */
  private MSentryPrivilege convertToMSentryPrivilege(TSentryPrivilege privilege)
      throws SentryInvalidInputException {
    MSentryPrivilege mSentryPrivilege = new MSentryPrivilege();
    mSentryPrivilege.setServerName(toNULLCol(safeTrimLower(privilege.getServerName())));
    mSentryPrivilege.setDbName(toNULLCol(safeTrimLower(privilege.getDbName())));
    mSentryPrivilege.setTableName(toNULLCol(safeTrimLower(privilege.getTableName())));
    mSentryPrivilege.setColumnName(toNULLCol(safeTrimLower(privilege.getColumnName())));
    mSentryPrivilege.setPrivilegeScope(safeTrim(privilege.getPrivilegeScope()));
    mSentryPrivilege.setAction(toNULLCol(safeTrimLower(privilege.getAction())));
    mSentryPrivilege.setCreateTime(System.currentTimeMillis());
    mSentryPrivilege.setURI(toNULLCol(safeTrim(privilege.getURI())));
    if ( !privilege.getGrantOption().equals(TSentryGrantOption.UNSET) ) {
      mSentryPrivilege.setGrantOption(Boolean.valueOf(privilege.getGrantOption().toString()));
    } else {
      mSentryPrivilege.setGrantOption(null);
    }
    return mSentryPrivilege;
  }

  static String safeTrim(String s) {
    if (s == null) {
      return null;
    }
    return s.trim();
  }

  static String safeTrimLower(String s) {
    if (s == null) {
      return null;
    }
    return s.trim().toLowerCase();
  }

  String getSentryVersion() throws Exception {
    MSentryVersion mVersion = getMSentryVersion();
    return mVersion.getSchemaVersion();
  }

  void setSentryVersion(final String newVersion, final String verComment)
      throws Exception {
    tm.executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            MSentryVersion mVersion;
            try {
              mVersion = getMSentryVersion();
              if (newVersion.equals(mVersion.getSchemaVersion())) {
                // specified version already in there
                return null;
              }
            } catch (SentryNoSuchObjectException e) {
              // if the version doesn't exist, then create it
              mVersion = new MSentryVersion();
            }
            mVersion.setSchemaVersion(newVersion);
            mVersion.setVersionComment(verComment);
            pm.makePersistent(mVersion);
            return null;
          }
        });
  }

  private MSentryVersion getMSentryVersion() throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<MSentryVersion>() {
          public MSentryVersion execute(PersistenceManager pm) throws Exception {
            try {
              Query query = pm.newQuery(MSentryVersion.class);
              List<MSentryVersion> mSentryVersions = (List<MSentryVersion>) query
                  .execute();
              pm.retrieveAll(mSentryVersions);
              if (mSentryVersions.isEmpty()) {
                throw new SentryNoSuchObjectException("No matching version found");
              }
              if (mSentryVersions.size() > 1) {
                throw new SentryAccessDeniedException(
                    "Metastore contains multiple versions");
              }
              return mSentryVersions.get(0);
            } catch (JDODataStoreException e) {
              if (e.getCause() instanceof MissingTableException) {
                throw new SentryAccessDeniedException("Version table not found. "
                    + "The sentry store is not set or corrupt ");
              } else {
                throw e;
              }
            }
          }
        });
  }

  /**
   * Drop given privilege from all roles
   */
  public void dropPrivilege(final TSentryAuthorizable tAuthorizable) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {

            TSentryPrivilege tPrivilege = toSentryPrivilege(tAuthorizable);
            try {
              if (isMultiActionsSupported(tPrivilege)) {
                for (String privilegeAction : ALL_ACTIONS) {
                  tPrivilege.setAction(privilegeAction);
                  dropPrivilegeForAllRoles(pm, new TSentryPrivilege(tPrivilege));
                }
              } else {
                dropPrivilegeForAllRoles(pm, new TSentryPrivilege(tPrivilege));
              }
            } catch (JDODataStoreException e) {
              throw new SentryInvalidInputException("Failed to get privileges: "
                  + e.getMessage());
            }
            return null;
          }
        });
  }

  /**
   * Rename given privilege from all roles drop the old privilege and create the new one
   * @param tAuthorizable
   * @param newTAuthorizable
   * @throws Exception
   */
  public void renamePrivilege(final TSentryAuthorizable tAuthorizable,
      final TSentryAuthorizable newTAuthorizable) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {

            TSentryPrivilege tPrivilege = toSentryPrivilege(tAuthorizable);
            TSentryPrivilege newPrivilege = toSentryPrivilege(newTAuthorizable);
            try {
              // In case of tables or DBs, check all actions
              if (isMultiActionsSupported(tPrivilege)) {
                for (String privilegeAction : ALL_ACTIONS) {
                  tPrivilege.setAction(privilegeAction);
                  newPrivilege.setAction(privilegeAction);
                  renamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
                }
              } else {
                renamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
              }
            } catch (JDODataStoreException e) {
              throw new SentryInvalidInputException("Failed to get privileges: "
                  + e.getMessage());
            }
            return null;
          }
        });
  }

  // Currently INSERT/SELECT/ALL are supported for Table and DB level privileges
  private boolean isMultiActionsSupported(TSentryPrivilege tPrivilege) {
    return tPrivilege.getDbName() != null;

  }
  // wrapper for dropOrRename
  private void renamePrivilegeForAllRoles(PersistenceManager pm,
      TSentryPrivilege tPrivilege,
      TSentryPrivilege newPrivilege) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    dropOrRenamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
  }

  /**
   * Drop given privilege from all roles
   * @param tPrivilege
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  private void dropPrivilegeForAllRoles(PersistenceManager pm,
      TSentryPrivilege tPrivilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    dropOrRenamePrivilegeForAllRoles(pm, tPrivilege, null);
  }

  /**
   * Drop given privilege from all roles Create the new privilege if asked
   * @param tPrivilege
   * @param pm
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  private void dropOrRenamePrivilegeForAllRoles(PersistenceManager pm,
      TSentryPrivilege tPrivilege,
      TSentryPrivilege newTPrivilege) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    HashSet<MSentryRole> roleSet = Sets.newHashSet();

    List<MSentryPrivilege> mPrivileges = getMSentryPrivileges(tPrivilege, pm);
    if (mPrivileges != null && !mPrivileges.isEmpty()) {
      for (MSentryPrivilege mPrivilege : mPrivileges) {
        roleSet.addAll(ImmutableSet.copyOf((mPrivilege.getRoles())));
      }
    }

    MSentryPrivilege parent = getMSentryPrivilege(tPrivilege, pm);
    for (MSentryRole role : roleSet) {
      // 1. get privilege and child privileges
      Set<MSentryPrivilege> privilegeGraph = Sets.newHashSet();
      if (parent != null) {
        privilegeGraph.add(parent);
        populateChildren(pm, Sets.newHashSet(role.getRoleName()), parent, privilegeGraph);
      } else {
        populateChildren(pm, Sets.newHashSet(role.getRoleName()), convertToMSentryPrivilege(tPrivilege),
            privilegeGraph);
      }
      // 2. revoke privilege and child privileges
      alterSentryRoleRevokePrivilegeCore(pm, role.getRoleName(), tPrivilege);
      // 3. add new privilege and child privileges with new tableName
      if (newTPrivilege != null) {
        for (MSentryPrivilege m : privilegeGraph) {
          TSentryPrivilege t = convertToTSentryPrivilege(m);
          if (newTPrivilege.getPrivilegeScope().equals(PrivilegeScope.DATABASE.name())) {
            t.setDbName(newTPrivilege.getDbName());
          } else if (newTPrivilege.getPrivilegeScope().equals(PrivilegeScope.TABLE.name())) {
            t.setTableName(newTPrivilege.getTableName());
          }
          alterSentryRoleGrantPrivilegeCore(pm, role.getRoleName(), t);
        }
      }
    }
  }

  private TSentryPrivilege toSentryPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryInvalidInputException {
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    tSentryPrivilege.setDbName(fromNULLCol(tAuthorizable.getDb()));
    tSentryPrivilege.setServerName(fromNULLCol(tAuthorizable.getServer()));
    tSentryPrivilege.setTableName(fromNULLCol(tAuthorizable.getTable()));
    tSentryPrivilege.setColumnName(fromNULLCol(tAuthorizable.getColumn()));
    tSentryPrivilege.setURI(fromNULLCol(tAuthorizable.getUri()));
    PrivilegeScope scope;
    if (!isNULL(tSentryPrivilege.getColumnName())) {
      scope = PrivilegeScope.COLUMN;
    } else if (!isNULL(tSentryPrivilege.getTableName())) {
      scope = PrivilegeScope.TABLE;
    } else if (!isNULL(tSentryPrivilege.getDbName())) {
      scope = PrivilegeScope.DATABASE;
    } else if (!isNULL(tSentryPrivilege.getURI())) {
      scope = PrivilegeScope.URI;
    } else {
      scope = PrivilegeScope.SERVER;
    }
    tSentryPrivilege.setPrivilegeScope(scope.name());
    tSentryPrivilege.setAction(AccessConstants.ALL);
    return tSentryPrivilege;
  }

  /**
   * <p>
   * Convert different forms of empty strings to @NULL_COL and return all other input strings unmodified.
   * <p>
   * Possible empty strings:
   * <ul>
   *   <li>null</li>
   *   <li>empty string ("")</li>
   * </ul>
   * <p>
   * This function is used to create proper MSentryPrivilege objects that are saved in the Sentry database from the user
   * supplied privileges (TSentryPrivilege). This function will ensure that the data we are putting into the database is
   * always consistent for various types of input from the user. Without this one can save a column as an empty string
   * or null or @NULL_COLL specifier.
   * <p>
   * @param s string input, and can be null.
   * @return original string if it is non-empty and @NULL_COL for empty strings.
   */
  public static String toNULLCol(String s) {
    return Strings.isNullOrEmpty(s) ? NULL_COL : s;
  }

  /**
   * <p>
   * Convert different forms of empty strings to an empty string("") and return all other input strings unmodified.
   * <p>
   * Possible empty strings:
   * <ul>
   *   <li>null</li>
   *   <li>empty string ("")</li>
   *   <li>@NULL_COLL</li>
   * </ul>
   * <p>
   * This function is used to create TSentryPrivilege objects and is essential in maintaining backward compatibility
   * for reading the data that is saved in the sentry database. And also to ensure the backward compatibility of read the
   * user passed column data (@see TSentryAuthorizable conversion to TSentryPrivilege)
   * <p>
   * @param s string input, and can be null.
   * @return original string if it is non-empty and "" for empty strings.
   */
  private static String fromNULLCol(String s) {
    return isNULL(s) ? "" : s;
  }

  /**
   * Function to check if a string is null, empty or @NULL_COLL specifier
   * @param s string input, and can be null.
   * @return True if the input string represents a NULL string - when it is null, empty or equals @NULL_COL
   */
  public static boolean isNULL(String s) {
    return Strings.isNullOrEmpty(s) || s.equals(NULL_COL);
  }

  /**
   * Grant option check
   * @param pm Persistence manager instance
   * @param grantorPrincipal User name
   * @param privilege Privilege to check
   * @throws SentryUserException
   */
  private void grantOptionCheck(PersistenceManager pm, String grantorPrincipal,
                                TSentryPrivilege privilege)
      throws SentryUserException {
    MSentryPrivilege mPrivilege = convertToMSentryPrivilege(privilege);
    if (grantorPrincipal == null) {
      throw new SentryInvalidInputException("grantorPrincipal should not be null");
    }
    Set<String> groups = SentryPolicyStoreProcessor.getGroupsFromUserName(conf, grantorPrincipal);
    if (groups == null || groups.isEmpty()) {
      throw new SentryGrantDeniedException(grantorPrincipal
          + " has no grant!");
    }

    // if grantor is in adminGroup, don't need to do check
    Set<String> admins = getAdminGroups();
    boolean isAdminGroup = false;
    if (groups != null && !admins.isEmpty()) {
      for (String g : groups) {
        if (admins.contains(g)) {
          isAdminGroup = true;
          break;
        }
      }
    }

    if (!isAdminGroup) {
      boolean hasGrant = false;
      Set<MSentryRole> roles = getRolesForGroups(pm, groups);
      for (MSentryRole role : roles) {
        Set<MSentryPrivilege> privilegeSet = role.getPrivileges();
        if (privilegeSet != null && !privilegeSet.isEmpty()) {
          // if role has a privilege p with grant option
          // and mPrivilege is a child privilege of p
          for (MSentryPrivilege p : privilegeSet) {
            if (p.getGrantOption() && p.implies(mPrivilege)) {
              hasGrant = true;
              break;
            }
          }
        }
      }

      if (!hasGrant) {
        throw new SentryGrantDeniedException(grantorPrincipal
            + " has no grant!");
      }
    }
  }

  // get adminGroups from conf
  private Set<String> getAdminGroups() {
    return Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}));
  }

  public Map<String, HashMap<String, String>> retrieveFullPrivilegeImage() throws Exception {
      return tm.executeTransaction(
        new TransactionBlock<Map<String, HashMap<String, String>>>() {
          public Map<String, HashMap<String, String>> execute(PersistenceManager pm) throws Exception {
            Map<String, HashMap<String, String>> retVal = new HashMap<>();
            Query query = pm.newQuery(MSentryPrivilege.class);
            QueryParamBuilder paramBuilder = newQueryParamBuilder();
            paramBuilder
                    .addNotNull(SERVER_NAME)
                    .addNotNull(DB_NAME)
                    .addNull(URI);

            query.setFilter(paramBuilder.toString());
            query.setOrdering("serverName ascending, dbName ascending, tableName ascending");
            List<MSentryPrivilege> privileges =
                    (List<MSentryPrivilege>) query.executeWithMap(paramBuilder.getArguments());
            for (MSentryPrivilege mPriv : privileges) {
              String authzObj = mPriv.getDbName();
              if (!isNULL(mPriv.getTableName())) {
                authzObj = authzObj + "." + mPriv.getTableName();
              }
              HashMap<String, String> pUpdate = retVal.get(authzObj);
              if (pUpdate == null) {
                pUpdate = new HashMap<String, String>();
                retVal.put(authzObj, pUpdate);
              }
              for (MSentryRole mRole : mPriv.getRoles()) {
                String existingPriv = pUpdate.get(mRole.getRoleName());
                if (existingPriv == null) {
                  pUpdate.put(mRole.getRoleName(), mPriv.getAction().toUpperCase());
                } else {
                  pUpdate.put(mRole.getRoleName(), existingPriv + ","
                          + mPriv.getAction().toUpperCase());
                }
              }
            }
            return retVal;
          }
        });
  }

  /**
   * @return Mapping of Role -> [Groups]
   */
  public Map<String, LinkedList<String>> retrieveFullRoleImage() throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<Map<String, LinkedList<String>>>() {
          public Map<String, LinkedList<String>> execute(PersistenceManager pm) throws Exception {
            Query query = pm.newQuery(MSentryGroup.class);
            @SuppressWarnings("unchecked")
            List<MSentryGroup> groups = (List<MSentryGroup>) query.execute();
            if (groups.isEmpty()) {
              return Collections.emptyMap();
            }

            Map<String, LinkedList<String>> retVal = new HashMap<>();
            for (MSentryGroup mGroup : groups) {
              for (MSentryRole role : mGroup.getRoles()) {
                LinkedList<String> rUpdate = retVal.get(role.getRoleName());
                if (rUpdate == null) {
                  rUpdate = new LinkedList<String>();
                  retVal.put(role.getRoleName(), rUpdate);
                }
                rUpdate.add(mGroup.getGroupName());
              }
            }
            return retVal;
          }
        });
  }

  /**
   * This returns a Mapping of Authz -> [Paths]
   */
  Map<String, Set<String>> retrieveFullPathsImage() throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<Map<String, Set<String>>>() {
        public Map<String, Set<String>> execute(PersistenceManager pm) throws Exception {
          Map<String, Set<String>> retVal = new HashMap<>();
          Query query = pm.newQuery(MAuthzPathsMapping.class);
          List<MAuthzPathsMapping> authzToPathsMappings = (List<MAuthzPathsMapping>) query.execute();
          for (MAuthzPathsMapping authzToPaths : authzToPathsMappings) {
            retVal.put(authzToPaths.getAuthzObjName(), authzToPaths.getPaths());
          }
          return retVal;
        }
      });
  }

  public void createAuthzPathsMapping(final String hiveObj,
      final Set<String> paths) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            createAuthzPathsMappingCore(pm, hiveObj, paths);
            return null;
          }
        });
  }

  private void createAuthzPathsMappingCore(PersistenceManager pm, String authzObj,
      Set<String> paths) throws SentryAlreadyExistsException {

    MAuthzPathsMapping mAuthzPathsMapping = getMAuthzPathsMapping(pm, authzObj);

    if (mAuthzPathsMapping == null) {
      mAuthzPathsMapping =
          new MAuthzPathsMapping(authzObj, paths, System.currentTimeMillis());
      pm.makePersistent(mAuthzPathsMapping);
    } else {
      throw new SentryAlreadyExistsException("AuthzObj: " + authzObj);
    }
  }

  /**
   * Get the MAuthzPathsMapping object from authzObj
   */
  public MAuthzPathsMapping getMAuthzPathsMapping(PersistenceManager pm, String authzObj) {
    Query query = pm.newQuery(MAuthzPathsMapping.class);
    query.setFilter("this.authzObjName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    return (MAuthzPathsMapping) query.execute(authzObj);
  }

  /**
   * This thread exists to clean up "orphaned" privilege rows in the database.
   * These rows aren't removed automatically due to the fact that there is
   * a many-to-many mapping between the roles and privileges, and the
   * detection and removal of orphaned privileges is a wee bit involved.
   * This thread hangs out until notified by the parent (the outer class)
   * and then runs a custom SQL statement that detects and removes orphans.
   */
  private class PrivCleaner implements Runnable {
    // Kick off priv orphan removal after this many notifies
    private static final int NOTIFY_THRESHOLD = 50;

    // How many times we've been notified; reset to zero after orphan removal
    // This begins at the NOTIFY_THRESHOLD, so that we clear any potential
    // orphans on startup.
    private int currentNotifies = NOTIFY_THRESHOLD;

    // Internal state for threads
    private boolean exitRequired = false;

    // This lock and condition are needed to implement a way to drop the
    // lock inside a while loop, and not hold the lock across the orphan
    // removal.
    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();

    /**
     * Waits in a loop, running the orphan removal function when notified.
     * Will exit after exitRequired is set to true by exit().  We are careful
     * to not hold our lock while removing orphans; that operation might
     * take a long time.  There's also the matter of lock ordering.  Other
     * threads start a transaction first, and then grab our lock; this thread
     * grabs the lock and then starts a transaction.  Handling this correctly
     * requires explicit locking/unlocking through the loop.
     */
    public void run() {
      while (true) {
        lock.lock();
        try {
          // Check here in case this was set during removeOrphanedPrivileges()
          if (exitRequired) {
            return;
          }
          while (currentNotifies <= NOTIFY_THRESHOLD) {
            try {
              cond.await();
            } catch (InterruptedException e) {
              // Interrupted
            }
            // Check here in case this was set while waiting
            if (exitRequired) {
              return;
            }
          }
          currentNotifies = 0;
        } finally {
          lock.unlock();
        }
        try {
          removeOrphanedPrivileges();
        } catch (Exception e) {
          LOGGER.warn("Privilege cleaning thread encountered an error: " +
                  e.getMessage());
        }
      }
    }

    /**
     * This is called when a privilege is removed from a role.  This may
     * or may not mean that the privilege needs to be removed from the
     * database; there may be more references to it from other roles.
     * As a result, we'll lazily run the orphan cleaner every
     * NOTIFY_THRESHOLD times this routine is called.
     * @param numDeletions The number of potentially orphaned privileges
     */
    void incPrivRemoval(int numDeletions) {
      if (privCleanerThread != null) {
        lock.lock();
        currentNotifies += numDeletions;
        if (currentNotifies > NOTIFY_THRESHOLD) {
          cond.signal();
        }
        lock.unlock();
      }
    }

    /**
     * Simple form of incPrivRemoval when only one privilege is deleted.
     */
    void incPrivRemoval() {
      incPrivRemoval(1);
    }

    /**
     * Tell this thread to exit. Safe to call multiple times, as it just
     * notifies the run() loop to finish up.
     */
    void exit() {
      if (privCleanerThread != null) {
        lock.lock();
        try {
          exitRequired = true;
          cond.signal();
        } finally {
          lock.unlock();
        }
      }
    }

    /**
     * Run a SQL query to detect orphaned privileges, and then delete
     * each one.  This is complicated by the fact that datanucleus does
     * not seem to play well with the mix between a direct SQL query
     * and operations on the database.  The solution that seems to work
     * is to split the operation into two transactions: the first is
     * just a read for privileges that look like they're orphans, the
     * second transaction will go and get each of those privilege objects,
     * verify that there are no roles attached, and then delete them.
     */
    @SuppressWarnings("unchecked")
    private void removeOrphanedPrivileges() {
      final String privDB = "SENTRY_DB_PRIVILEGE";
      final String privId = "DB_PRIVILEGE_ID";
      final String mapDB = "SENTRY_ROLE_DB_PRIVILEGE_MAP";
      final String privFilter =
              "select " + privId +
              " from " + privDB + " p" +
              " where not exists (" +
                  " select 1 from " + mapDB + " d" +
                  " where p." + privId + " != d." + privId +
              " )";
      boolean rollback = true;
      int orphansRemoved = 0;
      ArrayList<Object> idList = new ArrayList<>();
      PersistenceManager pm = pmf.getPersistenceManager();

      // Transaction 1: Perform a SQL query to get things that look like orphans
      try {
        Transaction transaction = pm.currentTransaction();
        transaction.begin();
        transaction.setRollbackOnly();  // Makes the tx read-only
        Query query = pm.newQuery("javax.jdo.query.SQL", privFilter);
        query.setClass(MSentryPrivilege.class);
        List<MSentryPrivilege> results = (List<MSentryPrivilege>) query.execute();
        for (MSentryPrivilege orphan : results) {
          idList.add(pm.getObjectId(orphan));
        }
        transaction.rollback();
        rollback = false;
      } finally {
        if (rollback && pm.currentTransaction().isActive()) {
          pm.currentTransaction().rollback();
        } else {
          LOGGER.debug("Found {} potential orphans", idList.size());
        }
      }

      if (idList.isEmpty()) {
        pm.close();
        return;
      }

      Preconditions.checkState(!rollback);

      // Transaction 2: For each potential orphan, verify it's really an
      // orphan and delete it if so
      rollback = true;
      try {
        Transaction transaction = pm.currentTransaction();
        transaction.begin();
        pm.refreshAll();  // Try to ensure we really have correct objects
        for (Object id : idList) {
          MSentryPrivilege priv = (MSentryPrivilege) pm.getObjectById(id);
          if (priv.getRoles().isEmpty()) {
            pm.deletePersistent(priv);
            orphansRemoved++;
          }
        }
        transaction.commit();
        pm.close();
        rollback = false;
      } finally {
        if (rollback) {
          rollbackTransaction(pm);
        } else {
          LOGGER.debug("Cleaned up {} orphaned privileges", orphansRemoved);
        }
      }
    }
  }

  /** get mapping datas for [group,role], [user,role] with the specific roles */
  @SuppressWarnings("unchecked")
  public Set<String> getAllRoleNames() throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<Set<String>>() {
          public Set<String> execute(PersistenceManager pm) throws Exception {
            return getAllRoleNames(pm);
          }
        });
  }

  /**
   * Get set of all role names
   * Should be executed inside transaction
   * @param pm PersistenceManager instance
   * @return Set of all role names, or an empty set if no roles are defined
   */
  private Set<String> getAllRoleNames(PersistenceManager pm) {
    List<MSentryRole> mSentryRoles = getAllRoles(pm);
    if (mSentryRoles.isEmpty()) {
      return Collections.emptySet();
    }

    return rolesToRoleNames(mSentryRoles);
  }

  /**
   * Return exception for nonexistent role
   * @param roleName Role name
   * @return SentryNoSuchObjectException with appropriate message
   */
  private static SentryNoSuchObjectException noSuchRole(String roleName) {
    return new SentryNoSuchObjectException("nonexistent role " + roleName);
  }

  /**
   * Return exception for nonexistent group
   * @param groupName Group name
   * @return SentryNoSuchObjectException with appropriate message
   */
  private static SentryNoSuchObjectException noSuchGroup(String groupName) {
    return new SentryNoSuchObjectException("nonexistent group + " + groupName);
  }

  /**
   * Return set of rolenames from a collection of roles
   * @param roles - collection of roles
   * @return set of role names for each role in collection
   */
  public static Set<String> rolesToRoleNames(final Iterable<MSentryRole> roles) {
    Set<String> roleNames = new HashSet<>();
    for (MSentryRole mSentryRole : roles) {
      roleNames.add(mSentryRole.getRoleName());
    }
    return roleNames;
  }

}
