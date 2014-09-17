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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

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
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.model.MSentryVersion;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * SentryStore is the data access object for Sentry data. Strings
 * such as role and group names will be normalized to lowercase
 * in addition to starting and ending whitespace.
 */
public class SentryStore {
  private static final UUID SERVER_UUID = UUID.randomUUID();

  public static String NULL_COL = "__NULL__";
  static final String DEFAULT_DATA_DIR = "sentry_policy_db";
  /**
   * Commit order sequence id. This is used by notification handlers
   * to know the order in which events where committed to the database.
   * This instance variable is incremented in incrementGetSequenceId
   * and read in commitUpdateTransaction. Synchronization on this
   * is required to read commitSequenceId.
   */
  private long commitSequenceId;
  private final PersistenceManagerFactory pmf;
  private Configuration conf;

  public SentryStore(Configuration conf) throws SentryNoSuchObjectException,
  SentryAccessDeniedException {
    commitSequenceId = 0;
    this.conf = conf;
    Properties prop = new Properties();
    prop.putAll(ServerConfig.SENTRY_STORE_DEFAULTS);
    String jdbcUrl = conf.get(ServerConfig.SENTRY_STORE_JDBC_URL, "").trim();
    Preconditions.checkArgument(!jdbcUrl.isEmpty(), "Required parameter " +
        ServerConfig.SENTRY_STORE_JDBC_URL + " missing");
    String user = conf.get(ServerConfig.SENTRY_STORE_JDBC_USER, ServerConfig.
        SENTRY_STORE_JDBC_USER_DEFAULT).trim();
    String pass = conf.get(ServerConfig.SENTRY_STORE_JDBC_PASS, ServerConfig.
        SENTRY_STORE_JDBC_PASS_DEFAULT).trim();
    String driverName = conf.get(ServerConfig.SENTRY_STORE_JDBC_DRIVER,
        ServerConfig.SENTRY_STORE_JDBC_DRIVER_DEFAULT);
    prop.setProperty(ServerConfig.JAVAX_JDO_URL, jdbcUrl);
    prop.setProperty(ServerConfig.JAVAX_JDO_USER, user);
    prop.setProperty(ServerConfig.JAVAX_JDO_PASS, pass);
    prop.setProperty(ServerConfig.JAVAX_JDO_DRIVER_NAME, driverName);
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (key.startsWith(ServerConfig.SENTRY_JAVAX_JDO_PROPERTY_PREFIX) ||
          key.startsWith(ServerConfig.SENTRY_DATANUCLEUS_PROPERTY_PREFIX)) {
        key = StringUtils.removeStart(key, ServerConfig.SENTRY_DB_PROPERTY_PREFIX);
        prop.setProperty(key, entry.getValue());
      }
    }


    boolean checkSchemaVersion = conf.get(
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION,
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION_DEFAULT).equalsIgnoreCase(
            "true");
    if (!checkSchemaVersion) {
      prop.setProperty("datanucleus.autoCreateSchema", "true");
      prop.setProperty("datanucleus.fixedDatastore", "false");
    }
    pmf = JDOHelper.getPersistenceManagerFactory(prop);
    verifySentryStoreSchema(conf, checkSchemaVersion);
  }

  // ensure that the backend DB schema is set
  private void verifySentryStoreSchema(Configuration serverConf,
      boolean checkVersion)
          throws SentryNoSuchObjectException, SentryAccessDeniedException {
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
    if (pmf != null) {
      pmf.close();
    }
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
   *
   * Synchronized because we obtain persistence manager
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
  Get the MSentry object from roleName
  Note: Should be called inside a transaction
   */
  private MSentryRole getMSentryRole(PersistenceManager pm, String roleName) {
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    MSentryRole sentryRole = (MSentryRole) query.execute(roleName);
    return sentryRole;
  }

  /**
   * Normalize the string values
   */
  private String trimAndLower(String input) {
    return input.trim().toLowerCase();
  }
  /**
   * Create a sentry role and persist it.
   * @param roleName: Name of the role being persisted
   * @returns commit context used for notification handlers
   * @throws SentryAlreadyExistsException
   */
  public CommitContext createSentryRole(String roleName)
      throws SentryAlreadyExistsException {
    roleName = trimAndLower(roleName);
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      MSentryRole mSentryRole = getMSentryRole(pm, roleName);
      if (mSentryRole == null) {
        MSentryRole mRole = new MSentryRole(roleName, System.currentTimeMillis());
        pm.makePersistent(mRole);
        CommitContext commit = commitUpdateTransaction(pm);
        rollbackTransaction = false;
        return commit;
      } else {
        throw new SentryAlreadyExistsException("Role: " + roleName);
      }
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  public CommitContext alterSentryRoleGrantPrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege privilege)
      throws SentryUserException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = trimAndLower(roleName);
    try {
      pm = openTransaction();
      // first do grant check
      grantOptionCheck(pm, grantorPrincipal, privilege);

      MSentryPrivilege mPrivilege =
          alterSentryRoleGrantPrivilegeCore(pm, roleName, privilege);
      // capture the new privilege
      if (mPrivilege != null) {
        convertToTSentryPrivilege(mPrivilege, privilege);
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

  private MSentryPrivilege alterSentryRoleGrantPrivilegeCore(PersistenceManager pm,
      String roleName, TSentryPrivilege privilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    MSentryPrivilege mPrivilege = null;
    MSentryRole mRole = getMSentryRole(pm, roleName);
    if (mRole == null) {
      throw new SentryNoSuchObjectException("Role: " + roleName);
    } else {

      if ((!isNULL(privilege.getTableName())) || (!isNULL(privilege.getDbName()))) {
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
            pm.makePersistent(mSelect);
          }
          if ((mInsert != null) && (mRole.getPrivileges().contains(mInsert))) {
            mInsert.removeRole(mRole);
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

  public CommitContext alterSentryRoleRevokePrivilege(String grantorPrincipal, String roleName,
      TSentryPrivilege tPrivilege) throws SentryUserException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = safeTrimLower(roleName);
    try {
      pm = openTransaction();
      // first do revoke check
      grantOptionCheck(pm, grantorPrincipal, tPrivilege);

      alterSentryRoleRevokePrivilegeCore(pm, roleName, tPrivilege);

      CommitContext commit = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commit;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  private void alterSentryRoleRevokePrivilegeCore(PersistenceManager pm,
      String roleName, TSentryPrivilege tPrivilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    MSentryRole mRole = (MSentryRole) query.execute(roleName);
    if (mRole == null) {
      throw new SentryNoSuchObjectException("Role: " + roleName);
    } else {
      query = pm.newQuery(MSentryPrivilege.class);
      MSentryPrivilege mPrivilege = getMSentryPrivilege(tPrivilege, pm);
      if (mPrivilege == null) {
        mPrivilege = convertToMSentryPrivilege(tPrivilege);
      } else {
        mPrivilege = (MSentryPrivilege) pm.detachCopy(mPrivilege);
      }

      Set<MSentryPrivilege> privilegeGraph = Sets.newHashSet();
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
      populateChildren(Sets.newHashSet(roleName), mPrivilege, privilegeGraph);
      for (MSentryPrivilege childPriv : privilegeGraph) {
        revokePartial(pm, tPrivilege, mRole, childPriv);
      }
      pm.makePersistent(mRole);
    }
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
    pm.makePersistent(persistedPriv);

    currentPrivilege.setAction(AccessConstants.ALL);
    persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
    if ((persistedPriv != null)&&(mRole.getPrivileges().contains(persistedPriv))) {
      persistedPriv.removeRole(mRole);
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
   * Explore Privilege graph and collect child privileges
   */
  private void populateChildren(Set<String> roleNames, MSentryPrivilege priv,
      Set<MSentryPrivilege> children) throws SentryInvalidInputException {
    if ((!isNULL(priv.getServerName())) || (!isNULL(priv.getDbName()))) {
      // Get all DBLevel Privs
      Set<MSentryPrivilege> childPrivs = getChildPrivileges(roleNames, priv);
      for (MSentryPrivilege childPriv : childPrivs) {
        // Only recurse for db level privs..
        if ((!isNULL(childPriv.getDbName())) && (!isNULL(childPriv.getTableName()))) {
          populateChildren(roleNames, childPriv, children);
        }
        children.add(childPriv);
      }
    }
  }

  private Set<MSentryPrivilege> getChildPrivileges(Set<String> roleNames,
      MSentryPrivilege parent) throws SentryInvalidInputException {
    // Table and URI do not have children
    if ((!isNULL(parent.getTableName()))||(!isNULL(parent.getURI()))) return new HashSet<MSentryPrivilege>();
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryPrivilege.class);
      query
          .declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
      List<String> rolesFiler = new LinkedList<String>();
      for (String rName : roleNames) {
        rolesFiler.add("role.roleName == \"" + rName.trim().toLowerCase() + "\"");
      }
      StringBuilder filters = new StringBuilder("roles.contains(role) "
          + "&& (" + Joiner.on(" || ").join(rolesFiler) + ")");
      filters.append(" && serverName == \"" + parent.getServerName() + "\"");
      if (!isNULL(parent.getDbName())) {
        filters.append(" && dbName == \"" + parent.getDbName() + "\"");
        filters.append(" && tableName != \"__NULL__\"");
      } else {
        filters.append(" && (dbName != \"__NULL__\" || URI != \"__NULL__\")");
      }

      query.setFilter(filters.toString());
      query
          .setResult("privilegeScope, serverName, dbName, tableName, URI, action, grantOption");
      Set<MSentryPrivilege> privileges = new HashSet<MSentryPrivilege>();
      for (Object[] privObj : (List<Object[]>) query.execute()) {
        MSentryPrivilege priv = new MSentryPrivilege();
        priv.setPrivilegeScope((String) privObj[0]);
        priv.setServerName((String) privObj[1]);
        priv.setDbName((String) privObj[2]);
        priv.setTableName((String) privObj[3]);
        priv.setURI((String) privObj[4]);
        priv.setAction((String) privObj[5]);
        priv.setGrantOption((Boolean) privObj[6]);
        privileges.add(priv);
      }
      rollbackTransaction = false;
      commitTransaction(pm);
      return privileges;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  private MSentryPrivilege getMSentryPrivilege(TSentryPrivilege tPriv, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryPrivilege.class);
    query.setFilter("this.serverName == \"" + toNULLCol(tPriv.getServerName()) + "\" "
				+ "&& this.dbName == \"" + toNULLCol(tPriv.getDbName()) + "\" "
				+ "&& this.tableName == \"" + toNULLCol(tPriv.getTableName()) + "\" "
				+ "&& this.URI == \"" + toNULLCol(tPriv.getURI()) + "\" "
				+ "&& this.grantOption == grantOption "
				+ "&& this.action == \"" + toNULLCol(tPriv.getAction().toLowerCase()) + "\"");
    query.declareParameters("Boolean grantOption");
    query.setUnique(true);
    Boolean grantOption = null;
    if (tPriv.getGrantOption().equals(TSentryGrantOption.TRUE)) {
      grantOption = true;
    } else if (tPriv.getGrantOption().equals(TSentryGrantOption.FALSE)) {
      grantOption = false;
    }
    Object obj = query.execute(grantOption);
    if (obj != null)
      return (MSentryPrivilege) obj;
    return null;
  }

  public CommitContext dropSentryRole(String roleName)
      throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = roleName.trim().toLowerCase();
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("this.roleName == t");
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

  public CommitContext alterSentryRoleAddGroups( String grantorPrincipal, String roleName,
      Set<TSentryGroup> groupNames)
          throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = roleName.trim().toLowerCase();
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("this.roleName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      MSentryRole role = (MSentryRole) query.execute(roleName);
      if (role == null) {
        throw new SentryNoSuchObjectException("Role: " + roleName);
      } else {
        query = pm.newQuery(MSentryGroup.class);
        query.setFilter("this.groupName == t");
        query.declareParameters("java.lang.String t");
        query.setUnique(true);
        List<MSentryGroup> groups = Lists.newArrayList();
        for (TSentryGroup tGroup : groupNames) {
          String groupName = tGroup.getGroupName().trim();
          MSentryGroup group = (MSentryGroup) query.execute(groupName);
          if (group == null) {
            group = new MSentryGroup(groupName, System.currentTimeMillis(),
                 Sets.newHashSet(role));
          }
          group.appendRole(role);
          groups.add(group);
        }
        pm.makePersistentAll(groups);
        CommitContext commit = commitUpdateTransaction(pm);
        rollbackTransaction = false;
        return commit;
      }
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  public CommitContext alterSentryRoleDeleteGroups(String roleName,
      Set<TSentryGroup> groupNames)
          throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = roleName.trim().toLowerCase();
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("this.roleName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      MSentryRole role = (MSentryRole) query.execute(roleName);
      if (role == null) {
        throw new SentryNoSuchObjectException("Role: " + roleName);
      } else {
        query = pm.newQuery(MSentryGroup.class);
        query.setFilter("this.groupName == t");
        query.declareParameters("java.lang.String t");
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
        CommitContext commit = commitUpdateTransaction(pm);
        rollbackTransaction = false;
        return commit;
      }
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @VisibleForTesting
  MSentryRole getMSentryRoleByName(String roleName)
      throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = roleName.trim().toLowerCase();
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("this.roleName == t");
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
      return sentryRole;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  private boolean hasAnyServerPrivileges(Set<String> roleNames, String serverName) {
    if ((roleNames.size() == 0)||(roleNames == null)) return false;
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryPrivilege.class);
      query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
      List<String> rolesFiler = new LinkedList<String>();
      for (String rName : roleNames) {
        rolesFiler.add("role.roleName == \"" + rName.trim().toLowerCase() + "\"");
      }
      StringBuilder filters = new StringBuilder("roles.contains(role) "
          + "&& (" + Joiner.on(" || ").join(rolesFiler) + ") ");
      filters.append("&& serverName == \"" + serverName + "\"");
      query.setFilter(filters.toString());
      query.setResult("count(this)");

      Long numPrivs = (Long) query.execute();
      rollbackTransaction = false;
      commitTransaction(pm);
      return (numPrivs > 0);
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }


  List<MSentryPrivilege> getMSentryPrivileges(Set<String> roleNames, TSentryAuthorizable authHierarchy) {
    if ((roleNames.size() == 0)||(roleNames == null)) return new ArrayList<MSentryPrivilege>();
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryPrivilege.class);
      query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
      List<String> rolesFiler = new LinkedList<String>();
      for (String rName : roleNames) {
        rolesFiler.add("role.roleName == \"" + rName.trim().toLowerCase() + "\"");
      }
      StringBuilder filters = new StringBuilder("roles.contains(role) "
          + "&& (" + Joiner.on(" || ").join(rolesFiler) + ") ");
      if ((authHierarchy != null) && (authHierarchy.getServer() != null)) {
        filters.append("&& serverName == \"" + authHierarchy.getServer().toLowerCase() + "\"");
        if (authHierarchy.getDb() != null) {
          filters.append(" && ((dbName == \"" + authHierarchy.getDb().toLowerCase() + "\") || (dbName == \"__NULL__\")) && (URI == \"__NULL__\")");
          if ((authHierarchy.getTable() != null)
              && !AccessConstants.ALL
                  .equalsIgnoreCase(authHierarchy.getTable())) {
            filters.append(" && ((tableName == \"" + authHierarchy.getTable().toLowerCase() + "\") || (tableName == \"__NULL__\")) && (URI == \"__NULL__\")");
          }
        }
        if (authHierarchy.getUri() != null) {
          filters.append(" && ((URI != \"__NULL__\") && (\"" + authHierarchy.getUri() + "\".startsWith(URI)) || (URI == \"__NULL__\")) && (dbName == \"__NULL__\")");
        }
      }
      query.setFilter(filters.toString());
      List<MSentryPrivilege> privileges = (List<MSentryPrivilege>) query.execute();
      rollbackTransaction = false;
      commitTransaction(pm);
      return privileges;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  List<MSentryPrivilege> getMSentryPrivilegesByAuth(Set<String> roleNames, TSentryAuthorizable authHierarchy) {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryPrivilege.class);
      StringBuilder filters = new StringBuilder();
      if ((roleNames.size() == 0)||(roleNames == null)) {
        filters.append(" !roles.isEmpty() ");
      } else {
        query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
        List<String> rolesFiler = new LinkedList<String>();
        for (String rName : roleNames) {
          rolesFiler.add("role.roleName == \"" + rName.trim().toLowerCase() + "\"");
        }
        filters.append("roles.contains(role) "
          + "&& (" + Joiner.on(" || ").join(rolesFiler) + ") ");
      }
      if ((authHierarchy.getServer() != null)) {
        filters.append("&& serverName == \"" +
            authHierarchy.getServer().toLowerCase() + "\"");
        if (authHierarchy.getDb() != null) {
          filters.append(" && (dbName == \"" +
              authHierarchy.getDb().toLowerCase() + "\") && (URI == \"__NULL__\")");
          if (authHierarchy.getTable() != null) {
            filters.append(" && (tableName == \"" +
                authHierarchy.getTable().toLowerCase() + "\")");
          } else {
            filters.append(" && (tableName == \"__NULL__\")");
          }
        } else if (authHierarchy.getUri() != null) {
          filters.append(" && (URI != \"__NULL__\") && (\"" + authHierarchy.getUri() +
              "\".startsWith(URI)) && (dbName == \"__NULL__\")");
        } else {
          filters.append(" && (dbName == \"__NULL__\") && (URI == \"__NULL__\")");
        }
      } else {
        // if no server, then return empty resultset
        return new ArrayList<MSentryPrivilege>();
      }
      FetchGroup grp = pm.getFetchGroup(
          org.apache.sentry.provider.db.service.model.MSentryPrivilege.class,
          "fetchRole");
      grp.addMember("roles");
      pm.getFetchPlan().addGroup("fetchRole");
      query.setFilter(filters.toString());
      List<MSentryPrivilege> privileges = (List<MSentryPrivilege>) query.execute();
      rollbackTransaction = false;
      commitTransaction(pm);
      return privileges;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(
      Set<String> groups, TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable authHierarchy)
      throws SentryInvalidInputException {
    Map<String, Set<TSentryPrivilege>> resultPrivilegeMap = Maps.newTreeMap();
    Set<String> roles = Sets.newHashSet();
    if (groups != null && !groups.isEmpty()) {
      roles = getRolesToQuery(groups, new TSentryActiveRoleSet(true, null));
    }
    if (activeRoles != null && !activeRoles.isAll()) {
      roles.addAll(activeRoles.getRoles());
    }

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
    return new TSentryPrivilegeMap(resultPrivilegeMap);
  }

  private Set<MSentryPrivilege> getMSentryPrivilegesByRoleName(String roleName)
      throws SentryNoSuchObjectException {
    MSentryRole mSentryRole = getMSentryRoleByName(roleName);
    return mSentryRole.getPrivileges();
  }

  /**
   * Gets sentry privilege objects for a given roleName from the persistence layer
   * @param roleName : roleName to look up
   * @return : Set of thrift sentry privilege objects
   * @throws SentryNoSuchObjectException
   */

  public Set<TSentryPrivilege> getAllTSentryPrivilegesByRoleName(String roleName)
      throws SentryNoSuchObjectException {
    return convertToTSentryPrivileges(getMSentryPrivilegesByRoleName(roleName));
  }


  /**
   * Gets sentry privilege objects for criteria from the persistence layer
   * @param roleNames : roleNames to look up (required)
   * @param authHierarchy : filter push down based on auth hierarchy (optional)
   * @return : Set of thrift sentry privilege objects
   * @throws SentryNoSuchObjectException
   */

  public Set<TSentryPrivilege> getTSentryPrivileges(Set<String> roleNames, TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    if (authHierarchy.getServer() == null) {
      throw new SentryInvalidInputException("serverName cannot be null !!");
    }
    if ((authHierarchy.getTable() != null) && (authHierarchy.getDb() == null)) {
      throw new SentryInvalidInputException("dbName cannot be null when tableName is present !!");
    }
    if ((authHierarchy.getUri() == null) && (authHierarchy.getDb() == null)) {
      throw new SentryInvalidInputException("One of uri or dbName must not be null !!");
    }
    return convertToTSentryPrivileges(getMSentryPrivileges(roleNames, authHierarchy));
  }


  private Set<MSentryRole> getMSentryRolesByGroupName(String groupName)
      throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      Set<MSentryRole> roles;
      pm = openTransaction();

      //If no group name was specified, return all roles
      if (groupName == null) {
        Query query = pm.newQuery(MSentryRole.class);
        roles = new HashSet<MSentryRole>((List<MSentryRole>)query.execute());
      } else {
        Query query = pm.newQuery(MSentryGroup.class);
        MSentryGroup sentryGroup;
        groupName = groupName.trim();
        query.setFilter("this.groupName == t");
        query.declareParameters("java.lang.String t");
        query.setUnique(true);
        sentryGroup = (MSentryGroup) query.execute(groupName);
        if (sentryGroup == null) {
          throw new SentryNoSuchObjectException("Group " + groupName);
        } else {
          pm.retrieve(sentryGroup);
        }
        roles = sentryGroup.getRoles();
      }
      for ( MSentryRole role: roles) {
        pm.retrieve(role);
      }
      commitTransaction(pm);
      rollbackTransaction = false;
      return roles;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  /**
   * Gets sentry role objects for a given groupName from the persistence layer
   * @param groupName : groupName to look up ( if null returns all roles for all groups)
   * @return : Set of thrift sentry role objects
   * @throws SentryNoSuchObjectException
   */
  public Set<TSentryRole> getTSentryRolesByGroupName(Set<String> groupNames,
      boolean checkAllGroups) throws SentryNoSuchObjectException {
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

  public Set<String> getRoleNamesForGroups(Set<String> groups) {
    Set<String> result = new HashSet<String>();
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryGroup.class);
      query.setFilter("this.groupName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      for (String group : groups) {
        MSentryGroup sentryGroup = (MSentryGroup) query.execute(group.trim());
        if (sentryGroup != null) {
          for (MSentryRole role : sentryGroup.getRoles()) {
            result.add(role.getRoleName());
          }
        }
      }
      rollbackTransaction = false;
      commitTransaction(pm);
      return result;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  private Set<MSentryRole> getRolesForGroups(PersistenceManager pm, Set<String> groups) {
    Set<MSentryRole> result = new HashSet<MSentryRole>();
    Query query = pm.newQuery(MSentryGroup.class);
    query.setFilter("this.groupName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    for (String group : groups) {
      MSentryGroup sentryGroup = (MSentryGroup) query.execute(group.trim());
      if (sentryGroup != null) {
        result.addAll(sentryGroup.getRoles());
      }
    }
    return result;
  }

  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups, TSentryActiveRoleSet roleSet) throws SentryInvalidInputException {
    return listSentryPrivilegesForProvider(groups, roleSet, null);
  }


  public Set<String> listSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    Set<String> result = Sets.newHashSet();
    Set<String> rolesToQuery = getRolesToQuery(groups, roleSet);
    List<MSentryPrivilege> mSentryPrivileges = getMSentryPrivileges(rolesToQuery, authHierarchy);

    for (MSentryPrivilege priv : mSentryPrivileges) {
      result.add(toAuthorizable(priv));
    }

    return result;
  }


  public boolean hasAnyServerPrivileges(Set<String> groups, TSentryActiveRoleSet roleSet, String server) {
    Set<String> rolesToQuery = getRolesToQuery(groups, roleSet);
    return hasAnyServerPrivileges(rolesToQuery, server);
  }



  private Set<String> getRolesToQuery(Set<String> groups,
      TSentryActiveRoleSet roleSet) {
    Set<String> activeRoleNames = toTrimedLower(roleSet.getRoles());

    Set<String> roleNamesForGroups = toTrimedLower(getRoleNamesForGroups(groups));
    Set<String> rolesToQuery = roleSet.isAll() ? roleNamesForGroups : Sets.intersection(activeRoleNames, roleNamesForGroups);
    return rolesToQuery;
  }

  @VisibleForTesting
  static String toAuthorizable(MSentryPrivilege privilege) {
    List<String> authorizable = new ArrayList<String>(4);
    authorizable.add(KV_JOINER.join(AuthorizableType.Server.name().toLowerCase(),
        privilege.getServerName()));
    if (isNULL(privilege.getURI())) {
      if (!isNULL(privilege.getDbName())) {
        authorizable.add(KV_JOINER.join(AuthorizableType.Db.name().toLowerCase(),
            privilege.getDbName()));
        if (!isNULL(privilege.getTableName())) {
          authorizable.add(KV_JOINER.join(AuthorizableType.Table.name().toLowerCase(),
              privilege.getTableName()));
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
  static Set<String> toTrimedLower(Set<String> s) {
    if (null == s) return new HashSet<String>();
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
    Set<TSentryPrivilege> privileges = new HashSet<TSentryPrivilege>();
    for(MSentryPrivilege mSentryPrivilege:mSentryPrivileges) {
      privileges.add(convertToTSentryPrivilege(mSentryPrivilege));
    }
    return privileges;
  }

  private Set<TSentryRole> convertToTSentryRoles(Set<MSentryRole> mSentryRoles) {
    Set<TSentryRole> roles = new HashSet<TSentryRole>();
    for(MSentryRole mSentryRole:mSentryRoles) {
      roles.add(convertToTSentryRole(mSentryRole));
    }
    return roles;
  }

  private TSentryRole convertToTSentryRole(MSentryRole mSentryRole) {
    TSentryRole role = new TSentryRole();
    role.setRoleName(mSentryRole.getRoleName());
    Set<TSentryGroup> sentryGroups = new HashSet<TSentryGroup>();
    for(MSentryGroup mSentryGroup:mSentryRole.getGroups()) {
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

  private TSentryPrivilege convertToTSentryPrivilege(MSentryPrivilege mSentryPrivilege) {
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
  private static String safeTrim(String s) {
    if (s == null) {
      return null;
    }
    return s.trim();
  }
  private static String safeTrimLower(String s) {
    if (s == null) {
      return null;
    }
    return s.trim().toLowerCase();
  }

  public String getSentryVersion() throws SentryNoSuchObjectException,
  SentryAccessDeniedException {
    MSentryVersion mVersion = getMSentryVersion();
    return mVersion.getSchemaVersion();
  }

  public void setSentryVersion(String newVersion, String verComment)
      throws SentryNoSuchObjectException, SentryAccessDeniedException {
    MSentryVersion mVersion;
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;

    try {
      mVersion = getMSentryVersion();
      if (newVersion.equals(mVersion.getSchemaVersion())) {
        // specified version already in there
        return;
      }
    } catch (SentryNoSuchObjectException e) {
      // if the version doesn't exist, then create it
      mVersion = new MSentryVersion();
    }
    mVersion.setSchemaVersion(newVersion);
    mVersion.setVersionComment(verComment);
    try {
      pm = openTransaction();
      pm.makePersistent(mVersion);
      rollbackTransaction = false;
      commitTransaction(pm);
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private MSentryVersion getMSentryVersion()
      throws SentryNoSuchObjectException, SentryAccessDeniedException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryVersion.class);
      List<MSentryVersion> mSentryVersions = (List<MSentryVersion>) query
          .execute();
      pm.retrieveAll(mSentryVersions);
      rollbackTransaction = false;
      commitTransaction(pm);
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
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  /**
   * Drop given privilege from all roles
   */
  public void dropPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;

    TSentryPrivilege tPrivilege = toSentryPrivilege(tAuthorizable);
    try {
      pm = openTransaction();

      if (isMultiActionsSupported(tPrivilege)) {
        for (String privilegeAction : Sets.newHashSet(AccessConstants.ALL,
            AccessConstants.SELECT, AccessConstants.INSERT)) {
          tPrivilege.setAction(privilegeAction);
          dropPrivilegeForAllRoles(pm, new TSentryPrivilege(tPrivilege));
        }
      } else {
        dropPrivilegeForAllRoles(pm, new TSentryPrivilege(tPrivilege));
      }
      rollbackTransaction = false;
      commitTransaction(pm);
    } catch (JDODataStoreException e) {
      throw new SentryInvalidInputException("Failed to get privileges: "
          + e.getMessage());
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  /**
   * Rename given privilege from all roles drop the old privilege and create the new one
   * @param tAuthorizable
   * @param newTAuthorizable
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  public void renamePrivilege(TSentryAuthorizable tAuthorizable,
      TSentryAuthorizable newTAuthorizable)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;

    TSentryPrivilege tPrivilege = toSentryPrivilege(tAuthorizable);
    TSentryPrivilege newPrivilege = toSentryPrivilege(newTAuthorizable);

    try {
      pm = openTransaction();
      // In case of tables or DBs, check all actions
      if (isMultiActionsSupported(tPrivilege)) {
        for (String privilegeAction : Sets.newHashSet(AccessConstants.ALL,
            AccessConstants.SELECT, AccessConstants.INSERT)) {
          tPrivilege.setAction(privilegeAction);
          newPrivilege.setAction(privilegeAction);
          renamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
        }
      } else {
        renamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
      }
      rollbackTransaction = false;
      commitTransaction(pm);
    } catch (JDODataStoreException e) {
      throw new SentryInvalidInputException("Failed to get privileges: "
          + e.getMessage());
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
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

    MSentryPrivilege mPrivilege = getMSentryPrivilege(tPrivilege, pm);
    if (mPrivilege != null) {
      roleSet.addAll(ImmutableSet.copyOf((mPrivilege.getRoles())));
    }
    for (MSentryRole role : roleSet) {
      alterSentryRoleRevokePrivilegeCore(pm, role.getRoleName(), tPrivilege);
      if (newTPrivilege != null) {
        alterSentryRoleGrantPrivilegeCore(pm, role.getRoleName(), newTPrivilege);
      }
    }
  }

  private TSentryPrivilege toSentryPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryInvalidInputException {
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    tSentryPrivilege.setDbName(fromNULLCol(tAuthorizable.getDb()));
    tSentryPrivilege.setServerName(fromNULLCol(tAuthorizable.getServer()));
    tSentryPrivilege.setTableName(fromNULLCol(tAuthorizable.getTable()));
    tSentryPrivilege.setURI(fromNULLCol(tAuthorizable.getUri()));
    PrivilegeScope scope;
    if (!isNULL(tSentryPrivilege.getTableName())) {
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

  public static String toNULLCol(String s) {
	return Strings.isNullOrEmpty(s) ? NULL_COL : s;
  }

  public static String fromNULLCol(String s) {
	return isNULL(s) ? "" : s;
  }

  public static boolean isNULL(String s) {
	return Strings.isNullOrEmpty(s) || s.equals(NULL_COL);
  }

  /**
   * Grant option check
   * @param pm
   * @param privilege
   * @throws SentryUserException
   */
  private void grantOptionCheck(PersistenceManager pm, String grantorPrincipal, TSentryPrivilege privilege)
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
    if (admins != null && !admins.isEmpty()) {
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
      if (roles != null && !roles.isEmpty()) {
        for (MSentryRole role: roles) {
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
}
