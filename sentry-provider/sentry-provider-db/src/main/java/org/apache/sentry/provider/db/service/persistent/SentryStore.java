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

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.provider.common.ProviderConstants;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.model.MSentryVersion;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * SentryStore is the data access object for Sentry data. Strings
 * such as role and group names will be normalized to lowercase
 * in addition to starting and ending whitespace.
 */
public class SentryStore {
  private static final UUID SERVER_UUID = UUID.randomUUID();
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

  public SentryStore(Configuration conf) throws SentryNoSuchObjectException,
  SentryAccessDeniedException {
    commitSequenceId = 0;
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
   * @param grantorPrincipal: TODO: Currently not used
   * @returns commit context used for notification handlers
   * @throws SentryAlreadyExistsException
   */
  public CommitContext createSentryRole(String roleName, String grantorPrincipal)
      throws SentryAlreadyExistsException {
    roleName = trimAndLower(roleName);
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      MSentryRole mSentryRole = getMSentryRole(pm, roleName);
      if (mSentryRole == null) {
        MSentryRole mRole = new MSentryRole(roleName, System.currentTimeMillis(), grantorPrincipal);
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

  //TODO: handle case where a) privilege already exists, b) role to privilege mapping already exists
  public CommitContext alterSentryRoleGrantPrivilege(String roleName, TSentryPrivilege privilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = trimAndLower(roleName);
    try {
      pm = openTransaction();
      MSentryRole mRole = getMSentryRole(pm, roleName);
      if (mRole == null) {
        throw new SentryNoSuchObjectException("Role: " + roleName);
      } else {
        MSentryPrivilege mPrivilege = getMSentryPrivilege(constructPrivilegeName(privilege), pm);
        if (mPrivilege == null) {
          mPrivilege = convertToMSentryPrivilege(privilege);
        }
        mPrivilege.appendRole(mRole);
        pm.makePersistent(mRole);
        pm.makePersistent(mPrivilege);

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


  public CommitContext alterSentryRoleRevokePrivilege(String roleName,
      TSentryPrivilege tPrivilege) throws SentryNoSuchObjectException, SentryInvalidInputException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    roleName = roleName.trim().toLowerCase();
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("this.roleName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      MSentryRole mRole = (MSentryRole) query.execute(roleName);
      if (mRole == null) {
        throw new SentryNoSuchObjectException("Role: " + roleName);
      } else {
        query = pm.newQuery(MSentryPrivilege.class);
        MSentryPrivilege mPrivilege = getMSentryPrivilege(constructPrivilegeName(tPrivilege), pm);
        if (mPrivilege == null) {
          revokePartialPrivilege(pm, mRole, tPrivilege);
          CommitContext commit = commitUpdateTransaction(pm);
          rollbackTransaction = false;
          return commit;
        } else {
          // remove privilege and role objects from each other's set. needed by
          // datanucleus to model m:n relationships correctly through a join table.
          mRole.removePrivilege(mPrivilege);
          mPrivilege.removeRole(mRole);
          CommitContext commit = commitUpdateTransaction(pm);
          rollbackTransaction = false;
          return commit;

        }
      }
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  /**
   * Our privilege model at present only allows ALL on server and databases.
   * However, roles can be granted ALL, SELECT, and INSERT on tables. When
   * a role has ALL and SELECT or INSERT are revoked, we need to remove the ALL
   * privilege and add SELECT (INSERT was revoked) or INSERT (SELECT was revoked).
   */
  private void revokePartialPrivilege(PersistenceManager pm, MSentryRole role,
      TSentryPrivilege tPrivilege)
          throws SentryNoSuchObjectException, SentryInvalidInputException {
    // only perform partial revoke if INSERT/SELECT were the action
    // and the privilege being revoked is on a table
    String action = tPrivilege.getAction();
    if (AccessConstants.ALL.equalsIgnoreCase(action) ||
        StringUtils.isEmpty(tPrivilege.getDbName()) || StringUtils.isEmpty(tPrivilege.getTableName())) {
      throw new SentryNoSuchObjectException("Unknown privilege: " + tPrivilege);
    }
    TSentryPrivilege tPrivilegeAll = new TSentryPrivilege(tPrivilege);
    tPrivilegeAll.setAction(AccessConstants.ALL);
    String allPrivilegeName = constructPrivilegeName(tPrivilegeAll);
    MSentryPrivilege allPrivilege = getMSentryPrivilege(allPrivilegeName, pm);
    if (allPrivilege == null) {
      throw new SentryNoSuchObjectException("Unknown privilege: " + tPrivilege);
    }
    role.removePrivilege(allPrivilege);
    if (AccessConstants.SELECT.equalsIgnoreCase(action)) {
      tPrivilege.setAction(AccessConstants.INSERT);
    } else if (AccessConstants.INSERT.equalsIgnoreCase(action)) {
      tPrivilege.setAction(AccessConstants.SELECT);
    } else {
      throw new IllegalStateException("Unexpected action: " + action);
    }
    role.appendPrivilege(convertToMSentryPrivilege(tPrivilege));
  }

  private MSentryPrivilege getMSentryPrivilege(String privilegeName, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryPrivilege.class);
    query.setFilter("this.privilegeName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    Object obj = query.execute(privilegeName);
    if (obj != null)
      return (MSentryPrivilege) obj;
    return null;
  }

  //TODO:Validate privilege scope?
  @VisibleForTesting
  public static String constructPrivilegeName(TSentryPrivilege privilege) throws SentryInvalidInputException {
    StringBuilder privilegeName = new StringBuilder();
    String serverName = privilege.getServerName();
    String dbName = privilege.getDbName();
    String tableName = privilege.getTableName();
    String uri = privilege.getURI();
    String action = privilege.getAction();
    PrivilegeScope scope;

    if (serverName == null) {
      throw new SentryInvalidInputException("Server name is null");
    }

    if (AccessConstants.SELECT.equalsIgnoreCase(action) ||
        AccessConstants.INSERT.equalsIgnoreCase(action)) {
      if (Strings.nullToEmpty(tableName).trim().isEmpty()) {
        throw new SentryInvalidInputException("Table name can't be null for SELECT/INSERT privilege");
      }
    }

    // Validate privilege scope
    try {
      scope = Enum.valueOf(PrivilegeScope.class, privilege.getPrivilegeScope().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new SentryInvalidInputException("Invalid Privilege scope: " +
          privilege.getPrivilegeScope());
    }
    if (PrivilegeScope.SERVER.equals(scope)) {
      if (StringUtils.isNotEmpty(dbName) || StringUtils.isNotEmpty(tableName)) {
        throw new SentryInvalidInputException("DB and TABLE names should not be "
            + "set for SERVER scope");
      }
    } else if (PrivilegeScope.DATABASE.equals(scope)) {
      if (StringUtils.isEmpty(dbName)) {
        throw new SentryInvalidInputException("DB name not set for DB scope");
      }
      if (StringUtils.isNotEmpty(tableName)) {
        StringUtils.isNotEmpty("TABLE names should not be set for DB scope");
      }
    } else if (PrivilegeScope.TABLE.equals(scope)) {
      if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
        throw new SentryInvalidInputException("TABLE or DB name not set for TABLE scope");
      }
    } else if (PrivilegeScope.URI.equals(scope)){
      if (StringUtils.isEmpty(uri)) {
        throw new SentryInvalidInputException("URI path not set for URI scope");
      }
      if (StringUtils.isNotEmpty(tableName)) {
        throw new SentryInvalidInputException("TABLE should not be set for URI scope");
      }
    } else {
      throw new SentryInvalidInputException("Unsupported operation scope: " + scope);
    }

    if (uri == null || uri.equals("")) {
      privilegeName.append(serverName);
      privilegeName.append("+");
      privilegeName.append(dbName);

      if (tableName != null && !tableName.equals("")) {
        privilegeName.append("+");
        privilegeName.append(tableName);
      }
      privilegeName.append("+");
      privilegeName.append(action);
    } else {
      privilegeName.append(serverName);
      privilegeName.append("+");
      privilegeName.append(uri);
    }
    return privilegeName.toString();
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

  public CommitContext alterSentryRoleAddGroups(String grantorPrincipal,
      String roleName, Set<TSentryGroup> groupNames)
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
          String groupName = tGroup.getGroupName().trim().toLowerCase();
          MSentryGroup group = (MSentryGroup) query.execute(groupName);
          if (group == null) {
            group = new MSentryGroup(groupName, System.currentTimeMillis(),
                grantorPrincipal, Sets.newHashSet(role));
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
          String groupName = tGroup.getGroupName().trim().toLowerCase();
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
          filters.append(" && ((dbName == \"" + authHierarchy.getDb().toLowerCase() + "\") || (dbName == null)) && (URI == null)");
          if ((authHierarchy.getTable() != null)
              && !AccessConstants.ALL
                  .equalsIgnoreCase(authHierarchy.getTable())) {
            filters.append(" && ((tableName == \"" + authHierarchy.getTable().toLowerCase() + "\") || (tableName == null)) && (URI == null)");
          }
        }
        if (authHierarchy.getUri() != null) {
          filters.append(" && ((\"" + authHierarchy.getUri() + "\".startsWith(URI)) || (URI == null)) && (dbName == null)");
        }
      }
      System.out.println("Filter String: " + filters.toString());
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
   * @param roleName : roleName to look up
   * @param serverName : serverName (required)
   * @param uri : URI (optional)
   * @param dbName : dbName (optional if tableName is null else required)
   * @param tableName : tableName (optional)
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
        groupName = groupName.trim().toLowerCase();
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
  public Set<TSentryRole> getTSentryRolesByGroupName(String groupName)
      throws SentryNoSuchObjectException {
    return convertToTSentryRoles(getMSentryRolesByGroupName(groupName));
  }

  private SetMultimap<String, String> getRoleToPrivilegeMap(Set<String> groups) {
    SetMultimap<String, String> result = HashMultimap.create();
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryGroup.class);
      query.setFilter("this.groupName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      for (String group : toTrimedLower(groups)) {
        MSentryGroup sentryGroup = (MSentryGroup) query.execute(group);
        if (sentryGroup != null) {
          for (MSentryRole role : sentryGroup.getRoles()) {
            for (MSentryPrivilege privilege : role.getPrivileges()) {
              result.put(role.getRoleName(), toAuthorizable(privilege));
            }
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

  private Set<String> getRoleNamesForGroups(Set<String> groups) {
    Set<String> result = new HashSet<String>();
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryGroup.class);
      query.setFilter("this.groupName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      for (String group : toTrimedLower(groups)) {
        MSentryGroup sentryGroup = (MSentryGroup) query.execute(group);
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

  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups, TSentryActiveRoleSet roleSet) throws SentryInvalidInputException {
    return listSentryPrivilegesForProvider(groups, roleSet, null);
  }


  public Set<String> listSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    Set<String> result = Sets.newHashSet();
    Set<String> activeRoleNames = toTrimedLower(roleSet.getRoles());

    Set<String> roleNamesForGroups = toTrimedLower(getRoleNamesForGroups(groups));
    Set<String> rolesToQuery = roleSet.isAll() ? roleNamesForGroups : Sets.intersection(activeRoleNames, roleNamesForGroups);
    List<MSentryPrivilege> mSentryPrivileges = getMSentryPrivileges(rolesToQuery, authHierarchy);

    for (MSentryPrivilege priv : mSentryPrivileges) {
      result.add(toAuthorizable(priv));
    }
    return result;
  }

  @VisibleForTesting
  static String toAuthorizable(MSentryPrivilege privilege) {
    List<String> authorizable = new ArrayList<String>(4);
    authorizable.add(KV_JOINER.join(AuthorizableType.Server.name().toLowerCase(),
        privilege.getServerName()));
    if (Strings.nullToEmpty(privilege.getURI()).isEmpty()) {
      if (!Strings.nullToEmpty(privilege.getDbName()).isEmpty()) {
        authorizable.add(KV_JOINER.join(AuthorizableType.Db.name().toLowerCase(),
            privilege.getDbName()));
        if (!Strings.nullToEmpty(privilege.getTableName()).isEmpty()) {
          authorizable.add(KV_JOINER.join(AuthorizableType.Table.name().toLowerCase(),
              privilege.getTableName()));
        }
      }
    } else {
      authorizable.add(KV_JOINER.join(AuthorizableType.URI.name().toLowerCase(),
          privilege.getURI()));
    }
    if (!Strings.nullToEmpty(privilege.getAction()).isEmpty()
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
    role.setGrantorPrincipal(mSentryRole.getGrantorPrincipal());

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

  /**
   * Converts thrift object to model object. Additionally does normalization
   * such as trimming whitespace and setting appropriate case.
   * @throws SentryInvalidInputException
   */
  private MSentryPrivilege convertToMSentryPrivilege(TSentryPrivilege privilege)
      throws SentryInvalidInputException {
    MSentryPrivilege mSentryPrivilege = new MSentryPrivilege();
    mSentryPrivilege.setServerName(safeTrimLower(privilege.getServerName()));
    mSentryPrivilege.setDbName(safeTrimLower(privilege.getDbName()));
    mSentryPrivilege.setTableName(safeTrimLower(privilege.getTableName()));
    mSentryPrivilege.setPrivilegeScope(safeTrim(privilege.getPrivilegeScope()));
    mSentryPrivilege.setAction(safeTrim(privilege.getAction()));
    mSentryPrivilege.setCreateTime(System.currentTimeMillis());
    mSentryPrivilege.setGrantorPrincipal(safeTrim(privilege.getGrantorPrincipal()));
    mSentryPrivilege.setURI(safeTrim(privilege.getURI()));
    mSentryPrivilege.setPrivilegeName(constructPrivilegeName(privilege));
    return mSentryPrivilege;
  }
  private String safeTrim(String s) {
    if (s == null) {
      return null;
    }
    return s.trim();
  }
  private String safeTrimLower(String s) {
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
}
