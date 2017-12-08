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

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.core.common.utils.SentryConstants.KV_JOINER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.jdo.FetchGroup;
import javax.jdo.JDODataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryGrantDeniedException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.UniquePathsUpdate;
import org.apache.sentry.hdfs.UpdateableAuthzPaths;
import org.apache.sentry.provider.db.service.model.MAuthzPathsMapping;
import org.apache.sentry.provider.db.service.model.MAuthzPathsSnapshotId;
import org.apache.sentry.provider.db.service.model.MSentryChange;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryHmsNotification;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryUser;
import org.apache.sentry.provider.db.service.model.MSentryVersion;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.model.MSentryUtil;
import org.apache.sentry.provider.db.service.model.MPath;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryMappingData;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilegeMap;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.CounterWait;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.sentry.hdfs.Updateable.Update;
import static org.apache.sentry.provider.db.service.persistent.QueryParamBuilder.newQueryParamBuilder;

/**
 * SentryStore is the data access object for Sentry data. Strings
 * such as role and group names will be normalized to lowercase
 * in addition to starting and ending whitespace.
 * <p>
 * We have several places where we rely on transactions to support
 * read/modify/write semantics for incrementing IDs.
 * This works but using DB support is rather expensive and we can
 * user in-core serializations to help with this a least within a
 * single node and rely on DB for multi-node synchronization.
 * <p>
 * This isn't much of a problem for path updates since they are
 * driven by HMSFollower which usually runs on a single leader
 * node, but permission updates originate from clients
 * directly and may be highly concurrent.
 * <p>
 * We are internally serializing all permissions update anyway, so doing
 * partial serialization on every node helps. For this reason all
 * SentryStore calls that affect permission deltas are serialized.
 * <p>
 * See <a href="https://issues.apache.org/jira/browse/SENTRY-1824">SENTRY-1824</a>
 * for more detail.
 */
public class SentryStore {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentryStore.class);

  public static final String NULL_COL = "__NULL__";
  public static final int INDEX_GROUP_ROLES_MAP = 0;
  public static final int INDEX_USER_ROLES_MAP = 1;

  // String constants for field names
  public static final String SERVER_NAME = "serverName";
  public static final String DB_NAME = "dbName";
  public static final String TABLE_NAME = "tableName";
  public static final String COLUMN_NAME = "columnName";
  public static final String ACTION = "action";
  public static final String URI = "URI";
  public static final String GRANT_OPTION = "grantOption";
  public static final String ROLE_NAME = "roleName";

  // Initial change ID for permission/path change. Auto increment
  // is starting from 1.
  public static final long INIT_CHANGE_ID = 1L;

  private static final long EMPTY_CHANGE_ID = 0L;

  public static final long EMPTY_NOTIFICATION_ID = 0L;

  // Representation for empty HMS snapshots not found on MAuthzPathsSnapshotId
  public static final long EMPTY_PATHS_SNAPSHOT_ID = 0L;

  // For counters, representation of the "unknown value"
  private static final long COUNT_VALUE_UNKNOWN = -1L;

  // Representation for unknown HMS notification ID
  private static final long NOTIFICATION_UNKNOWN = -1L;

  private static final String EMPTY_GRANTOR_PRINCIPAL = "--";


  private static final Set<String> ALL_ACTIONS = Sets.newHashSet(AccessConstants.ALL,
      AccessConstants.SELECT, AccessConstants.INSERT, AccessConstants.ALTER,
      AccessConstants.CREATE, AccessConstants.DROP, AccessConstants.INDEX,
      AccessConstants.LOCK);

  // Now partial revoke just support action with SELECT,INSERT and ALL.
  // e.g. If we REVOKE SELECT from a privilege with action ALL, it will leads to INSERT
  // Otherwise, if we revoke other privilege(e.g. ALTER,DROP...), we will remove it from a role directly.
  private static final Set<String> PARTIAL_REVOKE_ACTIONS = Sets.newHashSet(AccessConstants.ALL,
      AccessConstants.ACTION_ALL.toLowerCase(), AccessConstants.SELECT, AccessConstants.INSERT);

  // Datanucleus property controlling whether query results are loaded at commit time
  // to make query usable post-commit
  private static final String LOAD_RESULTS_AT_COMMIT = "datanucleus.query.loadResultsAtCommit";

  private final PersistenceManagerFactory pmf;
  private Configuration conf;
  private final TransactionManager tm;

  // When it is true, execute DeltaTransactionBlock to persist delta changes.
  // When it is false, do not execute DeltaTransactionBlock
  private boolean persistUpdateDeltas;

  /**
   * counterWait is used to synchronize notifications between Thrift and HMSFollower.
   * Technically it doesn't belong here, but the only thing that connects HMSFollower
   * and Thrift API is SentryStore. An alternative could be a singleton CounterWait or
   * some factory that returns CounterWait instances keyed by name, but this complicates
   * things unnecessary.
   * <p>
   * Keeping it here isn't ideal but serves the purpose until we find a better home.
   */
  private final CounterWait counterWait;

  public static Properties getDataNucleusProperties(Configuration conf)
          throws SentrySiteConfigurationException, IOException {
    Properties prop = new Properties();
    prop.putAll(ServerConfig.SENTRY_STORE_DEFAULTS);
    String jdbcUrl = conf.get(ServerConfig.SENTRY_STORE_JDBC_URL, "").trim();
    Preconditions.checkArgument(!jdbcUrl.isEmpty(), "Required parameter " +
        ServerConfig.SENTRY_STORE_JDBC_URL + " is missed");
    String user = conf.get(ServerConfig.SENTRY_STORE_JDBC_USER, ServerConfig.
        SENTRY_STORE_JDBC_USER_DEFAULT).trim();
    //Password will be read from Credential provider specified using property
    // CREDENTIAL_PROVIDER_PATH("hadoop.security.credential.provider.path" in sentry-site.xml
    // it falls back to reading directly from sentry-site.xml
    char[] passTmp = conf.getPassword(ServerConfig.SENTRY_STORE_JDBC_PASS);
    if (passTmp == null) {
      throw new SentrySiteConfigurationException("Error reading " +
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
     * Oracle doesn't support "repeatable-read" isolation level and testing
     * showed issues with "serializable" isolation level for Oracle 12,
     * so we use "read-committed" instead.
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
      String[] parts = jdbcUrl.split(":");
      if ((parts.length > 1) && parts[1].equals(oracleDb)) {
        // For Oracle JDBC driver, replace "repeatable-read" with "read-committed"
        prop.setProperty(ServerConfig.DATANUCLEUS_ISOLATION_LEVEL,
                "read-committed");
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

    // Schema verification should be set to false only for testing.
    // If it is set to false, appropriate datanucleus properties will be set so that
    // database schema is automatically created. This is desirable only for running tests.
    // Sentry uses <code>SentrySchemaTool</code> to create schema with the help of sql scripts.

    if (!checkSchemaVersion) {
      prop.setProperty("datanucleus.schema.autoCreateAll", "true");
      prop.setProperty("datanucleus.autoCreateSchema", "true");
      prop.setProperty("datanucleus.fixedDatastore", "false");
    }
    pmf = JDOHelper.getPersistenceManagerFactory(prop);
    tm = new TransactionManager(pmf, conf);
    verifySentryStoreSchema(checkSchemaVersion);
    long notificationTimeout = conf.getInt(ServerConfig.SENTRY_NOTIFICATION_SYNC_TIMEOUT_MS,
            ServerConfig.SENTRY_NOTIFICATION_SYNC_TIMEOUT_DEFAULT);
    counterWait = new CounterWait(notificationTimeout, TimeUnit.MILLISECONDS);
  }

  public void setPersistUpdateDeltas(boolean persistUpdateDeltas) {
    this.persistUpdateDeltas = persistUpdateDeltas;
  }


  public TransactionManager getTransactionManager() {
    return tm;
  }

  public CounterWait getCounterWait() {
    return counterWait;
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
    if (pmf != null) {
      pmf.close();
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
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
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
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
    return (List<MSentryRole>) query.execute();
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
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
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
              pm.setDetachAllOnCommit(false); // No need to detach objects
              Query query = pm.newQuery();
              query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
              query.setClass(tClass);
              query.setResult("count(this)");
              Long result = (Long)query.execute();
              return result;
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
   * @return Number of users
   */
  Gauge<Long> getUserCountGauge() {
    return new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getCount(MSentryUser.class);
      }
    };
  }

  /**
   * @return number of threads waiting for HMS notifications to be processed
   */
  public Gauge<Integer> getHMSWaitersCountGauge() {
    return new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return counterWait.waitersCount();
      }
    };
  }

  /**
   * @return current value of last processed notification ID
   */
  public Gauge<Long> getLastNotificationIdGauge() {
    return new Gauge<Long>() {
      @Override
      public Long getValue() {
        try {
          return getLastProcessedNotificationID();
        } catch (Exception e) {
          LOGGER.error("Can not read current notificationId", e);
          return NOTIFICATION_UNKNOWN;
        }
      }
    };
  }

  /**
   * @return ID of the path snapshot
   */
  public Gauge<Long> getLastPathsSnapshotIdGauge() {
    return new Gauge<Long>() {
      @Override
      public Long getValue() {
        try {
          return getCurrentAuthzPathsSnapshotID();
        } catch (Exception e) {
          LOGGER.error("Can not read current paths snapshot ID", e);
          return NOTIFICATION_UNKNOWN;
        }
      }
    };
  }

  /**
   * @return Permissions change ID
   */
  public Gauge<Long> getPermChangeIdGauge() {
    return new Gauge<Long>() {
      @Override
      public Long getValue() {
        try {
          return tm.executeTransaction(
              new TransactionBlock<Long>() {
                @Override
                public Long execute(PersistenceManager pm) throws Exception {
                  return getLastProcessedChangeIDCore(pm, MSentryPermChange.class);
                }
              }
          );
        } catch (Exception e) {
          LOGGER.error("Can not read current permissions change ID", e);
          return NOTIFICATION_UNKNOWN;
        }
      }
    };
  }

  /**
   * @return Path change id
   */
  public Gauge<Long> getPathChangeIdGauge() {
    return new Gauge<Long>() {
      @Override
      public Long getValue() {
        try {
          return tm.executeTransaction(
              new TransactionBlock<Long>() {
                @Override
                public Long execute(PersistenceManager pm) throws Exception {
                  return getLastProcessedChangeIDCore(pm, MSentryPathChange.class);
                }
              }
          );
        } catch (Exception e) {
          LOGGER.error("Can not read current path change ID", e);
          return NOTIFICATION_UNKNOWN;
        }
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
          new TransactionBlock<Object>() {
            public Object execute(PersistenceManager pm) throws Exception {
              pm.newQuery(MSentryRole.class).deletePersistentAll();
              pm.newQuery(MSentryGroup.class).deletePersistentAll();
              pm.newQuery(MSentryUser.class).deletePersistentAll();
              pm.newQuery(MSentryPrivilege.class).deletePersistentAll();
              pm.newQuery(MSentryPermChange.class).deletePersistentAll();
              pm.newQuery(MSentryPathChange.class).deletePersistentAll();
              pm.newQuery(MAuthzPathsMapping.class).deletePersistentAll();
              pm.newQuery(MPath.class).deletePersistentAll();
              pm.newQuery(MSentryHmsNotification.class).deletePersistentAll();
              pm.newQuery(MAuthzPathsSnapshotId.class).deletePersistentAll();
              return null;
            }
          });
    } catch (Exception e) {
      // the method only for test, log the error and ignore the exception
      LOGGER.error(e.getMessage(), e);
    }
  }

  /**
   * Purge a given delta change table, with a specified number of changes to be kept.
   *
   * @param cls the class of a perm/path delta change {@link MSentryPermChange} or
   *            {@link MSentryPathChange}.
   * @param pm a {@link PersistenceManager} instance.
   * @param changesToKeep the number of changes the caller want to keep.
   * @param <T> the type of delta change class.
   */
  @VisibleForTesting
  <T extends MSentryChange> void purgeDeltaChangeTableCore(
      Class<T> cls, PersistenceManager pm, long changesToKeep) {
    Preconditions.checkArgument(changesToKeep >= 0,
        "changes to keep must be a non-negative number");
    long lastChangedID = getLastProcessedChangeIDCore(pm, cls);
    long maxIDDeleted = lastChangedID - changesToKeep;

    Query query = pm.newQuery(cls);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");

    // It is an approximation of "SELECT ... LIMIT CHANGE_TO_KEEP" in SQL, because JDO w/ derby
    // does not support "LIMIT".
    // See: http://www.datanucleus.org/products/datanucleus/jdo/jdoql_declarative.html
    query.setFilter("changeID <= maxChangedIdDeleted");
    query.declareParameters("long maxChangedIdDeleted");
    long numDeleted = query.deletePersistentAll(maxIDDeleted);
    if (numDeleted > 0) {
      LOGGER.info(String.format("Purged %d of %s to changeID=%d",
              numDeleted, cls.getSimpleName(), maxIDDeleted));
    }
  }

  /**
   * Purge notification id table, keeping a specified number of entries.
   * @param pm a {@link PersistenceManager} instance.
   * @param changesToKeep  the number of changes the caller want to keep.
   */
  @VisibleForTesting
  protected void purgeNotificationIdTableCore(PersistenceManager pm,
      long changesToKeep) {
    Preconditions.checkArgument(changesToKeep > 0,
      "You need to keep at least one entry in SENTRY_HMS_NOTIFICATION_ID table");
    long lastNotificationID = getLastProcessedNotificationIDCore(pm);
    Query query = pm.newQuery(MSentryHmsNotification.class);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");

    // It is an approximation of "SELECT ... LIMIT CHANGE_TO_KEEP" in SQL, because JDO w/ derby
    // does not support "LIMIT".
    // See: http://www.datanucleus.org/products/datanucleus/jdo/jdoql_declarative.html
    query.setFilter("notificationId <= maxNotificationIdDeleted");
    query.declareParameters("long maxNotificationIdDeleted");
    long numDeleted = query.deletePersistentAll(lastNotificationID - changesToKeep);
    if (numDeleted > 0) {
      LOGGER.info("Purged {} of {}", numDeleted, MSentryHmsNotification.class.getSimpleName());
    }
  }

  /**
   * Purge delta change tables, {@link MSentryPermChange} and {@link MSentryPathChange}.
   * The number of deltas to keep is configurable
   */
  public void purgeDeltaChangeTables() {
    final int changesToKeep = conf.getInt(ServerConfig.SENTRY_DELTA_KEEP_COUNT,
            ServerConfig.SENTRY_DELTA_KEEP_COUNT_DEFAULT);
    LOGGER.info("Purging MSentryPathUpdate and MSentyPermUpdate tables, leaving {} entries",
            changesToKeep);
    try {
      tm.executeTransaction(new TransactionBlock<Object>() {
        @Override
        public Object execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          purgeDeltaChangeTableCore(MSentryPermChange.class, pm, changesToKeep);
          LOGGER.info("MSentryPermChange table has been purged.");
          purgeDeltaChangeTableCore(MSentryPathChange.class, pm, changesToKeep);
          LOGGER.info("MSentryPathUpdate table has been purged.");
          return null;
        }
      });
    } catch (Exception e) {
      LOGGER.error("Delta change cleaning process encountered an error", e);
    }
  }

  /**
   * Purge hms notification id table , {@link MSentryHmsNotification}.
   * The number of notifications id's to be kept is based on configuration
   * sentry.server.delta.keep.count
   */
  public void purgeNotificationIdTable() {
    final int changesToKeep = conf.getInt(ServerConfig.SENTRY_HMS_NOTIFICATION_ID_KEEP_COUNT,
      ServerConfig.SENTRY_HMS_NOTIFICATION_ID_KEEP_COUNT_DEFAULT);
    LOGGER.debug("Purging MSentryHmsNotification table, leaving {} entries",
      changesToKeep);
    try {
      tm.executeTransaction(new TransactionBlock<Object>() {
        @Override
        public Object execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          purgeNotificationIdTableCore(pm, changesToKeep);
          return null;
        }
      });
    } catch (Exception e) {
      LOGGER.error("MSentryHmsNotification cleaning process encountered an error", e);
    }
  }
  /**
   * Alter a given sentry role to grant a privilege.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param privilege the given privilege
   * @throws Exception
   */
  void alterSentryRoleGrantPrivilege(final String grantorPrincipal,
      final String roleName, final TSentryPrivilege privilege) throws Exception {
    tm.executeTransactionWithRetry(
      new TransactionBlock<Object>() {
        public Object execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          String trimmedRoleName = trimAndLower(roleName);
          // first do grant check
          grantOptionCheck(pm, grantorPrincipal, privilege);

          // Alter sentry Role and grant Privilege.
          MSentryPrivilege mPrivilege = alterSentryRoleGrantPrivilegeCore(
            pm, trimmedRoleName, privilege);

          if (mPrivilege != null) {
            // update the privilege to be the one actually updated.
            convertToTSentryPrivilege(mPrivilege, privilege);
          }
          return null;
        }
      });
  }

  /**
   * Alter a given sentry role to grant a set of privileges.
   * Internally calls alterSentryRoleGrantPrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName Role name
   * @param privileges Set of privileges
   * @throws Exception
   */
  public void alterSentryRoleGrantPrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> privileges) throws Exception {
    for (TSentryPrivilege privilege : privileges) {
      alterSentryRoleGrantPrivilege(grantorPrincipal, roleName, privilege);
    }
  }

  /**
   * Alter a given sentry role to grant a privilege, as well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param privilege the given privilege
   * @param update the corresponding permission delta update.
   * @throws Exception
   *
   */
  synchronized void alterSentryRoleGrantPrivilege(final String grantorPrincipal,
      final String roleName, final TSentryPrivilege privilege,
      final Update update) throws Exception {

    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        String trimmedRoleName = trimAndLower(roleName);
        // first do grant check
        grantOptionCheck(pm, grantorPrincipal, privilege);

        // Alter sentry Role and grant Privilege.
        MSentryPrivilege mPrivilege = alterSentryRoleGrantPrivilegeCore(pm,
          trimmedRoleName, privilege);

        if (mPrivilege != null) {
          // update the privilege to be the one actually updated.
          convertToTSentryPrivilege(mPrivilege, privilege);
        }
        return null;
      }
    });
  }

  /**
   * Alter a given sentry role to grant a set of privileges, as well as persist the
   * corresponding permission change to MSentryPermChange table in a single transaction.
   * Internally calls alterSentryRoleGrantPrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param privileges a Set of privileges
   * @param privilegesUpdateMap the corresponding <privilege, DeltaTransactionBlock> map
   * @throws Exception
   *
   */
  public void alterSentryRoleGrantPrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> privileges,
      final Map<TSentryPrivilege, Update> privilegesUpdateMap) throws Exception {

    Preconditions.checkNotNull(privilegesUpdateMap);
    for (TSentryPrivilege privilege : privileges) {
      Update update = privilegesUpdateMap.get(privilege);
      if (update != null) {
        alterSentryRoleGrantPrivilege(grantorPrincipal, roleName, privilege,
          update);
      } else {
        alterSentryRoleGrantPrivilege(grantorPrincipal, roleName, privilege);
      }
    }
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
        if (AccessConstants.ALL.equalsIgnoreCase(privilege.getAction())
            || AccessConstants.ACTION_ALL.equalsIgnoreCase(privilege.getAction())) {
          TSentryPrivilege tNotAll = new TSentryPrivilege(privilege);
          tNotAll.setAction(AccessConstants.SELECT);
          MSentryPrivilege mSelect = getMSentryPrivilege(tNotAll, pm);
          tNotAll.setAction(AccessConstants.INSERT);
          MSentryPrivilege mInsert = getMSentryPrivilege(tNotAll, pm);
          if ((mSelect != null) && mRole.getPrivileges().contains(mSelect)) {
            mSelect.removeRole(mRole);
            pm.makePersistent(mSelect);
          }
          if ((mInsert != null) && mRole.getPrivileges().contains(mInsert)) {
            mInsert.removeRole(mRole);
            pm.makePersistent(mInsert);
          }
        } else {
          // If Grant is for Either INSERT/SELECT and ALL already exists..
          // do nothing..
          TSentryPrivilege tAll = new TSentryPrivilege(privilege);
          tAll.setAction(AccessConstants.ALL);
          MSentryPrivilege mAll1 = getMSentryPrivilege(tAll, pm);
          tAll.setAction(AccessConstants.ACTION_ALL);
          MSentryPrivilege mAll2 = getMSentryPrivilege(tAll, pm);
          if (mAll1 != null && mRole.getPrivileges().contains(mAll1)) {
            return null;
          }
          if (mAll2 != null && mRole.getPrivileges().contains(mAll2)) {
            return null;
          }
        }
      }

      mPrivilege = getMSentryPrivilege(privilege, pm);
      if (mPrivilege == null) {
        mPrivilege = convertToMSentryPrivilege(privilege);
      }
      mPrivilege.appendRole(mRole);
      pm.makePersistent(mPrivilege);
    }
    return mPrivilege;
  }

  /**
  * Alter a given sentry role to revoke a privilege.
  *
  * @param grantorPrincipal User name
  * @param roleName the given role name
  * @param tPrivilege the given privilege
  * @throws Exception
  *
  */
  void alterSentryRoleRevokePrivilege(final String grantorPrincipal,
      final String roleName, final TSentryPrivilege tPrivilege) throws Exception {

    tm.executeTransactionWithRetry(
      new TransactionBlock<Object>() {
        public Object execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          String trimmedRoleName = safeTrimLower(roleName);
          // first do revoke check
          grantOptionCheck(pm, grantorPrincipal, tPrivilege);

          alterSentryRoleRevokePrivilegeCore(pm, trimmedRoleName, tPrivilege);
          return null;
        }
      });
  }

  /**
   * Alter a given sentry role to revoke a set of privileges.
   * Internally calls alterSentryRoleRevokePrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param tPrivileges a Set of privileges
   * @throws Exception
   *
   */
  public void alterSentryRoleRevokePrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> tPrivileges) throws Exception {
    for (TSentryPrivilege tPrivilege : tPrivileges) {
      alterSentryRoleRevokePrivilege(grantorPrincipal, roleName, tPrivilege);
    }
  }

  /**
   * Alter a given sentry role to revoke a privilege, as well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param tPrivilege the given privilege
   * @param update the corresponding permission delta update transaction block
   * @throws Exception
   *
   */
  private synchronized void alterSentryRoleRevokePrivilege(final String grantorPrincipal,
                                              final String roleName, final TSentryPrivilege tPrivilege,
                                              final Update update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        String trimmedRoleName = safeTrimLower(roleName);
        // first do revoke check
        grantOptionCheck(pm, grantorPrincipal, tPrivilege);

        alterSentryRoleRevokePrivilegeCore(pm, trimmedRoleName, tPrivilege);
        return null;
      }
    });
  }

  /**
   * Alter a given sentry role to revoke a set of privileges, as well as persist the
   * corresponding permission change to MSentryPermChange table in a single transaction.
   * Internally calls alterSentryRoleRevokePrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param tPrivileges a Set of privileges
   * @param privilegesUpdateMap the corresponding <privilege, Update> map
   * @throws Exception
   *
   */
  public void alterSentryRoleRevokePrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> tPrivileges,
      final Map<TSentryPrivilege, Update> privilegesUpdateMap)
          throws Exception {

    Preconditions.checkNotNull(privilegesUpdateMap);
    for (TSentryPrivilege tPrivilege : tPrivileges) {
      Update update = privilegesUpdateMap.get(tPrivilege);
      if (update != null) {
        alterSentryRoleRevokePrivilege(grantorPrincipal, roleName,
          tPrivilege, update);
      } else {
        alterSentryRoleRevokePrivilege(grantorPrincipal, roleName,
          tPrivilege);
      }
    }
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
    MSentryPrivilege persistedPriv =
      getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
    if (persistedPriv == null) {
      // The privilege corresponding to the currentPrivilege doesn't exist in the persistent
      // store, so we create a fake one for the code below. The fake one is not associated with
      // any role and shouldn't be stored in the persistent storage.
      persistedPriv = convertToMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege));
    }

    if (requestedPrivToRevoke.getAction().equalsIgnoreCase("ALL") ||
      requestedPrivToRevoke.getAction().equalsIgnoreCase("*")) {
      if (!persistedPriv.getRoles().isEmpty()) {
        persistedPriv.removeRole(mRole);
        if (persistedPriv.getRoles().isEmpty()) {
          pm.deletePersistent(persistedPriv);
        } else {
          pm.makePersistent(persistedPriv);
        }
      }
    } else if (requestedPrivToRevoke.getAction().equalsIgnoreCase(AccessConstants.SELECT)
      && !currentPrivilege.getAction().equalsIgnoreCase(AccessConstants.INSERT)) {
      revokeRolePartial(pm, mRole, currentPrivilege, persistedPriv, AccessConstants.INSERT);
    } else if (requestedPrivToRevoke.getAction().equalsIgnoreCase(AccessConstants.INSERT)
      && !currentPrivilege.getAction().equalsIgnoreCase(AccessConstants.SELECT)) {
      revokeRolePartial(pm, mRole, currentPrivilege, persistedPriv, AccessConstants.SELECT);
    }
  }

  private void revokeRolePartial(PersistenceManager pm, MSentryRole mRole,
                                 MSentryPrivilege currentPrivilege,
                                 MSentryPrivilege persistedPriv,
                                 String addAction) throws SentryInvalidInputException {
    // If table / URI, remove ALL
    persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(persistedPriv), pm);
    if (persistedPriv != null && !persistedPriv.getRoles().isEmpty()) {
      persistedPriv.removeRole(mRole);
      if (persistedPriv.getRoles().isEmpty()) {
        pm.deletePersistent(persistedPriv);
      } else {
        pm.makePersistent(persistedPriv);
      }
    }
    currentPrivilege.setAction(AccessConstants.ALL);
    persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
    if (persistedPriv != null && mRole.getPrivileges().contains(persistedPriv)) {
      persistedPriv.removeRole(mRole);
      if (persistedPriv.getRoles().isEmpty()) {
        pm.deletePersistent(persistedPriv);
      } else {
        pm.makePersistent(persistedPriv);
      }
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
                                       MSentryRole mRole, MSentryPrivilege mPrivilege)
    throws SentryInvalidInputException {
    if (PARTIAL_REVOKE_ACTIONS.contains(mPrivilege.getAction())) {
      // if this privilege is in {ALL,SELECT,INSERT}
      // we will do partial revoke
      revokePartial(pm, tPrivilege, mRole, mPrivilege);
    } else {
      // if this privilege is not ALL, SELECT nor INSERT,
      // we will revoke it from role directly
      MSentryPrivilege persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(mPrivilege), pm);
      if (persistedPriv != null && !persistedPriv.getRoles().isEmpty()) {
        persistedPriv.removeRole(mRole);
        if (persistedPriv.getRoles().isEmpty()) {
          pm.deletePersistent(persistedPriv);
        } else {
          pm.makePersistent(persistedPriv);
        }
      }
    }
  }

  /**
   * Explore Privilege graph and collect child privileges.
   * The responsibility to commit/rollback the transaction should be handled by the caller.
   */
  private void populateChildren(PersistenceManager pm, Set<String> roleNames, MSentryPrivilege priv,
      Collection<MSentryPrivilege> children) throws SentryInvalidInputException {
    Preconditions.checkNotNull(pm);
    if (!isNULL(priv.getServerName()) || !isNULL(priv.getDbName())
        || !isNULL(priv.getTableName())) {
      // Get all TableLevel Privs
      Set<MSentryPrivilege> childPrivs = getChildPrivileges(pm, roleNames, priv);
      for (MSentryPrivilege childPriv : childPrivs) {
        // Only recurse for table level privs..
        if (!isNULL(childPriv.getDbName()) && !isNULL(childPriv.getTableName())
            && !isNULL(childPriv.getColumnName())) {
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
              new MSentryPrivilege(scope, serverName, dbName, tableName,
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
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            dropSentryRoleCore(pm, roleName);
            return null;
          }
        });
  }

  /**
   * Drop a given sentry role. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param roleName the given role name
   * @param update the corresponding permission delta update
   * @throws Exception
   */
  public synchronized void dropSentryRole(final String roleName,
      final Update update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
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
    removePrivileges(pm, sentryRole);
    pm.deletePersistent(sentryRole);
  }

  /**
   * Removes all the privileges associated with
   * a particular role. After this dis-association if the
   * privilege doesn't have any roles associated it will be
   * removed from the underlying persistence layer.
   * @param pm Instance of PersistenceManager
   * @param sentryRole Role for which all the privileges are to be removed.
   */
  private void removePrivileges(PersistenceManager pm, MSentryRole sentryRole) {
    List<MSentryPrivilege> privilegesCopy = new ArrayList<>(sentryRole.getPrivileges());
    List<MSentryPrivilege> stalePrivileges = new ArrayList<>(0);

    sentryRole.removePrivileges();
    // with SENTRY-398 generic model
    sentryRole.removeGMPrivileges();
    for (MSentryPrivilege privilege : privilegesCopy) {
      if(privilege.getRoles().isEmpty()) {
        stalePrivileges.add(privilege);
      }
    }
    if(!stalePrivileges.isEmpty()) {
      pm.deletePersistentAll(stalePrivileges);
    }
  }
  /**
   * Assign a given role to a set of groups.
   *
   * @param grantorPrincipal grantorPrincipal currently is not used.
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @throws Exception
   */
  public void alterSentryRoleAddGroups(final String grantorPrincipal,
      final String roleName, final Set<TSentryGroup> groupNames) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            alterSentryRoleAddGroupsCore(pm, roleName, groupNames);
            return null;
          }
        });
  }

  /**
   * Assign a given role to a set of groups. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param grantorPrincipal grantorPrincipal currently is not used.
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @param update the corresponding permission delta update
   * @throws Exception
   */
  public synchronized void alterSentryRoleAddGroups(final String grantorPrincipal,
      final String roleName, final Set<TSentryGroup> groupNames,
      final Update update) throws Exception {

    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        alterSentryRoleAddGroupsCore(pm, roleName, groupNames);
        return null;
      }
    });
  }

  private void alterSentryRoleAddGroupsCore(PersistenceManager pm, String roleName,
      Set<TSentryGroup> groupNames) throws SentryNoSuchObjectException {

    // All role names are stored in lowercase.
    String lRoleName = trimAndLower(roleName);
    MSentryRole role = getRole(pm, lRoleName);
    if (role == null) {
      throw noSuchRole(lRoleName);
    }

    // Add the group to the specified role if it does not belong to the role yet.
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

  public void alterSentryRoleAddUsers(final String roleName,
      final Set<String> userNames) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            alterSentryRoleAddUsersCore(pm, roleName, userNames);
            return null;
          }
        });
  }

  private void alterSentryRoleAddUsersCore(PersistenceManager pm, String roleName,
      Set<String> userNames) throws SentryNoSuchObjectException {
    String trimmedRoleName = trimAndLower(roleName);
    MSentryRole role = getRole(pm, trimmedRoleName);
    if (role == null) {
      throw noSuchRole(trimmedRoleName);
    }
    Query query = pm.newQuery(MSentryUser.class);
    query.setFilter("this.userName == :userName");
    query.setUnique(true);
    List<MSentryUser> users = Lists.newArrayList();
    for (String userName : userNames) {
      userName = userName.trim();
      MSentryUser user = (MSentryUser) query.execute(userName);
      if (user == null) {
        user = new MSentryUser(userName, System.currentTimeMillis(), Sets.newHashSet(role));
      }
      user.appendRole(role);
      users.add(user);
    }
    pm.makePersistentAll(users);
  }

  public void alterSentryRoleDeleteUsers(final String roleName,
      final Set<String> userNames) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            String trimmedRoleName = trimAndLower(roleName);
            MSentryRole role = getRole(pm, trimmedRoleName);
            if (role == null) {
              throw noSuchRole(trimmedRoleName);
            } else {
              Query query = pm.newQuery(MSentryUser.class);
              query.setFilter("this.userName == :userName");
              query.setUnique(true);
              List<MSentryUser> users = Lists.newArrayList();
              for (String userName : userNames) {
                userName = userName.trim();
                MSentryUser user = (MSentryUser) query.execute(userName);
                if (user != null) {
                  user.removeRole(role);
                  users.add(user);
                }
              }
              pm.makePersistentAll(users);
            }
            return null;
          }
        });
  }

  /**
   * Revoke a given role to a set of groups.
   *
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @throws Exception
   */
  public void alterSentryRoleDeleteGroups(final String roleName,
      final Set<TSentryGroup> groupNames) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
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

  /**
   * Revoke a given role to a set of groups. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @param update the corresponding permission delta update
   * @throws Exception
   */
  public synchronized void alterSentryRoleDeleteGroups(final String roleName,
      final Set<TSentryGroup> groupNames, final Update update)
          throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        String trimmedRoleName = trimAndLower(roleName);
        MSentryRole role = getRole(pm, trimmedRoleName);
        if (role == null) {
          throw noSuchRole(trimmedRoleName);
        }

        // Remove the group from the specified role if it belongs to the role.
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
            return sentryRole;
          }
        });
  }

  /**
   * Gets the MSentryPrivilege from sentry persistent storage based on TSentryPrivilege
   * provided
   *
   * Method is currently used only in test framework
   * @param tPrivilege
   * @return MSentryPrivilege if the privilege is found in the storage
   * null, if the privilege is not found in the storage.
   * @throws Exception
   */
  @VisibleForTesting
  MSentryPrivilege findMSentryPrivilegeFromTSentryPrivilege(final TSentryPrivilege tPrivilege) throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<MSentryPrivilege>() {
        public MSentryPrivilege execute(PersistenceManager pm) throws Exception {
          return getMSentryPrivilege(tPrivilege, pm);
        }
      });
  }

  /**
   * Returns a list with all the privileges in the sentry persistent storage
   *
   * Method is currently used only in test framework
   * @return List of all sentry privileges in the store
   * @throws Exception
   */
  @VisibleForTesting
  List<MSentryPrivilege> getAllMSentryPrivileges () throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<List<MSentryPrivilege>>() {
        public List<MSentryPrivilege> execute(PersistenceManager pm) throws Exception {
          return getAllMSentryPrivilegesCore(pm);
        }
      });
  }

  /**
   * Method Returns all the privileges present in the persistent store as a list.
   * @param pm PersistenceManager
   * @returns list of all the privileges in the persistent store
   */
  private List<MSentryPrivilege> getAllMSentryPrivilegesCore (PersistenceManager pm) {
    Query query = pm.newQuery(MSentryPrivilege.class);
    return (List<MSentryPrivilege>) query.execute();
  }

  private boolean hasAnyServerPrivileges(final Set<String> roleNames, final String serverName) throws Exception {
    if (roleNames == null || roleNames.isEmpty()) {
      return false;
    }
    return tm.executeTransaction(
      new TransactionBlock<Boolean>() {
        public Boolean execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          Query query = pm.newQuery(MSentryPrivilege.class);
          query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
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
                                                      final TSentryAuthorizable
                                                              authHierarchy)
          throws Exception {
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

          query.setFilter(paramBuilder.toString());
          @SuppressWarnings("unchecked")
          List<MSentryPrivilege> result =
                  (List<MSentryPrivilege>)
                          query.executeWithMap(paramBuilder.getArguments());
          return result;
        }
      });
  }

  private List<MSentryPrivilege> getMSentryPrivilegesByAuth(final Set<String> roleNames,
                                                            final TSentryAuthorizable
                                                                    authHierarchy) throws Exception {
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
              // if no server, then return empty result
              return Collections.emptyList();
            }
            FetchGroup grp = pm.getFetchGroup(MSentryPrivilege.class, "fetchRole");
            grp.addMember("roles");
            pm.getFetchPlan().addGroup("fetchRole");
            query.setFilter(paramBuilder.toString());
            @SuppressWarnings("unchecked")
            List<MSentryPrivilege> result = (List<MSentryPrivilege>)query.
                    executeWithMap(paramBuilder.getArguments());
            return result;
          }
        });
  }

  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(Set<String> groups,
      TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable authHierarchy, boolean isAdmin)
          throws Exception {
    Map<String, Set<TSentryPrivilege>> resultPrivilegeMap = Maps.newTreeMap();
    Set<String> roles = getRolesToQuery(groups, null, new TSentryActiveRoleSet(true, null));

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
    if (authHierarchy.getTable() != null && authHierarchy.getDb() == null) {
      throw new SentryInvalidInputException("dbName cannot be null when tableName is present !!");
    }
    if (authHierarchy.getColumn() != null && authHierarchy.getTable() == null) {
      throw new SentryInvalidInputException("tableName cannot be null when columnName is present !!");
    }
    if (authHierarchy.getUri() == null && authHierarchy.getDb() == null) {
      throw new SentryInvalidInputException("One of uri or dbName must not be null !!");
    }
    return convertToTSentryPrivileges(getMSentryPrivileges(roleNames, authHierarchy));
  }

  /**
   * Return set of roles corresponding to the groups provided.<p>
   *
   * If groups contain a null group, return all available roles.<p>
   *
   * Everything is done in a single transaction so callers get a
   * fully-consistent view of the roles, so this can be called at the same tie as
   * some other method that modifies groups or roles.<p>
   *
   * <em><b>NOTE:</b> This function is performance-critical, so before you modify it, make
   * sure to measure performance effect. It is called every time when PolicyClient
   * (Hive or Impala) tries to get list of roles.
   * </em>
   *
   * @param groupNames Set of Sentry groups. Can contain {@code null}
   *                  in which case all roles should be returned
   * @param checkAllGroups If false, raise SentryNoSuchObjectException
   *                      if one of the groups is not available, otherwise
   *                      ignore non-existent groups
   * @return Set of TSentryRole toles corresponding to the given set of groups.
   * @throws SentryNoSuchObjectException if one of the groups is not present and
   * checkAllGroups is not set.
   * @throws Exception if DataNucleus operation fails.
   */
  public Set<TSentryRole> getTSentryRolesByGroupName(final Set<String> groupNames,
                                                     final boolean checkAllGroups) throws Exception {
    if (groupNames.isEmpty()) {
      return Collections.emptySet();
    }

    return tm.executeTransaction(
            new TransactionBlock<Set<TSentryRole>>() {
              @Override
              public Set<TSentryRole> execute(PersistenceManager pm) throws Exception {

                pm.setDetachAllOnCommit(false); // No need to detach objects

                // Pre-allocate large sets for role names and results.
                // roleNames is used to avoid adding the same role mutiple times into
                // result. The result is set, but comparisons between TSentryRole objects
                // is more expensive then String comparisons.
                Set<String> roleNames = new HashSet<>(1024);
                Set<TSentryRole> result = new HashSet<>(1024);

                for(String group: groupNames) {
                  if (group == null) {
                    // Special case - return all roles
                    List<MSentryRole> roles = getAllRoles(pm);
                    for (MSentryRole role: roles) {
                      result.add(convertToTSentryRole(role));
                    }
                    return result;
                  }

                  // Find group by name and all roles belonging to this group
                  String trimmedGroup = group.trim();
                  Query query = pm.newQuery(MSentryGroup.class);
                  query.setFilter("this.groupName == :groupName");
                  query.setUnique(true);
                  MSentryGroup mGroup = (MSentryGroup) query.execute(trimmedGroup);
                  if (mGroup != null) {
                    // For each unique role found, add a new TSentryRole version of the role to result.
                    for (MSentryRole role: mGroup.getRoles()) {
                      String roleName = role.getRoleName();
                      if (roleNames.add(roleName)) {
                        result.add(convertToTSentryRole(role));
                      }
                    }
                  } else if (!checkAllGroups) {
                      throw noSuchGroup(trimmedGroup);
                  }
                  query.closeAll();
                }
                return result;
              }
            });
  }

  public Set<String> getRoleNamesForGroups(final Set<String> groups) throws Exception {
    if ((groups == null) || groups.isEmpty()) {
      return ImmutableSet.of();
    }

    return tm.executeTransaction(
        new TransactionBlock<Set<String>>() {
          public Set<String>execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            return getRoleNamesForGroupsCore(pm, groups);
          }
        });
  }

  private Set<String> getRoleNamesForGroupsCore(PersistenceManager pm, Set<String> groups) {
    return convertToRoleNameSet(getRolesForGroups(pm, groups));
  }

  public Set<String> getRoleNamesForUsers(final Set<String> users) throws Exception {
    if ((users == null) || users.isEmpty()) {
      return ImmutableSet.of();
    }

    return tm.executeTransaction(
          new TransactionBlock<Set<String>>() {
            public Set<String> execute(PersistenceManager pm) throws Exception {
              pm.setDetachAllOnCommit(false); // No need to detach objects
              return getRoleNamesForUsersCore(pm,users);
            }
          });
  }

  private Set<String> getRoleNamesForUsersCore(PersistenceManager pm, Set<String> users) {
    return convertToRoleNameSet(getRolesForUsers(pm, users));
  }

  public Set<TSentryRole> getTSentryRolesByUserNames(final Set<String> users)
          throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<Set<TSentryRole>>() {
      public Set<TSentryRole> execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        Set<MSentryRole> mSentryRoles = getRolesForUsers(pm, users);
        // Since {@link MSentryRole#getGroups()} is lazy-loading,
        // the conversion should be done before transaction is committed.
        return convertToTSentryRoles(mSentryRoles);
        }
      });
  }

  public Set<MSentryRole> getRolesForGroups(PersistenceManager pm, Set<String> groups) {
    Set<MSentryRole> result = Sets.newHashSet();
    if (groups != null) {
      Query query = pm.newQuery(MSentryGroup.class);
      query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
      query.setFilter(":p1.contains(this.groupName)");
      List<MSentryGroup> sentryGroups = (List) query.execute(groups.toArray());
      if (sentryGroups != null) {
        for (MSentryGroup sentryGroup : sentryGroups) {
          result.addAll(sentryGroup.getRoles());
        }
      }
    }
    return result;
  }

  private Set<MSentryRole> getRolesForUsers(PersistenceManager pm, Set<String> users) {
    Set<MSentryRole> result = Sets.newHashSet();
    if (users != null) {
      Query query = pm.newQuery(MSentryUser.class);
      query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
      query.setFilter(":p1.contains(this.userName)");
      List<MSentryUser> sentryUsers = (List) query.execute(users.toArray());
      if (sentryUsers != null) {
        for (MSentryUser sentryUser : sentryUsers) {
          result.addAll(sentryUser.getRoles());
        }
      }
    }
    return result;
  }

  Set<String> listAllSentryPrivilegesForProvider(Set<String> groups, Set<String> users,
      TSentryActiveRoleSet roleSet) throws Exception {
    return listSentryPrivilegesForProvider(groups, users, roleSet, null);
  }


  public Set<String> listSentryPrivilegesForProvider(Set<String> groups, Set<String> users,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy) throws Exception {
    Set<String> result = Sets.newHashSet();
    Set<String> rolesToQuery = getRolesToQuery(groups, users, roleSet);
    List<MSentryPrivilege> mSentryPrivileges = getMSentryPrivileges(rolesToQuery, authHierarchy);
    for (MSentryPrivilege priv : mSentryPrivileges) {
      result.add(toAuthorizable(priv));
    }

    return result;
  }

  public boolean hasAnyServerPrivileges(Set<String> groups, Set<String> users,
      TSentryActiveRoleSet roleSet, String server) throws Exception {
    Set<String> rolesToQuery = getRolesToQuery(groups, users, roleSet);
    return hasAnyServerPrivileges(rolesToQuery, server);
  }

  private Set<String> getRolesToQuery(final Set<String> groups, final Set<String> users,
      final TSentryActiveRoleSet roleSet) throws Exception {
      return tm.executeTransaction(
        new TransactionBlock<Set<String>>() {
          public Set<String> execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            Set<String> activeRoleNames = toTrimedLower(roleSet.getRoles());

            Set<String> roleNames = Sets.newHashSet();
            roleNames.addAll(toTrimedLower(getRoleNamesForGroupsCore(pm, groups)));
            roleNames.addAll(toTrimedLower(getRoleNamesForUsersCore(pm, users)));
            return roleSet.isAll() ? roleNames : Sets.intersection(activeRoleNames,
                roleNames);
          }
        });
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
      .add(KV_JOINER.join(SentryConstants.PRIVILEGE_NAME.toLowerCase(),
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

  private Set<String> convertToRoleNameSet(Set<MSentryRole> mSentryRoles) {
    if (mSentryRoles.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> roleNameSet = new HashSet<>(mSentryRoles.size());
    for (MSentryRole role : mSentryRoles) {
      roleNameSet.add(role.getRoleName());
    }
    return roleNameSet;
  }

  private TSentryRole convertToTSentryRole(MSentryRole mSentryRole) {
    String roleName = mSentryRole.getRoleName().intern();
    Set<MSentryGroup> groups = mSentryRole.getGroups();
    Set<TSentryGroup> sentryGroups = new HashSet<>(groups.size());
    for(MSentryGroup mSentryGroup: groups) {
      TSentryGroup group = convertToTSentryGroup(mSentryGroup);
      sentryGroups.add(group);
    }

    return new TSentryRole(roleName, sentryGroups, EMPTY_GRANTOR_PRINCIPAL);
  }

  private TSentryGroup convertToTSentryGroup(MSentryGroup mSentryGroup) {
    return new TSentryGroup(mSentryGroup.getGroupName().intern());
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
        new TransactionBlock<Object>() {
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
            @SuppressWarnings("unchecked")
            List<MSentryVersion> mSentryVersions = (List<MSentryVersion>) query
                .execute();
            pm.retrieveAll(mSentryVersions);
            if (mSentryVersions.isEmpty()) {
              throw new SentryNoSuchObjectException("Matching Version");
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
   * Drop the given privilege from all roles.
   *
   * @param tAuthorizable the given authorizable object.
   * @throws Exception
   */
  public void dropPrivilege(final TSentryAuthorizable tAuthorizable) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects

            // Drop the give privilege for all possible actions from all roles.
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
   * Drop the given privilege from all roles. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param tAuthorizable the given authorizable object.
   * @param update the corresponding permission delta update.
   * @throws Exception
   */
  public synchronized void dropPrivilege(final TSentryAuthorizable tAuthorizable,
      final Update update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects

        // Drop the give privilege for all possible actions from all roles.
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
   * Rename the privilege for all roles. Drop the old privilege name and create the new one.
   *
   * @param oldTAuthorizable the old authorizable name needs to be renamed.
   * @param newTAuthorizable the new authorizable name
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  public void renamePrivilege(final TSentryAuthorizable oldTAuthorizable,
      final TSentryAuthorizable newTAuthorizable) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects

            // Drop the give privilege for all possible actions from all roles.
            TSentryPrivilege tPrivilege = toSentryPrivilege(oldTAuthorizable);
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

  /**
   * Rename the privilege for all roles. Drop the old privilege name and create the new one.
   * As well as persist the corresponding permission change to MSentryPermChange table in a
   * single transaction.
   *
   * @param oldTAuthorizable the old authorizable name needs to be renamed.
   * @param newTAuthorizable the new authorizable name
   * @param update the corresponding permission delta update.
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  public synchronized void renamePrivilege(final TSentryAuthorizable oldTAuthorizable,
      final TSentryAuthorizable newTAuthorizable, final Update update)
        throws Exception {

    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects

        // Drop the give privilege for all possible actions from all roles.
        TSentryPrivilege tPrivilege = toSentryPrivilege(oldTAuthorizable);
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
    Collection<MSentryRole> roleSet = new HashSet<>();
    List<MSentryPrivilege> mPrivileges = getMSentryPrivileges(tPrivilege, pm);
    for (MSentryPrivilege mPrivilege : mPrivileges) {
      roleSet.addAll(ImmutableSet.copyOf(mPrivilege.getRoles()));
    }
    // Dropping the privilege
    if (newTPrivilege == null) {
      for (MSentryRole role : roleSet) {
        alterSentryRoleRevokePrivilegeCore(pm, role.getRoleName(), tPrivilege);
      }
      return;
    }
    // Renaming privilege
    MSentryPrivilege parent = getMSentryPrivilege(tPrivilege, pm);
    if (parent != null) {
      // When all the roles associated with that privilege are revoked, privilege
      // will be removed from the database.
      // parent is an JDO object which is associated with privilege data in the database.
      // When the associated row is deleted in database, JDO should be not be
      // dereferenced. If object has to be used even after that it should have been detached.
      parent = pm.detachCopy(parent);
    }
    for (MSentryRole role : roleSet) {
      // 1. get privilege and child privileges
      Collection<MSentryPrivilege> privilegeGraph = new HashSet<>();
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
      for (MSentryPrivilege mPriv : privilegeGraph) {
        TSentryPrivilege tPriv = convertToTSentryPrivilege(mPriv);
        if (newTPrivilege.getPrivilegeScope().equals(PrivilegeScope.DATABASE.name())) {
          tPriv.setDbName(newTPrivilege.getDbName());
        } else if (newTPrivilege.getPrivilegeScope().equals(PrivilegeScope.TABLE.name())) {
          tPriv.setTableName(newTPrivilege.getTableName());
        }
        alterSentryRoleGrantPrivilegeCore(pm, role.getRoleName(), tPriv);
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
      // get all privileges for group and user
      Set<MSentryRole> roles = getRolesForGroups(pm, groups);
      roles.addAll(getRolesForUsers(pm, Sets.newHashSet(grantorPrincipal)));
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

  /**
   * Retrieves an up-to-date sentry permission snapshot.
   * <p>
   * It reads hiveObj to &lt role, privileges &gt mapping from {@link MSentryPrivilege}
   * table and role to groups mapping from {@link MSentryGroup}.
   * It also gets the changeID of latest delta update, from {@link MSentryPathChange}, that
   * the snapshot corresponds to.
   *
   * @return a {@link PathsImage} contains the mapping of hiveObj to
   *         &lt role, privileges &gt and the mapping of role to &lt Groups &gt.
   *         For empty image returns {@link #EMPTY_CHANGE_ID} and empty maps.
   * @throws Exception
   */
  public PermissionsImage retrieveFullPermssionsImage() throws Exception {
    return tm.executeTransaction(
    new TransactionBlock<PermissionsImage>() {
      public PermissionsImage execute(PersistenceManager pm)
      throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        // curChangeID could be 0, if Sentry server has been running before
        // enable SentryPlugin(HDFS Sync feature).
        long curChangeID = getLastProcessedChangeIDCore(pm, MSentryPermChange.class);
        Map<String, List<String>> roleImage = retrieveFullRoleImageCore(pm);
        Map<String, Map<String, String>> privilegeMap = retrieveFullPrivilegeImageCore(pm);

        return new PermissionsImage(roleImage, privilegeMap, curChangeID);
      }
    });
  }

  /**
   * Retrieves an up-to-date sentry privileges snapshot from {@code MSentryPrivilege} table.
   * The snapshot is represented by mapping of hiveObj to role privileges.
   *
   * @param pm PersistenceManager
   * @return a mapping of hiveObj to &lt role, privileges &gt
   * @throws Exception
   */
   private Map<String, Map<String, String>> retrieveFullPrivilegeImageCore(PersistenceManager pm)
        throws Exception {
     pm.setDetachAllOnCommit(false); // No need to detach objects

    Map<String, Map<String, String>> retVal = new HashMap<>();
    Query query = pm.newQuery(MSentryPrivilege.class);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");

    QueryParamBuilder paramBuilder = newQueryParamBuilder();
    paramBuilder.addNotNull(SERVER_NAME)
                .addNotNull(DB_NAME)
                .addNull(URI);

    query.setFilter(paramBuilder.toString());
    query.setOrdering("serverName ascending, dbName ascending, tableName ascending");
    @SuppressWarnings("unchecked")
    List<MSentryPrivilege> privileges =
            (List<MSentryPrivilege>) query.executeWithMap(paramBuilder.getArguments());
    for (MSentryPrivilege mPriv : privileges) {
      String authzObj = mPriv.getDbName();
      if (!isNULL(mPriv.getTableName())) {
        authzObj = authzObj + "." + mPriv.getTableName();
      }
      Map<String, String> pUpdate = retVal.get(authzObj);
      if (pUpdate == null) {
        pUpdate = new HashMap<>();
        retVal.put(authzObj, pUpdate);
      }
      for (MSentryRole mRole : mPriv.getRoles()) {
        String existingPriv = pUpdate.get(mRole.getRoleName());
        if (existingPriv == null) {
          pUpdate.put(mRole.getRoleName(), mPriv.getAction().toUpperCase());
        } else {
          pUpdate.put(mRole.getRoleName(), existingPriv + "," + mPriv.getAction().toUpperCase());
        }
      }
    }
    query.closeAll();
    return retVal;
  }

  /**
   * Retrieves an up-to-date sentry role snapshot from {@code MSentryGroup} table.
   * The snapshot is represented by a role to groups map.
   *
   * @param pm PersistenceManager
   * @return a mapping of Role to &lt Groups &gt
   * @throws Exception
   */
  private Map<String, List<String>> retrieveFullRoleImageCore(PersistenceManager pm)
          throws Exception {
    pm.setDetachAllOnCommit(false); // No need to detach objects
    Query query = pm.newQuery(MSentryGroup.class);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
    @SuppressWarnings("unchecked")
    List<MSentryGroup> groups = (List<MSentryGroup>) query.execute();
    if (groups.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, List<String>> retVal = new HashMap<>();
    for (MSentryGroup mGroup : groups) {
      for (MSentryRole role : mGroup.getRoles()) {
        List<String> rUpdate = retVal.get(role.getRoleName());
        if (rUpdate == null) {
          rUpdate = new ArrayList<>();
          retVal.put(role.getRoleName(), rUpdate);
        }
        rUpdate.add(mGroup.getGroupName());
      }
    }
    query.closeAll();
    return retVal;
  }

  /**
   * Retrieves an up-to-date hive paths snapshot. <p>
   * It reads hiveObj to paths mapping from {@link MAuthzPathsMapping} table and
   * gets the changeID of latest delta update, from {@link MSentryPathChange}, that
   * the snapshot corresponds to.
   *
   * NOTE: this method used to be used in the actual code but is now only used for tests
   * which should be refactored to use the new {@link #retrieveFullPathsImageUpdate} functionality.
   *
   * TODO: Remove retrieveFullPathsImage method and reimplement tests.
   *
   * @return an up-to-date hive paths snapshot contains mapping of hiveObj to &lt Paths &gt.
   *         For empty image return {@link #EMPTY_CHANGE_ID} and a empty map.
   * @throws Exception
   */
  public PathsImage retrieveFullPathsImage() throws Exception {
    return (PathsImage) tm.executeTransaction(
    new TransactionBlock() {
      public Object execute(PersistenceManager pm) throws Exception {
        // curChangeID could be 0 for the first full snapshot fetching
        // from HMS. It does not have corresponding delta update.
        pm.setDetachAllOnCommit(false); // No need to detach objects
        long curChangeID = getLastProcessedChangeIDCore(pm, MSentryPathChange.class);
        long curImageID = getCurrentAuthzPathsSnapshotID(pm);
        Map<String, Collection<String>> pathImage = retrieveFullPathsImageCore(pm, curImageID);

        return new PathsImage(pathImage, curChangeID, curImageID);
      }
    });
  }

  /**
   * Retrieves an up-to-date hive paths snapshot.
   * The image only contains PathsDump in it.
   * <p>
   * It reads hiveObj to paths mapping from {@link MAuthzPathsMapping} table and
   * gets the changeID of latest delta update, from {@link MSentryPathChange}, that
   * the snapshot corresponds to.
   *
   * @param prefixes path of Sentry managed prefixes. Ignore any path outside the prefix.
   * @return an up-to-date hive paths snapshot contains mapping of hiveObj to &lt Paths &gt.
   *         For empty image return {@link #EMPTY_CHANGE_ID} and a empty map.
   * @throws Exception
   */
  public PathsUpdate retrieveFullPathsImageUpdate(final String[] prefixes) throws Exception {
    return tm.executeTransaction(
            new TransactionBlock<PathsUpdate>() {
              public PathsUpdate execute(PersistenceManager pm) throws Exception {
                pm.setDetachAllOnCommit(false); // No need to detach objects
                long curImageID = getCurrentAuthzPathsSnapshotID(pm);
                long curChangeID = getLastProcessedChangeIDCore(pm, MSentryPathChange.class);
                PathsUpdate pathUpdate = new PathsUpdate(curChangeID, curImageID, true);
                // We ignore anything in the update and set it later to the assembled PathsDump
                UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(prefixes);
                // Extract all paths and put them into authzPaths
                retrieveFullPathsImageCore(pm, curImageID, authzPaths);
                pathUpdate.toThrift().setPathsDump(authzPaths.getPathsDump().createPathsDump(true));
                return pathUpdate;
              }
            });
  }

  /**
   * Retrieves an up-to-date hive paths snapshot from {@code MAuthzPathsMapping} table.
   * The snapshot is represented by a snapshot ID, and a map from hiveObj to paths.
   *
   * @return a mapping of hiveObj to &lt Paths &gt.
   */
  private Map<String, Collection<String>> retrieveFullPathsImageCore(PersistenceManager pm,
                                                                     long currentSnapshotID) {
    if (currentSnapshotID <= EMPTY_PATHS_SNAPSHOT_ID) {
      return Collections.emptyMap();
    }

    Query query = pm.newQuery(MAuthzPathsMapping.class);
    query.setFilter("this.authzSnapshotID == currentSnapshotID");
    query.declareParameters("long currentSnapshotID");
    Collection<MAuthzPathsMapping> authzToPathsMappings =
        (Collection<MAuthzPathsMapping>) query.execute(currentSnapshotID);

    if (authzToPathsMappings.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Collection<String>> retVal = new HashMap<>(authzToPathsMappings.size());
    for (MAuthzPathsMapping authzToPaths : authzToPathsMappings) {
      retVal.put(authzToPaths.getAuthzObjName(), authzToPaths.getPathStrings());
    }

    return retVal;
  }

  /**
   * Extract all paths and convert them into HMSPaths obect
   * @param pm Persistence manager
   * @param currentSnapshotID Image ID we are interested in
   * @param pathUpdate Destination for result
   */
  private void retrieveFullPathsImageCore(PersistenceManager pm,
                                          long currentSnapshotID,
                                          UpdateableAuthzPaths pathUpdate) {
    // Query for all MAuthzPathsMapping objects matching the given image ID
    Query query = pm.newQuery(MAuthzPathsMapping.class);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
    query.setFilter("this.authzSnapshotID == currentSnapshotID");
    query.declareParameters("long currentSnapshotID");
    Collection<MAuthzPathsMapping> authzToPathsMappings =
            (Collection<MAuthzPathsMapping>) query.execute(currentSnapshotID);

    // Walk each MAuthzPathsMapping object, get set of paths and push them all
    // into HMSPaths object contained in UpdateableAuthzPaths.
    for (MAuthzPathsMapping authzToPaths : authzToPathsMappings) {
      String  objName = authzToPaths.getAuthzObjName();
      // Convert path strings to list of components
      for (String path: authzToPaths.getPathStrings()) {
        String[] pathComponents = PathUtils.splitPath(path);
        List<String> paths = new ArrayList<>(pathComponents.length);
        Collections.addAll(paths, pathComponents);
        pathUpdate.applyAddChanges(objName, Collections.singletonList(paths));
      }
    }
  }

  /**
   * Delete all stored HMS notifications starting from given ID.<p>
   *
   * The purpose of the function is to clean up notifications in cases
   * were we recover from HMS notifications resets.
   *
   * @param pm Persistent manager instance
   * @param id initial ID. All notifications starting from this ID and above are
   *          removed.
   */
  private void deleteNotificationsSince(PersistenceManager pm, long id) {
    Query query = pm.newQuery(MSentryHmsNotification.class);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
    query.setFilter("notificationId >= currentNotificationId");
    query.declareParameters("long currentNotificationId");
    long numDeleted = query.deletePersistentAll(id);
    if (numDeleted > 0) {
      LOGGER.info("Purged {} notification entries starting from {}",
              numDeleted, id);
    }
  }

  /**
   * Persist an up-to-date HMS snapshot into Sentry DB in a single transaction with its latest
   * notification ID
   *
   * @param authzPaths paths to be be persisted
   * @param notificationID the latest notificationID associated with the snapshot
   * @throws Exception
   */
  public void persistFullPathsImage(final Map<String, Collection<String>> authzPaths,
      final long notificationID) throws Exception {
    tm.executeTransactionWithRetry(
      new TransactionBlock() {
        public Object execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          deleteNotificationsSince(pm, notificationID + 1);

          // persist the notidicationID
          pm.makePersistent(new MSentryHmsNotification(notificationID));

          // persist the full snapshot
          long snapshotID = getCurrentAuthzPathsSnapshotID(pm);
          long nextSnapshotID = snapshotID + 1;
          pm.makePersistent(new MAuthzPathsSnapshotId(nextSnapshotID));
          LOGGER.info("Attempting to commit new HMS snapshot with ID = {}", nextSnapshotID);
          for (Map.Entry<String, Collection<String>> authzPath : authzPaths.entrySet()) {
            pm.makePersistent(new MAuthzPathsMapping(nextSnapshotID, authzPath.getKey(), authzPath.getValue()));
          }
          return null;
        }
      });
  }

  /**
   * Get the last authorization path snapshot ID persisted.
   * Always executed in the transaction context.
   *
   * @param pm The PersistenceManager object.
   * @return the last persisted snapshot ID. It returns 0 if no rows are found.
   */
  private static long getCurrentAuthzPathsSnapshotID(PersistenceManager pm) {
    return getMaxPersistedIDCore(pm, MAuthzPathsSnapshotId.class, "authzSnapshotID", EMPTY_PATHS_SNAPSHOT_ID);
  }


  /**
   * Get the last authorization path snapshot ID persisted.
   * Always executed in the non-transaction context.
   * This is used for metrics, so no retries are attempted.
   *
   * @return the last persisted snapshot ID. It returns 0 if no rows are found.
   */
  private long getCurrentAuthzPathsSnapshotID() throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<Long>() {
          @Override
          public Long execute(PersistenceManager pm) throws Exception {
            return getCurrentAuthzPathsSnapshotID(pm);
          }
        }
    );
  }

  /**
   * Adds the authzObj and with a set of paths into the authzObj -> [Paths] mapping.
   * As well as persist the corresponding delta path change to MSentryPathChange
   * table in a single transaction.
   *
   * @param authzObj an authzObj
   * @param paths a set of paths need to be added into the authzObj -> [Paths] mapping
   * @param update the corresponding path delta update
   * @throws Exception
   */
  public void addAuthzPathsMapping(final String authzObj, final Collection<String> paths,
      final UniquePathsUpdate update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        addAuthzPathsMappingCore(pm, authzObj, paths);
        return null;
      }
    });
  }

  /**
   * Adds the authzObj and with a set of paths into the authzObj -> [Paths] mapping.
   * If the given authzObj already exists in the mapping, only need to add the new paths
   * into its mapping.
   *
   * @param pm PersistenceManager
   * @param authzObj an authzObj
   * @param paths a set of paths need to be added into the authzObj -> [Paths] mapping
   */
  private void addAuthzPathsMappingCore(PersistenceManager pm, String authzObj,
        Collection<String> paths) {
    long currentSnapshotID = getCurrentAuthzPathsSnapshotID(pm);
    if (currentSnapshotID <= EMPTY_PATHS_SNAPSHOT_ID) {
      LOGGER.warn("AuthzObj: {} cannot be persisted if paths snapshot ID does not exist yet.", authzObj);
    }

    MAuthzPathsMapping mAuthzPathsMapping = getMAuthzPathsMappingCore(pm, currentSnapshotID, authzObj);
    if (mAuthzPathsMapping == null) {
      mAuthzPathsMapping = new MAuthzPathsMapping(currentSnapshotID, authzObj, paths);
    } else {
      for (String path : paths) {
        mAuthzPathsMapping.addPath(new MPath(path));
      }
    }
    pm.makePersistent(mAuthzPathsMapping);
  }

  /**
   * Deletes a set of paths belongs to given authzObj from the authzObj -> [Paths] mapping.
   * As well as persist the corresponding delta path change to MSentryPathChange
   * table in a single transaction.
   *
   * @param authzObj an authzObj
   * @param paths a set of paths need to be deleted from the authzObj -> [Paths] mapping
   * @param update the corresponding path delta update
   */
  public void deleteAuthzPathsMapping(final String authzObj, final Iterable<String> paths,
      final UniquePathsUpdate update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        deleteAuthzPathsMappingCore(pm, authzObj, paths);
        return null;
      }
    });
  }

  /**
   * Deletes a set of paths belongs to given authzObj from the authzObj -> [Paths] mapping.
   *
   * @param pm PersistenceManager
   * @param authzObj an authzObj
   * @param paths a set of paths need to be deleted from the authzObj -> [Paths] mapping.
   * @throws SentryNoSuchObjectException if cannot find the existing authzObj or path.
   */
  private void deleteAuthzPathsMappingCore(PersistenceManager pm, String authzObj,
                                           Iterable<String> paths) {
    long currentSnapshotID = getCurrentAuthzPathsSnapshotID(pm);
    if (currentSnapshotID <= EMPTY_PATHS_SNAPSHOT_ID) {
      LOGGER.error("No paths snapshot ID is found. Cannot delete authzoObj: {}", authzObj);
    }

    MAuthzPathsMapping mAuthzPathsMapping = getMAuthzPathsMappingCore(pm, currentSnapshotID, authzObj);
    if (mAuthzPathsMapping != null) {
      for (String path : paths) {
        MPath mPath = mAuthzPathsMapping.getPath(path);
        if (mPath == null) {
          LOGGER.error("nonexistent path: {}", path);
        } else {
          mAuthzPathsMapping.removePath(mPath);
          pm.deletePersistent(mPath);
        }
      }
      pm.makePersistent(mAuthzPathsMapping);
    } else {
      LOGGER.error("nonexistent authzObj: {} on current paths snapshot ID #{}",
          authzObj, currentSnapshotID);
    }
  }

  /**
   * Deletes all entries of the given authzObj from the authzObj -> [Paths] mapping.
   * As well as persist the corresponding delta path change to MSentryPathChange
   * table in a single transaction.
   *
   * @param authzObj an authzObj to be deleted
   * @param update the corresponding path delta update
   */
  public void deleteAllAuthzPathsMapping(final String authzObj, final UniquePathsUpdate update)
        throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        deleteAllAuthzPathsMappingCore(pm, authzObj);
        return null;
      }
    });
  }

  /**
   * Deletes the entry of the given authzObj from the authzObj -> [Paths] mapping.
   *
   * @param pm PersistenceManager
   * @param authzObj an authzObj to be deleted
   * @throws SentryNoSuchObjectException if cannot find the existing authzObj
   */
  private void deleteAllAuthzPathsMappingCore(PersistenceManager pm, String authzObj) {
    long currentSnapshotID = getCurrentAuthzPathsSnapshotID(pm);
    if (currentSnapshotID <= EMPTY_PATHS_SNAPSHOT_ID) {
      LOGGER.error("No paths snapshot ID is found. Cannot delete authzoObj: {}", authzObj);
    }

    MAuthzPathsMapping mAuthzPathsMapping = getMAuthzPathsMappingCore(pm, currentSnapshotID, authzObj);
    if (mAuthzPathsMapping != null) {
      for (MPath mPath : mAuthzPathsMapping.getPaths()) {
        mAuthzPathsMapping.removePath(mPath);
        pm.deletePersistent(mPath);
      }
      pm.deletePersistent(mAuthzPathsMapping);
    } else {
      LOGGER.error("nonexistent authzObj: {} on current paths snapshot ID #{}",
          authzObj, currentSnapshotID);
    }
  }

  /**
   * Renames the existing authzObj to a new one in the authzObj -> [Paths] mapping.
   * And updates its existing path with a new path, while keeps the rest of its paths
   * untouched if there is any. As well as persist the corresponding delta path
   * change to MSentryPathChange table in a single transaction.
   *
   * @param oldObj the existing authzObj
   * @param newObj the new name to be changed to
   * @param oldPath a existing path of the given authzObj
   * @param newPath a new path to be changed to
   * @param update the corresponding path delta update
   */
  public void renameAuthzPathsMapping(final String oldObj, final String newObj,
      final String oldPath, final String newPath, final UniquePathsUpdate update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        renameAuthzPathsMappingCore(pm, oldObj, newObj, oldPath, newPath);
        return null;
      }
    });
  }

  /**
   * Renames the existing authzObj to a new one in the authzObj -> [Paths] mapping.
   * And updates its existing path with a new path, while keeps the rest of its paths
   * untouched if there is any.
   *
   * @param pm PersistenceManager
   * @param oldObj the existing authzObj
   * @param newObj the new name to be changed to
   * @param oldPath a existing path of the given authzObj
   * @param newPath a new path to be changed to
   * @throws SentryNoSuchObjectException if cannot find the existing authzObj or path.
   */
  private void renameAuthzPathsMappingCore(PersistenceManager pm, String oldObj,
        String newObj, String oldPath, String newPath) {
    long currentSnapshotID = getCurrentAuthzPathsSnapshotID(pm);
    if (currentSnapshotID <= EMPTY_PATHS_SNAPSHOT_ID) {
      LOGGER.error("No paths snapshot ID is found. Cannot rename authzoObj: {}", oldObj);
    }

    MAuthzPathsMapping mAuthzPathsMapping = getMAuthzPathsMappingCore(pm, currentSnapshotID, oldObj);
    if (mAuthzPathsMapping != null) {
      MPath mOldPath = mAuthzPathsMapping.getPath(oldPath);
      if (mOldPath == null) {
        LOGGER.error("nonexistent path: {}", oldPath);
      } else {
        mAuthzPathsMapping.removePath(mOldPath);
        pm.deletePersistent(mOldPath);
      }
      mAuthzPathsMapping.addPath(new MPath(newPath));
      mAuthzPathsMapping.setAuthzObjName(newObj);
      pm.makePersistent(mAuthzPathsMapping);
    } else {
      LOGGER.error("nonexistent authzObj: {} on current paths snapshot ID #{}",
          oldObj, currentSnapshotID);
    }
  }

  /**
   * Renames the existing authzObj to a new one in the authzObj -> [Paths] mapping,
   * but keeps its paths mapping as-is. As well as persist the corresponding delta path
   * change to MSentryPathChange table in a single transaction.
   *
   * @param oldObj the existing authzObj
   * @param newObj the new name to be changed to
   * @param update the corresponding path delta update
   */
  public void renameAuthzObj(final String oldObj, final String newObj,
      final UniquePathsUpdate update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        renameAuthzObjCore(pm, oldObj, newObj);
        return null;
      }
    });
  }

  /**
   * Renames the existing authzObj to a new one in the authzObj -> [Paths] mapping,
   * but keeps its paths mapping as-is.
   *
   * @param pm PersistenceManager
   * @param oldObj the existing authzObj
   * @param newObj the new name to be changed to
   * @throws SentryNoSuchObjectException if cannot find the existing authzObj.
   */
  private void renameAuthzObjCore(PersistenceManager pm, String oldObj,
      String newObj) {
    long currentSnapshotID = getCurrentAuthzPathsSnapshotID(pm);
    if (currentSnapshotID <= EMPTY_PATHS_SNAPSHOT_ID) {
      LOGGER.error("No paths snapshot ID is found. Cannot rename authzoObj: {}", oldObj);
    }

    MAuthzPathsMapping mAuthzPathsMapping = getMAuthzPathsMappingCore(pm, currentSnapshotID, oldObj);
    if (mAuthzPathsMapping != null) {
      mAuthzPathsMapping.setAuthzObjName(newObj);
      pm.makePersistent(mAuthzPathsMapping);
    } else {
      LOGGER.error("nonexistent authzObj: {} on current paths snapshot ID #{}",
          oldObj, currentSnapshotID);
    }
  }

  /**
   * Tells if there are any records in MAuthzPathsMapping
   *
   * @return true if there are no entries in <code>MAuthzPathsMapping</code>
   * false if there are entries
   * @throws Exception
   */
  public boolean isAuthzPathsMappingEmpty() throws Exception {
    return tm.executeTransactionWithRetry(
      new TransactionBlock<Boolean>() {
        public Boolean execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          return isTableEmptyCore(pm, MAuthzPathsMapping.class);
        }
      });
  }

  /**
   * Tells if there are any records in MSentryHmsNotification
   *
   * @return true if there are no entries in <code>MSentryHmsNotification</code>
   * false if there are entries
   * @throws Exception
   */
  public boolean isHmsNotificationEmpty() throws Exception {
    return tm.executeTransactionWithRetry(
        new TransactionBlock<Boolean>() {
          public Boolean execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            return isTableEmptyCore(pm, MSentryHmsNotification.class);
          }
        });
  }

  /**
   * Tells if there are any records in MAuthzPathsSnapshotId
   *
   * @return true if there are no entries in <code>MAuthzPathsSnapshotId</code>
   * false if there are entries
   * @throws Exception
   */
  public boolean isAuthzPathsSnapshotEmpty() throws Exception {
    return tm.executeTransactionWithRetry(
        new TransactionBlock<Boolean>() {
          public Boolean execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            return isTableEmptyCore(pm, MAuthzPathsSnapshotId.class);
          }
        });
  }

  /**
   * Updates authzObj -> [Paths] mapping to replace an existing path with a new one
   * given an authzObj. As well as persist the corresponding delta path change to
   * MSentryPathChange table in a single transaction.
   *
   * @param authzObj an authzObj
   * @param oldPath the existing path maps to the given authzObj
   * @param newPath a new path to replace the existing one
   * @param update the corresponding path delta update
   * @throws Exception
   */
  public void updateAuthzPathsMapping(final String authzObj, final String oldPath,
        final String newPath, final UniquePathsUpdate update) throws Exception {
    execute(update, new TransactionBlock<Object>() {
      public Object execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        updateAuthzPathsMappingCore(pm, authzObj, oldPath, newPath);
        return null;
      }
    });
  }

  /**
   * Updates authzObj -> [Paths] mapping to replace an existing path with a new one
   * given an authzObj.
   *
   * @param pm PersistenceManager
   * @param authzObj an authzObj
   * @param oldPath the existing path maps to the given authzObj
   * @param newPath a non-empty path to replace the existing one
   * @throws SentryNoSuchObjectException if no such path found
   *        in the authzObj -> [Paths] mapping.
   */
  private void updateAuthzPathsMappingCore(PersistenceManager pm, String authzObj,
        String oldPath, String newPath) {

    long currentSnapshotID = getCurrentAuthzPathsSnapshotID(pm);
    if (currentSnapshotID <= EMPTY_PATHS_SNAPSHOT_ID) {
      LOGGER.error("No paths snapshot ID is found. Cannot update authzoObj: {}", authzObj);
    }

    MAuthzPathsMapping mAuthzPathsMapping = getMAuthzPathsMappingCore(pm, currentSnapshotID, authzObj);
    if (mAuthzPathsMapping == null) {
      mAuthzPathsMapping = new MAuthzPathsMapping(currentSnapshotID, authzObj, Sets.newHashSet(newPath));
    } else {
      MPath mOldPath = mAuthzPathsMapping.getPath(oldPath);
      if (mOldPath == null) {
        LOGGER.error("nonexistent path: {}", oldPath);
      } else {
        mAuthzPathsMapping.removePath(mOldPath);
        pm.deletePersistent(mOldPath);
      }
      MPath mNewPath = new MPath(newPath);
      mAuthzPathsMapping.addPath(mNewPath);
    }
    pm.makePersistent(mAuthzPathsMapping);
  }

  /**
   * Get the MAuthzPathsMapping object from authzObj
   */
  private MAuthzPathsMapping getMAuthzPathsMappingCore(PersistenceManager pm,
        long authzSnapshotID, String authzObj) {
    Query query = pm.newQuery(MAuthzPathsMapping.class);
    query.setFilter("this.authzSnapshotID == authzSnapshotID && this.authzObjName == authzObjName");
    query.declareParameters("long authzSnapshotID, java.lang.String authzObjName");
    query.setUnique(true);
    return (MAuthzPathsMapping) query.execute(authzSnapshotID, authzObj);
  }

  /**
   * Checks if the table associated with class provided is empty
   *
   * @param pm PersistenceManager
   * @param clazz class
   * @return True is the table is empty
   * False if it not.
   */
  private boolean isTableEmptyCore(PersistenceManager pm, Class clazz) {
    Query query = pm.newQuery(clazz);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
    // setRange is implemented efficiently for MySQL, Postgresql (using the LIMIT SQL keyword)
    // and Oracle (using the ROWNUM keyword), with the query only finding the objects required
    // by the user directly in the datastore. For other RDBMS the query will retrieve all
    // objects up to the "to" record, and will not pass any unnecessary objects that are before
    // the "from" record.
    query.setRange(0, 1);
    return ((List<?>) query.execute()).isEmpty();
  }

  /**
   * Generic method used to query the maximum number (or ID) of a column from a specified class.
   *
   * @param pm The PersistenceManager object.
   * @param clazz The class name to query.
   * @param columnName The column name to query.
   * @return the maximum number persisted on the class. It returns NULL if the class has no rows.
   */
  private static long getMaxPersistedIDCore(PersistenceManager pm, Class clazz, String columnName, long defaultValue) {
    Query query = pm.newQuery(clazz);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
    query.setResult(String.format("max(%s)", columnName));
    Long maxValue = (Long) query.execute();
    return (maxValue != null) ? maxValue : defaultValue;
  }

  @VisibleForTesting
  List<MPath> getMPaths() throws Exception {
    return tm.executeTransaction(new TransactionBlock<List<MPath>>() {
      public List<MPath> execute(PersistenceManager pm) throws Exception {
        long currentSnapshotID = getCurrentAuthzPathsSnapshotID(pm);

        Query query = pm.newQuery("SQL",
            "SELECT p.PATH_NAME FROM AUTHZ_PATH p " +
               "JOIN AUTHZ_PATHS_MAPPING a ON a.AUTHZ_OBJ_ID = p.AUTHZ_OBJ_ID " +
               "WHERE a.AUTHZ_SNAPSHOT_ID = ?"
        );
        query.setResultClass(MPath.class);
        return (List<MPath>) query.execute(currentSnapshotID);
      }
    });
  }

  /**
   * Method detects orphaned privileges
   *
   * @return True, If there are orphan privileges
   * False, If orphan privileges are not found.
   * non-zero value if an orphan is found.
   * <p>
   * Method currently used only by tests.
   * <p>
   */

  @VisibleForTesting
  Boolean findOrphanedPrivileges() throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<Boolean>() {
        public Boolean execute(PersistenceManager pm) throws Exception {
          return findOrphanedPrivilegesCore(pm);
        }
      });
  }

  Boolean findOrphanedPrivilegesCore(PersistenceManager pm) {
    //Perform a SQL query to get things that look like orphans
    List<MSentryPrivilege> results = getAllMSentryPrivilegesCore(pm);
    List<Object> idList = new ArrayList<>(results.size());
    for (MSentryPrivilege orphan : results) {
      idList.add(pm.getObjectId(orphan));
    }
    if (idList.isEmpty()) {
      return false;
    }
    //For each potential orphan, verify it's really a orphan.
    // Moment an orphan is identified return 1 indicating an orphan is found.
    pm.refreshAll();  // Try to ensure we really have correct objects
    for (Object id : idList) {
      MSentryPrivilege priv = (MSentryPrivilege) pm.getObjectById(id);
      if (priv.getRoles().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /** get mapping datas for [group,role], [user,role] with the specific roles */
  @SuppressWarnings("unchecked")
  public List<Map<String, Set<String>>> getGroupUserRoleMapList(final Collection<String> roleNames)
          throws Exception {
      return tm.executeTransaction(
        new TransactionBlock<List<Map<String, Set<String>>>>() {
          public List<Map<String, Set<String>>> execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects

            Query query = pm.newQuery(MSentryRole.class);
            query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
            List<MSentryRole> mSentryRoles;
            if ((roleNames == null) || roleNames.isEmpty()) {
              mSentryRoles = (List<MSentryRole>)query.execute();
            } else {
              QueryParamBuilder paramBuilder = newQueryParamBuilder(QueryParamBuilder.Op.OR);
              paramBuilder.addSet("roleName == ", roleNames);
              query.setFilter(paramBuilder.toString());
              mSentryRoles =
                      (List<MSentryRole>) query.executeWithMap(paramBuilder.getArguments());
            }
            Map<String, Set<String>> groupRolesMap = getGroupRolesMap(mSentryRoles);
            Map<String, Set<String>> userRolesMap = getUserRolesMap(mSentryRoles);
            List<Map<String, Set<String>>> mapsList = new ArrayList<>();
            mapsList.add(INDEX_GROUP_ROLES_MAP, groupRolesMap);
            mapsList.add(INDEX_USER_ROLES_MAP, userRolesMap);
            return mapsList;
          }
        });
  }

  private Map<String, Set<String>> getGroupRolesMap(Collection<MSentryRole> mSentryRoles) {
    if (mSentryRoles.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Set<String>> groupRolesMap = new HashMap<>();
    // change the List<MSentryRole> -> Map<groupName, Set<roleName>>
    for (MSentryRole mSentryRole : mSentryRoles) {
      Set<MSentryGroup> groups = mSentryRole.getGroups();
      for (MSentryGroup group : groups) {
        String groupName = group.getGroupName();
        Set<String> rNames = groupRolesMap.get(groupName);
        if (rNames == null) {
          rNames = new HashSet<>();
        }
        rNames.add(mSentryRole.getRoleName());
        groupRolesMap.put(groupName, rNames);
      }
    }
    return groupRolesMap;
  }

  private Map<String, Set<String>> getUserRolesMap(Collection<MSentryRole> mSentryRoles) {
    if (mSentryRoles.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Set<String>> userRolesMap = new HashMap<>();
    // change the List<MSentryRole> -> Map<userName, Set<roleName>>
    for (MSentryRole mSentryRole : mSentryRoles) {
      Set<MSentryUser> users = mSentryRole.getUsers();
      for (MSentryUser user : users) {
        String userName = user.getUserName();
        Set<String> rNames = userRolesMap.get(userName);
        if (rNames == null) {
          rNames = new HashSet<>();
        }
        rNames.add(mSentryRole.getRoleName());
        userRolesMap.put(userName, rNames);
      }
    }
    return userRolesMap;
  }

  // get all mapping data for [role,privilege]
  Map<String, Set<TSentryPrivilege>> getRoleNameTPrivilegesMap() throws Exception {
    return getRoleNameTPrivilegesMap(null, null);
  }

  /**
   * @return mapping data for [role,privilege] with the specific auth object
   */
  public Map<String, Set<TSentryPrivilege>> getRoleNameTPrivilegesMap(final String dbName,
        final String tableName) throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<Map<String, Set<TSentryPrivilege>>>() {
        public Map<String, Set<TSentryPrivilege>> execute(PersistenceManager pm)
                throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          Query query = pm.newQuery(MSentryPrivilege.class);
          query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
          QueryParamBuilder paramBuilder = newQueryParamBuilder();

          if (!StringUtils.isEmpty(dbName)) {
              paramBuilder.add(DB_NAME, dbName);
          }
          if (!StringUtils.isEmpty(tableName)) {
              paramBuilder.add(TABLE_NAME, tableName);
          }
          query.setFilter(paramBuilder.toString());
          @SuppressWarnings("unchecked")
          List<MSentryPrivilege> mSentryPrivileges =
                  (List<MSentryPrivilege>) query.
                          executeWithMap(paramBuilder.getArguments());
          return getRolePrivilegesMap(mSentryPrivileges);
        }
      });
  }

  private Map<String, Set<TSentryPrivilege>> getRolePrivilegesMap(
          Collection<MSentryPrivilege> mSentryPrivileges) {
    if (mSentryPrivileges.isEmpty()) {
      return Collections.emptyMap();
    }

    // change the List<MSentryPrivilege> -> Map<roleName, Set<TSentryPrivilege>>
    Map<String, Set<TSentryPrivilege>> rolePrivilegesMap = new HashMap<>();
    for (MSentryPrivilege mSentryPrivilege : mSentryPrivileges) {
      TSentryPrivilege privilege = convertToTSentryPrivilege(mSentryPrivilege);
      for (MSentryRole mSentryRole : mSentryPrivilege.getRoles()) {
        String roleName = mSentryRole.getRoleName();
        Set<TSentryPrivilege> privileges = rolePrivilegesMap.get(roleName);
        if (privileges == null) {
          privileges = new HashSet<>();
        }
        privileges.add(privilege);
        rolePrivilegesMap.put(roleName, privileges);
      }
    }
    return rolePrivilegesMap;
  }

  /**
   * @return Set of all role names, or an empty set if no roles are defined
   */
  public Set<String> getAllRoleNames() throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<Set<String>>() {
          public Set<String> execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            return getAllRoleNamesCore(pm);
          }
        });
  }

  /**
   * Get set of all role names
   * Should be executed inside transaction
   * @param pm PersistenceManager instance
   * @return Set of all role names, or an empty set if no roles are defined
   */
  private Set<String> getAllRoleNamesCore(PersistenceManager pm) {
    List<MSentryRole> mSentryRoles = getAllRoles(pm);
    if (mSentryRoles.isEmpty()) {
      return Collections.emptySet();
    }

    return rolesToRoleNames(mSentryRoles);
  }

  /**
   * Get all groups as a map from group name to group
   * @param pm PersistenceManager instance
   * @return map of group names to group data for each group
   */
  private Map<String, MSentryGroup> getGroupNameTGroupMap(PersistenceManager pm) {
    Query query = pm.newQuery(MSentryGroup.class);
    @SuppressWarnings("unchecked")
    List<MSentryGroup> mSentryGroups = (List<MSentryGroup>) query.execute();
    if (mSentryGroups.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, MSentryGroup> existGroupsMap = new HashMap<>(mSentryGroups.size());
    // change the List<MSentryGroup> -> Map<groupName, MSentryGroup>
    for (MSentryGroup mSentryGroup : mSentryGroups) {
      existGroupsMap.put(mSentryGroup.getGroupName(), mSentryGroup);
    }
    return existGroupsMap;
  }

  /**
   * Get all users as a map from user name to user
   * @param pm PersistenceManager instance
   * @return map of user names to user data for each user
   */
  private Map<String, MSentryUser> getUserNameToUserMap(PersistenceManager pm) {
    Query query = pm.newQuery(MSentryUser.class);
    @SuppressWarnings("unchecked")
    List<MSentryUser> users = (List<MSentryUser>) query.execute();
    if (users.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, MSentryUser> existUsersMap = new HashMap<>(users.size());
    // change the List<MSentryUser> -> Map<userName, MSentryUser>
    for (MSentryUser user : users) {
      existUsersMap.put(user.getUserName(), user);
    }
    return existUsersMap;
  }

  @VisibleForTesting
  Map<String, MSentryRole> getRolesMap() throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<Map<String, MSentryRole>>() {
          public Map<String, MSentryRole> execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            List<MSentryRole> mSentryRoles = getAllRoles(pm);
            if (mSentryRoles.isEmpty()) {
              return Collections.emptyMap();
            }
            Map<String, MSentryRole> existRolesMap =
                    new HashMap<>(mSentryRoles.size());
            // change the List<MSentryRole> -> Map<roleName, Set<MSentryRole>>
            for (MSentryRole mSentryRole : mSentryRoles) {
              existRolesMap.put(mSentryRole.getRoleName(), mSentryRole);
            }

            return existRolesMap;
          }
        });
  }

  @VisibleForTesting
  Map<String, MSentryGroup> getGroupNameToGroupMap() throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<Map<String, MSentryGroup>>() {
        public Map<String, MSentryGroup> execute(PersistenceManager pm) throws Exception {
          return getGroupNameTGroupMap(pm);
        }
      });
  }

  @VisibleForTesting
  Map<String, MSentryUser> getUserNameToUserMap() throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<Map<String, MSentryUser>>() {
          public Map<String, MSentryUser> execute(PersistenceManager pm) throws Exception {
            return getUserNameToUserMap(pm);
          }
        });
  }

  @VisibleForTesting
  List<MSentryPrivilege> getPrivilegesList() throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<List<MSentryPrivilege>>() {
        public List<MSentryPrivilege> execute(PersistenceManager pm)
                throws Exception {
          Query query = pm.newQuery(MSentryPrivilege.class);
          return (List<MSentryPrivilege>) query.execute();
        }
      });
  }

  /**
   * Import the sentry mapping data.
   *
   * @param tSentryMappingData
   *        Include 2 maps to save the mapping data, the following is the example of the data
   *        structure:
   *        for the following mapping data:
   *        user1=role1,role2
   *        user2=role2,role3
   *        group1=role1,role2
   *        group2=role2,role3
   *        role1=server=server1->db=db1
   *        role2=server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2
   *        role3=server=server1->url=hdfs://localhost/path
   *
   *        The GroupRolesMap in TSentryMappingData will be saved as:
   *        {
   *        TSentryGroup(group1)={role1, role2},
   *        TSentryGroup(group2)={role2, role3}
   *        }
   *        The UserRolesMap in TSentryMappingData will be saved as:
   *        {
   *        TSentryUser(user1)={role1, role2},
   *        TSentryGroup(user2)={role2, role3}
   *        }
   *        The RolePrivilegesMap in TSentryMappingData will be saved as:
   *        {
   *        role1={TSentryPrivilege(server=server1->db=db1)},
   *        role2={TSentryPrivilege(server=server1->db=db1->table=tbl1),
   *        TSentryPrivilege(server=server1->db=db1->table=tbl2)},
   *        role3={TSentryPrivilege(server=server1->url=hdfs://localhost/path)}
   *        }
   * @param isOverwriteForRole
   *        The option for merging or overwriting the existing data during import, true for
   *        overwriting, false for merging
   */
  public void importSentryMetaData(final TSentryMappingData tSentryMappingData,
      final boolean isOverwriteForRole) throws Exception {
    tm.executeTransaction(
        new TransactionBlock<Object>() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.setDetachAllOnCommit(false); // No need to detach objects
            TSentryMappingData mappingData = lowercaseRoleName(tSentryMappingData);
            Set<String> roleNames = getAllRoleNamesCore(pm);

            Map<String, Set<TSentryGroup>> importedRoleGroupsMap = covertToRoleNameTGroupsMap(mappingData
                .getGroupRolesMap());
            Map<String, Set<String>> importedRoleUsersMap = covertToRoleUsersMap(mappingData
                .getUserRolesMap());
            Set<String> importedRoleNames = importedRoleGroupsMap.keySet();
            // if import with overwrite role, drop the duplicated roles in current DB first.
            if (isOverwriteForRole) {
              dropDuplicatedRoleForImport(pm, roleNames, importedRoleNames);
              // refresh the roleNames for the drop role
              roleNames = getAllRoleNamesCore(pm);
            }

            // Empty roleNames is most likely the COllections.emptySet().
            // We are going to modify roleNames below, so create an actual set.
            if (roleNames.isEmpty()) {
              roleNames = new HashSet<>();
            }

            // import the mapping data for [role,privilege], the roleNames will be updated
            importRolePrivilegeMapping(pm, roleNames, mappingData.getRolePrivilegesMap());
            // import the mapping data for [role,group], the roleNames will be updated
            importRoleGroupMapping(pm, roleNames, importedRoleGroupsMap);
            // import the mapping data for [role,user], the roleNames will be updated
            importRoleUserMapping(pm, roleNames, importedRoleUsersMap);
            return null;
          }
        });
  }

  // covert the Map[group->roles] to Map[role->groups]
  private Map<String, Set<TSentryGroup>> covertToRoleNameTGroupsMap(
      Map<String, Set<String>> groupRolesMap) {
    if (groupRolesMap == null || groupRolesMap.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Set<TSentryGroup>> roleGroupsMap = Maps.newHashMap();
    for (Map.Entry<String, Set<String>> entry : groupRolesMap.entrySet()) {
      Set<String> roleNames = entry.getValue();
      if (roleNames != null) {
        for (String roleName : roleNames) {
          Set<TSentryGroup> tSentryGroups = roleGroupsMap.get(roleName);
          if (tSentryGroups == null) {
            tSentryGroups = new HashSet<>();
          }
          tSentryGroups.add(new TSentryGroup(entry.getKey()));
          roleGroupsMap.put(roleName, tSentryGroups);
        }
      }
    }
    return roleGroupsMap;
  }

  // covert the Map[user->roles] to Map[role->users]
  private Map<String, Set<String>> covertToRoleUsersMap(
      Map<String, Set<String>> userRolesMap) {
    if (userRolesMap == null || userRolesMap.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Set<String>> roleUsersMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : userRolesMap.entrySet()) {
      Set<String> roleNames = entry.getValue();
      if (roleNames != null) {
        for (String roleName : roleNames) {
          Set<String> users = roleUsersMap.get(roleName);
          if (users == null) {
            users = new HashSet<>();
          }
          users.add(entry.getKey());
          roleUsersMap.put(roleName, users);
        }
      }
    }
    return roleUsersMap;
  }

  private void importRoleGroupMapping(PersistenceManager pm, Set<String> existRoleNames,
      Map<String, Set<TSentryGroup>> importedRoleGroupsMap) throws Exception {
    if (importedRoleGroupsMap == null || importedRoleGroupsMap.keySet() == null) {
      return;
    }
    for (Map.Entry<String, Set<TSentryGroup>> entry : importedRoleGroupsMap.entrySet()) {
      createRoleIfNotExist(pm, existRoleNames, entry.getKey());
      alterSentryRoleAddGroupsCore(pm, entry.getKey(), entry.getValue());
    }
  }

  private void importRoleUserMapping(PersistenceManager pm, Set<String> existRoleNames,
      Map<String, Set<String>> importedRoleUsersMap) throws Exception {
    if (importedRoleUsersMap == null || importedRoleUsersMap.keySet() == null) {
      return;
    }
    for (Map.Entry<String, Set<String>> entry : importedRoleUsersMap.entrySet()) {
      createRoleIfNotExist(pm, existRoleNames, entry.getKey());
      alterSentryRoleAddUsersCore(pm, entry.getKey(), entry.getValue());
    }
  }

  // drop all duplicated with the imported role
  private void dropDuplicatedRoleForImport(PersistenceManager pm, Set<String> existRoleNames,
      Set<String> importedRoleNames) throws Exception {
    Set<String> duplicatedRoleNames = Sets.intersection(existRoleNames, importedRoleNames);
    for (String droppedRoleName : duplicatedRoleNames) {
      dropSentryRoleCore(pm, droppedRoleName);
    }
  }

  // change all role name in lowercase
  private TSentryMappingData lowercaseRoleName(TSentryMappingData tSentryMappingData) {
    Map<String, Set<String>> sentryGroupRolesMap = tSentryMappingData.getGroupRolesMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap = tSentryMappingData
        .getRolePrivilegesMap();

    Map<String, Set<String>> newSentryGroupRolesMap = new HashMap<>();
    Map<String, Set<TSentryPrivilege>> newSentryRolePrivilegesMap = new HashMap<>();
    // for mapping data [group,role]
    for (Map.Entry<String, Set<String>> entry : sentryGroupRolesMap.entrySet()) {
      Collection<String> lowcaseRoles = Collections2.transform(entry.getValue(),
          new Function<String, String>() {
            @Override
            public String apply(String input) {
              return input.toLowerCase();
            }
          });
      newSentryGroupRolesMap.put(entry.getKey(), new HashSet<>(lowcaseRoles));
    }

    // for mapping data [role,privilege]
    for (Map.Entry<String,Set<TSentryPrivilege>> entry : sentryRolePrivilegesMap.entrySet()) {
      newSentryRolePrivilegesMap.put(entry.getKey().toLowerCase(), entry.getValue());
    }

    tSentryMappingData.setGroupRolesMap(newSentryGroupRolesMap);
    tSentryMappingData.setRolePrivilegesMap(newSentryRolePrivilegesMap);
    return tSentryMappingData;
  }

  // import the mapping data for [role,privilege]
  private void importRolePrivilegeMapping(PersistenceManager pm, Set<String> existRoleNames,
      Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap) throws Exception {
    if (sentryRolePrivilegesMap != null) {
      for (Map.Entry<String, Set<TSentryPrivilege>> entry : sentryRolePrivilegesMap.entrySet()) {
        // if the rolenName doesn't exist, create it and add it to existRoleNames
        createRoleIfNotExist(pm, existRoleNames, entry.getKey());
        // get the privileges for the role
        Set<TSentryPrivilege> tSentryPrivileges = entry.getValue();
        for (TSentryPrivilege tSentryPrivilege : tSentryPrivileges) {
          alterSentryRoleGrantPrivilegeCore(pm, entry.getKey(), tSentryPrivilege);
        }
      }
    }
  }

  private void createRoleIfNotExist(PersistenceManager pm,
      Set<String> existRoleNames, String roleName) throws Exception {
    String lowerRoleName = trimAndLower(roleName);
    // if the rolenName doesn't exist, create it.
    if (!existRoleNames.contains(lowerRoleName)) {
      // update the exist role name set
      existRoleNames.add(lowerRoleName);
      // Create role in the persistent storage
      pm.makePersistent(new MSentryRole(trimAndLower(roleName)));
    }
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

  /**
   * Return exception for nonexistent role
   * @param roleName Role name
   * @return SentryNoSuchObjectException with appropriate message
   */
  private static SentryNoSuchObjectException noSuchRole(String roleName) {
    return new SentryNoSuchObjectException("Role " + roleName);
  }

  /**
   * Return exception for nonexistent group
   * @param groupName Group name
   * @return SentryNoSuchObjectException with appropriate message
   */
  private static SentryNoSuchObjectException noSuchGroup(String groupName) {
    return new SentryNoSuchObjectException("Group " + groupName);

  }

  /**
   * Return exception for nonexistent update
   * @param changeID change ID
   * @return SentryNoSuchObjectException with appropriate message
   */
  private SentryNoSuchObjectException noSuchUpdate(final long changeID) {
    return new SentryNoSuchObjectException("nonexistent update + " + changeID);
  }

  /**
   * Gets the last processed change ID for perm/path delta changes.
   *
   * @param pm the PersistenceManager
   * @param changeCls the class of a delta c
   *
   * @return the last processed changedID for the delta changes. If no
   *         change found then return 0.
   */
  static <T extends MSentryChange> Long getLastProcessedChangeIDCore(
      PersistenceManager pm, Class<T> changeCls) {
    return getMaxPersistedIDCore(pm, changeCls, "changeID", EMPTY_CHANGE_ID);
  }

  /**
   * Gets the last processed Notification ID
   * <p>
   * As the table might have zero or one record, result of the query
   * might be null OR instance of MSentryHmsNotification.
   *
   * @param pm the PersistenceManager
   * @return EMPTY_NOTIFICATION_ID(0) when there are no notifications processed.
   * else  last NotificationID processed by HMSFollower
   */
  static Long getLastProcessedNotificationIDCore(
      PersistenceManager pm) {
    return getMaxPersistedIDCore(pm, MSentryHmsNotification.class, "notificationId", EMPTY_NOTIFICATION_ID);
  }

  /**
   * Set the notification ID of last processed HMS notification and remove all
   * subsequent notifications stored.
   */
  public void setLastProcessedNotificationID(final Long notificationId) throws Exception {
    LOGGER.debug("Persisting Last Processed Notification ID {}", notificationId);
    tm.executeTransaction(
      new TransactionBlock<Object>() {
        public Object execute(PersistenceManager pm) throws Exception {
          deleteNotificationsSince(pm, notificationId + 1);
          return pm.makePersistent(new MSentryHmsNotification(notificationId));
        }
      });
  }

  /**
   * Set the notification ID of last processed HMS notification.
   */
  public void persistLastProcessedNotificationID(final Long notificationId) throws Exception {
    LOGGER.debug("Persisting Last Processed Notification ID {}", notificationId);
    tm.executeTransaction(
            new TransactionBlock<Object>() {
              public Object execute(PersistenceManager pm) throws Exception {
                return pm.makePersistent(new MSentryHmsNotification(notificationId));
              }
            });
  }

  /**
   * Gets the last processed change ID for perm delta changes.
   *
   * Internally invoke {@link #getLastProcessedChangeIDCore(PersistenceManager, Class)}
   *
   * @return latest perm change ID.
   */
  public Long getLastProcessedPermChangeID() throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<Long>() {
        public Long execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          return getLastProcessedChangeIDCore(pm, MSentryPermChange.class);
        }
      });
  }

  /**
   * Gets the last processed change ID for path delta changes.
   *
   * @return latest path change ID.
   */
  public Long getLastProcessedPathChangeID() throws Exception {
    return tm.executeTransaction(
    new TransactionBlock<Long>() {
      public Long execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        return getLastProcessedChangeIDCore(pm, MSentryPathChange.class);
      }
    });
  }

  /**
   * Get the notification ID of last processed path delta change.
   *
   * @return the notification ID of latest path change. If no change
   *         found then return 0.
   */
  public Long getLastProcessedNotificationID() throws Exception {
    long notificationId = tm.executeTransaction(
    new TransactionBlock<Long>() {
      public Long execute(PersistenceManager pm) throws Exception {
        long notificationId =  getLastProcessedNotificationIDCore(pm);
        return notificationId;
      }
    });
    LOGGER.debug("Retrieving Last Processed Notification ID {}", notificationId);
    return notificationId;
  }

  /**
   * Gets the last processed HMS snapshot ID for path delta changes.
   *
   * @return latest path change ID.
   */
  public long getLastProcessedImageID() throws Exception {
    return tm.executeTransaction(new TransactionBlock<Long>() {
      @Override
      public Long execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        return getCurrentAuthzPathsSnapshotID(pm);
      }
    });
  }

  /**
   * Get the MSentryPermChange object by ChangeID.
   *
   * @param changeID the given changeID.
   * @return MSentryPermChange
   */
  public MSentryPermChange getMSentryPermChangeByID(final long changeID) throws Exception {
    return (MSentryPermChange) tm.executeTransaction(
      new TransactionBlock<Object>() {
        public Object execute(PersistenceManager pm) throws Exception {
          Query query = pm.newQuery(MSentryPermChange.class);
          query.setFilter("this.changeID == id");
          query.declareParameters("long id");
          List<MSentryPermChange> permChanges = (List<MSentryPermChange>)query.execute(changeID);
          if (permChanges == null) {
            throw noSuchUpdate(changeID);
          }
          if (permChanges.size() > 1) {
            throw new Exception("Inconsistent permission delta: " + permChanges.size()
                + " permissions for the same id, " + changeID);
          }

          return permChanges.get(0);
        }
      });
  }

  /**
   * Fetch all {@link MSentryChange} in the database.
   *
   * @param cls the class of the Sentry delta change.
   * @return a list of Sentry delta changes.
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  private <T extends MSentryChange> List<T> getMSentryChanges(final Class<T> cls)
      throws Exception {
    return tm.executeTransaction(
        new TransactionBlock<List<T>>() {
          public List<T> execute(PersistenceManager pm) throws Exception {
            Query query = pm.newQuery(cls);
            return (List<T>) query.execute();
          }
        });
  }

  /**
   * Fetch all {@link MSentryPermChange} in the database. It should only be used in the tests.
   *
   * @return a list of permission changes.
   * @throws Exception
   */
  @VisibleForTesting
  List<MSentryPermChange> getMSentryPermChanges() throws Exception {
    return getMSentryChanges(MSentryPermChange.class);
  }

  /**
   * Fetch all {@link MSentryHmsNotification} in the database. It should only be used in the tests.
   *
   * @return a list of notifications ids.
   * @throws Exception
   */
  @VisibleForTesting
  List<MSentryHmsNotification> getMSentryHmsNotificationCore() throws Exception {
    return tm.executeTransaction(
      new TransactionBlock<List<MSentryHmsNotification>>() {
        public List<MSentryHmsNotification> execute(PersistenceManager pm) throws Exception {
          Query query = pm.newQuery(MSentryHmsNotification.class);
          return (List<MSentryHmsNotification>) query.execute();
        }
      });
  }

    /**
   * Checks if any MSentryChange object exists with the given changeID.
   *
   * @param pm PersistenceManager
   * @param changeCls class instance of type {@link MSentryChange}
   * @param changeID changeID
   * @return true if found the MSentryChange object, otherwise false.
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  private <T extends MSentryChange> Boolean changeExistsCore(
          PersistenceManager pm, Class<T> changeCls, final long changeID)
              throws Exception {
    Query query = pm.newQuery(changeCls);
    query.addExtension(LOAD_RESULTS_AT_COMMIT, "false");
    query.setFilter("this.changeID == id");
    query.declareParameters("long id");
    List<T> changes = (List<T>)query.execute(changeID);
    return !changes.isEmpty();
  }

  /**
   * Checks if any MSentryPermChange object exists with the given changeID.
   *
   * @param changeID
   * @return true if found the MSentryPermChange object, otherwise false.
   * @throws Exception
   */
  public Boolean permChangeExists(final long changeID) throws Exception {
    return tm.executeTransaction(
    new TransactionBlock<Boolean>() {
      public Boolean execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        return changeExistsCore(pm, MSentryPermChange.class, changeID);
      }
    });
  }

  /**
   * Checks if any MSentryPathChange object exists with the given changeID.
   *
   * @param changeID
   * @return true if found the MSentryPathChange object, otherwise false.
   * @throws Exception
   */
  public Boolean pathChangeExists(final long changeID) throws Exception {
    return tm.executeTransaction(
    new TransactionBlock<Boolean>() {
      public Boolean execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false); // No need to detach objects
        return changeExistsCore(pm, MSentryPathChange.class, changeID);
      }
    });
  }

  /**
   * Gets the MSentryPathChange object by ChangeID.
   *
   * @param changeID the given changeID
   * @return the MSentryPathChange object with corresponding changeID.
   * @throws Exception
   */
  public MSentryPathChange getMSentryPathChangeByID(final long changeID) throws Exception {
    return (MSentryPathChange) tm.executeTransaction(
      new TransactionBlock<Object>() {
        public Object execute(PersistenceManager pm) throws Exception {
          Query query = pm.newQuery(MSentryPathChange.class);
          query.setFilter("this.changeID == id");
          query.declareParameters("long id");
          List<MSentryPathChange> pathChanges = (List<MSentryPathChange>)query.execute(changeID);
          if (pathChanges == null) {
            throw noSuchUpdate(changeID);
          }
          if (pathChanges.size() > 1) {
            throw new Exception("Inconsistent path delta: " + pathChanges.size()
                + " paths for the same id, " + changeID);
          }

          return pathChanges.get(0);
        }
      });
  }

  /**
   * Fetch all {@link MSentryPathChange} in the database. It should only be used in the tests.
   */
  @VisibleForTesting
  List<MSentryPathChange> getMSentryPathChanges() throws Exception {
    return getMSentryChanges(MSentryPathChange.class);
  }

  /**
   * Gets a list of MSentryChange objects greater than or equal to the given changeID.
   *
   * @param changeID
   * @return a list of MSentryChange objects. It can returns an empty list.
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  private <T extends MSentryChange> List<T> getMSentryChangesCore(PersistenceManager pm,
      Class<T> changeCls, final long changeID) throws Exception {
    Query query = pm.newQuery(changeCls);
    query.setFilter("this.changeID >= t");
    query.declareParameters("long t");
    query.setOrdering("this.changeID ascending");
    return (List<T>) query.execute(changeID);
  }

  /**
   * Gets a list of MSentryPathChange objects greater than or equal to the given changeID.
   * If there is any path delta missing in {@link MSentryPathChange} table, an empty list is returned.
   *
   * @param changeID  Requested changeID
   * @return a list of MSentryPathChange objects. May be empty.
   * @throws Exception
   */
  public List<MSentryPathChange> getMSentryPathChanges(final long changeID)
          throws Exception {
    return tm.executeTransaction(new TransactionBlock<List<MSentryPathChange>>() {
      public List<MSentryPathChange> execute(PersistenceManager pm) throws Exception {
        // 1. We first retrieve the entire list of latest delta changes since the changeID
        List<MSentryPathChange> pathChanges =
                getMSentryChangesCore(pm, MSentryPathChange.class, changeID);
        // 2. We then check for consistency issues with delta changes
        if (validateDeltaChanges(changeID, pathChanges)) {
          // If everything is in order, return the delta changes
          return pathChanges;
        }

        // Looks like delta change validation failed. Return an empty list.
        return Collections.emptyList();
      }
    });
  }

  /**
   * Gets a list of MSentryPermChange objects greater than or equal to the given ChangeID.
   * If there is any path delta missing in {@link MSentryPermChange} table, an empty list is returned.
   *
   * @param changeID Requested changeID
   * @return a list of MSentryPathChange objects. May be empty.
   * @throws Exception
   */
  public List<MSentryPermChange> getMSentryPermChanges(final long changeID)
      throws Exception {
    return tm.executeTransaction(new TransactionBlock<List<MSentryPermChange>>() {
      public List<MSentryPermChange> execute(PersistenceManager pm) throws Exception {
        // 1. We first retrieve the entire list of latest delta changes since the changeID
        List<MSentryPermChange> permChanges =
            getMSentryChangesCore(pm, MSentryPermChange.class, changeID);
        // 2. We then check for consistency issues with delta changes
        if (validateDeltaChanges(changeID, permChanges)) {
          // If everything is in order, return the delta changes
          return permChanges;
        }

        // Looks like delta change validation failed. Return an empty list.
        return Collections.emptyList();
      }
    });
  }

  /**
   * Validate if the delta changes are consistent with the requested changeID.
   * <p>
   *   1. Checks if the first delta changeID matches the requested changeID
   *   (This verifies the delta table still has entries starting from the changeID) <br/>
   *   2. Checks if there are any holes in the list of delta changes <br/>
   * </p>
   * @param changeID Requested changeID
   * @param changes List of delta changes
   * @return True if the delta changes are all consistent otherwise returns False
   */
  public <T extends MSentryChange> boolean validateDeltaChanges(final long changeID, List<T> changes) {
    if (changes.isEmpty()) {
      // If database has nothing more recent than CHANGE_ID return True
      return true;
    }

    // The first delta change from the DB should match the changeID
    // If it doesn't, then it means the delta table no longer has entries starting from the
    // requested CHANGE_ID
    if (changes.get(0).getChangeID() != changeID) {
      LOGGER.debug(String.format("Starting delta change from %s is off from the requested id. " +
          "Requested changeID: %s, Missing delta count: %s",
          changes.get(0).getClass().getCanonicalName(), changeID, changes.get(0).getChangeID() - changeID));
      return false;
    }

    // Check for holes in the delta changes.
    if (!MSentryUtil.isConsecutive(changes)) {
      String pathChangesIds = MSentryUtil.collapseChangeIDsToString(changes);
      LOGGER.error(String.format("Certain delta is missing in %s! The table may get corrupted. " +
          "Start changeID %s, Current size of elements = %s. path changeID list: %s",
          changes.get(0).getClass().getCanonicalName(), changeID, changes.size(), pathChangesIds));
      return false;
    }

    return true;
  }

  /**
   * Execute actual Perm/Path action transaction, e.g dropSentryRole, and persist corresponding
   * Update in a single transaction if persistUpdateDeltas is true.
   * Note that this method only applies to TransactionBlock that
   * does not have any return value.
   * <p>
   * Failure in any TransactionBlock would cause the whole transaction
   * to fail.
   *
   * @param update
   * @param transactionBlock
   * @throws Exception
   */
  private void execute(Update update,
        TransactionBlock<Object> transactionBlock) throws Exception {
    List<TransactionBlock<Object>> tbs = new ArrayList(2);

    if (persistUpdateDeltas) {
      tbs.add(new DeltaTransactionBlock(update));
    }

    tbs.add(transactionBlock);
    tm.executeTransactionBlocksWithRetry(tbs);
  }

  /**
   * Checks if a notification was already processed by searching for the hash value
   * on the MSentryPathChange table.
   *
   * @param hash A SHA-1 hex hash that represents a unique notification
   * @return True if the notification was already processed; False otherwise
   */
  public boolean isNotificationProcessed(final String hash) throws Exception {
    return tm.executeTransactionWithRetry(new TransactionBlock<Boolean>() {
      @Override
      public Boolean execute(PersistenceManager pm) throws Exception {
        pm.setDetachAllOnCommit(false);
        Query query = pm.newQuery(MSentryPathChange.class);
        query.setFilter("this.notificationHash == hash");
        query.setUnique(true);
        query.declareParameters("java.lang.String hash");
        MSentryPathChange changes = (MSentryPathChange) query.execute(hash);

        return changes != null;
      }
    });
  }
}
