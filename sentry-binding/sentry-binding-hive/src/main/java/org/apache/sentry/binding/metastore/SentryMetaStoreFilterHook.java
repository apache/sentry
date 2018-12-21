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
package org.apache.sentry.binding.metastore;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding.HiveHook;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter;

import java.util.List;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter.ObjectExtractor;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;

/**
 * {@code} SentryMetaStoreFilterHook} may be used by the HMS server to filter databases, tables
 * and partitions that are authorized to be seen by a user making the HMS request. Usage on the
 * {@link org.apache.hadoop.hive.metastore.HiveMetaStoreClient} was dropped when Hive authz V2 was
 * added in Hive v2.x and higher.
 *
 * <p/>
 * Connections to HMS are usually done as admins (or any of the Sentry service users), so this
 * class will not filter anything; but others component, such as Spark, can commonly make
 * this requests as a normal user, which require a proper authorization to to return only those
 * objects that the user is able to see.
 */
public class SentryMetaStoreFilterHook implements MetaStoreFilterHook {
  static final protected Log LOG = LogFactory.getLog(SentryMetaStoreFilterHook.class);

  private final HiveConf hiveConf;
  private HiveAuthzBinding hiveAuthzBinding;
  private HiveAuthzBindingFactory authzBindingFactory;
  private HiveAuthzConf authzConf;
  private Set<String> serviceUsers;

  /**
   * Instatiates a new {@code SentryMetaStoreFilterHook} object with a default
   * {@code HiveAuthzBindingFactory} implementation that calls the Sentry server to filter
   * the Metastore objects.
   *
   * @param hiveConf The HiveConf object that contains configuration to connect to Sentry.
   */
  public SentryMetaStoreFilterHook(HiveConf hiveConf) {
    this(hiveConf, HiveAuthzConf.getAuthzConf(hiveConf), new HiveAuthzBindingFactory() {
      @Override
      public HiveAuthzBinding fromMetaStoreConf(HiveConf hiveConf, HiveAuthzConf authzConf) throws Exception {
        return new HiveAuthzBinding(HiveHook.HiveMetaStore, hiveConf, authzConf);
      }

      @Override
      public String getUserName() {
        try {
          /*
           * Returns the HMS username by looking intto the UGI class. This is called during
           * the filtering calls (not in the constructor) because HMS may do these requests
           * with different users set in the UGI.
           */
          return Utils.getUGI().getShortUserName();
        } catch (Exception e) {
          throw new RuntimeException("Unable to get the HMS username: " + e.getMessage());
        }
      }
    });
  }

  /**
   * Instatiates a new {@code SentryMetaStoreFilterHook} object with a specific
   * {@code HiveAuthzBindingFactory} implementation to get authorization to filter the Metastore
   * objects.
   *
   * @param hiveConf The HiveConf object that contains configuration to connect to Sentry.
   * @param authzConf The HiveAuthzConf object that contains Sentry settings.
   * @param authzBindingFactory An implementation to get the sentry/hive binding object to get
   * authorization to filter the Metastore objects.
   */
  public SentryMetaStoreFilterHook(HiveConf hiveConf, HiveAuthzConf authzConf, HiveAuthzBindingFactory authzBindingFactory) {
    this.hiveConf = hiveConf;
    this.authzConf = authzConf;
    this.authzBindingFactory = authzBindingFactory;

    // Users must be case sensitive to be compatible with Hadoop and Unix user names.
    this.serviceUsers = Sets.newHashSet(authzConf.getTrimmedStringCollection(
      HiveAuthzConf.AuthzConfVars.AUTHZ_METASTORE_SERVICE_USERS.getVar()));

    LOG.info("SentryMetaStoreFilterHook initialized with service users: " + this.serviceUsers);
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) {
    return filterDb(dbList);
  }

  @Override
  public Database filterDatabase(Database dataBase)
      throws NoSuchObjectException {
    String name = dataBase.getName();
    if (filterDb(Collections.singletonList(name)).isEmpty()) {
      throw new NoSuchObjectException(String.format("Database %s does not exist", name));
    }

    return dataBase;
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) {
    return filterTab(dbName, tableList);
  }

  @Override
  public Table filterTable(Table table) throws NoSuchObjectException {
    String dbName = table.getDbName();
    String tableName = table.getTableName();

    if (filterTab(dbName, Collections.singletonList(tableName)).isEmpty()) {
      throw new NoSuchObjectException(String.format("Table %s.%s does not exist", dbName, tableName));
    }

    return table;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) {
    return filterTab(tableList);
  }

  // Sentry does not support partition filtering
  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) {
    return partitionList;
  }

  // Sentry does not support partition filtering
  @Override
  public List<PartitionSpec> filterPartitionSpecs(
      List<PartitionSpec> partitionSpecList) {
    return partitionSpecList;
  }

  // Sentry does not support partition filtering
  @Override
  public Partition filterPartition(Partition partition)
      throws NoSuchObjectException {
    return partition;
  }

  // Sentry does not support partition filtering
  @Override
  public List<String> filterPartitionNames(String dbName, String tblName,
      List<String> partitionNames) {
    return partitionNames;
  }

  // Sentry does not support index filtering
  @Override
  public Index filterIndex(Index index) throws NoSuchObjectException {
    return index;
  }

  // Sentry does not support index filtering
  @Override
  public List<String> filterIndexNames(String dbName, String tblName,
      List<String> indexList) {
    return indexList;
  }

  // Sentry does not support index filtering
  @Override
  public List<Index> filterIndexes(List<Index> indexeList) {
    return indexeList;
  }

  /**
   * Invoke Hive database filtering that removes the entries which use has no
   * privileges to access
   * @param dbList
   * @return
   * @throws MetaException
   */
  private List<String> filterDb(List<String> dbList) {
    // If the user is part of the Sentry service user list, then skip the authorization and
    // do not filter the objects.
    if (!needsAuthorization(authzBindingFactory.getUserName())) {
      return dbList;
    }

    try (HiveAuthzBinding authzBinding = getHiveAuthzBinding()) {
      MetastoreAuthzObjectFilter<String> filter = new MetastoreAuthzObjectFilter<>(authzBinding,
        new ObjectExtractor<String>() {
          @Override
          public String getDatabaseName(String o) {
            return o;
          }

          @Override
          public String getTableName(String s) {
            return null;
          }
        });

      return filter.filterDatabases(authzBindingFactory.getUserName(), dbList);
    } catch (Exception e) {
      LOG.warn("Error getting DB list ", e);
      return Collections.emptyList();
    }
  }

  /**
   * Invoke Hive table filtering that removes the entries which use has no
   * privileges to access
   * @param tabList
   * @return
   * @throws MetaException
   */
  private List<String> filterTab(String dbName, List<String> tabList) {
    // If the user is part of the Sentry service user list, then skip the authorization and
    // do not filter the objects.
    if (!needsAuthorization(authzBindingFactory.getUserName())) {
      return tabList;
    }

    try (HiveAuthzBinding authzBinding = getHiveAuthzBinding()) {
      MetastoreAuthzObjectFilter<String> filter = new MetastoreAuthzObjectFilter<>(authzBinding,
        new ObjectExtractor<String>() {
          @Override
          public String getDatabaseName(String o) {
            return dbName;
          }

          @Override
          public String getTableName(String o) {
            return o;
          }
        });

      return filter.filterTables(authzBindingFactory.getUserName(), tabList);
    } catch (Exception e) {
      LOG.warn("Error getting Table list ", e);
      return Collections.emptyList();
    }
  }

  /**
   * Invoke Hive table filtering that removes the entries which use has no
   * privileges to access
   * @param tabList
   * @return
   * @throws MetaException
   */
  private List<Table> filterTab(List<Table> tabList) {
    // If the user is part of the Sentry service user list, then skip the authorization and
    // do not filter the objects.
    if (!needsAuthorization(authzBindingFactory.getUserName())) {
      return tabList;
    }

    try (HiveAuthzBinding authzBinding = getHiveAuthzBinding()) {
      MetastoreAuthzObjectFilter<Table> filter = new MetastoreAuthzObjectFilter<>(authzBinding,
        new ObjectExtractor<Table>() {
          @Override
          public String getDatabaseName(Table o) {
            return (o != null) ? o.getDbName() : null;
          }

          @Override
          public String getTableName(Table o) {
            return (o != null) ? o.getTableName() : null;
          }
        });

      return filter.filterTables(authzBindingFactory.getUserName(), tabList);
    } catch (Exception e) {
      LOG.warn("Error getting Table list ", e);
      return Collections.emptyList();
    }
  }

  /**
   * load Hive auth provider
   * @return
   * @throws MetaException
   */
  private HiveAuthzBinding getHiveAuthzBinding() throws MetaException {
    if (hiveAuthzBinding == null) {
      try {
        hiveAuthzBinding = authzBindingFactory.fromMetaStoreConf(hiveConf, authzConf);
      } catch (Exception e) {
        throw new MetaException("The Sentry/Hive authz binding could not be created: "
          + e.getMessage());
      }
    }

    return hiveAuthzBinding;
  }

  private boolean needsAuthorization(String username) {
    return !serviceUsers.contains(username);
  }
}
