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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.sentry.binding.hive.HiveAuthzBindingHook;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/***
 * This class is the wrapper of ObjectStore which is the interface between the
 * application logic and the database store. Do the authorization or filter the
 * result when processing the metastore request.
 * eg:
 * Callers will only receive the objects back which they have privileges to
 * access.
 * If there is a request for the object list(like getAllTables()), the result
 * will be filtered to exclude object the requestor doesn't have privilege to
 * access.
 */
public class AuthorizingObjectStore extends ObjectStore {
  private static ImmutableSet<String> serviceUsers;
  private static HiveConf hiveConf;
  private static HiveAuthzConf authzConf;
  private static HiveAuthzBinding hiveAuthzBinding;
  private static String NO_ACCESS_MESSAGE_TABLE = "Table does not exist or insufficient privileges to access: ";
  private static String NO_ACCESS_MESSAGE_DATABASE = "Database does not exist or insufficient privileges to access: ";

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    return filterDatabases(super.getDatabases(pattern));
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    return filterDatabases(super.getAllDatabases());
  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    Database db = super.getDatabase(name);
    try {
      if (filterDatabases(Lists.newArrayList(name)).isEmpty()) {
        throw new NoSuchObjectException(getNoAccessMessageForDB(name));
      }
    } catch (MetaException e) {
      throw new NoSuchObjectException("Failed to authorized access to " + name
          + " : " + e.getMessage());
    }
    return db;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    Table table = super.getTable(dbName, tableName);
    if (table == null
        || filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
      return null;
    }
    return table;
  }

  @Override
  public Partition getPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new NoSuchObjectException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.getPartition(dbName, tableName, part_vals);
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName,
      int maxParts) throws MetaException, NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.getPartitions(dbName, tableName, maxParts);
  }

  @Override
  public List<String> getTables(String dbName, String pattern)
      throws MetaException {
    return filterTables(dbName, super.getTables(dbName, pattern));
  }
  
  @Override
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException {
    return super.getTableObjectsByName(dbname, filterTables(dbname, tableNames));
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    return filterTables(dbName, super.getAllTables(dbName));
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter,
      short maxTables) throws MetaException {
    return filterTables(dbName,
        super.listTableNamesByFilter(dbName, filter, maxTables));
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tableName,
      short max_parts) throws MetaException {
    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.listPartitionNames(dbName, tableName, max_parts);
  }

  @Override
  public List<String> listPartitionNamesByFilter(String dbName,
      String tableName, String filter, short max_parts) throws MetaException {
    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.listPartitionNamesByFilter(dbName, tableName, filter,
        max_parts);
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    if (filterTables(dbName, Lists.newArrayList(origTableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, origTableName));
    }
    return super.getIndex(dbName, origTableName, indexName);
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {
    if (filterTables(dbName, Lists.newArrayList(origTableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, origTableName));
    }
    return super.getIndexes(dbName, origTableName, max);
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName,
      short max) throws MetaException {
    if (filterTables(dbName, Lists.newArrayList(origTableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, origTableName));
    }
    return super.listIndexNames(dbName, origTableName, max);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName,
      String tblName, String filter, short maxParts) throws MetaException,
      NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionsByFilter(dbName, tblName, filter, maxParts);
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionsByNames(dbName, tblName, partNames);
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionWithAuth(dbName, tblName, partVals, user_name,
        group_names);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionsWithAuth(dbName, tblName, maxParts, userName,
        groupNames);
  }

  @Override
  public List<String> listPartitionNamesPs(String dbName, String tblName,
      List<String> part_vals, short max_parts) throws MetaException,
      NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.listPartitionNamesPs(dbName, tblName, part_vals, max_parts);
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String dbName,
      String tblName, List<String> part_vals, short max_parts, String userName,
      List<String> groupNames) throws MetaException, InvalidObjectException,
      NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.listPartitionsPsWithAuth(dbName, tblName, part_vals,
        max_parts, userName, groupNames);
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName,
      String tableName, List<String> colNames) throws MetaException,
      NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.getTableColumnStatistics(dbName, tableName, colNames);
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(
      String dbName, String tblName, List<String> partNames,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionColumnStatistics(dbName, tblName, partNames,
        colNames);
  }

  /**
   * Invoke Hive database filtering that removes the entries which use has no
   * privileges to access
   * @param dbList
   * @return
   * @throws MetaException
   */
  private List<String> filterDatabases(List<String> dbList)
      throws MetaException {
    if (needsAuthorization(getUserName())) {
      try {
        return HiveAuthzBindingHook.filterShowDatabases(getHiveAuthzBinding(),
            dbList, HiveOperation.SHOWDATABASES, getUserName());
      } catch (SemanticException e) {
        throw new MetaException("Error getting DB list " + e.getMessage());
      }
    } else {
      return dbList;
    }
  }

  /**
   * Invoke Hive table filtering that removes the entries which use has no
   * privileges to access
   * @param dbList
   * @return
   * @throws MetaException
   */
  private List<String> filterTables(String dbName, List<String> tabList)
      throws MetaException {
    if (needsAuthorization(getUserName())) {
      try {
        return HiveAuthzBindingHook.filterShowTables(getHiveAuthzBinding(),
            tabList, HiveOperation.SHOWTABLES, getUserName(), dbName);
      } catch (SemanticException e) {
        throw new MetaException("Error getting Table list " + e.getMessage());
      }
    } else {
      return tabList;
    }
  }

  /**
   * load Hive auth provider
   * 
   * @return
   * @throws MetaException
   */
  private HiveAuthzBinding getHiveAuthzBinding() throws MetaException {
    if (hiveAuthzBinding == null) {
      try {
        hiveAuthzBinding = new HiveAuthzBinding(HiveAuthzBinding.HiveHook.HiveMetaStore,
            getHiveConf(), getAuthzConf());
      } catch (Exception e) {
        throw new MetaException("Failed to load Hive binding " + e.getMessage());
      }
    }
    return hiveAuthzBinding;
  }

  private ImmutableSet<String> getServiceUsers() throws MetaException {
    if (serviceUsers == null) {
      serviceUsers = ImmutableSet.copyOf(toTrimed(Sets.newHashSet(getAuthzConf().getStrings(
          AuthzConfVars.AUTHZ_METASTORE_SERVICE_USERS.getVar(), new String[] { "" }))));
    }
    return serviceUsers;
  }

  private HiveConf getHiveConf() {
    if (hiveConf == null) {
      hiveConf = new HiveConf(getConf(), this.getClass());
    }
    return hiveConf;
  }

  private HiveAuthzConf getAuthzConf() throws MetaException {
    if (authzConf == null) {
      String hiveAuthzConf = getConf().get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
      if (hiveAuthzConf == null
          || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
        throw new MetaException("Configuration key "
            + HiveAuthzConf.HIVE_SENTRY_CONF_URL + " value '" + hiveAuthzConf
            + "' is invalid.");
      }
      try {
        authzConf = new HiveAuthzConf(new URL(hiveAuthzConf));
      } catch (MalformedURLException e) {
        throw new MetaException("Configuration key "
            + HiveAuthzConf.HIVE_SENTRY_CONF_URL
            + " specifies a malformed URL '" + hiveAuthzConf + "' "
            + e.getMessage());
      }
    }
    return authzConf;
  }

  /**
   * Extract the user from underlying auth subsystem
   * @return
   * @throws MetaException
   */
  private String getUserName() throws MetaException {
    try {
      return Utils.getUGI().getShortUserName();
    } catch (LoginException e) {
      throw new MetaException("Failed to get username " + e.getMessage());
    } catch (IOException e) {
      throw new MetaException("Failed to get username " + e.getMessage());
    }
  }

  /**
   * Check if the give user needs to be validated.
   * @param userName
   * @return
   */
  private boolean needsAuthorization(String userName) throws MetaException {
    return !getServiceUsers().contains(userName.trim());
  }

  private static Set<String> toTrimed(Set<String> s) {
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim());
    }
    return result;
  }

  private String getNoAccessMessageForTable(String dbName, String tableName) {
    return NO_ACCESS_MESSAGE_TABLE + "<" + dbName + ">.<" + tableName + ">";
  }

  private String getNoAccessMessageForDB(String dbName) {
    return NO_ACCESS_MESSAGE_DATABASE + "<" + dbName + ">";
  }
}
