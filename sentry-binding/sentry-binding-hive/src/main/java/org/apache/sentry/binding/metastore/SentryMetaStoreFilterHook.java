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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.HiveAuthzBindingHook;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;

import com.google.common.collect.Lists;

public class SentryMetaStoreFilterHook implements MetaStoreFilterHook {

  static final protected Log LOG = LogFactory.getLog(SentryMetaStoreFilterHook.class);

  private HiveAuthzBinding hiveAuthzBinding;
  private HiveAuthzConf authzConf;
  private final HiveConf hiveConf;

  public SentryMetaStoreFilterHook(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) {
    return filterDb(dbList);
  }

  @Override
  public Database filterDatabase(Database dataBase)
      throws NoSuchObjectException {
    return dataBase;
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) {
    return filterTab(dbName, tableList);
  }

  @Override
  public Table filterTable(Table table) throws NoSuchObjectException {
    return table;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) {
    return tableList;
  }

  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) {
    return partitionList;
  }

  @Override
  public List<PartitionSpec> filterPartitionSpecs(
      List<PartitionSpec> partitionSpecList) {
    return partitionSpecList;
  }

  @Override
  public Partition filterPartition(Partition partition)
      throws NoSuchObjectException {
    return partition;
  }

  @Override
  public List<String> filterPartitionNames(String dbName, String tblName,
      List<String> partitionNames) {
    return partitionNames;
  }

  @Override
  public Index filterIndex(Index index) throws NoSuchObjectException {
    return index;
  }

  @Override
  public List<String> filterIndexNames(String dbName, String tblName,
      List<String> indexList) {
    return null;
  }

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
    try {
      return HiveAuthzBindingHook.filterShowDatabases(getHiveAuthzBinding(),
          dbList, HiveOperation.SHOWDATABASES, getUserName());
    } catch (Exception e) {
      LOG.warn("Error getting DB list ", e);
      return new ArrayList<String>();
    } finally {
      close();
    }
  }

  /**
   * Invoke Hive table filtering that removes the entries which use has no
   * privileges to access
   * @param dbList
   * @return
   * @throws MetaException
   */
  private List<String> filterTab(String dbName, List<String> tabList) {
    try {
      return HiveAuthzBindingHook.filterShowTables(getHiveAuthzBinding(),
          tabList, HiveOperation.SHOWTABLES, getUserName(), dbName);
    } catch (Exception e) {
      LOG.warn("Error getting Table list ", e);
      return new ArrayList<String>();
    } finally {
      close();
    }
  }

  private String getUserName() {
    return getConf().get(HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME);
  }

  /**
   * load Hive auth provider
   * @return
   * @throws MetaException
   */
  private HiveAuthzBinding getHiveAuthzBinding() throws MetaException {
    if (hiveAuthzBinding == null) {
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
      try {
        hiveAuthzBinding = new HiveAuthzBinding(
            HiveAuthzBinding.HiveHook.HiveMetaStore, getConf(), authzConf);
      } catch (Exception e) {
        throw new MetaException("Failed to load Hive binding " + e.getMessage());
      }
    }
    return hiveAuthzBinding;
  }

  private HiveConf getConf() {
    return SessionState.get().getConf();
  }

  private void close() {
    if (hiveAuthzBinding != null) {
      hiveAuthzBinding.close();
      hiveAuthzBinding = null;
    }
  }
}
