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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter.ObjectExtractor;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.thrift.TException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class SentryHiveMetaStoreClient extends HiveMetaStoreClient implements
    IMetaStoreClient {

  private HiveAuthzBinding hiveAuthzBinding;
  private HiveAuthzConf authzConf;

  public SentryHiveMetaStoreClient(HiveConf conf) throws MetaException {
    super(conf);
  }

  public SentryHiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader)
      throws MetaException {
    super(conf, hookLoader, true);
  }

  @Override
  public List<String> getDatabases(String databasePattern) throws MetaException {
    return filterDatabases(super.getDatabases(databasePattern));
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    return filterDatabases(super.getAllDatabases());
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern)
      throws MetaException {
    return filterTables(dbName, super.getTables(dbName, tablePattern));
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    return filterTables(dbName, super.getAllTables(dbName));
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter,
      short maxTables) throws InvalidOperationException, UnknownDBException,
      TException {
    return filterTables(dbName,
        super.listTableNamesByFilter(dbName, filter, maxTables));
  }

  /**
   * Invoke Hive database filtering that removes the entries which use has no
   * privileges to access
   *
   * @param dbList
   * @return
   * @throws MetaException
   */
  private List<String> filterDatabases(List<String> dbList)
      throws MetaException {
    try {
      MetastoreAuthzObjectFilter<String> filter = new MetastoreAuthzObjectFilter<>(
        getHiveAuthzBinding(), new ObjectExtractor<String>() {
        @Override
        public String getDatabaseName(String o) {
          return o;
        }

        @Override
        public String getTableName(String o) {
          return null;
        }
      });

      return filter.filterDatabases(getUserName(), dbList);
    } catch (Exception e) {
      throw new MetaException("Error getting DB list " + e.getMessage());
    }
  }

  /**
   * Invoke Hive table filtering that removes the entries which use has no
   * privileges to access
   *
   * @param dbList
   * @return
   * @throws MetaException
   */
  private List<String> filterTables(String dbName, List<String> tabList)
      throws MetaException {
    try {
      MetastoreAuthzObjectFilter<String> filter = new MetastoreAuthzObjectFilter<>(
        getHiveAuthzBinding(), new ObjectExtractor<String>() {
        @Override
        public String getDatabaseName(String o) {
          return dbName;
        }

        @Override
        public String getTableName(String o) {
          return o;
        }
      });

      return filter.filterTables(getUserName(), tabList);
    } catch (Exception e) {
      throw new MetaException("Error getting Table list " + e.getMessage());
    }
  }

  private String getUserName() {
    return getConf().get(HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME);
  }

  /**
   * load Hive auth provider
   *
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

}
