/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.api.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_ALTER_WITH_POLICY_STORE;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.TSentryAuthorizable;
import org.apache.sentry.api.service.thrift.TSentryGrantOption;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;

import com.google.common.collect.Lists;
import org.apache.sentry.service.common.ServiceConstants;
import org.slf4j.Logger;

public final class SentryServiceUtil {

  private static boolean firstCallHDFSSyncEnabled = true;
  private static boolean hdfsSyncEnabled = false;

  // parse the privilege in String and get the TSentryPrivilege as result
  public static TSentryPrivilege convertToTSentryPrivilege(String privilegeStr) {
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    for (String authorizable : SentryConstants.AUTHORIZABLE_SPLITTER.split(privilegeStr)) {
      KeyValue tempKV = new KeyValue(authorizable);
      String key = tempKV.getKey();
      String value = tempKV.getValue();

      if (PolicyFileConstants.PRIVILEGE_SERVER_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setServerName(value);
      } else if (PolicyFileConstants.PRIVILEGE_DATABASE_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setDbName(value);
      } else if (PolicyFileConstants.PRIVILEGE_TABLE_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setTableName(value);
      } else if (PolicyFileConstants.PRIVILEGE_COLUMN_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setColumnName(value);
      } else if (PolicyFileConstants.PRIVILEGE_URI_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setURI(value);
      } else if (PolicyFileConstants.PRIVILEGE_ACTION_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setAction(value);
      } else if (PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME.equalsIgnoreCase(key)) {
        TSentryGrantOption grantOption = "true".equalsIgnoreCase(value) ? TSentryGrantOption.TRUE
            : TSentryGrantOption.FALSE;
        tSentryPrivilege.setGrantOption(grantOption);
      }
    }
    tSentryPrivilege.setPrivilegeScope(getPrivilegeScope(tSentryPrivilege));
    return tSentryPrivilege;
  }

  /**
   * Parse the object path from string to map.
   * @param objectPath the string format as db=db1->table=tbl1
   * @return Map
   */
  public static Map<String, String> parseObjectPath(String objectPath) {
    Map<String, String> objectMap = new HashMap<String, String>();
    if (StringUtils.isEmpty(objectPath)) {
      return objectMap;
    }
    for (String kvStr : SentryConstants.AUTHORIZABLE_SPLITTER.split(objectPath)) {
      KeyValue kv = new KeyValue(kvStr);
      String key = kv.getKey();
      String value = kv.getValue();

      if (PolicyFileConstants.PRIVILEGE_DATABASE_NAME.equalsIgnoreCase(key)) {
        objectMap.put(PolicyFileConstants.PRIVILEGE_DATABASE_NAME, value);
      } else if (PolicyFileConstants.PRIVILEGE_TABLE_NAME.equalsIgnoreCase(key)) {
        objectMap.put(PolicyFileConstants.PRIVILEGE_TABLE_NAME, value);
      }
    }
    return objectMap;
  }

  // for the different hierarchy for hive:
  // 1: server->url
  // 2: server->database->table->column
  // if both of them are found in the privilege string, the privilege scope will be set as
  // PrivilegeScope.URI
  public static String getPrivilegeScope(TSentryPrivilege tSentryPrivilege) {
    PrivilegeScope privilegeScope = PrivilegeScope.SERVER;
    if (!StringUtils.isEmpty(tSentryPrivilege.getURI())) {
      privilegeScope = PrivilegeScope.URI;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getColumnName())) {
      privilegeScope = PrivilegeScope.COLUMN;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getTableName())) {
      privilegeScope = PrivilegeScope.TABLE;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getDbName())) {
      privilegeScope = PrivilegeScope.DATABASE;
    }
    return privilegeScope.toString();
  }

  // convert TSentryPrivilege to privilege in string
  public static String convertTSentryPrivilegeToStr(TSentryPrivilege tSentryPrivilege) {
    List<String> privileges = Lists.newArrayList();
    if (tSentryPrivilege != null) {
      String serverName = tSentryPrivilege.getServerName();
      String dbName = tSentryPrivilege.getDbName();
      String tableName = tSentryPrivilege.getTableName();
      String columnName = tSentryPrivilege.getColumnName();
      String uri = tSentryPrivilege.getURI();
      String action = tSentryPrivilege.getAction();
      String grantOption = (tSentryPrivilege.getGrantOption() == TSentryGrantOption.TRUE ? "true"
          : "false");
      if (!StringUtils.isEmpty(serverName)) {
        privileges.add(SentryConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_SERVER_NAME,
            serverName));
        if (!StringUtils.isEmpty(uri)) {
          privileges.add(SentryConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_URI_NAME,
              uri));
        } else if (!StringUtils.isEmpty(dbName)) {
          privileges.add(SentryConstants.KV_JOINER.join(
              PolicyFileConstants.PRIVILEGE_DATABASE_NAME, dbName));
          if (!StringUtils.isEmpty(tableName)) {
            privileges.add(SentryConstants.KV_JOINER.join(
                PolicyFileConstants.PRIVILEGE_TABLE_NAME, tableName));
            if (!StringUtils.isEmpty(columnName)) {
              privileges.add(SentryConstants.KV_JOINER.join(
                  PolicyFileConstants.PRIVILEGE_COLUMN_NAME, columnName));
            }
          }
        }
        if (!StringUtils.isEmpty(action)) {
          privileges.add(SentryConstants.KV_JOINER.join(
              PolicyFileConstants.PRIVILEGE_ACTION_NAME, action));
        }
      }
      // only append the grant option to privilege string if it's true
      if ("true".equals(grantOption)) {
        privileges.add(SentryConstants.KV_JOINER.join(
            PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME, grantOption));
      }
    }
    return SentryConstants.AUTHORIZABLE_JOINER.join(privileges);
  }

  /**
   * Gracefully shut down an Executor service.
   * <p>
   * This code is based on the Javadoc example for the Executor service.
   * <p>
   * First call shutdown to reject incoming tasks, and then call
   * shutdownNow, if necessary, to cancel any lingering tasks.
   *
   * @param pool the executor service to shut down
   * @param poolName the name of the executor service to shut down to make it easy for debugging
   * @param timeout the timeout interval to wait for its termination
   * @param unit the unit of the timeout
   * @param logger the logger to log the error message if it cannot terminate. It could be null
   */
  public static void shutdownAndAwaitTermination(ExecutorService pool, String poolName,
                       long timeout, TimeUnit unit, Logger logger) {
    Preconditions.checkNotNull(pool);

    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(timeout, unit)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if ((!pool.awaitTermination(timeout, unit)) && (logger != null)) {
          logger.error("Executor service {} did not terminate",
              StringUtils.defaultIfBlank(poolName, "null"));
        }
      }
    } catch (InterruptedException ignored) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Check if Sentry is configured with HDFS sync enabled. Cache the result
   *
   * @param conf The Configuration object where HDFS sync configurations are set.
   * @return True if enabled; False otherwise.
   */
  public static boolean isHDFSSyncEnabled(Configuration conf) {
    if (firstCallHDFSSyncEnabled) {
      List<String> processorFactories =
          Arrays.asList(conf.get(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "").split(","));

      List<String> policyStorePlugins =
          Arrays.asList(
              conf.get(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "").split(","));

      hdfsSyncEnabled =
          processorFactories.contains("org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory")
              && policyStorePlugins.contains("org.apache.sentry.hdfs.SentryPlugin");
      firstCallHDFSSyncEnabled = false;
    }

    return hdfsSyncEnabled;
  }

    /**
     * Check if Sentry is configured with HDFS sync enabled without caching the result
     *
     * @param conf The Configuration object where HDFS sync configurations are set.
     * @return True if enabled; False otherwise.
     */
  public static boolean isHDFSSyncEnabledNoCache(Configuration conf) {

    List<String> processorFactories =
        Arrays.asList(conf.get(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "").split(","));

    List<String> policyStorePlugins =
        Arrays.asList(
            conf.get(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "").split(","));

    hdfsSyncEnabled =
        processorFactories.contains("org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory")
            && policyStorePlugins.contains("org.apache.sentry.hdfs.SentryPlugin");


    return hdfsSyncEnabled;
  }

  /**
   * Check if Sentry is configured with policy store sync enabled
   * @param conf
   * @return True if enabled; False otherwise
   */
  public static boolean isSyncPolicyStoreEnabled(Configuration conf) {
    boolean syncStoreOnCreate;
    boolean syncStoreOnDrop;
    boolean syncStoreOnAlter;

    syncStoreOnCreate  = Boolean
        .parseBoolean(conf.get(AUTHZ_SYNC_CREATE_WITH_POLICY_STORE.getVar(),
            AUTHZ_SYNC_CREATE_WITH_POLICY_STORE.getDefault()));
    syncStoreOnDrop = Boolean.parseBoolean(conf.get(AUTHZ_SYNC_DROP_WITH_POLICY_STORE.getVar(),
        AUTHZ_SYNC_DROP_WITH_POLICY_STORE.getDefault()));
    syncStoreOnAlter = Boolean.parseBoolean(conf.get(AUTHZ_SYNC_ALTER_WITH_POLICY_STORE.getVar(),
        AUTHZ_SYNC_ALTER_WITH_POLICY_STORE.getDefault()));

    return syncStoreOnCreate || syncStoreOnDrop || syncStoreOnAlter;
  }

  public static String getHiveMetastoreURI() {
    HiveConf hiveConf = new HiveConf();
    return hiveConf.get(ConfVars.METASTOREURIS.varname);
  }

  /**
   * Derives object name from database and table names by concatenating them
   *
   * @param authorizable for which is name is to be derived
   * @return authorizable name
   * @throws SentryInvalidInputException if argument provided does not have all the
   *                                     required fields set.
   */
  public static String getAuthzObj(TSentryAuthorizable authorizable)
    throws SentryInvalidInputException {
    return getAuthzObj(authorizable.getDb(), authorizable.getTable());
  }

  /**
   * Derives object name from database and table names by concatenating them
   *
   * @param dbName
   * @param tblName
   * @return authorizable name
   * @throws SentryInvalidInputException if argument provided does not have all the
   *                                     required fields set.
   */
  public static String getAuthzObj(String dbName, String tblName)
    throws SentryInvalidInputException {
    if (isNULL(dbName)) {
      throw new SentryInvalidInputException("Invalif input, DB name is missing");
    }
    return isNULL(tblName) ? dbName.toLowerCase() :
      (dbName + "." + tblName).toLowerCase();
  }

  private SentryServiceUtil() {
    // Make constructor private to avoid instantiation
  }

  private static boolean isNULL(String s) {
    return Strings.isNullOrEmpty(s) || s.equals("__NULL__");
  }
}
