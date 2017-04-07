/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fetch full snapshot of {@code <hiveObj, paths>} mappings from Hive.
 * Mappings for different tables are fetched concurrently by multiple threads from a pool.
 */
public final class FullUpdateInitializer implements AutoCloseable {

  private final ExecutorService threadPool;
  private final HiveMetaStoreClient client;
  private final int maxPartitionsPerCall;
  private final int maxTablesPerCall;
  private final Collection<Future<CallResult>> results = new Vector<>();
  private final AtomicInteger taskCounter = new AtomicInteger(0);
  private final int maxRetries;
  private final int waitDurationMillis;
  private final boolean failOnRetry;

  private static final Logger LOGGER = LoggerFactory.getLogger(FullUpdateInitializer.class);

  static final class CallResult {
    private final Exception failure;
    private final boolean successStatus;

    CallResult(Exception ex, boolean successStatus) {
      failure = ex;
      this.successStatus = successStatus;
    }

    boolean success() {
      return successStatus;
    }

    public Exception getFailure() {
      return failure;
    }
  }

  abstract class BaseTask implements Callable<CallResult> {

    /**
     *  Class represents retry strategy for BaseTask.
     */
    private final class RetryStrategy {
      private int retryStrategyMaxRetries = 0;
      private final int retryStrategyWaitDurationMillis;
      private int retries;
      private Exception exception;

      private RetryStrategy(int retryStrategyMaxRetries, int retryStrategyWaitDurationMillis) {
        this.retryStrategyMaxRetries = retryStrategyMaxRetries;
        retries = 0;

        // Assign default wait duration if negative value is provided.
        if (retryStrategyWaitDurationMillis > 0) {
          this.retryStrategyWaitDurationMillis = retryStrategyWaitDurationMillis;
        } else {
          this.retryStrategyWaitDurationMillis = 1000;
        }
      }

      public CallResult exec()  {

        // Retry logic is happening inside callable/task to avoid
        // synchronous waiting on getting the result.
        // Retry the failure task until reach the max retry number.
        // Wait configurable duration for next retry.
        for (int i = 0; i < retryStrategyMaxRetries; i++) {
          try {
            doTask();

            // Task succeeds, reset the exception and return
            // the successful flag.
            exception = null;
            return new CallResult(exception, true);
          } catch (Exception ex) {
            LOGGER.debug("Failed to execute task on " + (i + 1) + " attempts." +
            " Sleeping for " + retryStrategyWaitDurationMillis + " ms. Exception: " + ex.toString(), ex);
            exception = ex;

            try {
              Thread.sleep(retryStrategyWaitDurationMillis);
            } catch (InterruptedException exception) {
              // Skip the rest retries if get InterruptedException.
              // And set the corresponding retries number.
              retries = i;
              i = retryStrategyMaxRetries;
            }
          }

          retries = i;
        }

        // Task fails, return the failure flag.
        LOGGER.error("Task did not complete successfully after " + retries + 1
        + " tries", exception);
        return new CallResult(exception, false);
      }
    }

    private final RetryStrategy retryStrategy;

    BaseTask() {
      taskCounter.incrementAndGet();
      retryStrategy = new RetryStrategy(maxRetries, waitDurationMillis);
    }

    @Override
    public CallResult call() throws Exception {
      CallResult callResult = retryStrategy.exec();
      taskCounter.decrementAndGet();
      return callResult;
    }

    abstract void doTask() throws Exception;
  }

  class PartitionTask extends BaseTask {
    private final String dbName;
    private final String tblName;
    private final List<String> partNames;
    private final TPathChanges tblPathChange;

    PartitionTask(String dbName, String tblName, List<String> partNames,
    TPathChanges tblPathChange) {
      super();
      this.dbName = dbName;
      this.tblName = tblName;
      this.partNames = partNames;
      this.tblPathChange = tblPathChange;
    }

    @Override
    public void doTask() throws Exception {
      List<Partition> tblParts = client.getPartitionsByNames(dbName, tblName, partNames);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### Fetching partitions " +
        "[" + dbName + "." + tblName + "]" + "[" + partNames + "]");
      }
      for (Partition part : tblParts) {
        List<String> partPath = PathsUpdate.parsePath(part.getSd()
        .getLocation());
        if (partPath != null) {
          synchronized (tblPathChange) {
            tblPathChange.addToAddPaths(partPath);
          }
        }
      }
    }
  }

  class TableTask extends BaseTask {
    private final Database db;
    private final List<String> tableNames;
    private final PathsUpdate update;

    TableTask(Database db, List<String> tableNames, PathsUpdate update) {
      super();
      this.db = db;
      this.tableNames = tableNames;
      this.update = update;
    }

    @Override
    public void doTask() throws Exception {
      List<Table> tables = client.getTableObjectsByName(db.getName(), tableNames);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### Fetching tables [" + db.getName() + "][" +
        tableNames + "]");
      }
      for (Table tbl : tables) {
        TPathChanges tblPathChange;
        // Table names are case insensitive
        String tableName = tbl.getTableName().toLowerCase();
        Preconditions.checkArgument(tbl.getDbName().equalsIgnoreCase(db.getName()));
        synchronized (update) {
          tblPathChange = update.newPathChange(db.getName() + "." + tableName);
        }
        if (tbl.getSd().getLocation() != null) {
          List<String> tblPath =
          PathsUpdate.parsePath(tbl.getSd().getLocation());
          if (tblPath != null) {
            tblPathChange.addToAddPaths(tblPath);
          }
          List<String> tblPartNames = client.listPartitionNames(db.getName(), tableName, (short) -1);
          for (int i = 0; i < tblPartNames.size(); i += maxPartitionsPerCall) {
            List<String> partsToFetch =
            tblPartNames.subList(i, Math.min(
            i + maxPartitionsPerCall, tblPartNames.size()));
            Callable<CallResult> partTask =
            new PartitionTask(db.getName(), tableName,
            partsToFetch, tblPathChange);
            results.add(threadPool.submit(partTask));
          }
        }
      }
    }
  }

  class DbTask extends BaseTask {

    private final PathsUpdate update;
    private final String dbName;

    DbTask(PathsUpdate update, String dbName) {
      super();
      this.update = update;
      //Database names are case insensitive
      this.dbName = dbName.toLowerCase();
    }

    @Override
    public void doTask() throws Exception {
      Database db = client.getDatabase(dbName);
      List<String> dbPath = PathsUpdate.parsePath(db.getLocationUri());
      if (dbPath != null) {
        Preconditions.checkArgument(dbName.equalsIgnoreCase(db.getName()));
        synchronized (update) {
          update.newPathChange(dbName).addToAddPaths(dbPath);
        }
      }
      List<String> allTblStr = client.getAllTables(dbName);
      for (int i = 0; i < allTblStr.size(); i += maxTablesPerCall) {
        List<String> tablesToFetch =
        allTblStr.subList(i, Math.min(
        i + maxTablesPerCall, allTblStr.size()));
        Callable<CallResult> tableTask =
        new TableTask(db, tablesToFetch, update);
        results.add(threadPool.submit(tableTask));
      }
    }
  }

  public FullUpdateInitializer(HiveMetaStoreClient client, Configuration conf) {
    this.client = client;
    this.maxPartitionsPerCall = conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC_DEFAULT);
    this.maxTablesPerCall = conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC_DEFAULT);
    threadPool = Executors.newFixedThreadPool(conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS_DEFAULT));
    maxRetries = conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_MAX_NUM,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_MAX_NUM_DEFAULT);
    waitDurationMillis = conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_WAIT_DURAION_IN_MILLIS,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_WAIT_DURAION_IN_MILLIS_DEFAULT);
    failOnRetry = conf.getBoolean(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_FAIL_ON_PARTIAL_UPDATE,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_FAIL_ON_PARTIAL_UPDATE_DEFAULT);
  }

  public Map<String, Set<String>> createInitialUpdate() throws Exception {
    PathsUpdate tempUpdate = new PathsUpdate(-1, false);
    List<String> allDbStr = client.getAllDatabases();
    for (String dbName : allDbStr) {
      Callable<CallResult> dbTask = new DbTask(tempUpdate, dbName);
      results.add(threadPool.submit(dbTask));
    }

    while (taskCounter.get() != 0) {
      // Wait until no more tasks remain
      Thread.sleep(250);
    }

    for (Future<CallResult> result : results) {
      CallResult callResult = result.get();

      // Fail the HMS startup if tasks are not all successful and
      // fail on partial updates flag is set in the config.
      if (!callResult.success() && failOnRetry) {
        throw callResult.getFailure();
      }
    }

    return getAuthzObjToPathMapping(tempUpdate);
  }


  /**
   * Parsing a pathsUpdate to get the mapping of hiveObj -> [Paths].
   * It only processes {@link TPathChanges}.addPaths, since
   * {@link FullUpdateInitializer} only add paths when fetching
   * full HMS Paths snapshot. Each path represented as path tree
   * concatenated by "/". e.g 'usr/hive/warehouse'.
   *
   * @return mapping of hiveObj -> [Paths].
   */
  private Map<String, Set<String>> getAuthzObjToPathMapping(PathsUpdate pathsUpdate) {
    List<TPathChanges> tPathChanges = pathsUpdate.getPathChanges();
    if (tPathChanges.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Set<String>> authzObjToPath = new HashMap<>(tPathChanges.size());

    for (TPathChanges pathChanges : tPathChanges) {
      // Only processes TPathChanges.addPaths
      List<List<String>> addPaths = pathChanges.getAddPaths();
      Set<String> paths = new HashSet<>(addPaths.size());
      for (List<String> addPath : addPaths) {
        paths.add(PathsUpdate.cancatePath(addPath));
      }
      authzObjToPath.put(pathChanges.getAuthzObj(), paths);
    }

    return authzObjToPath;
  }

  @Override
  public void close() {
    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }
}
