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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Manage fetching full snapshot from HMS.
 * Snapshot is represented as a map from the hive object name to
 * the set of paths for this object.
 * The hive object name is either the Hive database name or
 * Hive database name joined with Hive table name as {@code dbName.tableName}.
 * All table partitions are stored under the table object.
 * <p>
 * Once {@link FullUpdateInitializer}, the {@link FullUpdateInitializer#getFullHMSSnapshot()}
 * method should be called to get the initial update.
 * <p>
 * It is important to close the {@link FullUpdateInitializer} object to prevent resource
 * leaks.
 * <p>
 * The usual way of using {@link FullUpdateInitializer} is
 * <pre>
 * {@code
 * try (FullUpdateInitializer updateInitializer =
 *      new FullUpdateInitializer(client, authzConf)) {
 *         Map<String, Set<String>> pathsUpdate = updateInitializer.getFullHMSSnapshot();
 *      return pathsUpdate;
 * }
 */
public final class FullUpdateInitializer implements AutoCloseable {

  /*
   * Implementation note.
   *
   * The snapshot is obtained using an executor. We follow the map/reduce model.
   * Each executor thread (mapper) obtains and returns a partial snapshot which are then
   * reduced to a single combined snapshot by getFullHMSSnapshot().
   *
   * Synchronization between the getFullHMSSnapshot() and executors is done using the
   * 'results' queue. The queue holds the futures for each scheduled task.
   * It is initially populated by getFullHMSSnapshot and each task may add new future
   * results to it. Only getFullHMSSnapshot() removes entries from the results queue.
   * This guarantees that once the results queue is empty there are no pending jobs.
   *
   * Since there are no other data sharing, the implementation is safe without
   * any other synchronization. It is not thread-safe for concurrent calls
   * to getFullHMSSnapshot().
   *
   */

  private final ExecutorService threadPool;
  private final HiveMetaStoreClient client;
  private final int maxPartitionsPerCall;
  private final int maxTablesPerCall;
  private final Deque<Future<CallResult>> results = new ConcurrentLinkedDeque<>();
  private final int maxRetries;
  private final int waitDurationMillis;

  private static final Logger LOGGER = LoggerFactory.getLogger(FullUpdateInitializer.class);

  private static final ObjectMapping emptyObjectMapping =
          new ObjectMapping(Collections.<String, Set<String>>emptyMap());

  /**
   * Extract path (not starting with "/") from the full URI
   * @param uri - resource URI (usually with scheme)
   * @return path if uri is valid or null
   */
  private static String pathFromURI(String uri) {
    try {
      return PathsUpdate.parsePath(uri);
    } catch (SentryMalformedPathException e) {
      LOGGER.warn(String.format("Ignoring invalid uri %s: %s",
              uri, e.getReason()));
      return null;
    }
  }

  /**
   * Mapping of object to set of paths.
   * Used to represent partial results from executor threads. Multiple
   * ObjectMapping objects are combined in a single mapping
   * to get the final result.
   */
  private static final class ObjectMapping {
    private final Map<String, Set<String>> objects;

    ObjectMapping(Map<String, Set<String>> objects) {
      this.objects = objects;
    }

    ObjectMapping(String authObject, String path) {
      Set<String> values = Collections.singleton(safeIntern(path));
      objects = ImmutableMap.of(authObject, values);
    }

    ObjectMapping(String authObject, Collection<String> paths) {
      Set<String> values = new HashSet<>(paths);
      objects = ImmutableMap.of(authObject, values);
    }

    Map<String, Set<String>> getObjects() {
      return objects;
    }
  }

  private static final class CallResult {
    private final Exception failure;
    private final boolean successStatus;
    private final ObjectMapping objectMapping;

    CallResult(Exception ex) {
      failure = ex;
      successStatus = false;
      objectMapping = emptyObjectMapping;
    }

    CallResult(ObjectMapping objectMapping) {
      failure = null;
      successStatus = true;
      this.objectMapping = objectMapping;
    }

    boolean success() {
      return successStatus;
    }

    ObjectMapping getObjectMapping() {
      return objectMapping;
    }

    public Exception getFailure() {
      return failure;
    }
  }

  private abstract class BaseTask implements Callable<CallResult> {

    /**
     *  Class represents retry strategy for BaseTask.
     */
    private final class RetryStrategy {
      private int retryStrategyMaxRetries = 0;
      private final int retryStrategyWaitDurationMillis;

      private RetryStrategy(int retryStrategyMaxRetries, int retryStrategyWaitDurationMillis) {
        this.retryStrategyMaxRetries = retryStrategyMaxRetries;

        // Assign default wait duration if negative value is provided.
        this.retryStrategyWaitDurationMillis = (retryStrategyWaitDurationMillis > 0) ?
                retryStrategyWaitDurationMillis : 1000;
      }

      @SuppressWarnings({"squid:S1141", "squid:S2142"})
      public CallResult exec()  {
        // Retry logic is happening inside callable/task to avoid
        // synchronous waiting on getting the result.
        // Retry the failure task until reach the max retry number.
        // Wait configurable duration for next retry.
        //
        // Only thrift exceptions are retried.
        // Other exceptions are propagated up the stack.
        Exception exception = null;
        try {
          // We catch all exceptions except Thrift exceptions which are retried
          for (int i = 0; i < retryStrategyMaxRetries; i++) {
            //noinspection NestedTryStatement
            try {
              return new CallResult(doTask());
            } catch (TException ex) {
              LOGGER.debug("Failed to execute task on " + (i + 1) + " attempts." +
                      " Sleeping for " + retryStrategyWaitDurationMillis + " ms. Exception: " +
                      ex.toString(), ex);
              exception = ex;

              try {
                Thread.sleep(retryStrategyWaitDurationMillis);
              } catch (InterruptedException ignored) {
                // Skip the rest retries if get InterruptedException.
                // And set the corresponding retries number.
                LOGGER.warn("Interrupted during update fetch during iteration " + (i + 1));
                break;
              }
            }
          }
        } catch (Exception ex) {
          exception = ex;
        }
        LOGGER.error("Failed to execute task", exception);
        // We will fail in the end, so we are shutting down the pool to prevent
        // new tasks from being scheduled.
        threadPool.shutdown();
        return new CallResult(exception);
      }
    }

    private final RetryStrategy retryStrategy;

    BaseTask() {
      retryStrategy = new RetryStrategy(maxRetries, waitDurationMillis);
    }

    @Override
    public CallResult call() throws Exception {
      return retryStrategy.exec();
    }

    abstract ObjectMapping doTask() throws TException;
  }

  private class PartitionTask extends BaseTask {
    private final String dbName;
    private final String tblName;
    private final String authName;
    private final List<String> partNames;

    PartitionTask(String dbName, String tblName, String authName,
                  List<String> partNames) {
      this.dbName = safeIntern(dbName);
      this.tblName = safeIntern(tblName);
      this.authName = safeIntern(authName);
      this.partNames = partNames;
    }

    @Override
    ObjectMapping doTask() throws TException {
      List<Partition> tblParts = client.getPartitionsByNames(dbName, tblName, partNames);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### Fetching partitions " +
                "[" + dbName + "." + tblName + "]" + "[" + partNames + "]");
      }
      Collection<String> partitionNames = new ArrayList<>(tblParts.size());
      for (Partition part : tblParts) {
        String partPath = pathFromURI(part.getSd().getLocation());
        if (partPath != null) {
          partitionNames.add(partPath.intern());
        }
      }
      return new ObjectMapping(authName, partitionNames);
    }
  }

  private class TableTask extends BaseTask {
    private final String dbName;
    private final List<String> tableNames;

    TableTask(Database db, List<String> tableNames) {
      dbName = safeIntern(db.getName());
      this.tableNames = tableNames;
    }

    @Override
    @SuppressWarnings({"squid:S2629", "squid:S135"})
    ObjectMapping doTask() throws TException {
      List<Table> tables = client.getTableObjectsByName(dbName, tableNames);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### Fetching tables [" + dbName + "][" +
        tableNames + "]");
      }
      Map<String, Set<String>> objectMapping = new HashMap<>(tables.size());
      for (Table tbl : tables) {
        // Table names are case insensitive
        if (!tbl.getDbName().equalsIgnoreCase(dbName)) {
          // Inconsistency in HMS data
          LOGGER.warn(String.format("DB name %s for table %s does not match %s",
                  tbl.getDbName(), tbl.getTableName(), dbName));
          continue;
        }

        String tableName = safeIntern(tbl.getTableName().toLowerCase());
        String authzObject = (dbName + "." + tableName).intern();
        List<String> tblPartNames = client.listPartitionNames(dbName, tableName, (short) -1);
        for (int i = 0; i < tblPartNames.size(); i += maxPartitionsPerCall) {
          List<String> partsToFetch = tblPartNames.subList(i,
                  Math.min(i + maxPartitionsPerCall, tblPartNames.size()));
          Callable<CallResult> partTask = new PartitionTask(dbName,
                  tableName, authzObject, partsToFetch);
          results.add(threadPool.submit(partTask));
        }
        String tblPath = safeIntern(pathFromURI(tbl.getSd().getLocation()));
        if (tblPath == null) {
          continue;
        }
        Set<String> paths = objectMapping.get(authzObject);
        if (paths == null) {
          paths = new HashSet<>(1);
          objectMapping.put(authzObject, paths);
        }
        paths.add(tblPath);
      }
      return new ObjectMapping(Collections.unmodifiableMap(objectMapping));
    }
  }

  private class DbTask extends BaseTask {

    private final String dbName;

    DbTask(String dbName) {
      //Database names are case insensitive
      this.dbName = safeIntern(dbName.toLowerCase());
    }

    @Override
    ObjectMapping doTask() throws TException {
      Database db = client.getDatabase(dbName);
      if (!dbName.equalsIgnoreCase(db.getName())) {
        LOGGER.warn("Database name {} does not match {}", db.getName(), dbName);
        return emptyObjectMapping;
      }
      List<String> allTblStr = client.getAllTables(dbName);
      for (int i = 0; i < allTblStr.size(); i += maxTablesPerCall) {
        List<String> tablesToFetch = allTblStr.subList(i,
                Math.min(i + maxTablesPerCall, allTblStr.size()));
        Callable<CallResult> tableTask = new TableTask(db, tablesToFetch);
        results.add(threadPool.submit(tableTask));
      }
      String dbPath = safeIntern(pathFromURI(db.getLocationUri()));
      return (dbPath != null) ? new ObjectMapping(dbName, dbPath) :
              emptyObjectMapping;
    }
  }

  public FullUpdateInitializer(HiveMetaStoreClient client, Configuration conf) {
    this.client = client;
    maxPartitionsPerCall = conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC_DEFAULT);
    maxTablesPerCall = conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC_DEFAULT);
    maxRetries = conf.getInt(
            ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_MAX_NUM,
            ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_MAX_NUM_DEFAULT);
    waitDurationMillis = conf.getInt(
            ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_WAIT_DURAION_IN_MILLIS,
            ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_WAIT_DURAION_IN_MILLIS_DEFAULT);
    threadPool = Executors.newFixedThreadPool(conf.getInt(
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS,
        ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS_DEFAULT));
  }

  /**
   * Get Full HMS snapshot.
   * @return Full snapshot of HMS objects.
   * @throws TException if Thrift error occured
   * @throws ExecutionException if there was a scheduling error
   * @throws InterruptedException if processing was interrupted
   */
  @SuppressWarnings("squid:S00112")
  public Map<String, Set<String>> getFullHMSSnapshot()
          throws Exception {
    // Get list of all HMS databases
    List<String> allDbStr = client.getAllDatabases();
    // Schedule async task for each database responsible for fetching per-database
    // objects.
    for (String dbName : allDbStr) {
      results.add(threadPool.submit(new DbTask(dbName)));
    }

    // Resulting full snapshot
    Map<String, Set<String>> fullSnapshot = new HashMap<>();

    // As async tasks complete, merge their results into full snapshot.
    while (!results.isEmpty()) {
      // This is the only thread that takes elements off the results list - all other threads
      // only add to it. Once the list is empty it can't become non-empty
      // This means that if we check that results is non-empty we can safely call pop() and
      // know that the result of poll() is not null.
      Future<CallResult> result = results.pop();
      // Wait for the task to complete
      CallResult callResult = result.get();
      // Fail if we got errors
      if (!callResult.success()) {
        throw callResult.getFailure();
      }
      // Merge values into fullUpdate
      Map<String, Set<String>> objectMapping =
              callResult.getObjectMapping().getObjects();
      for (Map.Entry<String, Set<String>> entry: objectMapping.entrySet()) {
        String key = entry.getKey();
        Set<String> val = entry.getValue();
        Set<String> existingSet = fullSnapshot.get(key);
        if (existingSet == null) {
          fullSnapshot.put(key, val);
          continue;
        }
        existingSet.addAll(val);
      }
    }
    return fullSnapshot;
  }

  @Override
  public void close() {
    threadPool.shutdownNow();
    try {
      threadPool.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
      LOGGER.warn("Interrupted shutdown");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Intern a string but only if it is not null
   * @param arg String to be interned, may be null
   * @return interned string or null
   */
  static String safeIntern(String arg) {
    return (arg != null) ? arg.intern() : null;
  }
}
