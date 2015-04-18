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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class MetastoreCacheInitializer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger
          (MetastoreCacheInitializer.class);

  static class CallResult {
    final Exception failure;

    CallResult(Exception ex) {
      failure = null;
    }
  }

  abstract class BaseTask implements Callable<CallResult> {

    BaseTask() { taskCounter.incrementAndGet(); }

    @Override
    public CallResult call() throws Exception {
      try {
        doTask();
      } catch (Exception ex) {
        // Ignore if object requested does not exists
        return new CallResult(
                (ex instanceof NoSuchObjectException) ? null : ex);
      } finally {
        taskCounter.decrementAndGet();
      }
      return new CallResult(null);
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
      List<Partition> tblParts =
              hmsHandler.get_partitions_by_names(dbName, tblName, partNames);
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
      List<Table> tables =
              hmsHandler.get_table_objects_by_name(db.getName(), tableNames);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### Fetching tables [" + db.getName() + "][" +
                tableNames + "]");
      }
      for (Table tbl : tables) {
        TPathChanges tblPathChange;
        synchronized (update) {
          tblPathChange = update.newPathChange(tbl.getDbName() + "." + tbl
                  .getTableName());
        }
        if (tbl.getSd().getLocation() != null) {
          List<String> tblPath =
                  PathsUpdate.parsePath(tbl.getSd().getLocation());
          tblPathChange.addToAddPaths(tblPath);
          List<String> tblPartNames =
                  hmsHandler.get_partition_names(db.getName(), tbl
                          .getTableName(), (short) -1);
          for (int i = 0; i < tblPartNames.size(); i += maxPartitionsPerCall) {
            List<String> partsToFetch =
                    tblPartNames.subList(i, Math.min(
                            i + maxPartitionsPerCall, tblPartNames.size()));
            Callable<CallResult> partTask =
                    new PartitionTask(db.getName(), tbl.getTableName(),
                            partsToFetch, tblPathChange);
            synchronized (results) {
              results.add(threadPool.submit(partTask));
            }
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
      this.dbName = dbName;
    }

    @Override
    public void doTask() throws Exception {
      Database db = hmsHandler.get_database(dbName);
      List<String> dbPath = PathsUpdate.parsePath(db.getLocationUri());
      if (dbPath != null) {
        synchronized (update) {
          update.newPathChange(db.getName()).addToAddPaths(dbPath);
        }
      }
      List<String> allTblStr = hmsHandler.get_all_tables(db.getName());
      for (int i = 0; i < allTblStr.size(); i += maxTablesPerCall) {
        List<String> tablesToFetch =
                allTblStr.subList(i, Math.min(
                        i + maxTablesPerCall, allTblStr.size()));
        Callable<CallResult> tableTask =
                new TableTask(db, tablesToFetch, update);
        synchronized (results) {
          results.add(threadPool.submit(tableTask));
        }
      }
    }
  }

  private final ExecutorService threadPool;
  private final IHMSHandler hmsHandler;
  private final int maxPartitionsPerCall;
  private final int maxTablesPerCall;
  private final List<Future<CallResult>> results =
          new ArrayList<Future<CallResult>>();
  private final AtomicInteger taskCounter = new AtomicInteger(0);

  MetastoreCacheInitializer(IHMSHandler hmsHandler, Configuration conf) {
    this.hmsHandler = hmsHandler;
    this.maxPartitionsPerCall = conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC_DEFAULT);
    this.maxTablesPerCall = conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC_DEFAULT);
    threadPool = Executors.newFixedThreadPool(conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS_DEFAULT));
  }

  UpdateableAuthzPaths createInitialUpdate() throws
          Exception {
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(new
            String[]{"/"});
    PathsUpdate tempUpdate = new PathsUpdate(-1, false);
    List<String> allDbStr = hmsHandler.get_all_databases();
    List<Future<CallResult>> results = new ArrayList<Future<CallResult>>();
    for (String dbName : allDbStr) {
      Callable<CallResult> dbTask = new DbTask(tempUpdate, dbName);
      results.add(threadPool.submit(dbTask));
    }

    while (taskCounter.get() > 0) {
      Thread.sleep(1000);
      // Wait until no more tasks remain
    }
    for (Future<CallResult> result : results) {
      CallResult callResult = result.get();
      if (callResult.failure != null) {
        throw new RuntimeException(callResult.failure);
      }
    }
    authzPaths.updatePartial(Lists.newArrayList(tempUpdate),
            new ReentrantReadWriteLock());
    return authzPaths;
  }


  @Override
  public void close() throws IOException {
    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }
}
