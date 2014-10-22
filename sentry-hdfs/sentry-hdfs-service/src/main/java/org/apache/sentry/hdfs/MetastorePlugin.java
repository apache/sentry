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
package org.apache.sentry.hdfs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.provider.db.SentryMetastoreListenerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Plugin implementation of {@link SentryMetastoreListenerPlugin} that hooks
 * into the sites in the {@link MetaStorePreEventListener} that deal with
 * creation/updation and deletion for paths.
 */
public class MetastorePlugin extends SentryMetastoreListenerPlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetastorePlugin.class);

  private final Configuration conf;
  private SentryHDFSServiceClient sentryClient;
  private UpdateableAuthzPaths authzPaths;

  //Initialized to some value > 1 so that the first update notification
 // will trigger a full Image fetch
  private final AtomicInteger seqNum = new AtomicInteger(5);
  private final ExecutorService threadPool;

  public MetastorePlugin(Configuration conf) {
    this.conf = new HiveConf((HiveConf)conf);
    this.conf.unset(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTOREURIS.varname);
    try {
      this.authzPaths = createInitialUpdate(HiveMetaStore.newHMSHandler("sentry.hdfs", (HiveConf)this.conf));
    } catch (Exception e1) {
      LOGGER.error("Could not create Initial AuthzPaths or HMSHandler !!", e1);
      throw new RuntimeException(e1);
    }
    try {
      sentryClient = new SentryHDFSServiceClient(conf);
    } catch (Exception e) {
      sentryClient = null;
      LOGGER.error("Could not connect to Sentry HDFS Service !!", e);
    }
    ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);
    threadPool.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          long lastSeenHMSPathSeqNum =
              MetastorePlugin.this.getClient().getLastSeenHMSPathSeqNum();
          if (lastSeenHMSPathSeqNum != seqNum.get()) {
            LOGGER.warn("Sentry not in sync with HMS [" + lastSeenHMSPathSeqNum + ", " + seqNum.get() + "]");
            PathsUpdate fullImageUpdate =
                MetastorePlugin.this.authzPaths.createFullImageUpdate(
                    seqNum.get());
            LOGGER.warn("Sentry not in sync with HMS !!");
            notifySentry(fullImageUpdate);
          }
        } catch (Exception e) {
          sentryClient = null;
          LOGGER.error("Error talking to Sentry HDFS Service !!", e);
        }
      }
    }, this.conf.getLong(ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS,
        ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT), 1000,
        TimeUnit.MILLISECONDS);
    this.threadPool = threadPool;
  }

  private UpdateableAuthzPaths createInitialUpdate(IHMSHandler hmsHandler) throws Exception {
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(new String[] {"/"});
    PathsUpdate tempUpdate = new PathsUpdate(-1, false);
    List<String> allDbStr = hmsHandler.get_all_databases();
    for (String dbName : allDbStr) {
      Database db = hmsHandler.get_database(dbName);
      tempUpdate.newPathChange(db.getName()).addToAddPaths(
          PathsUpdate.cleanPath(db.getLocationUri()));
      List<String> allTblStr = hmsHandler.get_all_tables(db.getName());
      for (String tblName : allTblStr) {
        Table tbl = hmsHandler.get_table(db.getName(), tblName);
        TPathChanges tblPathChange = tempUpdate.newPathChange(tbl
            .getDbName() + "." + tbl.getTableName());
        List<Partition> tblParts =
            hmsHandler.get_partitions(db.getName(), tbl.getTableName(), (short) -1);
        tblPathChange.addToAddPaths(PathsUpdate.cleanPath(tbl.getSd()
            .getLocation() == null ? db.getLocationUri() : tbl
            .getSd().getLocation()));
        for (Partition part : tblParts) {
          tblPathChange.addToAddPaths(PathsUpdate.cleanPath(part.getSd()
              .getLocation()));
        }
      }
    }
    authzPaths.updatePartial(Lists.newArrayList(tempUpdate),
        new ReentrantReadWriteLock());
    return authzPaths;
  }

  @Override
  public void addPath(String authzObj, String path) {
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj).addToAddPaths(PathsUpdate.cleanPath(path));
    notifySentry(update);
  }

  @Override
  public void removeAllPaths(String authzObj) {
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj).addToDelPaths(Lists.newArrayList(PathsUpdate.ALL_PATHS));
    notifySentry(update);
  }

  @Override
  public void removePath(String authzObj, String path) {
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj).addToDelPaths(PathsUpdate.cleanPath(path));
    notifySentry(update);
  }

  @Override
  public void renameAuthzObject(String oldName, String oldPath, String newName,
      String newPath) {
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(newName).addToAddPaths(PathsUpdate.cleanPath(newPath));
    update.newPathChange(oldName).addToDelPaths(PathsUpdate.cleanPath(oldPath));
    notifySentry(update);
  }

  private SentryHDFSServiceClient getClient() {
    if (sentryClient == null) {
      try {
        sentryClient = new SentryHDFSServiceClient(conf);
      } catch (IOException e) {
        sentryClient = null;
        LOGGER.error("Could not connect to Sentry HDFS Service !!", e);
      }
    }
    return sentryClient;
  }

  private PathsUpdate createHMSUpdate() {
    PathsUpdate update = new PathsUpdate(seqNum.incrementAndGet(), false);
    return update;
  }

  private void notifySentry(PathsUpdate update) {
    authzPaths.updatePartial(Lists.newArrayList(update), new ReentrantReadWriteLock());
    try {
      getClient().notifyHMSUpdate(update);
    } catch (Exception e) {
      LOGGER.error("Could not send update to Sentry HDFS Service !!", e);
    }
  }

}
