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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
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

  class SyncTask implements Runnable {
    @Override
    public void run() {
      if (!notificiationLock.tryLock()) {
        // No need to sync.. as metastore is in the process of pushing an update..
        return;
      }
      try {
        long lastSeenBySentry =
            MetastorePlugin.this.getClient().getLastSeenHMSPathSeqNum();
        long lastSent = lastSentSeqNum;
        if (lastSeenBySentry != lastSent) {
          LOGGER.warn("#### Sentry not in sync with HMS [" + lastSeenBySentry + ", "
              + lastSent + "]");
          PathsUpdate fullImageUpdate =
              MetastorePlugin.this.authzPaths.createFullImageUpdate(lastSent);
          notifySentryNoLock(fullImageUpdate);
          LOGGER.warn("#### Synced Sentry with update [" + lastSent + "]");
        }
      } catch (Exception e) {
        sentryClient = null;
        LOGGER.error("Error talking to Sentry HDFS Service !!", e);
      } finally {
        syncSent = true;
        notificiationLock.unlock();
      }
    }
  }

  private final Configuration conf;
  private SentryHDFSServiceClient sentryClient;
  private UpdateableAuthzPaths authzPaths;
  private Lock notificiationLock;

  //Initialized to some value > 1 so that the first update notification
 // will trigger a full Image fetch
  private final AtomicLong seqNum = new AtomicLong(5);

  // For some reason, HMS sometimes restarts the plugin
  private static volatile long lastSentSeqNum = -1;
  private volatile boolean syncSent = false;
  private final ExecutorService threadPool;

  static class ProxyHMSHandler extends HMSHandler {
    public ProxyHMSHandler(String name, HiveConf conf) throws MetaException {
      super(name, conf);
    }
  }

  public MetastorePlugin(Configuration conf) {
    this.notificiationLock = new ReentrantLock();
    this.conf = new HiveConf((HiveConf)conf);
    this.conf.unset(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTOREURIS.varname);
    try {
      this.authzPaths = createInitialUpdate(new ProxyHMSHandler("sentry.hdfs", (HiveConf)this.conf));
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
    threadPool.scheduleWithFixedDelay(new SyncTask(),
        this.conf.getLong(ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS,
            ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT),
        this.conf.getLong(ServerConfig.SENTRY_HDFS_SYNC_CHECKER_PERIOD_MS,
            ServerConfig.SENTRY_HDFS_SYNC_CHECKER_PERIOD_DEFAULT),
        TimeUnit.MILLISECONDS);
    this.threadPool = threadPool;
  }

  private UpdateableAuthzPaths createInitialUpdate(IHMSHandler hmsHandler) throws Exception {
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(new String[] {"/"});
    PathsUpdate tempUpdate = new PathsUpdate(-1, false);
    List<String> allDbStr = hmsHandler.get_all_databases();
    for (String dbName : allDbStr) {
      Database db = hmsHandler.get_database(dbName);
      List<String> dbPath = PathsUpdate.parsePath(db.getLocationUri());
      if(dbPath != null) {
        tempUpdate.newPathChange(db.getName()).addToAddPaths(dbPath);
      }
      List<String> allTblStr = hmsHandler.get_all_tables(db.getName());
      for (String tblName : allTblStr) {
        Table tbl = hmsHandler.get_table(db.getName(), tblName);
        TPathChanges tblPathChange = tempUpdate.newPathChange(tbl
            .getDbName() + "." + tbl.getTableName());
        List<Partition> tblParts =
            hmsHandler.get_partitions(db.getName(), tbl.getTableName(), (short) -1);
        List<String> tb1Path = PathsUpdate.parsePath(tbl.getSd().getLocation() == null ?
            db.getLocationUri() : tbl.getSd().getLocation());
        if(tb1Path != null) {
          tblPathChange.addToAddPaths(tb1Path);
        }
        for (Partition part : tblParts) {
          List<String> partPath = PathsUpdate.parsePath(part.getSd().getLocation());
          if(partPath != null) {
            tblPathChange.addToAddPaths(partPath);
          }
        }
      }
    }
    authzPaths.updatePartial(Lists.newArrayList(tempUpdate),
        new ReentrantReadWriteLock());
    return authzPaths;
  }

  @Override
  public void addPath(String authzObj, String path) {
    List<String> pathTree = PathsUpdate.parsePath(path);
    if(pathTree == null) {
      return;
    }
    LOGGER.debug("#### HMS Path Update ["
        + "OP : addPath, "
        + "authzObj : " + authzObj + ", "
        + "path : " + path + "]");
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj).addToAddPaths(pathTree);
    notifySentryAndApplyLocal(update);
  }

  @Override
  public void removeAllPaths(String authzObj, List<String> childObjects) {
    LOGGER.debug("#### HMS Path Update ["
        + "OP : removeAllPaths, "
        + "authzObj : " + authzObj + ", "
        + "childObjs : " + (childObjects == null ? "[]" : childObjects) + "]");
    PathsUpdate update = createHMSUpdate();
    if (childObjects != null) {
      for (String childObj : childObjects) {
        update.newPathChange(authzObj + "." + childObj).addToDelPaths(
            Lists.newArrayList(PathsUpdate.ALL_PATHS));
      }
    }
    update.newPathChange(authzObj).addToDelPaths(
        Lists.newArrayList(PathsUpdate.ALL_PATHS));
    notifySentryAndApplyLocal(update);
  }

  @Override
  public void removePath(String authzObj, String path) {
    if ("*".equals(path)) {
      removeAllPaths(authzObj, null);
    } else {
      List<String> pathTree = PathsUpdate.parsePath(path);
      if(pathTree == null) {
        return;
      }
      LOGGER.debug("#### HMS Path Update ["
          + "OP : removePath, "
          + "authzObj : " + authzObj + ", "
          + "path : " + path + "]");
      PathsUpdate update = createHMSUpdate();
      update.newPathChange(authzObj).addToDelPaths(pathTree);
      notifySentryAndApplyLocal(update);
    }
  }

  @Override
  public void renameAuthzObject(String oldName, String oldPath, String newName,
      String newPath) {
    PathsUpdate update = createHMSUpdate();
    LOGGER.debug("#### HMS Path Update ["
        + "OP : renameAuthzObject, "
        + "oldName : " + oldName + ","
        + "newPath : " + oldPath + ","
        + "newName : " + newName + ","
        + "newPath : " + newPath + "]");
    List<String> newPathTree = PathsUpdate.parsePath(newPath);
    if( newPathTree != null ) {
      update.newPathChange(newName).addToAddPaths(newPathTree);
    }
    List<String> oldPathTree = PathsUpdate.parsePath(oldPath);
    if( oldPathTree != null ) {
      update.newPathChange(oldName).addToDelPaths(oldPathTree);
    }
    notifySentryAndApplyLocal(update);
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
    LOGGER.debug("#### Creating HMS Path Update SeqNum : [" + seqNum.get() + "]");
    return update;
  }

  private void notifySentryNoLock(PathsUpdate update) {
    try {
      getClient().notifyHMSUpdate(update);
    } catch (Exception e) {
      LOGGER.error("Could not send update to Sentry HDFS Service !!", e);
    }
  }

  private void notifySentryAndApplyLocal(PathsUpdate update) {
    notificiationLock.lock();
    if (!syncSent) {
      new SyncTask().run();
    }
    try {
      authzPaths.updatePartial(Lists.newArrayList(update), new ReentrantReadWriteLock());
      notifySentryNoLock(update);
    } finally {
      lastSentSeqNum = update.getSeqNum();
      notificiationLock.unlock();
      LOGGER.debug("#### HMS Path Last update sent : ["+ lastSentSeqNum + "]");
    }
  }

}
