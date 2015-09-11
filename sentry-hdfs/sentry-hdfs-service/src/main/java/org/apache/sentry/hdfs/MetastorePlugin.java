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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
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
      if (MetastorePlugin.this.authzPaths == null) {
        LOGGER.info("#### Metastore Plugin cache has not finished" +
                "initialization.");
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
  private volatile UpdateableAuthzPaths authzPaths;
  private Lock notificiationLock;

  // Initialized to some value > 1.
  protected static final AtomicLong seqNum = new AtomicLong(5);

  // Has to match the value of seqNum
  protected static volatile long lastSentSeqNum = seqNum.get();
  private volatile boolean syncSent = false;
  private volatile boolean initComplete = false;
  private volatile boolean queueFlushComplete = false;
  private volatile Throwable initError = null;
  private final Queue<PathsUpdate> updateQueue = new LinkedList<PathsUpdate>();

  private final ExecutorService threadPool;
  private final Configuration sentryConf;

  static class ProxyHMSHandler extends HMSHandler {
    public ProxyHMSHandler(String name, HiveConf conf) throws MetaException {
      super(name, conf);
    }
  }

  private void updateSentryConfFromHiveConf(String property) {
    if (sentryConf.get(property) == null || sentryConf.get(property).isEmpty()) {
      String value = conf.get(property);
      if(value != null) {
        LOGGER.warn("Deprecated: Property " + property + " was set in hive-site.xml, " +
            "it should be instead set in sentry-site.xml");
        sentryConf.set(property, value);
      }
    }
  }

  @VisibleForTesting
  public Configuration getSentryConf() {
    return sentryConf;
  }

  public MetastorePlugin(Configuration conf, Configuration sentryConf) {
    this.notificiationLock = new ReentrantLock();
    this.conf = new HiveConf((HiveConf)conf);
    this.sentryConf = new Configuration(sentryConf);

    //TODO: Remove this in C6.
    //CDH-28899: Maintain backward compatibility. Allow reading the configs from hive-site
    // along with sentry-site, where sentry-site takes the precedence
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SERVER_RPC_ADDRESS);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SERVER_RPC_PORT);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SERVER_RPC_CONN_TIMEOUT);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SECURITY_MODE);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SECURITY_USE_UGI_TRANSPORT);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.PRINCIPAL);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.USE_COMPACT_TRANSPORT);

    this.conf.unset(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTOREURIS.varname);
    Thread initUpdater = new Thread() {
      @Override
      public void run() {
        MetastoreCacheInitializer cacheInitializer = null;
        try {
          cacheInitializer =
                  new MetastoreCacheInitializer(new ProxyHMSHandler("sentry.hdfs",
                        (HiveConf) MetastorePlugin.this.conf),
                          MetastorePlugin.this.conf);
          MetastorePlugin.this.authzPaths =
                  cacheInitializer.createInitialUpdate();
          LOGGER.info("#### Metastore Plugin initialization complete !!");
          synchronized (updateQueue) {
            while (!updateQueue.isEmpty()) {
              PathsUpdate update = updateQueue.poll();
              if (update != null) {
                processUpdate(update);
              }
            }
            queueFlushComplete = true;
          }
          LOGGER.info("#### Finished flushing queued updates to Sentry !!");
        } catch (Exception e) {
          LOGGER.error("#### Could not create Initial AuthzPaths or HMSHandler !!", e);
          initError = e;
        } finally {
          if (cacheInitializer != null) {
            try {
              cacheInitializer.close();
            } catch (Exception e) {
              LOGGER.info("#### Exception while closing cacheInitializer !!", e);
            }
          }
          initComplete = true;
        }
      }
    };
    if (this.conf.getBoolean(
            ServerConfig.SENTRY_HDFS_SYNC_METASTORE_CACHE_ASYNC_INIT_ENABLE,
            ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_ASYNC_INIT_ENABLE_DEFAULT)) {
      LOGGER.warn("#### Metastore Cache initialization is set to aync..." +
              "HDFS ACL synchronization will not happen until metastore" +
              "cache initialization is completed !!");
      initUpdater.start();
    } else {
      initUpdater.run();
    }
    try {
      sentryClient = SentryHDFSServiceClientFactory.create(this.sentryConf);
    } catch (Exception e) {
      sentryClient = null;
      LOGGER.error("Could not connect to Sentry HDFS Service !!", e);
    }
    ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);
    threadPool.scheduleWithFixedDelay(new SyncTask(),
            this.conf.getLong(ServerConfig
                            .SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS,
                    ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT),
            this.conf.getLong(ServerConfig.SENTRY_HDFS_SYNC_CHECKER_PERIOD_MS,
                    ServerConfig.SENTRY_HDFS_SYNC_CHECKER_PERIOD_DEFAULT),
            TimeUnit.MILLISECONDS);
    this.threadPool = threadPool;
  }

  @Override
  public void addPath(String authzObj, String path) {
    List<String> pathTree = PathsUpdate.parsePath(path);
    if(pathTree == null) {
      return;
    }
    LOGGER.debug("#### HMS Path Update ["
        + "OP : addPath, "
        + "authzObj : " + authzObj.toLowerCase() + ", "
        + "path : " + path + "]");
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj.toLowerCase()).addToAddPaths(pathTree);
    notifySentryAndApplyLocal(update);
  }

  @Override
  public void removeAllPaths(String authzObj, List<String> childObjects) {
    LOGGER.debug("#### HMS Path Update ["
        + "OP : removeAllPaths, "
        + "authzObj : " + authzObj.toLowerCase() + ", "
        + "childObjs : " + (childObjects == null ? "[]" : childObjects) + "]");
    PathsUpdate update = createHMSUpdate();
    if (childObjects != null) {
      for (String childObj : childObjects) {
        update.newPathChange(authzObj.toLowerCase() + "." + childObj).addToDelPaths(
            Lists.newArrayList(PathsUpdate.ALL_PATHS));
      }
    }
    update.newPathChange(authzObj.toLowerCase()).addToDelPaths(
            Lists.newArrayList(PathsUpdate.ALL_PATHS));
    notifySentryAndApplyLocal(update);
  }

  @Override
  public void removePath(String authzObj, String path) {
    if ("*".equals(path)) {
      removeAllPaths(authzObj.toLowerCase(), null);
    } else {
      List<String> pathTree = PathsUpdate.parsePath(path);
      if(pathTree == null) {
        return;
      }
      LOGGER.debug("#### HMS Path Update ["
          + "OP : removePath, "
          + "authzObj : " + authzObj.toLowerCase() + ", "
          + "path : " + path + "]");
      PathsUpdate update = createHMSUpdate();
      update.newPathChange(authzObj.toLowerCase()).addToDelPaths(pathTree);
      notifySentryAndApplyLocal(update);
    }
  }

  @Override
  public void renameAuthzObject(String oldName, String oldPath, String newName,
      String newPath) {
    if (oldName != null) {
      oldName = oldName.toLowerCase();
    }
    if (newName != null) {
      newName = newName.toLowerCase();
    }
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
        sentryClient = SentryHDFSServiceClientFactory.create(sentryConf);
      } catch (Exception e) {
        sentryClient = null;
        LOGGER.error("#### Could not connect to Sentry HDFS Service !!", e);
      }
    }
    return sentryClient;
  }

  private PathsUpdate createHMSUpdate() {
    PathsUpdate update = new PathsUpdate(seqNum.incrementAndGet(), false);
    LOGGER.debug("#### Creating HMS Path Update SeqNum : [" + seqNum.get() + "]");
    return update;
  }

  protected void notifySentryNoLock(PathsUpdate update) {
    try {
      getClient().notifyHMSUpdate(update);
    } catch (Exception e) {
      LOGGER.error("Could not send update to Sentry HDFS Service !!", e);
    }
  }

  protected void notifySentry(PathsUpdate update) {
    notificiationLock.lock();
    if (!syncSent) {
      new SyncTask().run();
    }
    try {
      notifySentryNoLock(update);
    } finally {
      lastSentSeqNum = update.getSeqNum();
      notificiationLock.unlock();
      LOGGER.debug("#### HMS Path Last update sent : ["+ lastSentSeqNum + "]");
    }
  }

  protected void applyLocal(PathsUpdate update) {
    authzPaths.updatePartial(Lists.newArrayList(update), new ReentrantReadWriteLock());
  }

  private void notifySentryAndApplyLocal(PathsUpdate update) {
    if (initComplete) {
      processUpdate(update);
    } else {
      if (initError == null) {
        synchronized (updateQueue) {
          if (!queueFlushComplete) {
            updateQueue.add(update);
          } else {
            processUpdate(update);
          }
        }
      } else {
        StringWriter sw = new StringWriter();
        initError.printStackTrace(new PrintWriter(sw));
        LOGGER.error("#### Error initializing Metastore Plugin" +
                "[" + sw.toString() + "] !!");
        throw new RuntimeException(initError);
      }
      LOGGER.warn("#### Path update [" + update.getSeqNum() + "] not sent to Sentry.." +
              "Metastore hasn't been initialized yet !!");
    }
  }

  protected void processUpdate(PathsUpdate update) {
    applyLocal(update);
    notifySentry(update);
  }

}
