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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.hdfs.Updateable.Update;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin.SentryPluginException;
import org.apache.sentry.provider.db.service.persistent.HAContext;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class for handling the cache update syncup via Curator path cache It
 * creates the path cache, a distributed lock and counter. The updated API
 * updates the counter, creates a znode zpath/counter and writes the data to it.
 * The caller should provider the cache callback handler class that posts the
 * update object to the required cache
 */
public class PluginCacheSyncUtil {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(PluginCacheSyncUtil.class);
  public static final long CACHE_GC_SIZE_THRESHOLD_HWM = 100;
  public static final long CACHE_GC_SIZE_THRESHOLD_LWM = 50;
  public static final long CACHE_GC_SIZE_MAX_CLEANUP = 1000;
  public static final long ZK_COUNTER_INIT_VALUE = 4;
  public static final long GC_COUNTER_INIT_VALUE = ZK_COUNTER_INIT_VALUE + 1;

  private final String zkPath;
  private final HAContext haContext;
  private final PathChildrenCache cache;
  private InterProcessSemaphoreMutex updatorLock, gcLock;
  private int lockTimeout;
  private DistributedAtomicLong updateCounter, gcCounter;
  private final ScheduledExecutorService gcSchedulerForZk = Executors
      .newScheduledThreadPool(1);

  public PluginCacheSyncUtil(String zkPath, final Configuration conf,
      PathChildrenCacheListener cacheListener) throws SentryPluginException {
    this.zkPath = zkPath;
    // Init ZK connection
    try {
      haContext = HAContext.getHAContext(conf);
    } catch (Exception e) {
      throw new SentryPluginException("Error creating HA context ", e);
    }
    haContext.startCuratorFramework();

    // Init path cache
    cache = new PathChildrenCache(haContext.getCuratorFramework(), zkPath
        + "/cache", true);
    // path cache callback
    cache.getListenable().addListener(cacheListener);
    try {
      cache.start();
    } catch (Exception e) {
      throw new SentryPluginException("Error creating ZK PathCache ", e);
    }
    updatorLock = new InterProcessSemaphoreMutex(
        haContext.getCuratorFramework(), zkPath + "/lock");
    lockTimeout = conf.getInt(
        ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS,
        ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT);
    gcLock = new InterProcessSemaphoreMutex(
        haContext.getCuratorFramework(), zkPath + "/gclock");

    updateCounter = new DistributedAtomicLong(haContext.getCuratorFramework(),
        zkPath + "/counter", haContext.getRetryPolicy());
    try {
      updateCounter.initialize(ZK_COUNTER_INIT_VALUE);
    } catch (Exception e) {
      LOGGER.error("Error initializing  counter for zpath " + zkPath, e);
    }

    // GC setup
    gcCounter = new DistributedAtomicLong(haContext.getCuratorFramework(),
        zkPath + "/gccounter", haContext.getRetryPolicy());
    try {
      gcCounter.initialize(GC_COUNTER_INIT_VALUE);
    } catch (Exception e) {
      LOGGER.error("Error initializing  counter for zpath " + zkPath, e);
    }
    final Runnable gcRunner = new Runnable() {
      public void run() {
        gcPluginCache(conf);
      }
    };
    gcSchedulerForZk.scheduleAtFixedRate(gcRunner, 10, 10, TimeUnit.MINUTES);
  }

  public void handleCacheUpdate(Update update) throws SentryPluginException {
    // post message to ZK cache
    try {
      // Acquire ZK lock for update cache sync. This ensures that the counter
      // increment and znode creation is atomic operation
      if (!updatorLock.acquire(lockTimeout, TimeUnit.MILLISECONDS)) {
        throw new SentryPluginException(
            "Failed to get ZK lock for update cache syncup");
      }
    } catch (Exception e1) {
      throw new SentryPluginException(
          "Error getting ZK lock for update cache syncup" + e1, e1);
    }

    try {
      try {
        // increment the global sequence counter if this is not a full update
        if (!update.hasFullImage()) {
          update.setSeqNum(updateCounter.increment().postValue());
        } else {
          if (updateCounter.get().preValue() < update.getSeqNum()) {
            updateCounter.add(update.getSeqNum() - updateCounter.get().preValue());
          }
        }
      } catch (Exception e1) {
        throw new SentryPluginException(
            "Error setting ZK counter for update cache syncup" + e1, e1);
      }

      // Create a new znode with the sequence number and write the update data
      // into it
      String updateSeq = String.valueOf(update.getSeqNum());
      String newPath = ZKPaths.makePath(zkPath + "/cache", updateSeq);
      try {
        haContext.getCuratorFramework().create().creatingParentsIfNeeded()
            .forPath(newPath, update.serialize());
      } catch (Exception e) {
        throw new SentryPluginException("error posting update to ZK ", e);
      }
    } finally {
      // release the ZK lock
      try {
        updatorLock.release();
      } catch (Exception e) {
        throw new SentryPluginException(
            "Error releasing ZK lock for update cache syncup" + e, e);
      }
    }
  }

  public static void setUpdateFromChildEvent(PathChildrenCacheEvent cacheEvent,
      Update update) throws IOException {
    byte eventData[] = cacheEvent.getData().getData();
    update.deserialize(eventData);
    String seqNum = ZKPaths.getNodeFromPath(cacheEvent.getData().getPath());
    update.setSeqNum(Integer.valueOf(seqNum));
  }

  public void close() throws IOException {
    cache.close();
  }

  public long getUpdateCounter() throws Exception {
    return updateCounter.get().preValue();
  }

  /**
   * Cleanup old znode of the plugin cache. The last cleaned and last created
   * node counters are stored in ZK. If the number of available nodes are more
   * than the high water mark, then we delete the old nodes till we reach low
   * water mark. The scheduler periodically runs the cleanup routine
   * @param conf
   */
  @VisibleForTesting
  public void gcPluginCache(Configuration conf) {
    try {
      // If we can acquire gc lock, then continue with znode cleanup
      if (!gcLock.acquire(500, TimeUnit.MILLISECONDS)) {
        return;
      }

      // If we have passed the High watermark, then start the cleanup
      Long updCount = updateCounter.get().preValue();
      Long gcCount = gcCounter.get().preValue();
      if (updCount - gcCount > CACHE_GC_SIZE_THRESHOLD_HWM) {
        Long numNodesToClean = Math.min(updCount - gcCount
            - CACHE_GC_SIZE_THRESHOLD_LWM, CACHE_GC_SIZE_MAX_CLEANUP);
        for (Long nodeNum = gcCount; nodeNum < gcCount + numNodesToClean; nodeNum++) {
          String pathToDelete = ZKPaths.makePath(zkPath + "/cache",
              Long.toString(nodeNum));
          try {
            haContext.getCuratorFramework().delete().forPath(pathToDelete);
            gcCounter.increment();
            LOGGER.debug("Deleted znode " + pathToDelete);
          } catch (NoNodeException eN) {
            // We might have endup with holes in the node counter due to network/ZK errors
            // Ignore the delete error if the node doesn't exist and move on
            gcCounter.increment();
          } catch (Exception e) {
            LOGGER.info("Error cleaning up node " + pathToDelete, e);
            break;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error cleaning the cache", e);
    } finally {
      if (gcLock.isAcquiredInThisProcess()) {
        try {
          gcLock.release();
        } catch (Exception e) {
          LOGGER.warn("Error releasing gc lock", e);
        }
      }
    }
  }

}
