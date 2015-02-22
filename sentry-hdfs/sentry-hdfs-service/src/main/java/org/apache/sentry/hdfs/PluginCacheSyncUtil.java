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
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final String zkPath;
  private final HAContext haContext;
  private final PathChildrenCache cache;
  private InterProcessSemaphoreMutex updatorLock;
  private int lockTimeout;
  private DistributedAtomicLong updateCounter;

  public PluginCacheSyncUtil(String zkPath, Configuration conf,
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

    updateCounter = new DistributedAtomicLong(haContext.getCuratorFramework(),
        zkPath + "/counter", haContext.getRetryPolicy());
    try {
      updateCounter.initialize((long) 4);
    } catch (Exception e) {
      LOGGER.error("Error initializing  counter for zpath " + zkPath, e);
    }
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
      // increment the global sequence counter
      try {
        update.setSeqNum(updateCounter.increment().postValue());
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
}
