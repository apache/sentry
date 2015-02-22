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
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.hdfs.UpdateForwarder;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin.SentryPluginException;
import org.apache.sentry.provider.db.service.persistent.HAContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

public class UpdateForwarderWithHA<K extends Updateable.Update> extends
UpdateForwarder<K> implements Updateable<K> {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateForwarderWithHA.class);
  private static final String UPDATABLE_TYPE_NAME = "ha_update_forwarder";

  public static class SentryHAPathChildrenCacheListener<K extends Updateable.Update>
  implements PathChildrenCacheListener {
    private final LinkedList<K> updateLog;
    private final K baseUpdate;
    private final UpdateForwarderWithHA<K> updateForwarder;

    public SentryHAPathChildrenCacheListener(LinkedList<K> updateLog,
        K baseUpdate, UpdateForwarderWithHA<K> updateForwarder) {
      this.updateLog = updateLog;
      this.baseUpdate = baseUpdate;
      this.updateForwarder = updateForwarder;
    }

    @Override
    public synchronized void childEvent(CuratorFramework client,
        PathChildrenCacheEvent event) throws Exception {
      switch ( event.getType() ) {
      case CHILD_ADDED:
        K newUpdate = (K) baseUpdate.getClass().newInstance();
        PluginCacheSyncUtil.setUpdateFromChildEvent(event, newUpdate);
        updateForwarder.postNotificationToLog(newUpdate);
        break;
      case INITIALIZED:
      case CHILD_UPDATED:
      case CHILD_REMOVED:
        break;
      case CONNECTION_RECONNECTED:
        // resume the node
        SentryPlugin.instance.setOutOfSync(false);
        break;
      case CONNECTION_SUSPENDED:
      case CONNECTION_LOST:
        // suspend the node
        SentryPlugin.instance.setOutOfSync(true);
        break;
      default:
        break;
      }
    }
  }

  private final String zkPath;
  private final PluginCacheSyncUtil pluginCacheSync;

  public UpdateForwarderWithHA(Configuration conf, Updateable<K> updateable,  K baseUpdate,
      ExternalImageRetriever<K> imageRetreiver, int updateLogSize) throws SentryPluginException {
    this(conf, updateable, baseUpdate, imageRetreiver, updateLogSize, INIT_UPDATE_RETRY_DELAY);
  }

  public UpdateForwarderWithHA(Configuration conf, Updateable<K> updateable, K baseUpdate,
      ExternalImageRetriever<K> imageRetreiver, int updateLogSize,
      int initUpdateRetryDelay) throws SentryPluginException {
    super(conf, updateable, imageRetreiver, updateLogSize, initUpdateRetryDelay);
    zkPath = conf.get(ServerConfig.SENTRY_HDFS_HA_ZOOKEEPER_NAMESPACE,
        ServerConfig.SENTRY_HDFS_HA_ZOOKEEPER_NAMESPACE_DEFAULT) + "/" +
        updateable.getUpdateableTypeName();
    pluginCacheSync = new PluginCacheSyncUtil(zkPath, conf,
        new SentryHAPathChildrenCacheListener<K>(getUpdateLog(), baseUpdate,
            this));
  }

  @Override
  public String getUpdateableTypeName() {
    return UPDATABLE_TYPE_NAME;
  }

  @Override
  public void handleUpdateNotification(final K update) throws SentryPluginException {
    pluginCacheSync.handleCacheUpdate(update);
  }

  private void postNotificationToLog(K update) throws SentryPluginException {
    super.handleUpdateNotification(update);
  }

  @Override
  public void close() throws IOException {
    pluginCacheSync.close();
  }

  @Override
  public boolean areAllUpdatesCommited() {
    try {
      if (lastCommittedSeqNum.get() == INIT_SEQ_NUM) {
        return false;
      }
      return lastCommittedSeqNum.get() == pluginCacheSync.getUpdateCounter();
    } catch (Exception e) {
      LOGGER.error("Error loading the update counter for ZK", e);
      return true;
    }
  }

}
