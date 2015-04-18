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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin.SentryPluginException;
import org.apache.sentry.binding.metastore.MetastoreAuthzBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastorePluginWithHA extends MetastorePlugin {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(MetastorePluginWithHA.class);
  public static class SentryMetastoreHACacheListener implements PathChildrenCacheListener {
    private MetastorePluginWithHA metastorePlugin;

    public SentryMetastoreHACacheListener(MetastorePluginWithHA metastorePlugin) {
      this.metastorePlugin = metastorePlugin;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
        throws Exception {
      switch ( event.getType() ) {
      case CHILD_ADDED:
        PathsUpdate newUpdate = new PathsUpdate();
        PluginCacheSyncUtil.setUpdateFromChildEvent(event, newUpdate);
        metastorePlugin.processCacheNotification(newUpdate);
        break;
      case INITIALIZED:
      case CHILD_UPDATED:
      case CHILD_REMOVED:
        break;
      case CONNECTION_RECONNECTED:
        MetastoreAuthzBinding.setSentryCacheOutOfSync(false);
        break;
      case CONNECTION_SUSPENDED:
      case CONNECTION_LOST:
        MetastoreAuthzBinding.setSentryCacheOutOfSync(true);
        break;
      default:
        break;
      }
    }
  }

  private String zkPath;
  private PluginCacheSyncUtil pluginCacheSync;

  public MetastorePluginWithHA(Configuration conf, Configuration sentryConfig) throws Exception {
    super(conf, sentryConfig);
    zkPath = sentryConfig.get(ServerConfig.SENTRY_METASTORE_HA_ZOOKEEPER_NAMESPACE,
        ServerConfig.SENTRY_METASTORE_HA_ZOOKEEPER_NAMESPACE_DEFAULT);

    pluginCacheSync = new PluginCacheSyncUtil(zkPath, sentryConfig,
        new SentryMetastoreHACacheListener(this));
    // start seq# from the last global seq
    seqNum.set(pluginCacheSync.getUpdateCounter());
    MetastorePlugin.lastSentSeqNum = seqNum.get();
  }

  @Override
  protected void processUpdate(PathsUpdate update) {
    try {
      // push to ZK in order to keep the metastore local cache in sync
      pluginCacheSync.handleCacheUpdate(update);

      // notify Sentry. Note that Sentry service already has a cache
      // sync mechanism to replicate this update to all other Sentry servers
      notifySentry(update);
    } catch (SentryPluginException e) {
      LOGGER.error("Error pushing update to cache", e);
    }
  }

  // apply the update to local cache
  private void processCacheNotification(PathsUpdate update) {
    super.applyLocal(update);
  }
}
