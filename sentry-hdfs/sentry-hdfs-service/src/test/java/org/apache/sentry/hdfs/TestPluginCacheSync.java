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

import static org.junit.Assert.*;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.TestUpdateForwarder.DummyUpdate;
import org.apache.sentry.provider.db.service.persistent.HAContext;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPluginCacheSync {
  // Test PathChildrenCacheListener to track the Update event received from ZK
  public static class TestCacheListener implements PathChildrenCacheListener {
    private DummyUpdate dummyUpdate;
    private boolean recievedEvent = false;

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
        throws Exception {
      switch (event.getType()) {
      case CHILD_ADDED:
        DummyUpdate newUpdate = new DummyUpdate();
        PluginCacheSyncUtil.setUpdateFromChildEvent(event, newUpdate);
        dummyUpdate = newUpdate;
        recievedEvent = true;
        break;
      default:
        break;
      }
    }

    public DummyUpdate getDummyUpdate() {
      return dummyUpdate;
    }

    public boolean isRecievedEvent() {
      return recievedEvent;
    }

    public void setRecievedEvent(boolean recievedEvent) {
      this.recievedEvent = recievedEvent;
    }
  }

  private static final String TEST_ZPATH = "/test";
  private static TestingServer testServer;
  private static Configuration conf;

  private PluginCacheSyncUtil pluginCache;

  @BeforeClass
  public static void preSetup() throws Exception {
    testServer = new TestingServer();
    testServer.start();
    conf = new Configuration();
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM,
        testServer.getConnectString());
  }

  @After
  public void destroy() throws Exception {
    pluginCache.close();
  }

  @AfterClass
  public static void shutDown() throws Exception {
    testServer.stop();
  }

  /**
   * Post a dummy update to PluginCacheSync. Verify that the update is received
   * by cache via ZK sync
   * @throws Exception
   */
  @Test
  public void testCachePost() throws Exception {
    TestCacheListener cacheListener = new TestCacheListener();
    pluginCache = new PluginCacheSyncUtil(TEST_ZPATH, conf,
        cacheListener);

    // post an update
    DummyUpdate dummyUpdate = new DummyUpdate();
    dummyUpdate.setState("foo");
    pluginCache.handleCacheUpdate(dummyUpdate);

    // wait for update to sync up
    int timeLeft = 5000;
    while (!cacheListener.isRecievedEvent() && (timeLeft > 0)) {
      Thread.sleep(200);
      timeLeft -= 200;
    }
    cacheListener.setRecievedEvent(false);
    DummyUpdate newUpdate = cacheListener.getDummyUpdate();
    assertEquals(dummyUpdate.getState(), newUpdate.getState());
  }

  @Test
  public void pluginCacheGC() throws Exception {
    pluginCache = new PluginCacheSyncUtil(TEST_ZPATH, conf,
        new TestCacheListener());

    // post updates
    for (int updCount = 1; updCount <= PluginCacheSyncUtil.CACHE_GC_SIZE_THRESHOLD_HWM + 2; updCount++) {
      DummyUpdate dummyUpdate = new DummyUpdate();
      dummyUpdate.setSeqNum(updCount);
      dummyUpdate.setState("foo");
      pluginCache.handleCacheUpdate(dummyUpdate);
    }

    // force gc
    pluginCache.gcPluginCache(conf);

    // count remaining znodes
    HAContext haContext = HAContext.getHAContext(conf);
    List<String> znodeList = haContext.getCuratorFramework().getChildren()
        .forPath(TEST_ZPATH + "/cache");
    assertFalse(znodeList.isEmpty());
    assertFalse(znodeList.contains(String
        .valueOf(PluginCacheSyncUtil.GC_COUNTER_INIT_VALUE)));
    assertFalse(znodeList.contains(String
        .valueOf(PluginCacheSyncUtil.GC_COUNTER_INIT_VALUE + 1)));
    assertTrue(znodeList.contains(String
        .valueOf(PluginCacheSyncUtil.CACHE_GC_SIZE_THRESHOLD_HWM)));
  }
}
