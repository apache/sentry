/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.service.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM_DEFAULT;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT;

/**
 * Determines the leadership status of the Sentry daemon.
 */
final class LeaderStatusAdaptor
      extends LeaderSelectorListenerAdapter implements Closeable {
  private static final Log LOG =
      LogFactory.getLog(LeaderStatusAdaptor.class);

  private final String LEADER_SELECTOR_SUFFIX = "leader";

  /**
   * The ZooKeeper path prefix to use.
   */
  private final String zkNamespace;

  /**
   * The Curator framework object.
   */
  private final CuratorFramework framework;

  /**
   * The listener which we should notify about HA state changes.
   */
  private final LeaderStatus.Listener listener;

  /**
   * The Curator LeaderSelector object.
   */
  private final LeaderSelector leaderSelector;

  /**
   * The lock which protects isActive.
   */
  private final ReentrantLock lock  = new ReentrantLock();

  /**
   * A condition variable which the takeLeadership function will wait on.
   */
  private final Condition cond = lock.newCondition();

  /**
   * The number of times this incarnation has become the leader.
   */
  private long becomeLeaderCount = 0;

  /**
   * True only if this LeaderStatusAdaptor is closed.
   */
  private boolean isClosed = false;

  /**
   * True only if this incarnation is currently active.
   */
  private boolean isActive = false;

  LeaderStatusAdaptor(String incarnationId, Configuration conf,
      LeaderStatus.Listener listener) {
    this.zkNamespace = conf.get(SENTRY_HA_ZOOKEEPER_NAMESPACE,
        SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT);
    String zkServers = conf.get(SENTRY_HA_ZOOKEEPER_QUORUM,
        SENTRY_HA_ZOOKEEPER_QUORUM_DEFAULT);
    if ((zkServers == null) || (zkServers.trim().isEmpty())) {
      throw new RuntimeException("You must configure some ZooKeeper " +
          "servers via " + SENTRY_HA_ZOOKEEPER_QUORUM + " when enabling HA");
    }
    this.framework = CuratorFrameworkFactory.newClient(zkServers,
            new ExponentialBackoffRetry(1000, 3));
    this.framework.start();
    this.listener = listener;
    this.leaderSelector = new LeaderSelector(this.framework,
        this.zkNamespace + "/" + LEADER_SELECTOR_SUFFIX, this);
    this.leaderSelector.setId(incarnationId);
    this.leaderSelector.autoRequeue();
    LOG.info("Created LeaderStatusAdaptor(zkNamespace=" + zkNamespace +
        ", incarnationId=" + incarnationId +
        ", zkServers='" + zkServers + "')");
  }

  public void start() {
    this.leaderSelector.start();
  }

  /**
   * Shut down the LeaderStatusAdaptor and wait for it to transition to
   * standby.
   */
  @Override
  public void close() throws IOException {
    // If the adaptor is already closed, calling close again is a no-op.
    // Setting isClosed also prevents activation after this point.
    lock.lock();
    try {
      if (isClosed) {
        return;
      }
      isClosed = true;
    } finally {
      lock.unlock();
    }

    // Shut down our Curator hooks.
    leaderSelector.close();

    // Wait for the adaptor to transition to standby state.
    lock.lock();
    try {
      while (isActive) {
        cond.awaitUninterruptibly();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return true if this client is the current leader.
   */
  public boolean isActive() {
    lock.lock();
    try {
      return isActive;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Deactivate the current client, if it is active.
   */
  public void deactivate() {
    lock.lock();
    try {
      if (isActive) {
        isActive = false;
        cond.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    lock.lock();
    try {
      if (isClosed) {
        LOG.info("LeaderStatusAdaptor: can't become active because the " +
            "adaptor is closed.");
        return;
      }
      isActive = true;
      becomeLeaderCount++;
      LOG.info("LeaderStatusAdaptor: becoming active.  " +
          "becomeLeaderCount=" + becomeLeaderCount);
      listener.becomeActive();
      while (isActive) {
        cond.await();
      }
    } finally {
      isActive = false;
      LOG.info("LeaderStatusAdaptor: becoming standby");
      try {
        listener.becomeStandby();
      } catch (Throwable t) {
        LOG.error("becomeStandby threw unexpected exception", t);
      }
      lock.unlock();
    }
  }
}
