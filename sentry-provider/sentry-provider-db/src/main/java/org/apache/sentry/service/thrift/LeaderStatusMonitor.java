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
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.HAContext;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.*;

/**
 * LeaderStatusMonitor participates in the distributed leader election protocol
 * and allows clients to access the global leaadership status.
 * <p>
 * LeaderStatusMonitor is a singleton that uses Curator framework via
 * {@link HAContext}.The leadership status can be accessed via the
 * {@link #isLeader()} method.<p>
 *
 * Usually leadership re-election is initiated by the Curator framework when one
 * of the nodes disconnects from ZooKeeper, but LeaderStatusMonitor also supports
 * voluntary release of the leadership via the {@link #deactivate()} method. This is
 * intended to be used for debugging purposes.
 * <p>
 * The class also simulates leader election in non-HA environments. In such cases its
 * {@link #isLeader()} method always returns True. The non-HA environment is determined
 * by the absence of the SENTRY_HA_ZOOKEEPER_QUORUM in the configuration.
 *
 * <h2>Implementation notes</h2>
 *
 * <h3>Initialization</h3>
 *
 * Class initialization is split between the constructor and the {@link #init()} method.
 * There are two reasons for it:
 * <ul>
 *     <li>We do not want to pass <strong>this</strong> reference to
 *     {@link HAContext#newLeaderSelector(String, LeaderSelectorListener)}
 *     until it is fully initialized</li>
 *     <li>We do not want to call {@link LeaderSelector#start()} method in constructor</li>
 * </ul>
 *
 * Since LeaderStatusMonitor is a singleton and an instance can only be obtained via the
 * {@link #getLeaderStatusMonitor(Configuration)} method, we hide this construction split
 * from the callers.
 *
 * <h3>Synchronization</h3>
 * Singleton synchronization is achieved using the synchronized class builder
 * {@link #getLeaderStatusMonitor(Configuration)}
 * <p>
 * Upon becoming a leader, the code loops in {@link #takeLeadership(CuratorFramework)}
 * until it receives a deactivation signal from {@link #deactivate()}. This is synchronized
 * using a {@link #lock} and condition variable {@link #cond}.
 * <p>
 * Access to the leadership status {@link #isLeader} is also protected by the {@link #lock}.
 * This isn't strictly necessary and a volatile field would be sufficient, but since we
 * already use the {@link #lock} this is more straightforward.
 */
@ThreadSafe
final class LeaderStatusMonitor
      extends LeaderSelectorListenerAdapter implements AutoCloseable {

  private static final Log LOG =
          LogFactory.getLog(LeaderStatusMonitor.class);

  private static final String LEADER_SELECTOR_SUFFIX = "leader";

  /** Unique instance of the singleton object */
  private static LeaderStatusMonitor leaderStatusMonitor = null;

  private final HAContext haContext;

  /** Unique string describing this instance */
  private final String incarnationId = generateIncarnationId();

  /** True when not using ZooKeeeper */
  private final boolean isSingleNodeMode;

  /** Lock and condition used to signal the leader to voluntary release leadership */
  private final Lock lock  = new ReentrantLock();
  /** Condition variable used to synchronize voluntary leadership release */
  private final Condition cond = lock.newCondition();
  /** Leadership status - true if leader. */
  @GuardedBy("lock")
  private boolean isLeader = false;

  /** Curator framework leader monitor */
  private LeaderSelector leaderSelector = null;

  /** The number of times this incarnation has become the leader. */
  private final AtomicLong leaderCount = new AtomicLong(0);

  /**
   * Constructor. Initialize state and create HA context if configuration
   * specifies ZooKeeper servers.
   * @param conf Configuration. The fields we are interested in are:
   *             <ul>
   *             <li>SENTRY_HA_ZOOKEEPER_QUORUM</li>
   *             </ul>
   *             Configuration is also passed to the
   *             {@link HAContext#newLeaderSelector(String, LeaderSelectorListener)}
   *             which uses more properties.
   * @throws Exception
   */
  private LeaderStatusMonitor(Configuration conf) throws Exception {
    // Only enable HA configuration if zookeeper is configured
    String zkServers = conf.get(SENTRY_HA_ZOOKEEPER_QUORUM, "");
    if (zkServers.isEmpty()) {
      isSingleNodeMode = true;
      haContext = null;
      isLeader = true;
      LOG.info("Leader election protocol disabled, assuming single active server");
      return;
    }
    isSingleNodeMode = false;
    haContext = HAContext.getHAServerContext(conf);

    LOG.info("Created LeaderStatusMonitor(incarnationId=" + incarnationId +
        ", zkServers='" + zkServers + "')");
  }

  /**
   * Second half of the constructor links this object with {@link HAContext} and
   * starts leader election protocol.
   */
  private void init() {
    if (isSingleNodeMode) {
      return;
    }

    leaderSelector = haContext.newLeaderSelector("/" + LEADER_SELECTOR_SUFFIX, this);
    leaderSelector.setId(incarnationId);
    leaderSelector.autoRequeue();
    leaderSelector.start();
  }

  /**
   *
   * @param conf Configuration. See {@link #LeaderStatusMonitor(Configuration)} for details.
   * @return A global LeaderStatusMonitor instance.
   * @throws Exception
   */
  @SuppressWarnings("LawOfDemeter")
  static synchronized LeaderStatusMonitor getLeaderStatusMonitor(Configuration conf)
          throws Exception {
    if (leaderStatusMonitor == null) {
      leaderStatusMonitor = new LeaderStatusMonitor(conf);
      leaderStatusMonitor.init();
    }
    return leaderStatusMonitor;
  }

  /**
   * @return number of times this leader was elected. Used for metrics.
   */
  long getLeaderCount() {
    return leaderCount.get();
  }

  /**
   * Shut down the LeaderStatusMonitor and wait for it to transition to
   * standby.
   */
  @Override
  public void close() {
    if (leaderSelector != null) {
      // Shut down our Curator hooks.
      leaderSelector.close();
    }
  }

  /**
   * Deactivate the current client, if it is active.
   * In non-HA case this is a no-op.
   */
  void deactivate() {
    if (isSingleNodeMode) {
      return;
    }
    lock.lock();
    try {
      cond.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return true iff we are the leader.
   * In non-HA case always returns true
   */
  boolean isLeader() {
    if (isSingleNodeMode) {
      return true;
    }
    lock.lock();
    @SuppressWarnings("FieldAccessNotGuarded")
    boolean leader = isLeader;
    lock.unlock();
    return leader;
  }

  /**
   * Curator framework callback which is called when we become a leader.
   * Should return only when we decide to resign.
   */
  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    leaderCount.incrementAndGet();
    LOG.info("LeaderStatusMonitor: becoming active. " +
            "leaderCount=" + leaderCount);
    lock.lock();
    try {
      isLeader = true;
      // Wait until we are interrupted or receive a signal
      cond.await();
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      LOG.info("LeaderStatusMonitor: interrupted");
    } finally {
      isLeader = false;
      lock.unlock();
      LOG.info("LeaderStatusMonitor: becoming standby");
    }
  }

  /**
   * Generate ID for the activator. <p>
   *
   * Ideally we would like something like host@pid, but Java doesn't provide a good
   * way to determine pid value, so we use
   * {@link RuntimeMXBean#getName()} which usually contains host
   * name and pid.
   */
  private static String generateIncarnationId() {
    return ManagementFactory.getRuntimeMXBean().getName();
  }

}
