/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package  org.apache.sentry.provider.db.service.persistent;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit Tests for LeaderStatusMonitor.
 * Use Curator TestingServer as Zookeeper Server.
 */
@SuppressWarnings("NestedTryStatement")
public class TestLeaderStatusMonitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestLeaderStatusMonitor.class);

  // Delay between retries
  private static final int DELAY_MS = 500;
  // Maximum number of tries before giving up while waiting for leader
  private static final int NTRIES = 360;
  // Number of times test is repeated
  private static final int ITERATIONS = 10;

  /**
   * Wait for some time (u to 500 seconds) until the monitor becomes active
   * @param monitor HA monitor
   * @return true if monitor is active, false otherwise
   */
  @SuppressWarnings("squid:S2925")
  private boolean isLeader(LeaderStatusMonitor monitor) {
    for (int i = 0; i < NTRIES; i++) {
      if (monitor.isLeader()) {
        return true;
      }
      try {
        sleep(DELAY_MS);
      } catch (InterruptedException ignored) {
        Thread.interrupted();
      }
    }
    return false;
  }

  /**
   * Simple test case - leader monitor without Zookeeper.
   * Should always be the leader.
   * @throws Exception
   */
  @Test
  public void testNoZk() throws Exception {
    Configuration conf = new Configuration();
    LeaderStatusMonitor monitor = new LeaderStatusMonitor(conf);
    assertTrue(monitor.isLeader());
  }

  /**
   * Single server scenario.
   * Should always be the leader.
   * Should continue to be the leader after resigning the leadership.
   *
   * <p>
   * <ol>
   * <li>Start ZK Server</li>
   * <li>Create monitor</li>
   * <li>Monitor should become active</li>
   * <li>Drop active status</li>
   * <li>Monitor should become active again</li>
   * </ol>
   * @throws Exception
   */
  @Test
  @SuppressWarnings("squid:S2925")
  public void testSingleServer() throws Exception {
    try(TestingServer zkServer = new TestingServer()) {
      zkServer.start();
      Configuration conf = new Configuration();
      conf.set(SENTRY_HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
      try(LeaderStatusMonitor monitor = new LeaderStatusMonitor(conf)) {
        monitor.init();
        for (int i = 0; i < ITERATIONS; i++) {
          assertTrue(isLeader(monitor));
          LOGGER.debug("testSingleServer(): deactivating leader");
          monitor.deactivate();
          sleep(2 * DELAY_MS);
          assertTrue(isLeader(monitor));
          LOGGER.info("testSingleServer({}, leaderCount = {}", i, monitor.getLeaderCount());
        }
        assertEquals(ITERATIONS + 1, monitor.getLeaderCount());
      }
    } finally {
      HAContext.resetHAContext();
    }
  }

  /**
   * Single server scenario with restarting ZK server
   * <p>
   * <ol>
   * <li>Start ZK Server</li>
   * <li>Create monitor</li>
   * <li>at some point monitor should become active</li>
   * <li>Restart ZK server</li>
   * <li>at some point monitor should become active again</li>
   * </ol>
   * @throws Exception
   */
  @Test
  public void testSingleServerZkRestart() throws Exception {
    try(TestingServer zkServer = new TestingServer()) {
      zkServer.start();
      Configuration conf = new Configuration();
      conf.set(SENTRY_HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
      try(LeaderStatusMonitor monitor = new LeaderStatusMonitor(conf)) {
        monitor.init();
        for (int i = 0; i < ITERATIONS; i++) {
          assertTrue(isLeader(monitor));
          LOGGER.debug("testSingleServerZkRestart(): restarting Zk server");
          zkServer.restart();
          assertTrue(isLeader(monitor));
          LOGGER.info("testSingleServerZkRestart({}, leaderCount = {}", i, monitor.getLeaderCount());
          assertEquals(i + 2, monitor.getLeaderCount());
        }
      }
    } finally {
      HAContext.resetHAContext();
    }
  }

  /**
   * Dual server scenario
   * <p>
   * <ol>
   * <li>Start ZK Server</li>
   * <li>Create monitor1 and monitor2</li>
   * <li>at some point one of monitors should become active</li>
   * <li>Drop active status on monitor 2</li>
   * <li>Monitor1 should become active</li>
   * <li>Drop active status on monitor1</li>
   * <li>Monitor2 should become active</li>
   * </ol>
   * @throws Exception
   */
  @Test
  @SuppressWarnings("squid:S2925")
  public void testTwoServers() throws Exception {
    try(TestingServer zkServer = new TestingServer()) {
      zkServer.start();
      Configuration conf = new Configuration();
      conf.set(SENTRY_HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
      try (LeaderStatusMonitor monitor1 = new LeaderStatusMonitor(conf, "1");
           LeaderStatusMonitor monitor2 = new LeaderStatusMonitor(conf, "2")) {
        monitor1.init();
        monitor2.init();
        // Wait until one of monitors is active
        for (int i = 0; i < NTRIES; i++) {
          if (monitor1.isLeader() || monitor2.isLeader()) {
            break;
          }
          try {
            sleep(DELAY_MS);
          } catch (InterruptedException ignored) {
            Thread.interrupted();
          }
        }

        for (int i = 0; i < ITERATIONS; i++) {
          monitor2.deactivate();
          assertTrue(isLeader(monitor1));
          monitor1.deactivate();
          assertTrue(isLeader(monitor2));
        }
      }
    } finally {
      HAContext.resetHAContext();
    }
  }
}
