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
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

import static org.apache.sentry.service.thrift.ServiceConstants.ClientConfig.SENTRY_HA_ENABLED;
import static org.apache.sentry.service.thrift.ServiceConstants.ClientConfig.SENTRY_HA_ZOOKEEPER_QUORUM;

final public class TestLeaderStatus {
  private static final Log LOG =
      LogFactory.getLog(TestLeaderStatus.class);

  /**
   * Test that when the configuration is non-HA, we always become active.
   */
  @Test(timeout = 60000)
  public void testNonHaLeaderStatus() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SENTRY_HA_ZOOKEEPER_QUORUM, "");
    final Semaphore activeSem = new Semaphore(0);
    final Semaphore standbySem = new Semaphore(0);
    LeaderStatus status = new LeaderStatus(new LeaderStatus.Listener() {
      @Override
      public void becomeActive() throws Exception {
        LOG.info("testNonHaLeaderStatus: becoming active");
        activeSem.release(2);
      }

      @Override
      public void becomeStandby() {
        activeSem.acquireUninterruptibly();
        LOG.info("testNonHaLeaderStatus: becoming standby");
        standbySem.release();
      }
    }, conf);
    status.start();
    activeSem.acquire();
    status.close();
    standbySem.acquire();
  }

  private static class CurrentTestActive {
    private String incarnationId;
    private String error = null;

    CurrentTestActive() {
      this.incarnationId = null;
      this.error = null;
    }

    synchronized void set(String incarnationId) {
      if (this.incarnationId != null) {
        error("set: there is already an " +
            "active incarnation " + this.incarnationId);
        return;
      }
      this.incarnationId = incarnationId;
    }

    synchronized void unset(String incarnationId) {
      if (this.incarnationId == null) {
        error("unset: there is no active incarnation.");
        return;
      }
      if (!this.incarnationId.equals(incarnationId)) {
        error("unset: can't deactivate " +
            incarnationId + " because " + this.incarnationId +
            " is the current active incarnation.");
        return;
      }
      this.incarnationId = null;
    }

    synchronized String get() {
      return this.incarnationId;
    }

    synchronized String getError() {
      return error;
    }

    synchronized void error(String error) {
      if (this.error == null) {
        this.error = error;
      }
      LOG.error(error);
    }

    String busyWaitForActive() throws InterruptedException {
      for (; ; ) {
        String cur = get();
        if (cur != null) {
          return cur;
        }
        Thread.sleep(2);
      }
    }

    String busyWaitForNextActive(String prevIncarnation)
        throws InterruptedException {
      for (; ; ) {
        String cur = get();
        if ((cur != null) && (!cur.equals(prevIncarnation))) {
          return cur;
        }
        Thread.sleep(2);
      }
    }
  }

  static class LeaderStatusContext implements Closeable {
    final LeaderStatus status;

    LeaderStatusContext(final CurrentTestActive active,
                        Configuration conf) throws Exception {
      this.status = new LeaderStatus(new LeaderStatus.Listener() {
        @Override
        public void becomeActive() throws Exception {
          LOG.info("LeaderStatusContext " + status.getIncarnationId() +
              " becoming active");
          active.set(status.getIncarnationId());
        }

        @Override
        public void becomeStandby() {
          LOG.info("LeaderStatusContext " + status.getIncarnationId() +
              " becoming standby");
          active.unset(status.getIncarnationId());
        }
      }, conf);
      this.status.start();
    }

    @Override
    public void close() throws IOException {
      this.status.close();
    }

    @Override
    public String toString() {
      return "LeaderStatusContext(" + status.getIncarnationId() + ")";
    }

    String getIncarnationId() {
      return status.getIncarnationId();
    }
  }

  @Test(timeout = 120000)
  public void testRacingClients() throws Exception {
    final int NUM_CLIENTS = 3;
    final Configuration conf = new Configuration();
    TestingServer server = new TestingServer();
    server.start();
    conf.setBoolean(SENTRY_HA_ENABLED, true);
    conf.set(SENTRY_HA_ZOOKEEPER_QUORUM, server.getConnectString());
    final CurrentTestActive active = new CurrentTestActive();
    List<LeaderStatusContext> contexts = new LinkedList<>();
    for (int i = 0; i < NUM_CLIENTS; i++) {
      try {
        contexts.add(new LeaderStatusContext(active, conf));
      } catch (Throwable t) {
        LOG.error("error creating LeaderStatusContext", t);
        throw new RuntimeException(t);
      }
    }
    LOG.info("Created " + NUM_CLIENTS + " SentryLeaderSelectorClient " +
        "objects.");
    String curIncarnation = active.busyWaitForActive();
    LOG.info("Closing LeaderStatus(" + curIncarnation + ").");
    for (Iterator<LeaderStatusContext> iter = contexts.iterator();
         iter.hasNext(); ) {
      LeaderStatusContext context = iter.next();
      if (context.getIncarnationId().equals(curIncarnation)) {
        CloseableUtils.closeQuietly(context);
        iter.remove();
      }
    }
    active.busyWaitForNextActive(curIncarnation);
    for (Iterator<LeaderStatusContext> iter = contexts.iterator();
         iter.hasNext(); ) {
      LeaderStatusContext context = iter.next();
      CloseableUtils.closeQuietly(context);
      iter.remove();
    }
    LOG.info("Closed all " + NUM_CLIENTS + " SentryLeaderSelectorClient " +
        "objects.");
    Assert.assertTrue(null == active.getError());
    server.close();
  }

  @Test(timeout = 60000)
  public void testGenerateIncarnationIDs() throws Exception {
    final int NUM_UNIQUE_IDS = 10000;
    HashSet<String> ids = new HashSet<String>();
    for (int i = 0; i < NUM_UNIQUE_IDS; i++) {
      ids.add(LeaderStatus.generateIncarnationId());
    }

    // Assert that there were no ID collisions
    Assert.assertEquals(NUM_UNIQUE_IDS, ids.size());

    // Assert that all IDs are 44 characters long and begin with a letter.
    for (String id : ids) {
      Assert.assertEquals(44, id.length());
      Assert.assertTrue(Character.isAlphabetic(id.charAt(0)));
    }

    // Assert that IDs contain only alphanumeric characters
    for (String id : ids) {
      for (int i = 0; i < id.length(); i++) {
        Assert.assertTrue(Character.isLetterOrDigit(id.charAt(i)));
      }
    }
  }
}
