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

package org.apache.sentry.service.thrift;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryStandbyException;
import org.apache.sentry.provider.db.service.persistent.Fencer;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

/**
 * The activator is used to access and modify the activation state of the sentry daemon.<p/>
 */
public class Activator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Activator.class);

  /**
   * The DataNucleus PersistenceManagerFactory to use.
   */
  private final PersistenceManagerFactory pmf;

  /**
   * The handler for LeaderStatus callbacks.
   */
  private final TransitionHandler handler;

  /**
   * LeaderStatus generates callbacks to let us know when we are active or
   * standby.  When HA is enabled, it manages ZK sessions.
   */
  private final LeaderStatus leaderStatus;

  /**
   * The fencer object.
   */
  private final Fencer fencer;

  /**
   * True if the Activator is active.
   *
   * This variable can be read without taking the lock, but must not be
   * written unless we hold the Activator lock.
   */
  private volatile boolean active = false;

  public Activator(Configuration conf) throws Exception {
    Properties props = SentryStore.getDataNucleusProperties(conf);
    this.pmf = JDOHelper.getPersistenceManagerFactory(props);
    this.handler = new TransitionHandler();
    this.leaderStatus = new LeaderStatus(handler, conf);
    this.fencer = new Fencer(this.leaderStatus.getIncarnationId(), pmf);
    this.leaderStatus.start();
  }

  @Override
  public void close() throws IOException {
    this.leaderStatus.close();
    this.pmf.close();
  }

  private class TransitionHandler implements LeaderStatus.Listener {
    @Override
    public void becomeActive() throws Exception {
      synchronized (Activator.this) {
        if (!active) {
          LOGGER.info("Activating " + leaderStatus.getIncarnationId());
          fencer.fence(pmf);
          active = true;
        }
      }
    }

    @Override
    public void becomeStandby() {
      synchronized (Activator.this) {
        if (active) {
          LOGGER.info("Deactivating " + leaderStatus.getIncarnationId());
          active = false;
        }
      }
    }
  }

  /**
   * Returns true if this Activator considers itself active.
   * Note that you must still use checkSqlFencing or another
   * means of fencing when performing modification operations.
   */
  public boolean isActive() {
    return active;
  }

  public synchronized String getIncarnationId() {
    return leaderStatus.getIncarnationId();
  }

  public Fencer getFencer() {
    return fencer;
  }

  /**
   * Verify that the current SQL transaction is safe. 
   */
  public void checkSqlFencing(PersistenceManager pm)
        throws SentryStandbyException {
    // Before invoking the fencer, first check if we believe that we are
    // active.  This avoids wasted effort.
    if (!active) {
      throw new SentryStandbyException("The daemon is not active");
    }
    // If we believe that we are active, add a query to the current transaction
    // which will confirm that fact.
    fencer.checkSqlFencing(pm);
  }
}
