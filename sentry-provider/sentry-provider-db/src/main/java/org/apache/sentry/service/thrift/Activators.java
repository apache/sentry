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

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

/**
 * A global map from incarnation IDs to Activator objects.<p/>
 *
 * This is used to access the current global Activator.  Normally there will
 * only be one Activator used in a sentry process.  There may be multiple
 * Activator objects in the case where we are running unit tests.
 */
public enum Activators {
  INSTANCE;

  private final HashMap<String, Activator> acts = new HashMap<String, Activator>();

  Activators() {}

  public synchronized void put(Activator act) {
    acts.put(act.getIncarnationId(), act);
  }

  public Activator create(Configuration conf) throws Exception {
    Activator act = new Activator(conf);
    put(act);
    return act;
  }

  public Activator get(Configuration conf) {
    String key = conf.get(ServiceConstants.CURRENT_INCARNATION_ID_KEY);
    if (key == null) {
      throw new RuntimeException("No " +
          ServiceConstants.CURRENT_INCARNATION_ID_KEY + "set.");
    }
    return get(key);
  }

  private synchronized Activator get(String incarnationId) {
    Activator act = acts.get(incarnationId);
    if (act == null) {
      throw new RuntimeException("No activator found with " +
          "incarnationId " + incarnationId);
    }
    return act;
  }

  public synchronized void remove(Activator act) {
    Activator removed = acts.remove(act.getIncarnationId());
    if (removed == null) {
      throw new RuntimeException("No activator found with " +
          "incarnationId " + act.getIncarnationId());
    }
  }
}
