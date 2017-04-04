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

import com.codahale.metrics.Timer;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * PathDeltaRetriever retrieves delta updates of Hive Paths from a persistent
 * storage and translates them into a collection of {@code PathsUpdate} that the
 * consumers, such as HDFS NameNode, can understand.
 * <p>
 * It is a thread safe class, as all the underlying database operation is thread safe.
 */
@ThreadSafe
public class PathDeltaRetriever implements DeltaRetriever<PathsUpdate> {

  private final SentryStore sentryStore;

  PathDeltaRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public Collection<PathsUpdate> retrieveDelta(long seqNum) throws Exception {
    try (final Timer.Context timerContext =
                 SentryHdfsMetricsUtil.getDeltaPathChangesTimer.time()) {
      Collection<MSentryPathChange> mSentryPathChanges =
              sentryStore.getMSentryPathChanges(seqNum);

      SentryHdfsMetricsUtil.getDeltaPathChangesHistogram.update(mSentryPathChanges.size());

      if (mSentryPathChanges.isEmpty()) {
        return Collections.emptyList();
      }

      Collection<PathsUpdate> updates = new ArrayList<>(mSentryPathChanges.size());
      for (MSentryPathChange mSentryPathChange : mSentryPathChanges) {
        // Gets the changeID from the persisted MSentryPathChange.
        long changeID = mSentryPathChange.getChangeID();
        // Creates a corresponding PathsUpdate and deserialize the
        // persisted delta update in JSON format to TPathsUpdate with
        // associated changeID.
        PathsUpdate pathsUpdate = new PathsUpdate();
        pathsUpdate.JSONDeserialize(mSentryPathChange.getPathChange());
        pathsUpdate.setSeqNum(changeID);
        updates.add(pathsUpdate);
      }
      return updates;
    }
  }

  @Override
  public boolean isDeltaAvailable ( long seqNum) throws Exception {
    return sentryStore.pathChangeExists(seqNum);
  }

  @Override
  public long getLatestDeltaID () throws Exception {
    return sentryStore.getLastProcessedPathChangeID();
  }
}