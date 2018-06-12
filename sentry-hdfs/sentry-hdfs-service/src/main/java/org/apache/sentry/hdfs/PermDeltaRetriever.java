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

import com.codahale.metrics.Timer.Context;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * PermDeltaRetriever retrieves delta updates of Sentry permission from a persistent
 * storage and translates it into a collection of {@code PermissionsUpdate} that the
 * consumers, such as HDFS NameNode, can understand.
 * <p>
 * It is a thread safe class, as all the underlying database operation is thread safe.
 */
@ThreadSafe
public class PermDeltaRetriever implements DeltaRetriever<PermissionsUpdate> {

  private final SentryStore sentryStore;

  PermDeltaRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public List<PermissionsUpdate> retrieveDelta(long seqNum, long imgNum) throws Exception {
    try (final Context timerContext =
                 SentryHdfsMetricsUtil.getDeltaPermChangesTimer.time()) {
      Collection<MSentryPermChange> mSentryPermChanges =
              sentryStore.getMSentryPermChanges(seqNum);

      SentryHdfsMetricsUtil.getDeltaPermChangesHistogram.update(mSentryPermChanges.size());

      if (mSentryPermChanges.isEmpty()) {
        return Collections.emptyList();
      }

      List<PermissionsUpdate> updates = new ArrayList<>(mSentryPermChanges.size());
      for (MSentryPermChange mSentryPermChange : mSentryPermChanges) {
        // Get the changeID from the persisted MSentryPermChange
        long changeID = mSentryPermChange.getChangeID();
        // Create a corresponding PermissionsUpdate and deserialize the
        // persisted delta update in JSON format to TPermissionsUpdate with
        // associated changeID.
        PermissionsUpdate permsUpdate = new PermissionsUpdate();
        permsUpdate.JSONDeserialize(mSentryPermChange.getPermChange());
        permsUpdate.setSeqNum(changeID);
        Collection<TPrivilegeChanges> privChanges = permsUpdate.getPrivilegeUpdates();
        for(TPrivilegeChanges privChange : privChanges) {
          DBUpdateForwarder.translateOwnerPrivileges(privChange.getAddPrivileges());
          DBUpdateForwarder.translateOwnerPrivileges(privChange.getDelPrivileges());
        }
        updates.add(permsUpdate);
      }
      return updates;
    }
  }

  @Override
  public boolean isDeltaAvailable(long seqNum) throws Exception {
    return sentryStore.permChangeExists(seqNum);
  }

  @Override
  public long getLatestDeltaID() throws Exception {
    return sentryStore.getLastProcessedPermChangeID();
  }
}