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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * DBUpdateForwarder propagates a complete snapshot or delta update of either
 * Sentry Permissions ({@code PermissionsUpdate}) or Sentry representation of
 * HMS Paths ({@code PathsUpdate}), retrieved from a persistent storage, to a
 * Sentry client, e.g HDFS NameNode.
 * <p>
 * It is a thread safe class, as all the underlying database operation is thread safe.
 */
@ThreadSafe
class DBUpdateForwarder<K extends Updateable.Update> {

  private final ImageRetriever<K> imageRetriever;
  private final DeltaRetriever<K> deltaRetriever;
  private static final Logger LOGGER = LoggerFactory.getLogger(DBUpdateForwarder.class);

  DBUpdateForwarder(final ImageRetriever<K> imageRetriever,
      final DeltaRetriever<K> deltaRetriever) {
    this.imageRetriever = imageRetriever;
    this.deltaRetriever = deltaRetriever;
  }

  /**
   * Retrieves all delta updates from the requested sequence number (inclusive) from
   * a persistent storage.
   * It first checks if there is such newer deltas exists in the persistent storage.
   * If there is, returns a list of delta updates.
   * Otherwise, a complete snapshot will be returned.
   *
   * @param seqNum the requested sequence number
   * @return a list of delta updates, e.g. {@link PathsUpdate} or {@link PermissionsUpdate}
   */
   List<K> getAllUpdatesFrom(long seqNum) throws Exception {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("#### GetAllUpdatesFrom [reqSeqNum = {} ]", seqNum);
    }

    // No newer updates available than the requested one.
    long curSeqNum = deltaRetriever.getLatestDeltaID();
    if (seqNum > curSeqNum) {
      return Collections.emptyList();
    }

    // Checks if there is newer deltas exists in the persistent storage.
    // If there is, returns a list of delta updates.
    if ((seqNum != SentryStore.INIT_CHANGE_ID) &&
          deltaRetriever.isDeltaAvailable(seqNum)) {
      Collection<K> deltas = deltaRetriever.retrieveDelta(seqNum);
      if (!deltas.isEmpty()) {
        return new LinkedList<>(deltas);
      }
    }

    // Otherwise, a complete snapshot will be returned.
    List<K> retVal = new LinkedList<>();
    retVal.add(imageRetriever.retrieveFullImage());
    return retVal;
  }
}
