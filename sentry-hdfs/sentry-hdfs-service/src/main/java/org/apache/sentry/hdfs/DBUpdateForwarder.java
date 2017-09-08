/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import static org.apache.sentry.hdfs.ServiceConstants.IMAGE_NUMBER_UPDATE_UNINITIALIZED;
import static org.apache.sentry.hdfs.ServiceConstants.SEQUENCE_NUMBER_FULL_UPDATE_REQUEST;

import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.sentry.service.thrift.SentryServiceState;
import org.apache.sentry.service.thrift.SentryStateBank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Retrieves all delta updates from the requested sequence number (inclusive) or a single full
   * update with its own sequence number from a persistent storage.
   * <p>
   * As part of the requested sequence number, an image number may also be used that identifies whether
   * new full updates are persisted and need to be retrieved instead of delta updates.
   * <p>
   * It first checks if there is such image number exists and/or has newer images persisted.
   * If a newer image is found, then it will return it as a new single full update.
   * Otherwise. it checks if there is such newer deltas exists in the persistent storage.
   * If there is, returns a list of delta updates.
   * Otherwise, an empty list is returned.
   *
   * @param imgNum the requested image number (>= 0).
   *               A value < 0 is identified as an unused value, and full updates would be returned
   *               only if the sequence number if <= 0.
   * @param seqNum the requested sequence number.
   *               Values <= 0 will be recognized as full updates request (unless an image number is used).
   * @return a list of full or delta updates (a full update is returned as a single-element list),
   *         e.g. {@link PathsUpdate} or {@link PermissionsUpdate}
   */
  List<K> getAllUpdatesFrom(long seqNum, long imgNum) throws Exception {
    LOGGER.debug("GetAllUpdatesFrom sequence number {}, image number {}", seqNum, imgNum);

    // An imgNum >= 0 are valid values for image identifiers (0 means a full update is requested)
    if (imgNum >= IMAGE_NUMBER_UPDATE_UNINITIALIZED) {
      long curImgNum = imageRetriever.getLatestImageID();
      LOGGER.debug("Current image number is {}", curImgNum);

      if (curImgNum == IMAGE_NUMBER_UPDATE_UNINITIALIZED) {
        // Sentry has not fetched a full HMS snapshot yet.
        return Collections.emptyList();
      }

      if (curImgNum > imgNum) {
        // In case a new HMS snapshot has been processed, then return a full paths image.
        LOGGER.info("A newer full update is found with image number: {}", curImgNum);
        return retrieveFullImage();
      }
    }

    /*
     * If no new images are found, then continue with checking for delta updates
     */

    long curSeqNum = deltaRetriever.getLatestDeltaID();
    LOGGER.debug("Current sequence number is {}", curSeqNum);

    if (seqNum > curSeqNum) {
      // No new notifications were processed.
      return Collections.emptyList();
    }

    // Checks if newer deltas exist in the persistent storage.
    // If there are, return the list of delta updates.
    if (seqNum > SEQUENCE_NUMBER_FULL_UPDATE_REQUEST && deltaRetriever.isDeltaAvailable(seqNum)) {
      List<K> deltas = deltaRetriever.retrieveDelta(seqNum, imgNum);
      if (!deltas.isEmpty()) {
        LOGGER.info("Newer delta updates are found up to sequence number: {}", curSeqNum);
        return deltas;
      }
    }

    // If the sequence number is < 0 or the requested delta is not available, then we
    // return a full update.
    LOGGER.info("A full update is returned due to an unavailable sequence number: {}", seqNum);
    return retrieveFullImage();
  }

  private List<K> retrieveFullImage() throws Exception {
    if (SentryStateBank.isEnabled(SentryServiceState.COMPONENT, SentryServiceState.FULL_UPDATE_RUNNING)){
      LOGGER.debug("A full update is being loaded. Delaying updating client with full image until its finished.");
      return Collections.emptyList();
    }
    else {
      return Collections.singletonList(imageRetriever.retrieveFullImage());
    }
  }
}
