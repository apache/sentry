/*
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
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import java.util.Map;
import java.util.Set;

/**
 * PathImageRetriever obtains a complete snapshot of Hive Paths from a persistent
 * storage and translate it into {@code PathsUpdate} that the consumers, such as
 * HDFS NameNod, can understand.
 */
public class PathImageRetriever implements ImageRetriever<PathsUpdate> {

  private final SentryStore sentryStore;

  PathImageRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public PathsUpdate retrieveFullImage(long seqNum) throws Exception {
    try (final Timer.Context timerContext =
        SentryHdfsMetricsUtil.getRetrievePathFullImageTimer.time()) {

      // Reads a up-to-date complete snapshot of Hive paths from the
      // persistent storage, along with the sequence number of latest
      // delta change the snapshot corresponds to.
      PathsImage pathsImage = sentryStore.retrieveFullPathsImage();
      long curSeqNum = pathsImage.getCurSeqNum();
      Map<String, Set<String>> pathImage = pathsImage.getPathImage();

      // Translates the complete Hive paths snapshot into a PathsUpdate.
      // Adds all <hiveObj, paths> mapping to be included in this paths update.
      // And label it with the latest delta change sequence number for consumer
      // to be aware of the next delta change it should continue with.
      // TODO: use curSeqNum from DB instead of seqNum when doing SENTRY-1613
      PathsUpdate pathsUpdate = new PathsUpdate(seqNum, true);
      for (Map.Entry<String, Set<String>> pathEnt : pathImage.entrySet()) {
        TPathChanges pathChange = pathsUpdate.newPathChange(pathEnt.getKey());

        for (String path : pathEnt.getValue()) {
          pathChange.addToAddPaths(PathsUpdate.splitPath(path));
        }
      }

      SentryHdfsMetricsUtil.getPathChangesHistogram.update(pathsUpdate
            .getPathChanges().size());
      return pathsUpdate;
    }
  }
}