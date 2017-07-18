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
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

/**
 * PathImageRetriever obtains a complete snapshot of Hive Paths from a persistent
 * storage and translates it into {@code PathsUpdate} that the consumers, such as
 * HDFS NameNode, can understand.
 * <p>
 * It is a thread safe class, as all the underlying database operation is thread safe.
 */
@ThreadSafe
class PathImageRetriever implements ImageRetriever<PathsUpdate> {

  private static final String[] root = {"/"};
  private final SentryStore sentryStore;

  PathImageRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public PathsUpdate retrieveFullImage() throws Exception {
    try (final Timer.Context timerContext =
        SentryHdfsMetricsUtil.getRetrievePathFullImageTimer.time()) {

      // Reads a up-to-date complete snapshot of Hive paths from the
      // persistent storage, along with the sequence number of latest
      // delta change the snapshot corresponds to.
      PathsImage pathsImage = sentryStore.retrieveFullPathsImage();
      long curImgNum = pathsImage.getCurImgNum();
      long curSeqNum = pathsImage.getId();
      Map<String, Set<String>> pathImage = pathsImage.getPathImage();

      // Translates the complete Hive paths snapshot into a PathsUpdate.
      // Adds all <hiveObj, paths> mapping to be included in this paths update.
      // And label it with the latest delta change sequence number for consumer
      // to be aware of the next delta change it should continue with.
      PathsUpdate pathsUpdate = new PathsUpdate(curSeqNum, curImgNum, true);
      for (Map.Entry<String, Set<String>> pathEnt : pathImage.entrySet()) {
        TPathChanges pathChange = pathsUpdate.newPathChange(pathEnt.getKey());

        for (String path : pathEnt.getValue()) {
          pathChange.addToAddPaths(Lists.newArrayList(Splitter.on("/").split(path)));
        }
      }

      SentryHdfsMetricsUtil.getPathChangesHistogram.update(pathsUpdate
          .getPathChanges().size());

      // Translate PathsUpdate that contains a full image to TPathsDump for
      // consumer (NN) to be able to quickly construct UpdateableAuthzPaths
      // from TPathsDump.
      UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(root);
      authzPaths.updatePartial(Lists.newArrayList(pathsUpdate),
          new ReentrantReadWriteLock());
      pathsUpdate.toThrift().setPathsDump(authzPaths.getPathsDump().createPathsDump());
      return pathsUpdate;
    }
  }

  @Override
  public long getLatestImageID() throws Exception {
    return sentryStore.getLastProcessedImageID();
  }
}
