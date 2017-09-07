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
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import javax.annotation.concurrent.ThreadSafe;

/**
 * PathImageRetriever obtains a complete snapshot of Hive Paths from a persistent
 * storage and translates it into {@code PathsUpdate} that the consumers, such as
 * HDFS NameNode, can understand.
 * <p>
 * It is a thread safe class, as all the underlying database operation is thread safe.
 */
@ThreadSafe
class PathImageRetriever implements ImageRetriever<PathsUpdate> {

  private final SentryStore sentryStore;
  /** List of prefixes managed by Sentry */
  private final String[] prefixes;

  PathImageRetriever(SentryStore sentryStore, String[] prefixes) {
    this.sentryStore = sentryStore;
    this.prefixes = prefixes;
  }

  @Override
  /**
   * Retrieve full image from SentryStore.
   * The image only contains PathsDump and is only useful for sending to the NameNode.
   */
  public PathsUpdate retrieveFullImage() throws Exception {
    try (final Timer.Context timerContext =
        SentryHdfsMetricsUtil.getRetrievePathFullImageTimer.time()) {
      return sentryStore.retrieveFullPathsImageUpdate(prefixes);
    }
  }

  @Override
  public long getLatestImageID() throws Exception {
    return sentryStore.getLastProcessedImageID();
  }
}
