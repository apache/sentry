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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

/**
 * A wrapper class over the PathsUpdate class that also stores a unique hash that identifies
 * unique HMS notification events.
 *
 * HMS notification events may have duplicated events IDs. To keep this unique, a SHA-1 hash
 * calculation is done and persisted on the SENTRY_PATH_CHANGE table.
 *
 * TODO: Stop using this class once HIVE-16886 is fixed.
 */
public class UniquePathsUpdate extends PathsUpdate {
  private final String eventHash;

  public UniquePathsUpdate(NotificationEvent event, boolean hasFullImage) {
    super(event.getEventId(), hasFullImage);
    this.eventHash = sha1(event);
  }

  public UniquePathsUpdate(String eventHash, long eventId, boolean hasFullImage) {
    super(eventId, hasFullImage);
    this.eventHash = eventHash;
  }

  public String getEventHash() {
    return eventHash;
  }

  public static String sha1(NotificationEvent event) {
    StringBuilder sb = new StringBuilder();

    sb.append(event.getEventId());
    sb.append(event.getEventTime());
    sb.append(event.getEventType());
    sb.append(event.getDbName());
    sb.append(event.getTableName());
    sb.append(event.getMessage());

    return DigestUtils.shaHex(sb.toString());
  }
}
