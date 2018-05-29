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
package org.apache.sentry.provider.db.service.model;

/**
 * Database backend store for HMS Notification ID's. All the notifications that are processed
 * by sentry are stored.
 * <p>
 * <p> HMS notification ID's are stored in separate table for three reasons</p>
 * <ol>
 * <li>SENTRY_PATH_CHANGE is not updated for every notification that is received from HMS. There
 * are cases where HMSFollower doesn't process notifications and skip's them. Depending on
 * SENTRY_PATH_CHANGE information may not provide the last notification processed.</li>
 * <li> There could be cases where HMSFollower thread in multiple sentry servers acting as a
 * leader and process HMS notifications. we need to avoid processing the notifications
 * multiple times. This can be made sure by always having some number of notification
 * information always regardless of purging interval.</li>
 * <li>SENTRY_PATH_CHANGE information stored can typically be removed once namenode plug-in
 * has processed the update.</li>
 * </ol>
 * <p>
 * As the purpose and usage of notification ID information is different from PATH update info,
 * it locally makes sense to store notification ID separately.
 * </p>
 */
public class MSentryHmsNotification {
  private long notificationId;

  public MSentryHmsNotification(long notificationId) {
    this.notificationId = notificationId;
  }

  public long getId() {
    return notificationId;
  }

  public void setId(long notificationId) {
    this.notificationId = notificationId;
  }

  @Override
  public int hashCode() {
    return (int) notificationId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    MSentryHmsNotification other = (MSentryHmsNotification) obj;

    return (notificationId == other.notificationId);
  }
}