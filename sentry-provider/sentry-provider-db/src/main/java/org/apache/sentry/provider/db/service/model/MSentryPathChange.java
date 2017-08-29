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

package org.apache.sentry.provider.db.service.model;

import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.thrift.TException;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;

/**
 * Database backend store for HMS path delta change. Each record contains
 * change ID, HMS notification ID, JSON format of a single
 * &lt Hive Obj, HDFS Path &gt change, and timestamp.
 * <p>
 * e.g. for add paths change in JSON format.
 * <pre>
 * {@code
 * {
 *  "hasFullImage":1,
 *  "seqNum":1,
 *  "pathChanges":[
 *    {
 *      "authzObj":"db1.tbl12",
 *      "addPaths":[
 *        [
 *          "db1",
 *          "tbl12",
 *          "part121"
 *        ]
 *      ],
 *      "delPaths":[]
 *    }
 *  ]
 * }
 * }
 * </pre>
 * <p>
 * Any changes to this objects require re-running the maven build so DN
 * can re-enhance.
 */

@PersistenceCapable
public class MSentryPathChange implements MSentryChange {

  @PrimaryKey
  //This value is auto incremented by JDO
  private long changeID;

  // Path change in JSON format.
  private String pathChange;
  private long createTimeMs;
  private String notificationHash;

  public MSentryPathChange(long changeID, String notificationHash, PathsUpdate pathChange) throws TException {
    // Each PathsUpdate maps to a MSentryPathChange object.
    // The PathsUpdate is generated from a HMS notification log,
    // the notification ID is stored as seqNum and
    // the notification update is serialized as JSON string.
    this.changeID = changeID;

    /*
     * notificationHash is a unique identifier for the HMS notification used to prevent
     * the same HMS notification message to be processed twice.
     * The current HMS code may send different notifications messages with the same ID. To
     * keep this ID unique, we calculate the SHA-1 hash of the full message received.
     * TODO: This is a temporary fix until HIVE-16886 fixes the issue with duplicated IDs
     */
    this.notificationHash = notificationHash;

    this.pathChange = pathChange.JSONSerialize();
    this.createTimeMs = System.currentTimeMillis();
  }

  public long getCreateTimeMs() {
    return createTimeMs;
  }

  public String getPathChange() {
    return pathChange;
  }

  public long getChangeID() {
    return changeID;
  }

  public String getNotificationHash() {
    return notificationHash;
  }

  @Override
  public String toString() {
    return "MSentryChange [changeID=" + changeID + " , notificationHash= "
        + notificationHash +" , pathChange= " + pathChange +
        ", createTime=" + createTimeMs +  "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Long.valueOf(changeID).hashCode();
    result = prime * result + notificationHash.hashCode();
    result = prime * result + ((pathChange == null) ? 0 : pathChange.hashCode());
    return result;
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

    MSentryPathChange other = (MSentryPathChange) obj;
    if (changeID != other.changeID) {
      return false;
    }

    if (!notificationHash.equals(other.notificationHash)) {
      return false;
    }

    if (createTimeMs != other.createTimeMs) {
      return false;
    }

    if (pathChange == null) {
      return other.pathChange == null;
    }

    return pathChange.equals(other.pathChange);
  }
}
