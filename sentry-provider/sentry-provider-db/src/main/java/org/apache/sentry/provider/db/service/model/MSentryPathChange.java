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

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;

/**
 * Database backend store for HMS path delta change. Each record contains
 * change ID, JSON format of a single &lt Hive Obj, HDFS Path &gt change, and
 * timestamp.
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
public class MSentryPathChange {

  @PrimaryKey
  private long changeID;

  // Path change in JSON format.
  private String pathChange;
  private long createTimeMs;

  public MSentryPathChange(long changeID, String pathChange, long createTime) {
    this.changeID = changeID;
    this.pathChange = pathChange;
    this.createTimeMs = createTime;
  }

  public void setCreateTimeMs(long createTimeMs) {
    this.createTimeMs = createTimeMs;
  }

  public long getCreateTimeMs() {
    return createTimeMs;
  }

  public void setPathChange(String pathChange) {
    this.pathChange = pathChange;
  }

  public String getPathChange() {
    return pathChange;
  }

  public long getChangeID() {
    return changeID;
  }

  public void setChangeID(long changeID) {
    this.changeID = changeID;
  }

  @Override
  public String toString() {
    return "MSentryChange [changeID=" + changeID + " , pathChange= " + pathChange + ", createTime=" + createTimeMs +  "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Long.valueOf(changeID).hashCode();
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

    if (!pathChange.equals(other.pathChange)) {
      return false;
    }

    if (createTimeMs != other.createTimeMs) {
      return false;
    }

    return true;
  }
}