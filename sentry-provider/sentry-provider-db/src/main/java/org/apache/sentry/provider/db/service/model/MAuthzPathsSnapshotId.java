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
 * This class is used to persist new authz paths snapshots IDs. An authz path snapshot ID is required by
 * the MAuthzPathsMapping to detect new HMS snapshots created by the HMSFollower.
 */
@PersistenceCapable
public class MAuthzPathsSnapshotId {
  @PrimaryKey
  private long authzSnapshotID;

  public MAuthzPathsSnapshotId(long authzSnapshotID) {
    this.authzSnapshotID = authzSnapshotID;
  }

  @Override
  public String toString() {
    return "MAuthzPathsSnapshotId authzSnapshotID=[" + authzSnapshotID + "]";
  }

  @Override
  public int hashCode() {
    return (int)authzSnapshotID;
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

    MAuthzPathsSnapshotId other = (MAuthzPathsSnapshotId) obj;
    return (authzSnapshotID == other.authzSnapshotID);
  }
}
