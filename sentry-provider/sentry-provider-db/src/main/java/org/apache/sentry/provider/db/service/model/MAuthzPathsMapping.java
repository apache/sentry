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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Transactional database backend for storing HMS Paths Updates. Any changes to this object
 * require re-running the maven build so DN can re-enhance.
 */
@PersistenceCapable
public class MAuthzPathsMapping {

  private long authzSnapshotID;
  private String authzObjName;
  private Set<MPath> paths;
  private long createTimeMs;

  public MAuthzPathsMapping(long authzSnapshotID, String authzObjName, Collection<String> paths) {
    this.authzSnapshotID = authzSnapshotID;
    this.authzObjName = MSentryUtil.safeIntern(authzObjName);
    this.paths = new HashSet<>(paths.size());
    for (String path : paths) {
      this.paths.add(new MPath(path));
    }
    this.createTimeMs = System.currentTimeMillis();
  }

  public long getCreateTime() {
    return createTimeMs;
  }

  public void setCreateTime(long createTime) {
    this.createTimeMs = createTime;
  }

  public String getAuthzObjName() {
    return authzObjName;
  }

  public void setAuthzObjName(String authzObjName) {
    this.authzObjName = authzObjName;
  }

  public void setPaths(Set<MPath> paths) {
    this.paths = paths;
  }

  public Set<MPath> getPaths() {
    return paths;
  }

  public boolean removePath(MPath path) {
    return paths.remove(path);
  }

  public void addPath(MPath path) {
    paths.add(path);
  }

  /**
   * Gets MPath object that has the given path value.
   * TODO: Try to avoid loop lookup in future for performance improvement.
   *
   * @param path the given path name
   * @return an Path object that has the given path value.
   */
  public MPath getPath(String path) {
    for (MPath mPath : paths) {
      if (mPath.getPath().equals(path)) {
        return mPath;
      }
    }
    return null;
  }

  /**
   * @return collection of paths strings contained in this object.
   */
  public Collection<String> getPathStrings() {
    Collection<String> pathValues = new ArrayList<>(this.paths.size());
    for (MPath path : this.paths) {
      pathValues.add(path.getPath());
    }
    return pathValues;
  }

  @Override
  public String toString() {
    return "MSentryPathsUpdate authzSnapshotID=[" + authzSnapshotID + "], authzObj=[" + authzObjName
        + "], paths=[" + paths.toString() + "], createTimeMs=[" + createTimeMs + "]";
  }

  @Override
  public int hashCode() {
    return authzObjName == null ? 0 : authzObjName.hashCode();
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

    MAuthzPathsMapping other = (MAuthzPathsMapping) obj;

    if (authzObjName == null) {
      if (other.authzObjName != null) {
        return false;
      }
    } else if (!authzObjName.equals(other.authzObjName)) {
      return false;
    } else if (authzSnapshotID != other.authzSnapshotID) {
      return false;
   }

    return true;
  }
}
