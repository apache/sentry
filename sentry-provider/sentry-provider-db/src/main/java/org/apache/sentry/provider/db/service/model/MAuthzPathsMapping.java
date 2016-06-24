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
import java.util.Set;

/**
 * Transactional database backend for storing HMS Paths Updates. Any changes to this object
 * require re-running the maven build so DN can re-enhance.
 */
@PersistenceCapable
public class MAuthzPathsMapping {

  private String authzObjName;
  private Set<String> paths;
  private long createTimeMs;

  public MAuthzPathsMapping(String authzObjName, Set<String> paths, long createTimeMs) {
    this.authzObjName = authzObjName;
    this.paths = paths;
    this.createTimeMs = createTimeMs;
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

  public void setPaths(Set<String> paths) {
    this.paths = paths;
  }

  public Set<String> getPaths() {
    return paths;
  }

  @Override
  public String toString() {
    return "MSentryPathsUpdate [authzObj=" + authzObjName + "], paths=[" + paths.toString()
        + "], createTimeMs=[" + String.valueOf(createTimeMs) + "]";
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
    }

    return true;
  }
}
