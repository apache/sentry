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
 * Used to store the path for the Authorizable object
 *
 * New class is created for path in order to have 1 to many mapping
 * between path and Authorizable object.
 *
 * There are performance issues in persisting instances of MPath at bulk because of the FK mapping with
 * MAuthzPathsMapping. This FK mapping limits datanucleus from inserting these entries in batches.
 * MPathToPersist was introduced to solve this performance issue.
 * Note: This class should not be used to insert paths, instead {@link MAuthzPathsMapping.MPathToPersist} should be used.
 *  If MPath is used to persist paths, there will be duplicate entry exceptions as both JDO's MPath and
 *  {@link MAuthzPathsMapping.MPathToPersist} maintain their own sequence for PATH_ID column.

 */
public class MPath {
  private String path;

  public MPath(String path) {
    this.path = MSentryUtil.safeIntern(path);
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) { this.path = path; }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((path == null) ? 0 : path.hashCode());
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

    MPath other = (MPath) obj;

    if (path == null) {
      return other.path == null;
    }

    return path.equals(other.path);
  }
}