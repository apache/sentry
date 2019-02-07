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

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.annotations.NotPersistent;
import javax.jdo.annotations.PersistenceCapable;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.sentry.provider.db.service.persistent.QueryParamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transactional database backend for storing HMS Paths Updates. Any changes to this object
 * require re-running the maven build so DN can re-enhance.
 *
 */
@PersistenceCapable
public class MAuthzPathsMapping {
  private static final Logger LOGGER = LoggerFactory
          .getLogger(MAuthzPathsMapping.class);

  private long authzObjectId;

  private long authzSnapshotID;
  private String authzObjName;
  // Used to fetch the paths from the sentry backend store.
  private Set<MPath> pathsPersisted;
  @NotPersistent
  // Used to add paths.
  private Set<String> pathsToPersist;
  private long createTimeMs;

  public MAuthzPathsMapping(long authzSnapshotID, long authzObjectId, String authzObjName, Collection<String> paths) {
    this.authzSnapshotID = authzSnapshotID;
    this.authzObjectId = authzObjectId;
    this.authzObjName = MSentryUtil.safeIntern(authzObjName);
    this.pathsPersisted = Collections.EMPTY_SET;
    if(paths != null && !paths.isEmpty()) {
      this.pathsToPersist = new HashSet<>(paths.size());
      for (String path : paths) {
        this.pathsToPersist.add(path);
      }
    }
    this.createTimeMs = System.currentTimeMillis();
  }

  public long getAuthzObjectId() {
    return authzObjectId;
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

  public Set<MPath> getPathsPersisted() {
    return pathsPersisted;
  }

  public void addPathToPersist(Collection<String> paths) {
    if(paths == null || paths.isEmpty()) {
      return;
    }
    if(pathsToPersist == null) {
      pathsToPersist = new HashSet<>(paths.size());
    }
    pathsToPersist.addAll(paths);
  }

  /**
   * Gets MPath object that has the given path value.
   * TODO: Try to avoid loop lookup in future for performance improvement.
   *
   * @param path the given path name
   * @return an Path object that has the given path value.
   */
  public MPath getPathsPersisted(String path) {
    for (MPath mPath : pathsPersisted) {
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
    Collection<String> pathValues = new ArrayList<>(this.pathsPersisted.size());
    for (MPath path : this.pathsPersisted) {
      pathValues.add(path.getPath());
    }
    return pathValues;
  }

  @Override
  public String toString() {
    return "MSentryPathsUpdate authzSnapshotID=[" + authzSnapshotID + "], authzObjectId=[" + authzObjectId + "]," +
            "authzObj=[" + authzObjName + "], mPaths=[" + ((pathsPersisted != null) ? pathsPersisted.toString() : "")
            + "], paths=[" + ((pathsToPersist != null) ? pathsToPersist.toString() : "") + "]," +
            "createTimeMs=[" + createTimeMs + "]";
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
    } else if (authzObjectId != other.authzObjectId) {
      return false;
    }

    return true;
  }

  /**
   * Persist the MAuthzPathsMapping by inserting entries into AUTHZ_PATH table in batches.
   * @param pm Persistence manager instance
   */
  public void makePersistent(PersistenceManager pm) {
    pm.makePersistent(this);
    if(pathsToPersist == null || pathsToPersist.isEmpty()) {
      return;
    }
    for (String path : pathsToPersist) {
      pm.makePersistent(new MPathToPersist(authzObjectId, path));
    }
    pathsToPersist.clear();
  }

  /**
   * Delete the paths associated with MAuthzPathsMapping entry.
   * @param pm Persistence manager instance
   * @param paths paths to be removed.
   */
  public void deletePersistent(PersistenceManager pm, Collection<String> paths) {
    Query query = pm.newQuery(MPathToPersist.class);
    QueryParamBuilder paramBuilder = QueryParamBuilder.addPathFilter(null, paths).addObject("authzObjectId",
        authzObjectId);
    query.setFilter(paramBuilder.toString());
    long delCount = query.deletePersistentAll(paramBuilder.getArguments());
    LOGGER.debug("Entries deleted are %d", delCount);
  }

  /**
  * There are performance issues in persisting instances of MPath at bulk because of the FK mapping with
  *  MAuthzPathsMapping. This FK mapping limits datanucleus from inserting these entries in batches.
  *  MPathToPersist is introduced to solve this performance issue.
  *  Note: Only {@link MPathToPersist} should be used to persist paths instead of {@link MPath} otherwise there
  *  will be duplicate entry exceptions as both JDO's MPath and  {@link MPathToPersist}
  *  maintain their own sequence for PATH_ID column.
  */
  @PersistenceCapable
  public static class MPathToPersist {
    private String path;
    private long authzObjectId;


    public MPathToPersist(long authzObjectId, String path) {
      this.authzObjectId = authzObjectId;
      this.path = MSentryUtil.safeIntern(path);
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

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

      MPathToPersist other = (MPathToPersist) obj;

      if (path == null) {
        return other.path == null;
      } else if (authzObjectId != other.authzObjectId) {
        return false;
      }

      return path.equals(other.path);
    }
  }
}
