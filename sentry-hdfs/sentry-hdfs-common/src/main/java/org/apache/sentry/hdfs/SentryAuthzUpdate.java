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
package org.apache.sentry.hdfs;

import java.util.List;

public class SentryAuthzUpdate {

  private final List<PermissionsUpdate> permUpdates;
  private final List<PathsUpdate> pathUpdates;

  public SentryAuthzUpdate(List<PermissionsUpdate> permUpdates,
      List<PathsUpdate> pathUpdates) {
    this.permUpdates = permUpdates;
    this.pathUpdates = pathUpdates;
  }

  public List<PermissionsUpdate> getPermUpdates() {
    return permUpdates;
  }

  public List<PathsUpdate> getPathUpdates() {
    return pathUpdates;
  }

  public String dumpContent() {
    StringBuffer sb = new StringBuffer(getClass().getSimpleName());
    if (permUpdates != null && !permUpdates.isEmpty()) {
      sb.append(", perms[").append(permUpdates.size()).append(']').append(permUpdates);
    }
    if (pathUpdates != null && !pathUpdates.isEmpty()) {
      sb.append(", paths[").append(pathUpdates.size()).append(']').append(pathUpdates);
    }
    return sb.toString();
  }
    
  public boolean isEmpty() {
    return (permUpdates == null || permUpdates.isEmpty()) &&
           (pathUpdates == null || pathUpdates.isEmpty());
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer(getClass().getSimpleName());
    if (permUpdates != null && !permUpdates.isEmpty()) {
      sb.append(", perms[").append(permUpdates.size()).append(']');
    }
    if (pathUpdates != null && !pathUpdates.isEmpty()) {
      sb.append(", paths[").append(pathUpdates.size()).append(']');
    }
    return sb.toString();
  }
}
