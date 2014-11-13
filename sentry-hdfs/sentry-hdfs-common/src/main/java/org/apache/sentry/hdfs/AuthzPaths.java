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

/**
 * A public interface of the fundamental APIs exposed by the implementing
 * data structure. The primary client of this interface is the Namenode
 * plugin.
 */
public interface AuthzPaths {

  /**
   * Check if a Path belongs to the configured prefix set
   * @param pathElements : A path split into segments
   * @return Is Path under configured prefix
   */
  public boolean isUnderPrefix(String[] pathElements);

  /**
   * Returns the authorizable Object (database/table) associated with this path.
   * Unlike {@link #findAuthzObjectExactMatch(String[])}, if not match is
   * found, it will return the first ancestor that has an associated
   * authorizable object.
   * @param pathElements : A path split into segments
   * @return A authzObject associated with this path
   */
  public String findAuthzObject(String[] pathElements);

  /**
   * Returns the authorizable Object (database/table) associated with this path.
   * @param pathElements : A path split into segments
   * @return A authzObject associated with this path
   */
  public String findAuthzObjectExactMatch(String[] pathElements);

  /**
   * Return a Dumper that may return a more optimized over the
   * wire representation of the internal data-structures.
   * @return
   */
  public AuthzPathsDumper<? extends AuthzPaths> getPathsDump();

}
