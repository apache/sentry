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

import java.util.Set;

/**
 * A public interface of the fundamental APIs exposed by the implementing
 * data structure. The primary client of this interface is the Namenode
 * plugin.
 */
public interface AuthzPaths {

  /**
   * Check if a Path belongs to the configured prefix set.
   *
   * @param pathElements A path split into segments
   * @return Returns if Path under configured prefix or not.
   */
   boolean isUnderPrefix(String[] pathElements);

  /**
   * Returns all authorizable Objects (database/table/partition) associated
   * with this path. Unlike {@link #findAuthzObjectExactMatches(String[])},
   * if not match is found, it will return the first ancestor that has the
   * associated authorizable objects.
   *
   * @param pathElements A path split into segments
   * @return Returns a set of authzObjects authzObject associated with this path
   */
   Set<String> findAuthzObject(String[] pathElements);

  /**
   * Returns all authorizable Objects (database/table/partition) associated
   * with this path.
   *
   * @param pathElements A path split into segments
   * @return Returns a set of authzObjects associated with this path
   */
   Set<String> findAuthzObjectExactMatches(String[] pathElements);

  /**
   * Return a Dumper that may return a more optimized over the
   * wire representation of the internal data-structures.
   *
   * @return Returns the AuthzPathsDumper.
   */
   AuthzPathsDumper<? extends AuthzPaths> getPathsDump();

}
