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

import java.util.Collection;

import static org.apache.sentry.hdfs.Updateable.Update;

/**
 * DeltaRetriever obtains a delta update of either Sentry Permissions or Sentry
 * representation of HMS Paths.
 * <p>
 * Sentry permissions are represented as {@link PermissionsUpdate} and HMS Paths
 * are represented as {@link PathsUpdate}. The delta update contains change
 * from a state to another.
 * The {@link #retrieveDelta(long)} method obtains such delta update from a persistent storage.
 * Delta update is propagated to a consumer of Sentry, such as HDFS NameNode whenever
 * the consumer needs to synchronize the update.
 */
public interface DeltaRetriever<K extends Update> {

  /**
   * Retrieves all delta updates of type {@link Update} newer than or equal with
   * the given sequence number/change ID (inclusive) from a persistent storage.
   * An empty collection can be returned.
   *
   * @param seqNum the given seq number
   * @return a collect of delta updates of type K
   * @throws Exception when there is an error in operation on persistent storage
   */
  Collection<K> retrieveDelta(long seqNum) throws Exception;

  /**
   * Checks if there the delta update is available, given the sequence number/change
   * ID, from a persistent storage.
   *
   * @param seqNum the given seq number
   * @return true if there are such delta updates available.
   *         Otherwise it will be false.
   * @throws Exception when there is an error in operation on persistent storage
   */
  boolean isDeltaAvailable(long seqNum) throws Exception;

  /**
   * Gets the latest updated delta ID.
   *
   * @return the latest updated delta ID.
   * @throws Exception when there is an error in operation on persistent storage
   */
  long getLatestDeltaID() throws Exception;
}
