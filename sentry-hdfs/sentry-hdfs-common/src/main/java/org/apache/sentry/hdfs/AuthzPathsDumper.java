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

import org.apache.sentry.hdfs.service.thrift.TPathsDump;

public interface AuthzPathsDumper<K extends AuthzPaths> {

  /**
   * Creates a TPathsDump thrift object from the data in memory (K instance).
   *
   * @param minimizeSize if true, the code will make an effort to minimize the
   *                     size of the serialized message by, for example,
   *                     performing customized interning of duplicate strings.
   *                     So far this is optional since, in particular, messages
   *                     created with minimizeSize == false are compatible with
   *                     the older TPathsDump messages.
   */
  TPathsDump createPathsDump(boolean minimizeSize);

  /**
   * Creates data in memory (an instance of K) from TPathsDump thrift object.
   */
  K initializeFromDump(TPathsDump pathsDump);

}
