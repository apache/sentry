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
package org.apache.sentry.tests.e2e.minisentry;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.service.thrift.SentryService;

public interface SentrySrv {

  /**
   * Start all the sentry services
   * @throws Exception
   */
  void startAll() throws Exception;

  /**
   * Start the given server
   * @param serverNum
   *          - Server number (0 to N-1)
   * @throws Exception
   */
  void start(int serverNum) throws Exception ;

  /**
   * retart HMSFollower with new configuration
   * @param newConf new configuration
   * @param serverNum Server number
   * @throws Exception
   */
  void restartHMSFollower(Configuration newConf, int serverNum,
      long sleepTime) throws Exception ;

  /**
   * Stop all the Sentry servers
   * @throws Exception
   */
  void stopAll() throws Exception;

  /**
   * Stop the specified Sentry server
   * @param serverNum
   *          - Server number (0 to N-1)
   * @throws Exception
   */
  void stop(int serverNum) throws Exception ;

  /**
   * Get the underlying Sentry service object
   * @param serverNum
   *          - Server number (0 to N-1)
   * @return
   */
  SentryService get(int serverNum);

  /**
   * Stop all the nodes and ZK if started. The SentrySrv can't be reused once
   * closed.
   */
  void close();

  /**
   * Get the number of active clients connections across servers
   */
  long getNumActiveClients();

  /**
   * Get the number of active clients connections for the given server
   */
  long getNumActiveClients(int serverNum);

  /**
   * Get the total number of clients connected so far
   */
  long getTotalClients();

  /**
   * Get the total number of clients connected so far
   */
  long getTotalClients(int serverNum);

}
