/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.common.transport;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration interface for Sentry Thrift Clients
 * <p>
 * The purpose of the interface is to abstract the knowledge of specific configuration keys
 * and provide an API to extract various thrift-related configuration from a Config object
 * This Configuration interface should be implemented for all the sentry clients to get
 * the transport configuration.
 */
interface SentryClientTransportConfigInterface {

  /**
   * @param conf configuration
   * @return number of times should client re-create the transport and try to connect
   * before finally giving up.
   */
  int getSentryRpcRetryTotal(Configuration conf);

  /**
   * @param conf configuration
   * @return time interval in milli-secs that the client should wait before
   * retrying to establish connection to the server.
   */
  long getSentryRpcConnRetryDelayInMs(Configuration conf);

  /**
   * @param conf configuration
   * @return True, if kerberos should be enabled.
   * False, Iff kerberos is enabled.
   */
  boolean isKerberosEnabled(Configuration conf);

  /**
   * @param conf configuration
   * @return True, if Ugi transport has to be used
   * False, If not.
   */
  boolean useUserGroupInformation(Configuration conf);

  /**
   * @param conf configuration
   * @return principle for the particular sentry service
   */
  String getSentryPrincipal(Configuration conf);

  /**
   * Port in RPC Addresses configured is optional
   * @param conf configuration
   * @return comma-separated list of available sentry server addresses.
   */
  String getSentryServerRpcAddress(Configuration conf);

  /**
   * Port in RPC Addresses configured is optional. If a port is not provided for a server
   * listed in RPC configuration, this configuration is used as a default port.
   * @param conf configuration
   * @return port where sentry server is listening.
   */
  int getServerRpcPort(Configuration conf);

  /**
   * @param conf configuration
   * @return time interval in milli-secs that the client should wait for
   * establishment of connection to the server. If the connection
   * is not established with-in this interval client should try connecting
   * to next configured server
   */
  int getServerRpcConnTimeoutInMs(Configuration conf);

  /**
   * Maximum number of connections in the pool.
   * See {@link org.apache.commons.pool2.impl.GenericObjectPoolConfig#setMaxTotal(int)}
   * @param conf configuration
   * @return maximum number of connection objects in the pool
   */
  int getPoolMaxTotal(Configuration conf);

  /**
   * Minimum number of idle obects on the pool.
   * See {@link org.apache.commons.pool2.impl.GenericObjectPoolConfig#setMinIdle(int)}
   * @param conf Configuration
   * @return Minimum idle connections to keep in the pool
   */
  int getPoolMinIdle(Configuration conf);

  /**
   * Maximum number of idle connections in the pool.
   * See {@link org.apache.commons.pool2.impl.GenericObjectPoolConfig#setMaxIdle(int)}
   * @param conf Configuration
   * @return Maximum number of idle connections in the pool
   */
  int getPoolMaxIdle(Configuration conf);

  /**
   * This is the minimum amount of time an object may sit idle in the pool
   * before it is eligible for eviction.
   * See {@link org.apache.commons.pool2.impl.GenericObjectPoolConfig#setMinEvictableIdleTimeMillis}
   * @param conf Configuration
   * @return The value for the pool minimum eviction time.
   */
  long getMinEvictableTimeSec(Configuration conf);

  /**
   * The number of seconds to sleep between runs of the idle object evictor thread.
   * When non-positive, no idle object evictor thread will be run.
   * See {@link GenericObjectPoolConfig#getTimeBetweenEvictionRunsMillis()}
   * @param conf Configuration
   * @return The number of seconds to sleep between runs of the idle object evictor thread.
   */
  long getTimeBetweenEvictionRunsSec(Configuration conf);

  /**
   * @param conf configuration
   * @return True if using load-balancing between Sentry servers
   */
  boolean isLoadBalancingEnabled(Configuration conf);

  /**
   * @param conf configuration
   * @return true if transport pools are enabled
   */
  boolean isTransportPoolEnabled(Configuration conf);
}
