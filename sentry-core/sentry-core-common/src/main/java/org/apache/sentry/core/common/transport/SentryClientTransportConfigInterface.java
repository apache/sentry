/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.MissingConfigurationException;

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
   * @return number of times client retry logic should iterate through all
   * the servers before giving up.
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
  int getSentryFullRetryTotal(Configuration conf) throws MissingConfigurationException;

  /**
   * @param conf configuration
   * @return number of times should client re-create the transport and try to connect
   * before finally giving up.
   */
  int getSentryRpcRetryTotal(Configuration conf);

  /**
   * @param conf configuration
   * @return True, if kerberos should be enabled.
   * False, Iff kerberos is enabled.
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
  boolean isKerberosEnabled(Configuration conf) throws MissingConfigurationException;

  /**
   * @param conf configuration
   * @return True, if Ugi transport has to be used
   * False, If not.
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
  boolean useUserGroupInformation(Configuration conf) throws MissingConfigurationException;

  /**
   * @param conf configuration
   * @return principle for the particular sentry service
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
  String getSentryPrincipal(Configuration conf) throws MissingConfigurationException;

  /**
   * Port in RPC Addresses configured is optional
   * @param conf configuration
   * @return comma-separated list of available sentry server addresses.
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
  String getSentryServerRpcAddress(Configuration conf) throws MissingConfigurationException;

  /**
   * Port in RPC Addresses configured is optional. If a port is not provided for a server
   * listed in RPC configuration, this configuration is used as a default port.
   * @param conf configuration
   * @return port where sentry server is listening.
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
  int getServerRpcPort(Configuration conf) throws MissingConfigurationException;

  /**
   * @param conf configuration
   * @return time interval in milli-secs that the client should wait for
   * establishment of connection to the server. If the connection
   * is not established with-in this interval client should try connecting
   * to next configured server
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
  int getServerRpcConnTimeoutInMs(Configuration conf) throws MissingConfigurationException;

  /**
   *
   * @param conf configuration
   * @return True if the client should load balance connections between multiple servers
   * @throws MissingConfigurationException if property is mandatory and is missing in
   *                                       configuration.
   */
   boolean isLoadBalancingEnabled(Configuration conf)throws MissingConfigurationException;
}
