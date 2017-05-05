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

package org.apache.sentry.core.common.transport;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.MissingConfigurationException;

import static org.apache.sentry.core.common.transport.SentryClientTransportConstants.HDFSClientConstants.*;
import static org.apache.sentry.core.common.transport.SentryClientTransportConstants.KERBEROS_MODE;

/**
 * Provides configuration values and the configuration string for the HDFS sentry
 * client
 * <p>
 * Curently used by <code>SentryHDFSServiceClient</code>.
 */
public final class SentryHDFSClientTransportConfig
  implements SentryClientTransportConfigInterface {
  public SentryHDFSClientTransportConfig() { }

  @Override
  public boolean isKerberosEnabled(Configuration conf) throws MissingConfigurationException {
    return (conf.get(SECURITY_MODE, KERBEROS_MODE).trim()
      .equalsIgnoreCase((KERBEROS_MODE)));
  }

  @Override
  public int getSentryFullRetryTotal(Configuration conf) throws MissingConfigurationException {
    return conf.getInt(SENTRY_FULL_RETRY_TOTAL, SENTRY_FULL_RETRY_TOTAL_DEFAULT);
  }

  @Override
  public int getSentryRpcRetryTotal(Configuration conf) {
    return conf.getInt(SENTRY_RPC_RETRY_TOTAL, SENTRY_RPC_RETRY_TOTAL_DEFAULT);
  }

  @Override
  public boolean useUserGroupInformation(Configuration conf)
    throws MissingConfigurationException {
    return Boolean.valueOf(conf.get(SECURITY_USE_UGI_TRANSPORT, "true"));
  }

  @Override
  public String getSentryPrincipal(Configuration conf) throws MissingConfigurationException {
    String principle = conf.get(PRINCIPAL);
    if (principle != null && !principle.isEmpty()) {
      return principle;
    }
    throw new MissingConfigurationException(PRINCIPAL);
  }

  @Override
  public String getSentryServerRpcAddress(Configuration conf)
    throws MissingConfigurationException {
    String serverAddress = conf.get(SERVER_RPC_ADDRESS);
    if (serverAddress != null && !serverAddress.isEmpty()) {
      return serverAddress;
    }
    throw new MissingConfigurationException(SERVER_RPC_ADDRESS);
  }

  @Override
  public int getServerRpcPort(Configuration conf) throws MissingConfigurationException {
    return conf.getInt(SERVER_RPC_PORT, SentryClientTransportConstants.RPC_PORT_DEFAULT);
  }

  @Override
  public int getServerRpcConnTimeoutInMs(Configuration conf)
    throws MissingConfigurationException {
    return conf.getInt(SERVER_RPC_CONN_TIMEOUT, SERVER_RPC_CONN_TIMEOUT_DEFAULT);
  }

  @Override
  public boolean isLoadBalancingEnabled(Configuration conf)
    throws MissingConfigurationException {
    return conf.getBoolean(SENTRY_CLIENT_LOAD_BALANCING, SENTRY_CLIENT_LOAD_BALANCING_DEFAULT);
  }
}
