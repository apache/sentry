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

import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.Sasl;

import com.google.common.collect.ImmutableMap;

public class ServiceConstants {

  private static final ImmutableMap<String, String> SASL_PROPERTIES;

  static {
    Map<String, String> saslProps = new HashMap<String, String>();
    saslProps.put(Sasl.SERVER_AUTH, "true");
    saslProps.put(Sasl.QOP, "auth-conf");
    SASL_PROPERTIES = ImmutableMap.copyOf(saslProps);
  }

  public static class ServerConfig {
    public static final ImmutableMap<String, String> SASL_PROPERTIES = ServiceConstants.SASL_PROPERTIES;
    /**
     * This configuration parameter is only meant to be used for testing purposes.
     */
    public static final String SENTRY_HDFS_INTEGRATION_PATH_PREFIXES = "sentry.hdfs.integration.path.prefixes";
    public static final String[] SENTRY_HDFS_INTEGRATION_PATH_PREFIXES_DEFAULT =
        new String[]{"/user/hive/warehouse"};
    public static final String SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS = "sentry.hdfs.init.update.retry.delay.ms";
    public static final int SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT = 10000;

  }
  public static class ClientConfig {
    public static final ImmutableMap<String, String> SASL_PROPERTIES = ServiceConstants.SASL_PROPERTIES;

    public static final String SECURITY_MODE = "sentry.hdfs.service.security.mode";
    public static final String SECURITY_MODE_KERBEROS = "kerberos";
    public static final String SECURITY_MODE_NONE = "none";
    public static final String SECURITY_USE_UGI_TRANSPORT = "sentry.hdfs.service.security.use.ugi";
    public static final String PRINCIPAL = "sentry.hdfs.service.server.principal";

    public static final String SERVER_RPC_PORT = "sentry.hdfs.service.client.server.rpc-port";
    public static final int SERVER_RPC_PORT_DEFAULT = 8038;

    public static final String SERVER_RPC_ADDRESS = "sentry.hdfs.service.client.server.rpc-address";

    public static final String SERVER_RPC_CONN_TIMEOUT = "sentry.hdfs.service.client.server.rpc-connection-timeout";
    public static final int SERVER_RPC_CONN_TIMEOUT_DEFAULT = 200000;
  }

}
