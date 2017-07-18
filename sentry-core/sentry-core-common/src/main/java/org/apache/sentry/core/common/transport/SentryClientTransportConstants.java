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


import java.util.concurrent.TimeUnit;

/**
 * Defines configuration strings needed for sentry thrift clients to handle the transport level
 * operations.
 * <p>
 * This class is abstracted by <code>SentryClientTransportConfigInterface</code>.
 * Clients that needs these configuration string use the implementations of interface
 * <code>SentryClientTransportConfigInterface</code>.
 */
public final class SentryClientTransportConstants {

  /**
   * max retry num for client rpc
   * {link RetryClientInvocationHandler#invokeImpl(Object, Method, Object[])}
   */
  static final String SENTRY_RPC_RETRY_TOTAL = "sentry.service.client.rpc.retry-total";
  static final int SENTRY_RPC_RETRY_TOTAL_DEFAULT = 3;

  /**
   * full retry num for getting the connection in non-pool model
   * In a full retry, it will cycle through all available sentry servers
   */
  static final String SENTRY_FULL_RETRY_TOTAL =
    "sentry.service.client.connection.full.retry-total";
  static final int SENTRY_FULL_RETRY_TOTAL_DEFAULT = 2;

  /**
   * Enable load balancing between servers
   */
  static final String SENTRY_CLIENT_LOAD_BALANCING =
          "sentry.service.client.connection.loadbalance";
  static final boolean SENTRY_CLIENT_LOAD_BALANCING_DEFAULT = true;

  static final int RPC_PORT_DEFAULT = 8038;

  private SentryClientTransportConstants() {
  }

  /**
   * Defines configuration strings needed for sentry thrift policy clients to handle
   * the transport level operations.
   */
  static class PolicyClientConstants {
    //configuration for server port
    static final String SERVER_RPC_PORT = "sentry.service.client.server.rpc-port";

    //configuration for server address. It can be coma seperated list of server addresses.
    static final String SERVER_RPC_ADDRESS = "sentry.service.client.server.rpc-addresses";

    /**
     * This configuration parameter is only meant to be used for testing purposes.
     */
    static final String SECURITY_MODE = "sentry.service.security.mode";

    /**
     * full retry num for getting the connection in non-pool model
     * In a full retry, it will cycle through all available sentry servers
     */
    static final String SENTRY_FULL_RETRY_TOTAL =
      SentryClientTransportConstants.SENTRY_FULL_RETRY_TOTAL;
    static final int SENTRY_FULL_RETRY_TOTAL_DEFAULT =
      SentryClientTransportConstants.SENTRY_FULL_RETRY_TOTAL_DEFAULT;

    public static final String SECURITY_USE_UGI_TRANSPORT = "sentry.service.security.use.ugi";
    static final String PRINCIPAL = "sentry.service.server.principal";

    //configration for the client connection timeout.
    static final String SERVER_RPC_CONN_TIMEOUT =
      "sentry.service.client.server.rpc-connection-timeout";

    static final int SERVER_RPC_CONN_TIMEOUT_DEFAULT = 200000;

    /**
     * max retry num for client rpc
     * {link RetryClientInvocationHandler#invokeImpl(Object, Method, Object[])}
     */
    static final String SENTRY_RPC_RETRY_TOTAL = "sentry.service.client.rpc.retry-total";
    static final int SENTRY_RPC_RETRY_TOTAL_DEFAULT = 3;

    // commons-pool configuration
    static final String SENTRY_POOL_ENABLE = "sentry.service.client.connection.pool.enabled";
    static final boolean SENTRY_POOL_ENABLE_DEFAULT = true;

    /** Allow unlimited number of idle connections */
    static final String SENTRY_POOL_MAX_TOTAL = "sentry.service.client.connection.pool.max-total";
    static final int SENTRY_POOL_MAX_TOTAL_DEFAULT = -1;
    static final String SENTRY_POOL_MAX_IDLE = "sentry.service.client.connection.pool.max-idle";
    static final int SENTRY_POOL_MAX_IDLE_DEFAULT = 400;
    static final String SENTRY_POOL_MIN_IDLE = "sentry.service.client.connection.pool.min-idle";
    static final int SENTRY_POOL_MIN_IDLE_DEFAULT = 10;
    static final String SENTRY_POOL_MIN_EVICTION_TIME_SEC =
            "sentry.service.client.connection.pool.eviction.mintime.sec";
    // 2 minutes seconds min time before eviction
    static final long SENTRY_POOL_MIN_EVICTION_TIME_SEC_DEFAULT =
            TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES);;
    static final String SENTRY_POOL_EVICTION_INTERVAL_SEC =
            "sentry.service.client.connection.pool.eviction.interval.sec";
    // Run eviction thread every minute
    static final long SENTRY_POOL_EVICTION_INTERVAL_SEC_DEFAULT =
            TimeUnit.MILLISECONDS.convert(1L, TimeUnit.MINUTES);

    static final String SENTRY_CLIENT_LOAD_BALANCING =
            SentryClientTransportConstants.SENTRY_CLIENT_LOAD_BALANCING;
    static final boolean SENTRY_CLIENT_LOAD_BALANCING_DEFAULT =
            SentryClientTransportConstants.SENTRY_CLIENT_LOAD_BALANCING_DEFAULT;
  }

  /**
   * Defines configuration strings needed for sentry HDFS clients to handle the transport level
   * operations.
   */
  public static class HDFSClientConstants {

    //Default server port
    static final int SERVER_RPC_PORT_DEFAULT = SentryClientTransportConstants.RPC_PORT_DEFAULT;

    //configuration for server port
    static final String SERVER_RPC_PORT = "sentry.hdfs.service.client.server.rpc-port";

    //configuration for server address. It can be coma seperated list of server addresses.
    static final String SERVER_RPC_ADDRESS = "sentry.hdfs.service.client.server.rpc-addresses";

    /**
     * This configuration parameter is only meant to be used for testing purposes.
     */
    static final String SECURITY_MODE = "sentry.hdfs.service.security.mode";

    /**
     * full retry num for getting the connection in non-pool model
     * In a full retry, it will cycle through all available sentry servers
     */
    static final String SENTRY_FULL_RETRY_TOTAL =
      SentryClientTransportConstants.SENTRY_FULL_RETRY_TOTAL;

    static final int SENTRY_FULL_RETRY_TOTAL_DEFAULT =
      SentryClientTransportConstants.SENTRY_FULL_RETRY_TOTAL_DEFAULT;

    public static final String SECURITY_USE_UGI_TRANSPORT = "sentry.hdfs.service.security.use.ugi";

    static final String PRINCIPAL = "sentry.hdfs.service.server.principal";

    //configration for the client connection timeout.
    static final String SERVER_RPC_CONN_TIMEOUT =
      "sentry.hdfs.service.client.server.rpc-connection-timeout";

    static final int SERVER_RPC_CONN_TIMEOUT_DEFAULT = 200000;

    /**
     * max retry num for client rpc
     * {link RetryClientInvocationHandler#invokeImpl(Object, Method, Object[])}
     */
    static final String SENTRY_RPC_RETRY_TOTAL =
      SentryClientTransportConstants.SENTRY_RPC_RETRY_TOTAL;

    static final int SENTRY_RPC_RETRY_TOTAL_DEFAULT = 3;

    // commons-pool configuration - disable pool for HDFS clients
    static final String SENTRY_POOL_ENABLE = "sentry.hdfs.service.client.connection.pool.enable";
    static final boolean SENTRY_POOL_ENABLE_DEFAULT = false;

    /** Total maximum number of open connections. There shouldn't be many. */
    static final String SENTRY_POOL_MAX_TOTAL = "sentry.hdfs.service.client.connection.pool.max-total";
    static final int SENTRY_POOL_MAX_TOTAL_DEFAULT = 16;
    /** Maximum number of idle connections to keep */
    static final String SENTRY_POOL_MAX_IDLE = "sentry.hdfs.service.client.connection.pool.max-idle";
    static final int SENTRY_POOL_MAX_IDLE_DEFAULT = 2;
    static final String SENTRY_POOL_MIN_IDLE = "sentry.hdfs.service.client.connection.pool.min-idle";
    static final int SENTRY_POOL_MIN_IDLE_DEFAULT = 1;
    static final String SENTRY_POOL_MIN_EVICTION_TIME_SEC =
            "sentry.hdfs.service.client.connection.pool.eviction.mintime.sec";
    // No evictions for HDFS connections by default
    static final long SENTRY_POOL_MIN_EVICTION_TIME_SEC_DEFAULT = 0L;
    static final String SENTRY_POOL_EVICTION_INTERVAL_SEC =
            "sentry.hdfs.service.client.connection.pool.eviction.interval.sec";
    static final long SENTRY_POOL_EVICTION_INTERVAL_SEC_DEFAULT = -1L;
    static final String SENTRY_CLIENT_LOAD_BALANCING =
            SentryClientTransportConstants.SENTRY_CLIENT_LOAD_BALANCING;
    static final boolean SENTRY_CLIENT_LOAD_BALANCING_DEFAULT =
            SentryClientTransportConstants.SENTRY_CLIENT_LOAD_BALANCING_DEFAULT;
  }
}
