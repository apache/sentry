/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.api.common;


import org.apache.sentry.service.common.ServiceConstants;

public class ApiConstants {

  public static class SentryPolicyServiceConstants {
    //from SentryPolicyStoreProcessor and SentryGenericPolicyProcessor
    public static final String SENTRY_GENERIC_SERVICE_NAME = "SentryGenericPolicyService";
    public static final String SENTRY_POLICY_SERVICE_NAME = "SentryPolicyService";
  }

  public static class ClientConfig {
    public static final String SERVER_RPC_PORT = "sentry.service.client.server.rpc-port";
    public static final int SERVER_RPC_PORT_DEFAULT = ServiceConstants.ServerConfig.RPC_PORT_DEFAULT;
    public static final String SERVER_RPC_ADDRESS = "sentry.service.client.server.rpc-addresses";
    public static final String SERVER_RPC_CONN_TIMEOUT = "sentry.service.client.server.rpc-connection-timeout";

    // HA configuration
    public static final String SENTRY_HA_ZOOKEEPER_QUORUM = ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM;
    public static final String SENTRY_HA_ZOOKEEPER_NAMESPACE = ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE;
    public static final String SERVER_HA_ZOOKEEPER_NAMESPACE_DEFAULT = ServiceConstants.ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT;

    // connection pool configuration
    public static final String SENTRY_POOL_ENABLED = "sentry.service.client.connection.pool.enabled";
    public static final boolean SENTRY_POOL_ENABLED_DEFAULT = false;

    // commons-pool configuration for pool size
    public static final String SENTRY_POOL_MAX_TOTAL = "sentry.service.client.connection.pool.max-total";
    public static final int SENTRY_POOL_MAX_TOTAL_DEFAULT = 8;
    public static final String SENTRY_POOL_MAX_IDLE = "sentry.service.client.connection.pool.max-idle";
    public static final int SENTRY_POOL_MAX_IDLE_DEFAULT = 8;
    public static final String SENTRY_POOL_MIN_IDLE = "sentry.service.client.connection.pool.min-idle";
    public static final int SENTRY_POOL_MIN_IDLE_DEFAULT = 0;

    // retry num for getting the connection from connection pool
    public static final String SENTRY_POOL_RETRY_TOTAL = "sentry.service.client.connection.pool.retry-total";
    public static final int SENTRY_POOL_RETRY_TOTAL_DEFAULT = 3;

    // max message size for thrift messages
    public static final String SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE = "sentry.policy.client.thrift.max.message.size";
    public static final long SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE_DEFAULT = 100 * 1024 * 1024;

    // client retry settings
    public static final String RETRY_COUNT_CONF = "sentry.provider.backend.db.retry.count";
    public static final int RETRY_COUNT_DEFAULT = 3;
    public static final String RETRY_INTERVAL_SEC_CONF = "sentry.provider.backend.db.retry.interval.seconds";
    public static final int RETRY_INTERVAL_SEC_DEFAULT = 30;

    // provider backend cache settings
    public static final String ENABLE_CACHING = "sentry.provider.backend.generic.cache.enabled";
    public static final boolean ENABLE_CACHING_DEFAULT = false;
    public static final String CACHE_TTL_MS = "sentry.provider.backend.generic.cache.ttl.ms";
    public static final long CACHING_TTL_MS_DEFAULT = 30000;
    public static final String CACHE_UPDATE_FAILURES_BEFORE_PRIV_REVOKE = "sentry.provider.backend.generic.cache.update.failures.count";
    public static final int CACHE_UPDATE_FAILURES_BEFORE_PRIV_REVOKE_DEFAULT = 3;
    public static final String PRIVILEGE_CONVERTER = "sentry.provider.backend.generic.privilege.converter";

    public static final String COMPONENT_TYPE = "sentry.provider.backend.generic.component-type";
    public static final String SERVICE_NAME = "sentry.provider.backend.generic.service-name";
  }

  /* Privilege operation scope */
  public enum PrivilegeScope {
    SERVER,
    URI,
    DATABASE,
    TABLE,
    COLUMN
  }
}