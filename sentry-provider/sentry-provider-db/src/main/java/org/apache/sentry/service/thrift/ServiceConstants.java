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
package org.apache.sentry.service.thrift;

import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.Sasl;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.sentry.provider.db.service.thrift.SentryMetrics;

public class ServiceConstants {

  private static final ImmutableMap<String, String> SASL_PROPERTIES;

  static {
    Map<String, String> saslProps = new HashMap<String, String>();
    saslProps.put(Sasl.SERVER_AUTH, "true");
    saslProps.put(Sasl.QOP, "auth-conf");
    SASL_PROPERTIES = ImmutableMap.copyOf(saslProps);
  }

  public static class ConfUtilties {
    public static final Splitter CLASS_SPLITTER = Splitter.onPattern("[\\s,]")
        .trimResults().omitEmptyStrings();
  }
  public static class ServiceArgs {
    public static final String CONFIG_FILE_SHORT = "c";
    public static final String CONFIG_FILE_LONG = "conffile";
  }

  public static class ServerConfig {
    public static final ImmutableMap<String, String> SASL_PROPERTIES = ServiceConstants.SASL_PROPERTIES;
    /**
     * This configuration parameter is only meant to be used for testing purposes.
     */
    public static final String SECURITY_MODE = "sentry.service.security.mode";
    public static final String SECURITY_MODE_KERBEROS = "kerberos";
    public static final String SECURITY_MODE_NONE = "none";
    public static final String SECURITY_USE_UGI_TRANSPORT = "sentry.service.security.use.ugi";
    public static final String ADMIN_GROUPS = "sentry.service.admin.group";
    public static final String PRINCIPAL = "sentry.service.server.principal";
    public static final String KEY_TAB = "sentry.service.server.keytab";
    public static final String RPC_PORT = "sentry.service.server.rpc-port";
    public static final int RPC_PORT_DEFAULT = 8038;
    public static final String RPC_ADDRESS = "sentry.service.server.rpc-address";
    public static final String RPC_ADDRESS_DEFAULT = "0.0.0.0";
    public static final String RPC_MAX_THREADS = "sentry.service.server-max-threads";
    public static final int RPC_MAX_THREADS_DEFAULT = 500;
    public static final String RPC_MIN_THREADS = "sentry.service.server-min-threads";
    public static final int RPC_MIN_THREADS_DEFAULT = 10;
    public static final String ALLOW_CONNECT = "sentry.service.allow.connect";

    public static final String SENTRY_POLICY_STORE_PLUGINS = "sentry.policy.store.plugins";
    public static final String SENTRY_POLICY_STORE_PLUGINS_DEFAULT = "";

    public static final String SENTRY_METASTORE_PLUGINS = "sentry.metastore.plugins";
    public static final String SENTRY_METASTORE_PLUGINS_DEFAULT = "";

    public static final String PROCESSOR_FACTORIES = "sentry.service.processor.factories";
    public static final String PROCESSOR_FACTORIES_DEFAULT =
        "org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessorFactory" +
            ",org.apache.sentry.provider.db.generic.service.thrift.SentryGenericPolicyProcessorFactory";
    public static final String SENTRY_STORE_JDBC_URL = "sentry.store.jdbc.url";
    public static final String SENTRY_STORE_JDBC_USER = "sentry.store.jdbc.user";
    public static final String SENTRY_STORE_JDBC_USER_DEFAULT = "Sentry";
    public static final String SENTRY_STORE_JDBC_PASS = "sentry.store.jdbc.password";
    public static final String SENTRY_STORE_JDBC_DRIVER = "sentry.store.jdbc.driver";
    public static final String SENTRY_STORE_JDBC_DRIVER_DEFAULT = "org.apache.derby.jdbc.EmbeddedDriver";
    // The configuration for the maximum number of retries per db transaction,
    // the default value is 3 times
    public static final String SENTRY_STORE_TRANSACTION_RETRY = "sentry.store.transaction.retry";
    public static final int SENTRY_STORE_TRANSACTION_RETRY_DEFAULT = 3;
    // The configuration for the delay (in milliseconds) between retries,
    // the default value is 500 ms
    public static final String SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS =
        "sentry.store.transaction.retry.wait.time.millis";
    public static final int SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS_DEFAULT = 500;

    public static final String JAVAX_JDO_URL = "javax.jdo.option.ConnectionURL";
    public static final String JAVAX_JDO_USER = "javax.jdo.option.ConnectionUserName";
    public static final String JAVAX_JDO_PASS = "javax.jdo.option.ConnectionPassword";
    public static final String JAVAX_JDO_DRIVER_NAME = "javax.jdo.option.ConnectionDriverName";

    public static final String DATANUCLEUS_ISOLATION_LEVEL = "datanucleus.transactionIsolation";
    public static final String DATANUCLEUS_REPEATABLE_READ = "repeatable-read";

    public static final String SENTRY_DB_PROPERTY_PREFIX = "sentry.";
    public static final String SENTRY_JAVAX_JDO_PROPERTY_PREFIX = SENTRY_DB_PROPERTY_PREFIX + "javax.jdo";
    public static final String SENTRY_DATANUCLEUS_PROPERTY_PREFIX = SENTRY_DB_PROPERTY_PREFIX + "datanucleus";

    public static final String SENTRY_VERIFY_SCHEM_VERSION = "sentry.verify.schema.version";
    public static final String SENTRY_VERIFY_SCHEM_VERSION_DEFAULT = "true";

    public static final String SENTRY_SERVICE_NAME = "sentry.service.name";
    public static final String SENTRY_SERVICE_NAME_DEFAULT = "Sentry-Service";

    public static final String SENTRY_STORE_GROUP_MAPPING = "sentry.store.group.mapping";
    public static final String SENTRY_STORE_GROUP_MAPPING_RESOURCE = "sentry.store.group.mapping.resource";
    public static final String SENTRY_STORE_HADOOP_GROUP_MAPPING = "org.apache.sentry.provider.common.HadoopGroupMappingService";
    public static final String SENTRY_STORE_LOCAL_GROUP_MAPPING = "org.apache.sentry.provider.file.LocalGroupMappingService";
    public static final String SENTRY_STORE_GROUP_MAPPING_DEFAULT = SENTRY_STORE_HADOOP_GROUP_MAPPING;

    public static final String SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL = "sentry.store.orphaned.privilege.removal";
    public static final String SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL_DEFAULT = "false";
    public static final String SENTRY_STORE_CLEAN_PERIOD_SECONDS =
        "sentry.store.clean.period.seconds";
    public static final long SENTRY_STORE_CLEAN_PERIOD_SECONDS_DEFAULT = 43200; // 12 hours.
    public static final String SENTRY_HA_ENABLED = "sentry.ha.enabled";
    public static final boolean SENTRY_HA_ENABLED_DEFAULT = false;
    public static final String SENTRY_HA_ZK_PROPERTY_PREFIX = "sentry.ha.zookeeper.";
    public static final String SENTRY_HA_ZOOKEEPER_SECURITY = SENTRY_HA_ZK_PROPERTY_PREFIX + "security";
    public static final boolean SENTRY_HA_ZOOKEEPER_SECURITY_DEFAULT = false;
    public static final String SENTRY_HA_ZOOKEEPER_QUORUM = SENTRY_HA_ZK_PROPERTY_PREFIX + "quorum";
    public static final String SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT = SENTRY_HA_ZK_PROPERTY_PREFIX + "session.retries.max.count";
    public static final int SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT_DEFAULT = 3;
    public static final String SENTRY_HA_ZOOKEEPER_SLEEP_BETWEEN_RETRIES_MS = SENTRY_HA_ZK_PROPERTY_PREFIX + "session.sleep.between.retries.ms";
    public static final int SENTRY_HA_ZOOKEEPER_SLEEP_BETWEEN_RETRIES_MS_DEFAULT = 100;
    public static final String SENTRY_HA_ZOOKEEPER_NAMESPACE = SENTRY_HA_ZK_PROPERTY_PREFIX + "namespace";
    public static final String SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT = "sentry";
    // principal and keytab for client to be able to connect to secure ZK. Needed for Sentry HA with secure ZK
    public static final String SERVER_HA_ZOOKEEPER_CLIENT_PRINCIPAL = "sentry.zookeeper.client.principal";
    public static final String SERVER_HA_ZOOKEEPER_CLIENT_KEYTAB = "sentry.zookeeper.client.keytab";
    public static final String SERVER_HA_ZOOKEEPER_CLIENT_TICKET_CACHE = "sentry.zookeeper.client.ticketcache";
    public static final String SERVER_HA_ZOOKEEPER_CLIENT_TICKET_CACHE_DEFAULT = "false";
    public static final String SERVER_HA_STANDBY_SIG = "sentry.ha.standby.signal";
    public static final ImmutableMap<String, String> SENTRY_STORE_DEFAULTS =
        ImmutableMap.<String, String>builder()
        .put("datanucleus.connectionPoolingType", "BoneCP")
        .put("datanucleus.validateTables", "false")
        .put("datanucleus.validateColumns", "false")
        .put("datanucleus.validateConstraints", "false")
        .put("datanucleus.storeManagerType", "rdbms")
        .put("datanucleus.autoCreateSchema", "false")
        .put("datanucleus.fixedDatastore", "true")
        .put("datanucleus.autoStartMechanismMode", "checked")
        .put(DATANUCLEUS_ISOLATION_LEVEL, DATANUCLEUS_REPEATABLE_READ)
        .put("datanucleus.cache.level2", "false")
        .put("datanucleus.cache.level2.type", "none")
        .put("datanucleus.query.sql.allowAll", "true")
        .put("datanucleus.identifierFactory", "datanucleus1")
        .put("datanucleus.rdbms.useLegacyNativeValueStrategy", "true")
        .put("datanucleus.plugin.pluginRegistryBundleCheck", "LOG")
        .put("javax.jdo.PersistenceManagerFactoryClass",
            "org.datanucleus.api.jdo.JDOPersistenceManagerFactory")
            .put("javax.jdo.option.DetachAllOnCommit", "true")
            .put("javax.jdo.option.NonTransactionalRead", "false")
            .put("javax.jdo.option.NonTransactionalWrite", "false")
            .put("javax.jdo.option.Multithreaded", "true")
            .build();

    // InitialDelay and period time for HMSFollower thread.
    public static final String SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS = "sentry.hmsfollower.init.delay.mills";
    public static final long SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS_DEFAULT = 0;
    public static final String SENTRY_HMSFOLLOWER_INTERVAL_MILLS = "sentry.hmsfollower.interval.mills";
    public static final long SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT = 500;

    public static final String SENTRY_NOTIFICATION_LOG_ENABLED = "sentry.notification.log.enabled";
    public static final boolean SENTRY_NOTIFICATION_LOG_ENABLED_DEFAULT = true;
    public static final String SENTRY_WEB_ENABLE = "sentry.service.web.enable";
    public static final Boolean SENTRY_WEB_ENABLE_DEFAULT = false;
    public static final String SENTRY_WEB_PORT = "sentry.service.web.port";
    public static final int SENTRY_WEB_PORT_DEFAULT = 51000;
    // Reporter is either "console" or "jmx"
    public static final String SENTRY_REPORTER = "sentry.service.reporter";
    public static final String SENTRY_REPORTER_JMX = SentryMetrics.Reporting.JMX.name(); //case insensitive
    public static final String SENTRY_REPORTER_CONSOLE = SentryMetrics.Reporting.CONSOLE.name();//case insensitive
    // for console reporter, reporting interval in seconds
    public static final String SENTRY_REPORTER_INTERVAL_SEC =
            "sentry.service.reporter.interval.sec";
    // Report every 5 minutes by default
    public static final int SENTRY_REPORTER_INTERVAL_DEFAULT = 300;


    // Web Security
    public static final String SENTRY_WEB_SECURITY_PREFIX = "sentry.service.web.authentication";
    public static final String SENTRY_WEB_SECURITY_TYPE = SENTRY_WEB_SECURITY_PREFIX + ".type";
    public static final String SENTRY_WEB_SECURITY_TYPE_NONE = "NONE";
    public static final String SENTRY_WEB_SECURITY_TYPE_KERBEROS = "KERBEROS";
    public static final String SENTRY_WEB_SECURITY_PRINCIPAL = SENTRY_WEB_SECURITY_PREFIX + ".kerberos.principal";
    public static final String SENTRY_WEB_SECURITY_KEYTAB = SENTRY_WEB_SECURITY_PREFIX + ".kerberos.keytab";
    public static final String SENTRY_WEB_SECURITY_ALLOW_CONNECT_USERS = SENTRY_WEB_SECURITY_PREFIX + ".allow.connect.users";

    // Web SSL
    public static final String SENTRY_WEB_USE_SSL = "sentry.web.use.ssl";
    public static final String SENTRY_WEB_SSL_KEYSTORE_PATH = "sentry.web.ssl.keystore.path";
    public static final String SENTRY_WEB_SSL_KEYSTORE_PASSWORD = "sentry.web.ssl.keystore.password";
    public static final String SENTRY_SSL_PROTOCOL_BLACKLIST = "sentry.ssl.protocol.blacklist";

    // Blacklist SSL protocols that are not secure (e.g., POODLE vulnerability)
    public static final String[] SENTRY_SSL_PROTOCOL_BLACKLIST_DEFAULT = {"SSLv2", "SSLv2Hello", "SSLv3"};

    // Flag to enable admin servlet
    public static final String SENTRY_WEB_ADMIN_SERVLET_ENABLED = "sentry.web.admin.servlet.enabled";
    public static final boolean SENTRY_WEB_ADMIN_SERVLET_ENABLED_DEFAULT = false;

    // max message size for thrift messages
    public static String SENTRY_POLICY_SERVER_THRIFT_MAX_MESSAGE_SIZE = "sentry.policy.server.thrift.max.message.size";
    public static long SENTRY_POLICY_SERVER_THRIFT_MAX_MESSAGE_SIZE_DEFAULT = 100 * 1024 * 1024;

    // action factories for external components
    public static final String SENTRY_COMPONENT_ACTION_FACTORY_FORMAT = "sentry.%s.action.factory";

    // Sentry is never a client to other Kerberos Services, it should not be required to renew the TGT
    @Deprecated
    public static final String SENTRY_KERBEROS_TGT_AUTORENEW = "sentry.service.kerberos.tgt.autorenew";
    @Deprecated
    public static final Boolean SENTRY_KERBEROS_TGT_AUTORENEW_DEFAULT = false;

    /**
     * Number of path/priv deltas to keep around during cleaning
     * The value which is too small may cause unnecessary full snapshots sent to the Name Node
     * A value which is too large may cause slowdown due to too many deltas lying around in the DB.
     */
    public static final String SENTRY_DELTA_KEEP_COUNT = "sentry.server.delta.keep.count";
    public static final int SENTRY_DELTA_KEEP_COUNT_DEFAULT = 200;
  }

  public static class ClientConfig {
    public static final String SERVER_RPC_PORT = "sentry.service.client.server.rpc-port";
    public static final int SERVER_RPC_PORT_DEFAULT = ServerConfig.RPC_PORT_DEFAULT;
    public static final String SERVER_RPC_ADDRESS = "sentry.service.client.server.rpc-address";
    public static final String SERVER_RPC_CONN_TIMEOUT = "sentry.service.client.server.rpc-connection-timeout";

    // HA configuration
    public static final String SENTRY_HA_ENABLED = "sentry.ha.enabled";
    public static final String SENTRY_HA_ZOOKEEPER_QUORUM = ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM;
    public static final String SENTRY_HA_ZOOKEEPER_NAMESPACE = ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE;
    public static final String SERVER_HA_ZOOKEEPER_NAMESPACE_DEFAULT = ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT;

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
    public static String SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE = "sentry.policy.client.thrift.max.message.size";
    public static long SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE_DEFAULT = 100 * 1024 * 1024;

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
  }

  /**
   * Thrift generates terrible constant class names
   */
  public static class ThriftConstants extends org.apache.sentry.service.thrift.sentry_common_serviceConstants {
    public static final int TSENTRY_SERVICE_VERSION_CURRENT = TSENTRY_SERVICE_V2;
  }

  /* Privilege operation scope */
  public static enum PrivilegeScope {
    SERVER,
    URI,
    DATABASE,
    TABLE,
    COLUMN
  }

  public static final String SENTRY_ZK_JAAS_NAME = "Sentry";
  public static final String CURRENT_INCARNATION_ID_KEY = "current.incarnation.key";
}
