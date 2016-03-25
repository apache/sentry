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
package org.apache.sentry.binding.hive.v2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.sentry.binding.hive.HiveAuthzBindingHookBase;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.authorizer.DefaultSentryAccessController;
import org.apache.sentry.binding.hive.v2.authorizer.DefaultSentryValidator;
import org.apache.sentry.binding.hive.v2.authorizer.SentryHiveAccessController;
import org.apache.sentry.binding.hive.v2.authorizer.SentryHiveAuthorizationValidator;
import org.apache.sentry.binding.hive.v2.authorizer.SentryHiveAuthorizer;

import com.google.common.annotations.VisibleForTesting;

public class SentryAuthorizerFactory implements HiveAuthorizerFactory {
  public static final String HIVE_SENTRY_ACCESS_CONTROLLER =
      "hive.security.sentry.access.controller";
  public static final String HIVE_SENTRY_AUTHORIZATION_CONTROLLER =
      "hive.security.sentry.authorization.controller";
  private HiveAuthzConf authzConf;

  @Override
  public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx)
          throws HiveAuthzPluginException {
    HiveAuthzSessionContext sessionContext;
    try {
      this.authzConf = HiveAuthzBindingHookBase.loadAuthzConf(conf);
      sessionContext = applyTestSettings(ctx, conf);
      assertHiveCliAuthDisabled(conf, sessionContext);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }
    SentryHiveAccessController accessController =
        getAccessController(conf, authzConf, authenticator, sessionContext);
    SentryHiveAuthorizationValidator authzValidator =
        getAuthzValidator(conf, authzConf, authenticator);

    return new SentryHiveAuthorizer(accessController, authzValidator);
  }

  private HiveAuthzSessionContext applyTestSettings(HiveAuthzSessionContext ctx, HiveConf conf) {
    if (conf.getBoolVar(ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE)
        && ctx.getClientType() == CLIENT_TYPE.HIVECLI) {
      // create new session ctx object with HS2 as client type
      HiveAuthzSessionContext.Builder ctxBuilder = new HiveAuthzSessionContext.Builder(ctx);
      ctxBuilder.setClientType(CLIENT_TYPE.HIVESERVER2);
      return ctxBuilder.build();
    }
    return ctx;
  }

  private void assertHiveCliAuthDisabled(HiveConf conf, HiveAuthzSessionContext ctx)
      throws HiveAuthzPluginException {
    if (ctx.getClientType() == CLIENT_TYPE.HIVECLI
        && conf.getBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      throw new HiveAuthzPluginException(
          "SQL standards based authorization should not be enabled from hive cli"
              + "Instead the use of storage based authorization in hive metastore is reccomended. Set "
              + ConfVars.HIVE_AUTHORIZATION_ENABLED.varname + "=false to disable authz within cli");
    }
  }

  /**
   * just for testing
   */
  @VisibleForTesting
  protected HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthzConf authzConf, HiveAuthenticationProvider authenticator,
      HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {
    SentryHiveAccessController accessController =
        getAccessController(conf, authzConf, authenticator, ctx);
    SentryHiveAuthorizationValidator authzValidator =
        getAuthzValidator(conf, authzConf, authenticator);

    return new SentryHiveAuthorizer(accessController, authzValidator);
  }

  /**
   * Get instance of SentryAccessController from configuration
   * Default return DefaultSentryAccessController
   *
   * @param conf
   * @param authzConf
   * @param hiveAuthzBinding
   * @param authenticator
   * @throws HiveAuthzPluginException
   */
  public static SentryHiveAccessController getAccessController(HiveConf conf,
      HiveAuthzConf authzConf, HiveAuthenticationProvider authenticator,
      HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {
    Class<? extends SentryHiveAccessController> clazz =
        conf.getClass(HIVE_SENTRY_ACCESS_CONTROLLER, DefaultSentryAccessController.class,
            SentryHiveAccessController.class);

    if (clazz == null) {
      // should not happen as default value is set
      throw new HiveAuthzPluginException("Configuration value " + HIVE_SENTRY_ACCESS_CONTROLLER
          + " is not set to valid SentryAccessController subclass");
    }

    try {
      return new DefaultSentryAccessController(conf, authzConf, authenticator, ctx);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }

  }

  /**
   * Get instance of SentryAuthorizationValidator from configuration
   * Default return DefaultSentryAuthorizationValidator
   *
   * @param conf
   * @param authzConf
   * @param authenticator
   * @throws HiveAuthzPluginException
   */
  public static SentryHiveAuthorizationValidator getAuthzValidator(HiveConf conf,
      HiveAuthzConf authzConf, HiveAuthenticationProvider authenticator)
      throws HiveAuthzPluginException {
    Class<? extends SentryHiveAuthorizationValidator> clazz =
        conf.getClass(HIVE_SENTRY_AUTHORIZATION_CONTROLLER, DefaultSentryValidator.class,
            SentryHiveAuthorizationValidator.class);

    if (clazz == null) {
      // should not happen as default value is set
      throw new HiveAuthzPluginException("Configuration value "
          + HIVE_SENTRY_AUTHORIZATION_CONTROLLER
          + " is not set to valid SentryAuthorizationValidator subclass");
    }

    try {
      return new DefaultSentryValidator(conf, authzConf, authenticator);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }

  }
}
