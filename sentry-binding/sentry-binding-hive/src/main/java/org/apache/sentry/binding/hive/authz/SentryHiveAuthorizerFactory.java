/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.binding.hive.authz;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;

/**
 * Factory class that creates a HiveAuthorizer implementation for the Hive authorization V2
 * API.
 * <p>
 * In order to use this class, the hive-site.xml should be configured in the following way:
 * <p>
 * <property>
 *   <name>hive.security.authorization.manager</name>
 *   <value>org.apache.sentry.binding.hive.authz.SentryHiveAuthorizerFactory</value>
 * </property>
 */
public class SentryHiveAuthorizerFactory implements HiveAuthorizerFactory {
  @Override
  public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider hiveAuthenticator, HiveAuthzSessionContext ctx)
      throws HiveAuthzPluginException {
    HiveAuthzConf authzConf = HiveAuthzBindingHookBase.loadAuthzConf(conf);
    HiveAuthzSessionContext sessionContext = applyTestSettings(ctx, conf);

    SentryHiveAccessController accessController;
    SentryHiveAuthorizationValidator authValidator;
    try {
      accessController =
          new DefaultSentryAccessController(conf, authzConf, hiveAuthenticator, sessionContext);

      authValidator =
          new DefaultSentryValidator(conf, authzConf, hiveAuthenticator);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }



    return new SentryHiveAuthorizerImpl(accessController, authValidator);
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
}
