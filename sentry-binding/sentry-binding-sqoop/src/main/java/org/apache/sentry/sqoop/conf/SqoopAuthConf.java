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
package org.apache.sentry.sqoop.conf;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;

public class SqoopAuthConf extends Configuration {
  /**
   * Configuration key used in sqoop.properties to point at sentry-site.xml
   */
  public static final String SENTRY_SQOOP_SITE_URL = "sentry.sqoop.site.url";
  /**
   * Config setting definitions
   */
  public static enum AuthzConfVars {
    AUTHZ_PROVIDER("sentry.sqoop.provider","org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider"),
    AUTHZ_PROVIDER_RESOURCE("sentry.sqoop.provider.resource", ""),
    AUTHZ_PROVIDER_BACKEND(
        "sentry.sqoop.provider.backend",
        "org.apache.sentry.provider.db.generic.SentryGenericProviderBackend"),
    AUTHZ_POLICY_ENGINE("sentry.sqoop.policy.engine","org.apache.sentry.policy.sqoop.SimpleSqoopPolicyEngine"),
    AUTHZ_SERVER_NAME("sentry.sqoop.name", ""),
    AUTHZ_TESTING_MODE("sentry.sqoop.testing.mode", "false");

    private final String varName;
    private final String defaultVal;

    AuthzConfVars(String varName, String defaultVal) {
      this.varName = varName;
      this.defaultVal = defaultVal;
    }

    public String getVar() {
      return varName;
    }

    public String getDefault() {
      return defaultVal;
    }

    public static String getDefault(String varName) {
      for (AuthzConfVars oneVar : AuthzConfVars.values()) {
        if (oneVar.getVar().equalsIgnoreCase(varName)) {
          return oneVar.getDefault();
        }
      }
      return null;
    }
  }

  public static final String AUTHZ_SITE_FILE = "sentry-site.xml";

  public SqoopAuthConf(URL sqoopAuthzSiteURL) {
    super(true);
    addResource(sqoopAuthzSiteURL);
  }

  @Override
  public String get(String varName) {
    return get(varName, AuthzConfVars.getDefault(varName));
  }
}
