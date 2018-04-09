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
package org.apache.sentry.binding.solr.conf;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolrAuthzConf extends Configuration {

  /**
   * Config setting definitions
   */
  public static enum AuthzConfVars {
    AUTHZ_PROVIDER("sentry.solr.provider",
      "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider"),
    AUTHZ_PROVIDER_RESOURCE("sentry.solr.provider.resource", ""),
    AUTHZ_PROVIDER_BACKEND("sentry.solr.provider.backend", "org.apache.sentry.provider.file.SimpleFileProviderBackend"),
    AUTHZ_POLICY_ENGINE("sentry.solr.policy.engine", "org.apache.sentry.policy.engine.common.CommonPolicyEngine"),

    AUTHZ_PROVIDER_DEPRECATED("sentry.provider",
      "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider");

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
        if(oneVar.getVar().equalsIgnoreCase(varName)) {
          return oneVar.getDefault();
        }
      }
      return null;
    }
  }

  private static final Map<String, AuthzConfVars> currentToDeprecatedProps = new HashMap<>();
  static {
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_PROVIDER.getVar(), AuthzConfVars.AUTHZ_PROVIDER_DEPRECATED);
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(SolrAuthzConf.class);
  public static final String AUTHZ_SITE_FILE = "sentry-site.xml";

  /**
   * This constructor allows configuration of Sentry including sentry-site.xml and core-site.xml
   *
   * @param sentryConf Location of a folder (on local file-system) storing
   *                   sentry Hadoop configuration files
   */
  public SolrAuthzConf(List<URL> sentryConf) {
    super(true);
    for (URL u : sentryConf) {
      addResource(u, true);
    }
  }

  @Override
  public String get(String varName) {
    String retVal = super.get(varName);
    if (retVal == null) {
      // check if the deprecated value is set here
      if (currentToDeprecatedProps.containsKey(varName)) {
          AuthzConfVars var = currentToDeprecatedProps.get(varName);
          retVal = super.get(var.getVar());
      }
      if (retVal == null) {
        retVal = AuthzConfVars.getDefault(varName);
      } else {
        LOG.warn("Using the deprecated config setting " + currentToDeprecatedProps.get(varName).getVar() + " instead of " + varName);
      }
    }
    return retVal;
  }
}
