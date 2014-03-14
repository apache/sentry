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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolrAuthzConf extends Configuration {

  /**
   * Config setting definitions
   */
  public static enum AuthzConfVars {
    AUTHZ_PROVIDER("sentry.provider",
      "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider"),
    AUTHZ_PROVIDER_RESOURCE("sentry.solr.provider.resource", ""),
    AUTHZ_PROVIDER_BACKEND("sentry.solr.provider.backend", "org.apache.sentry.provider.file.SimpleFileProviderBackend"),
    AUTHZ_POLICY_ENGINE("sentry.solr.policy.engine", "org.apache.sentry.policy.search.SimpleSearchPolicyEngine");

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

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(SolrAuthzConf.class);
  public static final String AUTHZ_SITE_FILE = "sentry-site.xml";

  public SolrAuthzConf(URL solrAuthzSiteURL) {
    super(false);
    addResource(solrAuthzSiteURL);
  }

  @Override
  public String get(String varName) {
    return get(varName, AuthzConfVars.getDefault(varName));
  }
}
