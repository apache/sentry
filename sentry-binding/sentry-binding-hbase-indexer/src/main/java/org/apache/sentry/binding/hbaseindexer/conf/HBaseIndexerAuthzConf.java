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
package org.apache.sentry.binding.hbaseindexer.conf;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains configuration entries for HBase Indexer Binding
 */
public class HBaseIndexerAuthzConf extends Configuration {

  /**
   * Config setting definitions
   */
  public static enum AuthzConfVars {
    AUTHZ_PROVIDER("sentry.provider",
      "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider"),
    AUTHZ_PROVIDER_RESOURCE("sentry.hbaseindexer.provider.resource", ""),
    AUTHZ_PROVIDER_BACKEND("sentry.hbaseindexer.provider.backend", "org.apache.sentry.provider.file.SimpleFileProviderBackend"),
    AUTHZ_POLICY_ENGINE("sentry.hbaseindexer.policy.engine", "org.apache.sentry.policy.engine.common.CommonPolicyEngine"),
    AUTHZ_SERVICE_NAME("sentry.hbaseindexer.service", "KS_INDEXER-1"),
    // binding uses hbase regionserver specification to login, but hbase regionservers
    // support putting _HOST instead of fqdn and doing the translation at runtime.
    // Setting this property tells sentry how to do the translation.
    PRINCIPAL_HOSTNAME("sentry.hbaseindexer.principal.hostname", null);

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
      .getLogger(HBaseIndexerAuthzConf.class);

  public HBaseIndexerAuthzConf(URL hbaseIndexerAuthzSiteURL) {
    super(true);
    addResource(hbaseIndexerAuthzSiteURL);
  }

  @Override
  public String get(String varName) {
    return get(varName, AuthzConfVars.getDefault(varName));
  }
}
