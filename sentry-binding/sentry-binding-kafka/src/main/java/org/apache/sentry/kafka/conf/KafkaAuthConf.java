/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.kafka.conf;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.policy.kafka.SimpleKafkaPolicyEngine;
import org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;

public class KafkaAuthConf extends Configuration {
  /**
   * Configuration key used in kafka.properties to point at sentry-site.xml
   */
  public static final String SENTRY_KAFKA_SITE_URL = "sentry.kafka.site.url";
  public static final String AUTHZ_SITE_FILE = "sentry-site.xml";
  public static final String KAFKA_SUPER_USERS = "kafka.superusers";
  public static final String KAFKA_SERVICE_INSTANCE_NAME = "sentry.kafka.service.instance";
  public static final String KAFKA_SERVICE_USER_NAME = "sentry.kafka.service.user.name";

  /**
   * Config setting definitions
   */
  public static enum AuthzConfVars {
    AUTHZ_PROVIDER("sentry.kafka.provider", HadoopGroupResourceAuthorizationProvider.class.getName()),
    AUTHZ_PROVIDER_RESOURCE("sentry.kafka.provider.resource", ""),
    AUTHZ_PROVIDER_BACKEND("sentry.kafka.provider.backend", SentryGenericProviderBackend.class.getName()),
    AUTHZ_POLICY_ENGINE("sentry.kafka.policy.engine", SimpleKafkaPolicyEngine.class.getName()),
    AUTHZ_INSTANCE_NAME(KAFKA_SERVICE_INSTANCE_NAME, "kafka"),
    AUTHZ_SERVICE_USER_NAME(KAFKA_SERVICE_USER_NAME, "kafka");

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

  public KafkaAuthConf(URL kafkaAuthzSiteURL) {
    super(true);
    addResource(kafkaAuthzSiteURL);
  }

  @Override
  public String get(String varName) {
    return get(varName, AuthzConfVars.getDefault(varName));
  }
}
