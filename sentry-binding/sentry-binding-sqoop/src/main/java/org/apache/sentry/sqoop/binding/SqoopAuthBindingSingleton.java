/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.sqoop.binding;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.sqoop.conf.SqoopAuthConf;
import org.apache.sentry.sqoop.conf.SqoopAuthConf.AuthzConfVars;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.security.SecurityConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class SqoopAuthBindingSingleton {
  private static Logger log = LoggerFactory.getLogger(SqoopAuthBindingSingleton.class);
  private static SqoopAuthBindingSingleton instance = null;

  private SqoopAuthBinding binding;

  private SqoopAuthBindingSingleton() {
    SqoopAuthBinding tmpBinding = null;
    try {
      String serverName = SqoopConfiguration.getInstance().getContext().getString(SecurityConstants.SERVER_NAME);
      if (Strings.isNullOrEmpty(serverName)) {
        throw new IllegalArgumentException(SecurityConstants.SERVER_NAME + " can't be null or empty");
      }
      SqoopAuthConf conf = loadAuthzConf();
      validateSentrySqoopConfig(conf);
      tmpBinding = new SqoopAuthBinding(conf, serverName.trim());
      log.info("SqoopAuthBinding created successfully");
    } catch (Exception ex) {
      log.error("Unable to create SqoopAuthBinding", ex);
      throw new RuntimeException("Unable to create SqoopAuthBinding: " + ex.getMessage(), ex);
    }
    binding = tmpBinding;
  }

  private SqoopAuthConf loadAuthzConf() {
    String sentry_site = SqoopConfiguration.getInstance().getContext()
        .getString(SqoopAuthConf.SENTRY_SQOOP_SITE_URL);
    if (Strings.isNullOrEmpty(sentry_site)) {
      throw new IllegalArgumentException("Configuration key " + SqoopAuthConf.SENTRY_SQOOP_SITE_URL
          + " value '" + sentry_site + "' is invalid.");
    }

    SqoopAuthConf sqoopAuthConf = null;
    try {
      sqoopAuthConf = new SqoopAuthConf(new URL(sentry_site));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Configuration key " + SqoopAuthConf.SENTRY_SQOOP_SITE_URL
          + " specifies a malformed URL '" + sentry_site + "'", e);
    }
    return sqoopAuthConf;
  }

  private void validateSentrySqoopConfig(SqoopAuthConf conf) {
    boolean isTestingMode = Boolean.parseBoolean(conf.get(AuthzConfVars.AUTHZ_TESTING_MODE.getVar(),
                            AuthzConfVars.AUTHZ_TESTING_MODE.getDefault()));
    String authentication = SqoopConfiguration.getInstance().getContext()
                            .getString(SecurityConstants.AUTHENTICATION_TYPE, SecurityConstants.TYPE.SIMPLE.name());
    String kerberos = SecurityConstants.TYPE.KERBEROS.name();
    if(!isTestingMode && !kerberos.equalsIgnoreCase(authentication)) {
      throw new IllegalArgumentException(SecurityConstants.AUTHENTICATION_TYPE + "can't be set simple mode in non-testing mode");
    }
  }

  public static SqoopAuthBindingSingleton getInstance() {
    if (instance != null) {
      return instance;
    }
    instance = new SqoopAuthBindingSingleton();
    return instance;
  }

  public SqoopAuthBinding getAuthBinding() {
    return binding;
  }
}
