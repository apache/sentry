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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

public class KerberosConfiguration extends javax.security.auth.login.Configuration {
  static {
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
  }

  private String principal;
  private String keytab;
  private boolean isInitiator;
  private static final boolean IBM_JAVA =  System.getProperty("java.vendor").contains("IBM");

  private KerberosConfiguration(String principal, File keytab,
      boolean client) {
    this.principal = principal;
    this.keytab = keytab.getAbsolutePath();
    this.isInitiator = client;
  }

  public static javax.security.auth.login.Configuration createClientConfig(String principal,
      File keytab) {
    return new KerberosConfiguration(principal, keytab, true);
  }

  public static javax.security.auth.login.Configuration createServerConfig(String principal,
      File keytab) {
    return new KerberosConfiguration(principal, keytab, false);
  }

  private static String getKrb5LoginModuleName() {
    return (IBM_JAVA ? "com.ibm.security.auth.module.Krb5LoginModule"
            : "com.sun.security.auth.module.Krb5LoginModule");
  }

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
    Map<String, String> options = new HashMap<String, String>();

    if (IBM_JAVA) {
      // IBM JAVA's UseKeytab covers both keyTab and useKeyTab options
      options.put("useKeytab",keytab.startsWith("file://") ? keytab : "file://" + keytab);

      options.put("principal", principal);
      options.put("refreshKrb5Config", "true");

      // Both "initiator" and "acceptor"
      options.put("credsType", "both");
    } else {
      options.put("keyTab", keytab);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", Boolean.toString(isInitiator));
    }

    String ticketCache = System.getenv("KRB5CCNAME");
    if (IBM_JAVA) {
      // If cache is specified via env variable, it takes priority
      if (ticketCache != null) {
        // IBM JAVA only respects system property so copy ticket cache to system property
        // The first value searched when "useDefaultCcache" is true.
        System.setProperty("KRB5CCNAME", ticketCache);
      } else {
    	ticketCache = System.getProperty("KRB5CCNAME");
      }

      if (ticketCache != null) {
        options.put("useDefaultCcache", "true");
        options.put("renewTGT", "true");
      }
    } else {
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
    }
    options.put("debug", "true");

    return new AppConfigurationEntry[]{
        new AppConfigurationEntry(getKrb5LoginModuleName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)};
  }
}

