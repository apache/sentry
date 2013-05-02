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
package org.apache.access.binding.hive.conf;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


public class HiveAuthzConf extends Configuration {
  /**
   * Config setting definitions
   */
  public static enum AuthzConfVars {
    AUTHZ_PROVIDER("hive.access.provider",
        "org.apache.access.provider.file.ResourceAuthorizationProvider"),
        AUTHZ_PROVIDER_RESOURCE("hive.access.provider.resource", ""),
        AUTHZ_SERVER_NAME("hive.access.server", "HS2"),
        AUTHZ_RESTRICT_DEFAULT_DB("hive.access.restrict.defaultDB", "false"),
        ACCESS_TESTING_MODE("hive.access.testing.mode", "false")
        ;

    private final String varName;
    private final String defaultVal;

    AuthzConfVars(String varName, String defaultVal) {
      this.varName = varName;
      this.defaultVal = defaultVal;
    }

    public String getVar() {
      return varName;
    }

    public String getDefault(String varName) {
      return valueOf(varName).defaultVal;
    }
  }

  private static final  Log LOG = LogFactory.getLog(HiveAuthzConf.class);
  private static final Object staticLock = new Object();
  public static final String AUTHZ_SITE_FILE = "access-site.xml";
  private static URL hiveAuthzSiteURL;

  public HiveAuthzConf() {
    super(false);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = HiveAuthzConf.class.getClassLoader();
    }

    // Look for hive-site.xml on the CLASSPATH and log its location if found.
    synchronized (staticLock) {
      hiveAuthzSiteURL = classLoader.getResource(AUTHZ_SITE_FILE);
      if (hiveAuthzSiteURL == null) {
        LOG.warn("Access site file " + AUTHZ_SITE_FILE + " not found");
      } else {
        addResource(hiveAuthzSiteURL);
      }
    }

    applySystemProperties();
  }
  /**
   * Apply system properties to this object if the property name is defined in ConfVars
   * and the value is non-null and not an empty string.
   */
  private void applySystemProperties() {
    Map<String, String> systemProperties = getConfSystemProperties();
    for (Entry<String, String> systemProperty : systemProperties.entrySet()) {
      this.set(systemProperty.getKey(), systemProperty.getValue());
    }
  }

  /**
   * This method returns a mapping from config variable name to its value for all config variables
   * which have been set using System properties
   */
  public static Map<String, String> getConfSystemProperties() {
    Map<String, String> systemProperties = new HashMap<String, String>();

    for (AuthzConfVars oneVar : AuthzConfVars.values()) {
      String value = System.getProperty(oneVar.getVar());
      if (value != null && value.length() > 0) {
        systemProperties.put(oneVar.getVar(), value);
      }
    }
    return systemProperties;
  }

  @Override
  public String get(String varName) {
    String retVal = super.get(varName);
    if (retVal == null) {
      retVal = AuthzConfVars.valueOf(varName).getDefault(varName);
    }
    return retVal;
  }
}
