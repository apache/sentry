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
package org.apache.sentry.binding.hive.conf;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveAuthzConf extends Configuration {

  /**
   * Configuration key used in hive-site.xml to point at sentry-site.xml
   */
  public static final String HIVE_ACCESS_CONF_URL = "hive.access.conf.url";
  public static final String HIVE_SENTRY_CONF_URL = "hive.sentry.conf.url";
  public static final String HIVE_ACCESS_SUBJECT_NAME = "hive.access.subject.name";
  public static final String HIVE_SENTRY_SUBJECT_NAME = "hive.sentry.subject.name";

  /**
   * Config setting definitions
   */
  public static enum AuthzConfVars {
    AUTHZ_PROVIDER("hive.sentry.provider",
        "org.apache.sentry.provider.file.ResourceAuthorizationProvider"),
        AUTHZ_PROVIDER_RESOURCE("hive.sentry.provider.resource", ""),
        AUTHZ_SERVER_NAME("hive.sentry.server", "HS2"),
        AUTHZ_RESTRICT_DEFAULT_DB("hive.sentry.restrict.defaultDB", "false"),
        SENTRY_TESTING_MODE("hive.sentry.testing.mode", "false"),
        AUTHZ_UDF_WHITELIST("hive.sentry.udf.whitelist", HIVE_UDF_WHITE_LIST),
        AUTHZ_ALLOW_HIVE_IMPERSONATION("hive.sentry.allow.hive.impersonation", "false"),
        AUTHZ_ONFAILURE_HOOKS("hive.sentry.failure.hooks", ""),

        AUTHZ_PROVIDER_DEPRECATED("hive.access.provider",
        "org.apache.sentry.provider.file.ResourceAuthorizationProvider"),
        AUTHZ_PROVIDER_RESOURCE_DEPRECATED("hive.access.provider.resource", ""),
        AUTHZ_SERVER_NAME_DEPRECATED("hive.access.server", "HS2"),
        AUTHZ_RESTRICT_DEFAULT_DB_DEPRECATED("hive.access.restrict.defaultDB", "false"),
        SENTRY_TESTING_MODE_DEPRECATED("hive.access.testing.mode", "false"),
        AUTHZ_UDF_WHITELIST_DEPRECATED("hive.access.udf.whitelist", HIVE_UDF_WHITE_LIST),
        AUTHZ_ALLOW_HIVE_IMPERSONATION_DEPRECATED("hive.access.allow.hive.impersonation", "false"),
        AUTHZ_ONFAILURE_HOOKS_DEPRECATED("hive.access.failure.hooks", ""),

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

  private static final String HIVE_UDF_WHITE_LIST =
    "abs,acos,and,array,array_contains,ascii,asin,assert_true,atan,avg," +
    "between,bin,case,ceil,ceiling,coalesce,collect_set,compute_stats,concat,concat_ws," +
    "context_ngrams,conv,corr,cos,count,covar_pop,covar_samp,create_union,date_add,date_sub," +
    "datediff,day,dayofmonth,degrees,div,e,elt,ewah_bitmap,ewah_bitmap_and,ewah_bitmap_empty," +
    "ewah_bitmap_or,exp,explode,field,find_in_set,floor,format_number,from_unixtime," +
    "from_utc_timestamp,get_json_object,hash,hex,histogram_numeric,hour,if,in,in_file,index," +
    "inline,instr,isnotnull,isnull," + // java_method is skipped
    "json_tuple,lcase,length,like,ln,locate,log," +
    "log10,log2,lower,lpad,ltrim,map,map_keys,map_values,max,min," +
    "minute,month,named_struct,negative,ngrams,not,or,parse_url,parse_url_tuple,percentile," +
    "percentile_approx,pi,pmod,positive,pow,power,printf,radians,rand," + // reflect is skipped
    "regexp,regexp_extract,regexp_replace,repeat,reverse,rlike,round,rpad,rtrim,second," +
    "sentences,sign,sin,size,sort_array,space,split,sqrt,stack,std," +
    "stddev,stddev_pop,stddev_samp,str_to_map,struct,substr,substring,sum,tan,to_date," +
    "to_utc_timestamp,translate,trim,ucase,unhex,union_map,unix_timestamp,upper,var_pop,var_samp," +
    "variance,weekofyear,when,xpath,xpath_boolean,xpath_double,xpath_float,xpath_int,xpath_long," +
    "xpath_number,xpath_short,xpath_string,year";

  private static final Map<String, AuthzConfVars> deprecatedConfigs =
      new HashMap<String, AuthzConfVars>();
  static {
   deprecatedConfigs.put(AuthzConfVars.AUTHZ_PROVIDER_DEPRECATED.getVar(), AuthzConfVars.AUTHZ_PROVIDER);
   deprecatedConfigs.put(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE_DEPRECATED.getVar(), AuthzConfVars.AUTHZ_PROVIDER_RESOURCE);
   deprecatedConfigs.put(AuthzConfVars.AUTHZ_SERVER_NAME_DEPRECATED.getVar(), AuthzConfVars.AUTHZ_SERVER_NAME);
   deprecatedConfigs.put(AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB_DEPRECATED.getVar(), AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB);
   deprecatedConfigs.put(AuthzConfVars.SENTRY_TESTING_MODE_DEPRECATED.getVar(), AuthzConfVars.SENTRY_TESTING_MODE);
   deprecatedConfigs.put(AuthzConfVars.AUTHZ_UDF_WHITELIST_DEPRECATED.getVar(), AuthzConfVars.AUTHZ_UDF_WHITELIST);
   deprecatedConfigs.put(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION_DEPRECATED.getVar(), AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION);
   deprecatedConfigs.put(AuthzConfVars.AUTHZ_ONFAILURE_HOOKS_DEPRECATED.getVar(), AuthzConfVars.AUTHZ_ONFAILURE_HOOKS);
  };

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzConf.class);
  public static final String AUTHZ_SITE_FILE = "sentry-site.xml";

  public HiveAuthzConf(URL hiveAuthzSiteURL) {
    super(false);
    addResource(hiveAuthzSiteURL);
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
      // check if the deprecated value is set here
      if (deprecatedConfigs.containsKey(varName)) {
        retVal = super.get(deprecatedConfigs.get(varName).getVar());
      }
      if (retVal == null) {
        retVal = AuthzConfVars.getDefault(varName);
      } else {
        Log.info("Using the deprecated config setting " + deprecatedConfigs.get(varName).getVar() +
            " instead of " + varName);
      }
    }
    return retVal;
  }
}
