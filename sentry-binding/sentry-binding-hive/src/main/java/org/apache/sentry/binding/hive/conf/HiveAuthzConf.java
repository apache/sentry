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
    AUTHZ_PROVIDER("sentry.provider",
      "org.apache.sentry.provider.file.ResourceAuthorizationProvider"),
    AUTHZ_PROVIDER_RESOURCE("sentry.hive.provider.resource", ""),
    AUTHZ_SERVER_NAME("sentry.hive.server", "HS2"),
    AUTHZ_RESTRICT_DEFAULT_DB("sentry.hive.restrict.defaultDB", "false"),
    SENTRY_TESTING_MODE("sentry.hive.testing.mode", "false"),
    AUTHZ_UDF_WHITELIST("sentry.hive.udf.whitelist", HIVE_UDF_WHITE_LIST),
    AUTHZ_ALLOW_HIVE_IMPERSONATION("sentry.hive.allow.hive.impersonation", "false"),
    AUTHZ_ONFAILURE_HOOKS("sentry.hive.failure.hooks", ""),

    AUTHZ_PROVIDER_DEPRECATED("hive.sentry.provider",
      "org.apache.sentry.provider.file.ResourceAuthorizationProvider"),
    AUTHZ_PROVIDER_RESOURCE_DEPRECATED("hive.sentry.provider.resource", ""),
    AUTHZ_SERVER_NAME_DEPRECATED("hive.sentry.server", "HS2"),
    AUTHZ_RESTRICT_DEFAULT_DB_DEPRECATED("hive.sentry.restrict.defaultDB", "false"),
    SENTRY_TESTING_MODE_DEPRECATED("hive.sentry.testing.mode", "false"),
    AUTHZ_UDF_WHITELIST_DEPRECATED("hive.sentry.udf.whitelist", HIVE_UDF_WHITE_LIST),
    AUTHZ_ALLOW_HIVE_IMPERSONATION_DEPRECATED("hive.sentry.allow.hive.impersonation", "false"),
    AUTHZ_ONFAILURE_HOOKS_DEPRECATED("hive.sentry.failure.hooks", "");

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
    "xpath_number,xpath_short,xpath_string,year,base64,cume_dist, decode, dense_rank, first_value," +
    "lag, last_value, lead, noop, noopwithmap, ntile, nvl, percent_rank, rank, to_unix_timestamp," +
    "unbase64,windowingtablefunction";

  // map of current property names - > deprecated property names.
  // The binding layer code should work if the deprecated property names are provided,
  // as long as the new property names aren't also provided.  Since the binding code
  // only calls the new property names, we require a map from current names to deprecated
  // names in order to check if the deprecated name of a property was set.
  private static final Map<String, AuthzConfVars> currentToDeprecatedProps =
      new HashMap<String, AuthzConfVars>();
  static {
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_PROVIDER.getVar(), AuthzConfVars.AUTHZ_PROVIDER_DEPRECATED);
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), AuthzConfVars.AUTHZ_PROVIDER_RESOURCE_DEPRECATED);
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), AuthzConfVars.AUTHZ_SERVER_NAME_DEPRECATED);
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB_DEPRECATED);
    currentToDeprecatedProps.put(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), AuthzConfVars.SENTRY_TESTING_MODE_DEPRECATED);
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_UDF_WHITELIST.getVar(), AuthzConfVars.AUTHZ_UDF_WHITELIST_DEPRECATED);
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION.getVar(), AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION_DEPRECATED);
    currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(), AuthzConfVars.AUTHZ_ONFAILURE_HOOKS_DEPRECATED);
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
      if (currentToDeprecatedProps.containsKey(varName)) {
        retVal = super.get(currentToDeprecatedProps.get(varName).getVar());
      }
      if (retVal == null) {
        retVal = AuthzConfVars.getDefault(varName);
      } else {
        Log.warn("Using the deprecated config setting " + currentToDeprecatedProps.get(varName).getVar() +
            " instead of " + varName);
      }
    }
    return retVal;
  }
}
