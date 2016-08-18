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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
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
    public static final String HIVE_SENTRY_AUTH_ERRORS = "sentry.hive.authorization.errors";
    public static final String HIVE_SENTRY_MOCK_COMPILATION = "sentry.hive.mock.compilation";
    public static final String HIVE_SENTRY_MOCK_ERROR = "sentry.hive.mock.error";
    public static final String HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE = "No valid privileges";
    /**
     * Property used to persist the role set in the session. This is not public for now.
     */
    public static final String SENTRY_ACTIVE_ROLE_SET = "hive.sentry.active.role.set";

    public static final String HIVE_SENTRY_SECURITY_COMMAND_WHITELIST =
            "hive.sentry.security.command.whitelist";
    public static final String HIVE_SENTRY_SECURITY_COMMAND_WHITELIST_DEFAULT =
            "set,reset,reload";

    public static final String HIVE_SENTRY_SERDE_WHITELIST = "hive.sentry.serde.whitelist";
    public static final String HIVE_SENTRY_SERDE_WHITELIST_DEFAULT = "org.apache.hadoop.hive.serde2";

    // Disable the serde Uri privileges by default for backward compatibilities.
    public static final String HIVE_SENTRY_SERDE_URI_PRIVILIEGES_ENABLED = "hive.sentry.turn.on.serde.uri.privileges";
    public static final boolean HIVE_SENTRY_SERDE_URI_PRIVILIEGES_ENABLED_DEFAULT = false;

    public static final String HIVE_UDF_WHITE_LIST =
            "concat,substr,substring,space,repeat,ascii,lpad,rpad,size,round,floor,sqrt,ceil," +
                    "ceiling,rand,abs,pmod,ln,log2,sin,asin,cos,acos,log10,log,exp,power,pow,sign,pi," +
                    "degrees,radians,atan,tan,e,conv,bin,hex,unhex,base64,unbase64,encode,decode,upper," +
                    "lower,ucase,lcase,trim,ltrim,rtrim,length,reverse,field,find_in_set,initcap,like," +
                    "rlike,regexp,regexp_replace,regexp_extract,parse_url,nvl,split,str_to_map,translate" +
                    ",positive,negative,day,dayofmonth,month,year,hour,minute,second,from_unixtime," +
                    "to_date,weekofyear,last_day,date_add,date_sub,datediff,add_months,get_json_object," +
                    "xpath_string,xpath_boolean,xpath_number,xpath_double,xpath_float,xpath_long," +
                    "xpath_int,xpath_short,xpath,+,-,*,/,%,div,&,|,^,~,current_database,isnull," +
                    "isnotnull,if,in,and,or,=,==,<=>,!=,<>,<,<=,>,>=,not,!,between,ewah_bitmap_and," +
                    "ewah_bitmap_or,ewah_bitmap_empty,boolean,tinyint,smallint,int,bigint,float,double," +
                    "string,date,timestamp,binary,decimal,varchar,char,max,min,sum,count,avg,std,stddev," +
                    "stddev_pop,stddev_samp,variance,var_pop,var_samp,covar_pop,covar_samp,corr," +
                    "histogram_numeric,percentile_approx,collect_set,collect_list,ngrams," +
                    "context_ngrams,ewah_bitmap,compute_stats,percentile," +
                    "array,assert_true,map,struct,named_struct,create_union,case,when,hash,coalesce," +
                    "index,in_file,instr,locate,elt,concat_ws,sort_array," +
                    "array_contains,sentences,map_keys,map_values,format_number,printf,greatest,least," +
                    "from_utc_timestamp,to_utc_timestamp,unix_timestamp,to_unix_timestamp,explode," +
                    "inline,json_tuple,parse_url_tuple,posexplode,stack,lead,lag,row_number,rank," +
                    "dense_rank,percent_rank,cume_dist,ntile,first_value,last_value,noop,noopwithmap," +
                    "noopstreaming,noopwithmapstreaming,windowingtablefunction,matchpath";

    public static final String HIVE_UDF_BLACK_LIST = "reflect,reflect2,java_method";

    /**
     * Config setting definitions
     */
    public static enum AuthzConfVars {
        AUTHZ_PROVIDER("sentry.provider",
                "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider"),
        AUTHZ_PROVIDER_RESOURCE("sentry.hive.provider.resource", ""),
        AUTHZ_PROVIDER_BACKEND("sentry.hive.provider.backend", "org.apache.sentry.provider.file.SimpleFileProviderBackend"),
        AUTHZ_POLICY_ENGINE("sentry.hive.policy.engine", "org.apache.sentry.policy.db.SimpleDBPolicyEngine"),
        AUTHZ_POLICY_FILE_FORMATTER(
                "sentry.hive.policy.file.formatter",
                "org.apache.sentry.binding.hive.SentryIniPolicyFileFormatter"),
        AUTHZ_SERVER_NAME("sentry.hive.server", ""),
        AUTHZ_RESTRICT_DEFAULT_DB("sentry.hive.restrict.defaultDB", "false"),
        SENTRY_TESTING_MODE("sentry.hive.testing.mode", "false"),
        AUTHZ_ALLOW_HIVE_IMPERSONATION("sentry.hive.allow.hive.impersonation", "false"),
        AUTHZ_ONFAILURE_HOOKS("sentry.hive.failure.hooks", ""),
        AUTHZ_METASTORE_SERVICE_USERS("sentry.metastore.service.users", null),
        AUTHZ_SYNC_ALTER_WITH_POLICY_STORE("sentry.hive.sync.alter", "true"),
        AUTHZ_SYNC_CREATE_WITH_POLICY_STORE("sentry.hive.sync.create", "false"),
        AUTHZ_SYNC_DROP_WITH_POLICY_STORE("sentry.hive.sync.drop", "true"),

        AUTHZ_PROVIDER_DEPRECATED("hive.sentry.provider",
                "org.apache.sentry.provider.file.ResourceAuthorizationProvider"),
        AUTHZ_PROVIDER_RESOURCE_DEPRECATED("hive.sentry.provider.resource", ""),
        AUTHZ_SERVER_NAME_DEPRECATED("hive.sentry.server", ""),
        AUTHZ_RESTRICT_DEFAULT_DB_DEPRECATED("hive.sentry.restrict.defaultDB", "false"),
        SENTRY_TESTING_MODE_DEPRECATED("hive.sentry.testing.mode", "false"),
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
        currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION.getVar(), AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION_DEPRECATED);
        currentToDeprecatedProps.put(AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(), AuthzConfVars.AUTHZ_ONFAILURE_HOOKS_DEPRECATED);
    };

    private static final Logger LOG = LoggerFactory
            .getLogger(HiveAuthzConf.class);
    public static final String AUTHZ_SITE_FILE = "sentry-site.xml";
    private final String hiveAuthzSiteFile;

    public HiveAuthzConf(URL hiveAuthzSiteURL) {
        super();
        LOG.info("DefaultFS: " + super.get("fs.defaultFS"));
        addResource(hiveAuthzSiteURL);
        applySystemProperties();
        LOG.info("DefaultFS: " + super.get("fs.defaultFS"));
        this.hiveAuthzSiteFile = hiveAuthzSiteURL.toString();
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
        return get(varName, null);
    }

    @Override
    public String get(String varName, String defaultVal) {
        String retVal = super.get(varName);
        if (retVal == null) {
            // check if the deprecated value is set here
            if (currentToDeprecatedProps.containsKey(varName)) {
                retVal = super.get(currentToDeprecatedProps.get(varName).getVar());
            }
            if (retVal == null) {
                retVal = AuthzConfVars.getDefault(varName);
            } else {
                LOG.warn("Using the deprecated config setting " + currentToDeprecatedProps.get(varName).getVar() +
                        " instead of " + varName);
            }
        }
        if (retVal == null) {
            retVal = defaultVal;
        }
        return retVal;
    }

    public String getHiveAuthzSiteFile() {
        return hiveAuthzSiteFile;
    }

    /**
     * Extract the authz config file path from given hive conf and load the authz config
     * @param hiveConf
     * @return
     * @throws IllegalArgumentException
     */
    public static HiveAuthzConf getAuthzConf(HiveConf hiveConf)
            throws IllegalArgumentException {
        boolean depreicatedConfigFile = false;

        String hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
        if (hiveAuthzConf == null
                || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
            hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_ACCESS_CONF_URL);
            depreicatedConfigFile = true;
        }

        if (hiveAuthzConf == null
                || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
            throw new IllegalArgumentException("Configuration key "
                    + HiveAuthzConf.HIVE_SENTRY_CONF_URL + " value '" + hiveAuthzConf
                    + "' is invalid.");
        }

        try {
            return new HiveAuthzConf(new URL(hiveAuthzConf));
        } catch (MalformedURLException e) {
            if (depreicatedConfigFile) {
                throw new IllegalArgumentException("Configuration key "
                        + HiveAuthzConf.HIVE_ACCESS_CONF_URL
                        + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
            } else {
                throw new IllegalArgumentException("Configuration key "
                        + HiveAuthzConf.HIVE_SENTRY_CONF_URL
                        + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
            }
        }
    }
}
