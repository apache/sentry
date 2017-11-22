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
package org.apache.sentry.binding.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.apache.sentry.binding.hive.authz.SentryHiveAuthorizerFactory;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.authz.HiveAuthzBindingHookBase;
import com.google.common.base.Joiner;

public class HiveAuthzBindingSessionHook
    implements org.apache.hive.service.cli.session.HiveSessionHook {

  public static final String SEMANTIC_HOOK = HiveAuthzBindingHook.class.getName();
  public static final String SCRATCH_DIR_PERMISSIONS = "700";
  public static final String ACCESS_RESTRICT_LIST = Joiner.on(",").join(
    ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
    ConfVars.PREEXECHOOKS.varname,
    ConfVars.SCRATCHDIR.varname,
    ConfVars.LOCALSCRATCHDIR.varname,
    ConfVars.METASTOREURIS.varname,
    ConfVars.METASTORECONNECTURLKEY.varname,
    ConfVars.HADOOPBIN.varname,
    ConfVars.HIVESESSIONID.varname,
    ConfVars.HIVEAUXJARS.varname,
    ConfVars.SCRATCHDIRPERMISSION.varname,
    ConfVars.HIVE_SECURITY_COMMAND_WHITELIST.varname,
    ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
    ConfVars.HIVE_CAPTURE_TRANSFORM_ENTITY.varname,
    ConfVars.HIVERELOADABLEJARS.varname,
    HiveAuthzConf.HIVE_ACCESS_CONF_URL,
    HiveAuthzConf.HIVE_SENTRY_CONF_URL,
    HiveAuthzConf.HIVE_ACCESS_SUBJECT_NAME,
    HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME,
    HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET
    );
  public static final String WILDCARD_ACL_VALUE = "*";

  /**
   * The session hook for sentry authorization that sets the required session level configuration
   * 1. Setup the sentry hooks -
   *    semantic, exec and filter hooks
   * 2. Set additional config properties required for auth
   *      set HIVE_EXTENDED_ENITITY_CAPTURE = true
   *      set SCRATCHDIRPERMISSION = 700
   * 3. Add sensitive config parameters to the config restrict list so that they can't be overridden by users
   */
  @Override
  public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
    // Add sentry hooks to the session configuration
    HiveConf sessionConf = sessionHookContext.getSessionConf();

    // set semantic hooks to request authorization permissions to sentry
    appendConfVar(sessionConf, ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        SEMANTIC_HOOK);

    // set security command list
    HiveAuthzConf authzConf = HiveAuthzBindingHookBase.loadAuthzConf(sessionConf);
    String commandWhitelist =
        authzConf.get(HiveAuthzConf.HIVE_SENTRY_SECURITY_COMMAND_WHITELIST,
            HiveAuthzConf.HIVE_SENTRY_SECURITY_COMMAND_WHITELIST_DEFAULT);
    sessionConf.setVar(ConfVars.HIVE_SECURITY_COMMAND_WHITELIST, commandWhitelist);

    // set additional configuration properties required for auth
    sessionConf.setVar(ConfVars.SCRATCHDIRPERMISSION, SCRATCH_DIR_PERMISSIONS);

    // Enable compiler to capture transform URI referred in the query
    sessionConf.setBoolVar(ConfVars.HIVE_CAPTURE_TRANSFORM_ENTITY, true);

    // set user name
    sessionConf.set(HiveAuthzConf.HIVE_ACCESS_SUBJECT_NAME, sessionHookContext.getSessionUser());
    sessionConf.set(HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME, sessionHookContext.getSessionUser());

    // Set MR ACLs to session user
    updateJobACL(sessionConf, JobContext.JOB_ACL_VIEW_JOB,
        sessionHookContext.getSessionUser());
    updateJobACL(sessionConf, JobContext.JOB_ACL_MODIFY_JOB,
        sessionHookContext.getSessionUser());

    // setup restrict list
    sessionConf.addToRestrictList(ACCESS_RESTRICT_LIST);

    // enable hive authorization V2
    sessionConf.setBoolean(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, true);
    sessionConf.set(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER.varname,
        SessionStateUserAuthenticator.class.getName());
    sessionConf.set(ConfVars.HIVE_AUTHORIZATION_MANAGER.varname,
        SentryHiveAuthorizerFactory.class.getName());
  }

  // Setup given sentry hooks
  private void appendConfVar(HiveConf sessionConf, String confVar,
      String sentryConfVal) {
    String currentValue = sessionConf.get(confVar, "").trim();
    if (currentValue.isEmpty()) {
      currentValue = sentryConfVal;
    } else {
      currentValue = sentryConfVal + "," + currentValue;
    }
    sessionConf.set(confVar, currentValue);
  }

  // Setup ACL to include the session user
  private void updateJobACL(HiveConf sessionConf, String aclName,
      String sessionUser) {
    String aclString = sessionConf.get(aclName, "");
    // An empty ACL, replace it with the user
    if (aclString.isEmpty()) {
      aclString = sessionUser;
    } else {
      // ACLs can start with a space if only groups are configured
      if (aclString.startsWith(" ")) {
        aclString = sessionUser + aclString;
      } else {
        // Do not replace the wildcard ACL, it would restrict access
        boolean isWildcard = (aclString.contains(WILDCARD_ACL_VALUE) &&
            aclString.trim().equals(WILDCARD_ACL_VALUE));
        if (!isWildcard) {
          aclString = sessionUser + "," + aclString;
        }
      }
    }
    sessionConf.set(aclName, aclString.trim());
  }
}
