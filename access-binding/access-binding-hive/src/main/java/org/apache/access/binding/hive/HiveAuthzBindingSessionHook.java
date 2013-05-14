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
package org.apache.access.binding.hive;

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHookContext;

public class HiveAuthzBindingSessionHook
    implements org.apache.hive.service.cli.session.HiveSessionHook {

  public static final String SEMANTIC_HOOK =
    "org.apache.access.binding.hive.HiveAuthzBindingHook";
  public static final String PRE_EXEC_HOOK =
    "org.apache.access.binding.hive.HiveAuthzBindingPreExecHook";
  public static final String FILTER_HOOK =
    "org.apache.access.binding.hive.HiveAuthzBindingHook";
  public static final String ACCESS_RESTRICT_LIST =
    ConfVars.SEMANTIC_ANALYZER_HOOK.varname + "," +
    ConfVars.PREEXECHOOKS.varname + "," +
    ConfVars.HIVE_EXEC_FILTER_HOOK.varname + "," +
    ConfVars.HIVE_EXTENDED_ENITITY_CAPTURE.varname + "," +
    ConfVars.SCRATCHDIR.varname + "," +
    ConfVars.LOCALSCRATCHDIR.varname + "," +
    ConfVars.HIVE_SERVER2_AUTHZ_EXTERNAL_EXEC.varname + "," +
    HiveAuthzConf.HIVE_ACCESS_CONF_URL;

  @Override
  public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
    // Add access hooks to the session configuration
    HiveConf sessionConf = sessionHookContext.getSessionConf();
    appendConfVar(sessionConf, ConfVars.SEMANTIC_ANALYZER_HOOK, SEMANTIC_HOOK);
    appendConfVar(sessionConf, ConfVars.PREEXECHOOKS, PRE_EXEC_HOOK);
    appendConfVar(sessionConf, ConfVars.HIVE_EXEC_FILTER_HOOK, FILTER_HOOK);

    // setup config
    sessionConf.setBoolVar(ConfVars.HIVE_EXTENDED_ENITITY_CAPTURE, true);
    sessionConf.setBoolVar(ConfVars.HIVE_SERVER2_AUTHZ_EXTERNAL_EXEC, true);

    // setup restrict list
    appendConfVar(sessionConf, ConfVars.HIVE_CONF_RESTRICTED_LIST, ACCESS_RESTRICT_LIST);
  }

  // Setup given access hooks
  private void appendConfVar(HiveConf sessionConf, ConfVars confVar, String accessConfVal) {
    String currentValue = sessionConf.getVar(confVar);
    if ((currentValue == null) || currentValue.isEmpty()) {
      currentValue = accessConfVal;
    } else {
      currentValue = accessConfVal + "," + currentValue;
    }
    sessionConf.setVar(confVar, currentValue);
  }
}
