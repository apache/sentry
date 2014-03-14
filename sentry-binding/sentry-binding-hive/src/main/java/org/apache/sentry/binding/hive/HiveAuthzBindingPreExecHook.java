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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveExtendedOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveAuthzBindingPreExecHook implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBindingPreExecHook.class);

  /**
   * Raise error if the given query contains transforms
   */
  @Override
  public void run(HookContext hookContext) throws Exception {
    HiveAuthzBinding hiveAuthzBinding =  HiveAuthzBinding.get(hookContext.getConf());
    try {
      QueryPlan qPlan = hookContext.getQueryPlan();
      if ((qPlan == null) || (qPlan.getQueryProperties() == null)) {
        return;
      }
      // validate server level permissions permission for transforms
      if (qPlan.getQueryProperties().usesScript()) {
        if (hiveAuthzBinding == null) {
          LOG.warn("No authorization binding found, skipping the authorization for transform");
          return;
        }
        List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>> ();
        List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>> ();
        List<DBModelAuthorizable> serverHierarchy = new ArrayList<DBModelAuthorizable>();

        serverHierarchy.add(hiveAuthzBinding.getAuthServer());
        outputHierarchy.add(serverHierarchy);
        hiveAuthzBinding.authorize(HiveOperation.QUERY,
          HiveAuthzPrivilegesMap.getHiveExtendedAuthzPrivileges(HiveExtendedOperation.TRANSFORM),
          new Subject(hookContext.getUserName()), inputHierarchy, outputHierarchy);
      }
    } finally {
      if (hiveAuthzBinding != null) {
        hiveAuthzBinding.clear(hookContext.getConf());
      }
    }
  }

}
