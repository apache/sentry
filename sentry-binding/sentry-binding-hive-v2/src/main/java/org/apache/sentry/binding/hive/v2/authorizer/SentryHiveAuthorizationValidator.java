/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.sentry.binding.hive.v2.authorizer;

import java.util.List;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

/**
 * This class used to do authorization validate. Check if current user has privileges to do the
 * operation and filter the select results.
 */
public abstract class SentryHiveAuthorizationValidator implements HiveAuthorizationValidator {

  /**
   * Check if current user has privileges to perform given operation type hiveOpType on the given
   * input and output objects.
   * 
   * @param hiveOpType
   * @param inputHObjs
   * @param outputHObjs
   * @param context
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void checkPrivileges(HiveOperationType hiveOpType,
      List<HivePrivilegeObject> inputHObjs, List<HivePrivilegeObject> outputHObjs,
      HiveAuthzContext context) throws HiveAuthzPluginException, HiveAccessControlException;


  /**
   * Filter the select results according current user's permission. remove the object which current
   * user do not have any privilege on it.
   * 
   * @param listObjs
   * @param context
   */
  @Override
  public abstract List<HivePrivilegeObject> filterListCmdObjects(
      List<HivePrivilegeObject> listObjs, HiveAuthzContext context);
}
