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
package org.apache.sentry.provider.db.tools.command.hive;

import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;

/**
 * The class for admin command to grant privilege to role.
 */
public class GrantPrivilegeToRoleCmd implements Command {

  private String roleName;
  private String privilegeStr;

  public GrantPrivilegeToRoleCmd(String roleName, String privilegeStr) {
    this.roleName = roleName;
    this.privilegeStr = privilegeStr;
  }

  @Override
  public void execute(SentryPolicyServiceClient client, String requestorName) throws Exception {
    TSentryPrivilege tSentryPrivilege = CommandUtil.convertToTSentryPrivilege(privilegeStr);
    client.grantPrivilege(requestorName, roleName, tSentryPrivilege);
  }
}
