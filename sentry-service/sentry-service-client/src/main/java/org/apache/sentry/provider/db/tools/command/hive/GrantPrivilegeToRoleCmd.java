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
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants;

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
    boolean grantOption = tSentryPrivilege.getGrantOption().equals(TSentryGrantOption.TRUE) ? true : false;
    if (ServiceConstants.PrivilegeScope.SERVER.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      client.grantServerPrivilege(requestorName, roleName, tSentryPrivilege.getServerName(),
              tSentryPrivilege.getAction(), grantOption);
    } else if (ServiceConstants.PrivilegeScope.DATABASE.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      client.grantDatabasePrivilege(requestorName, roleName, tSentryPrivilege.getServerName(),
              tSentryPrivilege.getDbName(), tSentryPrivilege.getAction(), grantOption);
    } else if (ServiceConstants.PrivilegeScope.TABLE.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      client.grantTablePrivilege(requestorName, roleName, tSentryPrivilege.getServerName(),
              tSentryPrivilege.getDbName(), tSentryPrivilege.getTableName(),
              tSentryPrivilege.getAction(), grantOption);
    } else if (ServiceConstants.PrivilegeScope.COLUMN.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      client.grantColumnPrivilege(requestorName, roleName, tSentryPrivilege.getServerName(),
              tSentryPrivilege.getDbName(), tSentryPrivilege.getTableName(),
              tSentryPrivilege.getColumnName(), tSentryPrivilege.getAction(), grantOption);
    } else if (ServiceConstants.PrivilegeScope.URI.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      client.grantURIPrivilege(requestorName, roleName, tSentryPrivilege.getServerName(),
              tSentryPrivilege.getURI(), grantOption);
    }
  }
}
