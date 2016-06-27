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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;

import java.util.List;
import java.util.Set;

/**
 * The class for admin command to list privileges.
 */
public class ListPrivilegesCmd implements Command {

  private String roleName;

  public ListPrivilegesCmd(String roleName) {
    this.roleName = roleName;
  }

  @Override
  public void execute(SentryPolicyServiceClient client, String requestorName) throws Exception {
    Set<TSentryPrivilege> privileges = client
            .listAllPrivilegesByRoleName(requestorName, roleName);
    if (privileges != null) {
      for (TSentryPrivilege privilege : privileges) {
        String privilegeStr = convertToPrivilegeStr(privilege);
        System.out.println(privilegeStr);
      }
    }
  }

  // convert TSentryPrivilege to privilege in string
  private String convertToPrivilegeStr(TSentryPrivilege tSentryPrivilege) {
    List<String> privileges = Lists.newArrayList();
    if (tSentryPrivilege != null) {
      String serverName = tSentryPrivilege.getServerName();
      String dbName = tSentryPrivilege.getDbName();
      String tableName = tSentryPrivilege.getTableName();
      String columnName = tSentryPrivilege.getColumnName();
      String uri = tSentryPrivilege.getURI();
      String action = tSentryPrivilege.getAction();
      String grantOption = (tSentryPrivilege.getGrantOption() == TSentryGrantOption.TRUE ? "true"
              : "false");
      if (!StringUtils.isEmpty(serverName)) {
        privileges.add(SentryConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_SERVER_NAME,
                serverName));
        if (!StringUtils.isEmpty(uri)) {
          privileges.add(SentryConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_URI_NAME,
                  uri));
        } else if (!StringUtils.isEmpty(dbName)) {
          privileges.add(SentryConstants.KV_JOINER.join(
                  PolicyFileConstants.PRIVILEGE_DATABASE_NAME, dbName));
          if (!StringUtils.isEmpty(tableName)) {
            privileges.add(SentryConstants.KV_JOINER.join(
                    PolicyFileConstants.PRIVILEGE_TABLE_NAME, tableName));
            if (!StringUtils.isEmpty(columnName)) {
              privileges.add(SentryConstants.KV_JOINER.join(
                      PolicyFileConstants.PRIVILEGE_COLUMN_NAME, columnName));
            }
          }
        }
        if (!StringUtils.isEmpty(action)) {
          privileges.add(SentryConstants.KV_JOINER.join(
                  PolicyFileConstants.PRIVILEGE_ACTION_NAME, action));
        }
      }
      // only append the grant option to privilege string if it's true
      if ("true".equals(grantOption)) {
        privileges.add(SentryConstants.KV_JOINER.join(
                PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME, grantOption));
      }
    }
    return SentryConstants.AUTHORIZABLE_JOINER.join(privileges);
  }
}
