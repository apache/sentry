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

package org.apache.sentry.cli.tools;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.cli.tools.command.hive.*;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * SentryShellHive is an admin tool, and responsible for the management of repository.
 * The following function are supported:
 * create role, drop role, add group to role, delete group from role, grant privilege to role,
 * revoke privilege from role, list roles for group, list privilege for role.
 */
public class SentryShellHive extends SentryShellCommon {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryShellHive.class);

  public void run() throws Exception {

    try(SentryPolicyServiceClient client =
                SentryServiceClientFactory.create(getSentryConf())) {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      String requestorName = ugi.getShortUserName();
      ShellCommand command = new HiveShellCommand(client);

      // check the requestor name
      if (StringUtils.isEmpty(requestorName)) {
        // The exception message will be recorded in the log file.
        throw new Exception("The requestor name is empty.");
      }

      if (isCreateRole) {
        command.createRole(requestorName, roleName);
      } else if (isDropRole) {
        command.dropRole(requestorName, roleName);
      } else if (isAddRoleGroup) {
        Set<String> groups = Sets.newHashSet(groupName.split(SentryShellCommon.GROUP_SPLIT_CHAR));
        command.grantRoleToGroups(requestorName, roleName, groups);
      } else if (isDeleteRoleGroup) {
        Set<String> groups = Sets.newHashSet(groupName.split(SentryShellCommon.GROUP_SPLIT_CHAR));
        command.revokeRoleFromGroups(requestorName, roleName, groups);
      } else if (isGrantPrivilegeRole) {
        command.grantPrivilegeToRole(requestorName, roleName, privilegeStr);
      } else if (isRevokePrivilegeRole) {
        command.revokePrivilegeFromRole(requestorName, roleName, privilegeStr);
      } else if (isListRole) {
        List<String> roles = command.listRoles(requestorName, groupName);
        for (String role : roles) {
          System.out.println(role);
        }
      } else if (isListPrivilege) {
        List<String> privileges = command.listPrivileges(requestorName, roleName);
        for (String privilege : privileges) {
          System.out.println(privilege);
        }
      } else if (isListGroup) {
        List<String> groups = command.listGroupRoles(requestorName);
        for (String group : groups) {
          System.out.println(group);
        }
      }
    }
  }

  private Configuration getSentryConf() {
    Configuration conf = new Configuration();
    conf.addResource(new Path(confPath), true);
    return conf;
  }

  public static void main(String[] args) throws Exception {
    SentryShellHive sentryShell = new SentryShellHive();
    try {
      sentryShell.executeShell(args);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      Throwable current =  e;
      // find the first printable message;
      while (current != null && current.getMessage() == null) {
        current = current.getCause();
      }

      if (current != null) {
        System.out.println("The operation failed." +
           (current.getMessage() == null ? "" : "  Message: " + current.getMessage()));
      }
    }
  }

}
