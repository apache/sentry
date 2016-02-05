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

package org.apache.sentry.provider.db.tools;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.tools.command.hive.*;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SentryShellHive is an admin tool, and responsible for the management of repository.
 * The following function are supported:
 * create role, drop role, add group to role, delete group from role, grant privilege to role,
 * revoke privilege from role, list roles for group, list privilege for role.
 */
public class SentryShellHive extends SentryShellCommon {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryShellHive.class);

  public void run() throws Exception {
    Command command = null;
    SentryPolicyServiceClient client = SentryServiceClientFactory.create(getSentryConf());
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    String requestorName = ugi.getShortUserName();

    if (isCreateRole) {
      command = new CreateRoleCmd(roleName);
    } else if (isDropRole) {
      command = new DropRoleCmd(roleName);
    } else if (isAddRoleGroup) {
      command = new GrantRoleToGroupsCmd(roleName, groupName);
    } else if (isDeleteRoleGroup) {
      command = new RevokeRoleFromGroupsCmd(roleName, groupName);
    } else if (isGrantPrivilegeRole) {
      command = new GrantPrivilegeToRoleCmd(roleName, privilegeStr);
    } else if (isRevokePrivilegeRole) {
      command = new RevokePrivilegeFromRoleCmd(roleName, privilegeStr);
    } else if (isListRole) {
      command = new ListRolesCmd(groupName);
    } else if (isListPrivilege) {
      command = new ListPrivilegesCmd(roleName);
    }

    // check the requestor name
    if (StringUtils.isEmpty(requestorName)) {
      // The exception message will be recoreded in log file.
      throw new Exception("The requestor name is empty.");
    }

    if (command != null) {
      command.execute(client, requestorName);
    }
  }

  private Configuration getSentryConf() {
    Configuration conf = new Configuration();
    conf.addResource(new Path(confPath));
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
       System.out.println("The operation failed." +
          (current.getMessage() == null ? "" : "  Message: " + current.getMessage()));
    }
  }

}
