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

package org.apache.sentry.provider.db.generic.tools;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.tools.command.*;
import org.apache.sentry.provider.db.tools.SentryShellCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SentryShellKafka is an admin tool, and responsible for the management of repository.
 * The following commands are supported:
 * create role, drop role, add group to role, grant privilege to role,
 * revoke privilege from role, list roles, list privilege for role.
 */
public class SentryShellKafka extends SentryShellCommon {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryShellKafka.class);
  public static final String KAFKA_SERVICE_NAME = "sentry.service.client.kafka.service.name";

  @Override
  public void run() throws Exception {
    Command command = null;
    String component = AuthorizationComponent.KAFKA;
    Configuration conf = getSentryConf();

    String service = conf.get(KAFKA_SERVICE_NAME, "kafka1");
    SentryGenericServiceClient client = SentryGenericServiceClientFactory.create(conf);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    String requestorName = ugi.getShortUserName();

    if (isCreateRole) {
      command = new CreateRoleCmd(roleName, component);
    } else if (isDropRole) {
      command = new DropRoleCmd(roleName, component);
    } else if (isAddRoleGroup) {
      command = new AddRoleToGroupCmd(roleName, groupName, component);
    } else if (isDeleteRoleGroup) {
      command = new DeleteRoleFromGroupCmd(roleName, groupName, component);
    } else if (isGrantPrivilegeRole) {
      command = new GrantPrivilegeToRoleCmd(roleName, component,
          privilegeStr, new KafkaTSentryPrivilegeConvertor(component, service));
    } else if (isRevokePrivilegeRole) {
      command = new RevokePrivilegeFromRoleCmd(roleName, component,
          privilegeStr, new KafkaTSentryPrivilegeConvertor(component, service));
    } else if (isListRole) {
      command = new ListRolesCmd(groupName, component);
    } else if (isListPrivilege) {
      command = new ListPrivilegesByRoleCmd(roleName, component,
          service, new KafkaTSentryPrivilegeConvertor(component, service));
    }

    // check the requestor name
    if (StringUtils.isEmpty(requestorName)) {
      // The exception message will be recorded in log file.
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
    SentryShellKafka sentryShell = new SentryShellKafka();
    try {
      sentryShell.executeShell(args);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      Throwable current = e;
      // find the first printable message;
      while (current != null && current.getMessage() == null) {
        current = current.getCause();
      }
      String error = "";
      if (current != null && current.getMessage() != null) {
        error = "Message: " + current.getMessage();
      }
      System.out.println("The operation failed. " + error);
      System.exit(1);
    }
  }

}
