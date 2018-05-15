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

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.cli.tools.command.GenericShellCommand;
import org.apache.sentry.provider.db.generic.tools.GenericPrivilegeConverter;
import org.apache.sentry.provider.db.generic.tools.TSentryPrivilegeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * SentryShellGeneric is an admin tool, and responsible for the management of repository.
 * The following commands are supported:
 * create role, drop role, add group to role, grant privilege to role,
 * revoke privilege from role, list roles, list privilege for role.
 */
public class SentryShellGeneric extends SentryShellCommon {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryShellGeneric.class);
  private static final String KAFKA_SERVICE_NAME = "sentry.service.client.kafka.service.name";
  private static final String SOLR_SERVICE_NAME = "sentry.service.client.solr.service.name";
  private static final String SQOOP_SERVICE_NAME = "sentry.service.client.sqoop.service.name";

  @Override
  public void run() throws Exception {
    String component = getComponent();
    Configuration conf = getSentryConf();

    String service = getService(conf);
    try (SentryGenericServiceClient client =
                SentryGenericServiceClientFactory.create(conf)) {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      String requestorName = ugi.getShortUserName();
      TSentryPrivilegeConverter converter = getPrivilegeConverter(component, service);
      ShellCommand command = new GenericShellCommand(client, component, service, converter);

      // check the requestor name
      if (StringUtils.isEmpty(requestorName)) {
        // The exception message will be recorded in log file.
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

  protected GenericPrivilegeConverter getPrivilegeConverter(String component, String service) {
    return new GenericPrivilegeConverter(component, service);
  }

  protected String getComponent() throws Exception {
    if (type == TYPE.kafka) {
      return AuthorizationComponent.KAFKA;
    } else if (type == TYPE.solr) {
      return "SOLR";
    } else if (type == TYPE.sqoop) {
      return AuthorizationComponent.SQOOP;
    }

    throw new Exception("Invalid type specified for SentryShellGeneric: " + type);
  }

  protected String getService(Configuration conf) throws Exception {
    if (type == TYPE.kafka) {
      return conf.get(KAFKA_SERVICE_NAME, AuthorizationComponent.KAFKA);
    } else if (type == TYPE.solr) {
      return conf.get(SOLR_SERVICE_NAME, "service1");
    } else if (type == TYPE.sqoop) {
      return conf.get(SQOOP_SERVICE_NAME, "sqoopServer1");
    }

    throw new Exception("Invalid type specified for SentryShellGeneric: " + type);
  }

  private Configuration getSentryConf() {
    Configuration conf = new Configuration();
    conf.addResource(new Path(confPath), true);
    return conf;
  }

  public static void main(String[] args) throws Exception {
    SentryShellGeneric sentryShell = new SentryShellGeneric();
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
