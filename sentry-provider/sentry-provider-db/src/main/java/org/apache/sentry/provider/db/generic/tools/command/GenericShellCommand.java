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
package org.apache.sentry.provider.db.generic.tools.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.tools.SentryShellCommon;
import org.apache.sentry.provider.db.tools.ShellCommand;

import com.google.common.collect.Sets;

/**
 * The ShellCommand implementation for the Generic clients
 */
public class GenericShellCommand implements ShellCommand {

  private final SentryGenericServiceClient client;
  private final String component;
  private final TSentryPrivilegeConverter converter;
  private final String serviceName;

  public GenericShellCommand(SentryGenericServiceClient client, String component, String serviceName,
                             TSentryPrivilegeConverter converter) {
    this.client = client;
    this.component = component;
    this.serviceName = serviceName;
    this.converter = converter;
  }

  public void createRole(String requestorName, String roleName) throws SentryUserException {
    client.createRole(requestorName, roleName, component);
  }

  public void dropRole(String requestorName, String roleName) throws SentryUserException {
    client.dropRole(requestorName, roleName, component);
  }

  public void grantPrivilegeToRole(String requestorName, String roleName, String privilege) throws SentryUserException {
    TSentryPrivilege sentryPrivilege = converter.fromString(privilege);
    client.grantPrivilege(requestorName, roleName, component, sentryPrivilege);
  }

  public void grantRoleToGroups(String requestorName, String roleName, String groups) throws SentryUserException {
    Set<String> groupSet = Sets.newHashSet(groups.split(SentryShellCommon.GROUP_SPLIT_CHAR));
    client.grantRoleToGroups(requestorName, roleName, component, groupSet);
  }

  public void revokePrivilegeFromRole(String requestorName, String roleName, String privilege) throws SentryUserException {
    TSentryPrivilege sentryPrivilege = converter.fromString(privilege);
    client.revokePrivilege(requestorName, roleName, component, sentryPrivilege);
  }

  public void revokeRoleFromGroups(String requestorName, String roleName, String groups) throws SentryUserException {
    Set<String> groupSet = Sets.newHashSet(groups.split(SentryShellCommon.GROUP_SPLIT_CHAR));
    client.revokeRoleFromGroups(requestorName, roleName, component, groupSet);
  }

  public List<String> listRoles(String requestorName, String roleName, String group) throws SentryUserException {
    Set<TSentryRole> roles;
    if (StringUtils.isEmpty(group)) {
      roles = client.listAllRoles(requestorName, component);
    } else {
      roles = client.listRolesByGroupName(requestorName, group, component);
    }

    List<String> result = new ArrayList<>();
    if (roles != null) {
      for (TSentryRole role : roles) {
        result.add(role.getRoleName());
      }
    }

    return result;
  }

  public List<String> listPrivileges(String requestorName, String roleName) throws SentryUserException {
    Set<TSentryPrivilege> privileges = client
        .listAllPrivilegesByRoleName(requestorName, roleName, component, serviceName);

    List<String> result = new ArrayList<>();
    if (privileges != null) {
      for (TSentryPrivilege privilege : privileges) {
        String privilegeStr = converter.toString(privilege);
        result.add(privilegeStr);
      }
    }

    return result;
  }
}
