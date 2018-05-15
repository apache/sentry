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
package org.apache.sentry.cli.tools.command.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.api.service.thrift.TSentryGroup;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.api.service.thrift.TSentryRole;
import org.apache.sentry.cli.tools.ShellCommand;
import org.apache.sentry.api.common.SentryServiceUtil;

/**
 * The ShellCommand implementation for Hive.
 */
public class HiveShellCommand implements ShellCommand {

  private final SentryPolicyServiceClient client;

  public HiveShellCommand(SentryPolicyServiceClient client) {
    this.client = client;
  }

  public void createRole(String requestorName, String roleName) throws SentryUserException {
    client.createRole(requestorName, roleName);
  }

  public void dropRole(String requestorName, String roleName) throws SentryUserException {
    client.dropRole(requestorName, roleName);
  }

  public void grantPrivilegeToRole(String requestorName, String roleName, String privilege) throws SentryUserException {
    TSentryPrivilege tSentryPrivilege = SentryServiceUtil.convertToTSentryPrivilege(privilege);
    CommandUtil.validatePrivilegeHierarchy(tSentryPrivilege);
    client.grantPrivilege(requestorName, roleName, tSentryPrivilege);
  }

  public void grantRoleToGroups(String requestorName, String roleName, Set<String> groups) throws SentryUserException {
    client.grantRoleToGroups(requestorName, roleName, groups);
  }

  public void revokePrivilegeFromRole(String requestorName, String roleName, String privilege) throws SentryUserException {
    TSentryPrivilege tSentryPrivilege = SentryServiceUtil.convertToTSentryPrivilege(privilege);
    CommandUtil.validatePrivilegeHierarchy(tSentryPrivilege);
    client.revokePrivilege(requestorName, roleName, tSentryPrivilege);
  }

  public void revokeRoleFromGroups(String requestorName, String roleName, Set<String> groups) throws SentryUserException {
    client.revokeRoleFromGroups(requestorName, roleName, groups);
  }

  public List<String> listRoles(String requestorName, String group) throws SentryUserException {
    Set<TSentryRole> roles;
    if (StringUtils.isEmpty(group)) {
      roles = client.listAllRoles(requestorName);
    } else {
      roles = client.listRolesByGroupName(requestorName, group);
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
        .listAllPrivilegesByRoleName(requestorName, roleName);

    List<String> result = new ArrayList<>();
    if (privileges != null) {
      for (TSentryPrivilege privilege : privileges) {
        String privilegeStr = SentryServiceUtil.convertTSentryPrivilegeToStr(privilege);
        result.add(privilegeStr);
      }
    }
    return result;
  }

  public List<String> listGroupRoles(String requestorName) throws SentryUserException {
    Set<TSentryRole> roles = client.listAllRoles(requestorName);
    if (roles == null || roles.isEmpty()) {
      return Collections.emptyList();
    }

    // Set of all group names
    Set<String> groupNames = new HashSet<>();

    // Map group to set of roles
    Map<String, Set<String>> groupInfo = new HashMap<>();

    // Get all group names
    for (TSentryRole role: roles) {
      for (TSentryGroup group : role.getGroups()) {
        String groupName = group.getGroupName();
        groupNames.add(groupName);
        Set<String> groupRoles = groupInfo.get(groupName);
        if (groupRoles != null) {
          // Add a new or existing role
          groupRoles.add(role.getRoleName());
          continue;
        }
        // Never seen this group before
        groupRoles = new HashSet<>();
        groupRoles.add(role.getRoleName());
        groupInfo.put(groupName, groupRoles);
      }
    }

    List<String> groups = new ArrayList<>(groupNames);

    // Produce printable result as
    // group1 = role1, role2, ...
    // group2 = ...
    List<String> result = new LinkedList<>();
    for (String groupName: groups) {
      result.add(groupName + " = " + StringUtils.join(groupInfo.get(groupName), ", "));
    }

    return result;
  }

}
