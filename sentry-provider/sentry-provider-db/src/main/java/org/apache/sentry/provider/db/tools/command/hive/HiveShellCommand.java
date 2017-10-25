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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.tools.SentryShellCommon;
import org.apache.sentry.provider.db.tools.ShellCommand;
import org.apache.sentry.service.thrift.SentryServiceUtil;

import com.google.common.collect.Sets;

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

  public void grantRoleToGroups(String requestorName, String roleName, String groups) throws SentryUserException {
    Set<String> groupSet = Sets.newHashSet(groups.split(SentryShellCommon.GROUP_SPLIT_CHAR));
    client.grantRoleToGroups(requestorName, roleName, groupSet);
  }

  public void revokePrivilegeFromRole(String requestorName, String roleName, String privilege) throws SentryUserException {
    TSentryPrivilege tSentryPrivilege = SentryServiceUtil.convertToTSentryPrivilege(privilege);
    CommandUtil.validatePrivilegeHierarchy(tSentryPrivilege);
    client.revokePrivilege(requestorName, roleName, tSentryPrivilege);
  }

  public void revokeRoleFromGroups(String requestorName, String roleName, String groups) throws SentryUserException {
    Set<String> groupSet = Sets.newHashSet(groups.split(SentryShellCommon.GROUP_SPLIT_CHAR));
    client.revokeRoleFromGroups(requestorName, roleName, groupSet);
  }

  public List<String> listRoles(String requestorName, String roleName, String group) throws SentryUserException {
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

}
