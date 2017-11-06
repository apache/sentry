/*
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

package org.apache.sentry.shell;

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.service.thrift.*;
import org.apache.sentry.provider.db.tools.SentryShellCommon;
import org.apache.sentry.provider.db.tools.ShellCommand;
import org.apache.sentry.provider.db.tools.command.hive.HiveShellCommand;

import java.util.*;

/**
 * ShellUtil implements actual commands
 */
class ShellUtil {

    private final ShellCommand command;
    private final String authUser;

    ShellUtil(SentryPolicyServiceClient sentryClient, String authUser) {
        this.authUser = authUser;
        command = new HiveShellCommand(sentryClient);
    }

    List<String> listRoles() {
        return listRoles(null);
    }

    List<String> listRoles(String group) {
        try {
            // TODO remove "null" here
            List<String> result = command.listRoles(authUser, null, group);
            Collections.sort(result);
            return result;
        } catch (SentryUserException e) {
            System.out.printf("failed to list roles with group %s: %s\n",
                              group, e.toString());
            return Collections.emptyList();
        }
    }

    void createRoles(String ...roles) {
        for (String role : roles) {
            try {
                command.createRole(authUser, role);
            } catch (SentryUserException e) {
                System.out.printf("failed to create role %s: %s\n",
                        role, e.toString());
            }
        }
    }

    void dropRoles(String ...roles) {
        for (String role : roles) {
            try {
                command.dropRole(authUser, role);
            } catch (SentryUserException e) {
                System.out.printf("failed to drop role %s: %s\n",
                        role, e.toString());
            }
        }
    }

    List<String> listGroupRoles() {
        // TODO
        return Collections.emptyList();
        /*
        Set<TSentryRole> roles = null;

        try {
            roles = sentryClient.listAllRoles(authUser);
        } catch (SentryUserException e) {
            System.out.println("Error reading roles: " + e.toString());
        }

        if (roles == null || roles.isEmpty()) {
            return new ArrayList<>();
        }

        // Set of all group names
        Set<String> groupNames = new HashSet<>();

        // Map group to set of roles
        Map<String, Set<String>> groupInfo = new HashMap<>();

        // Get all group names
        for (TSentryRole role: roles) {
            for (TSentryGroup group: role.getGroups()) {
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
        Collections.sort(groups);

        // Produce printable result as
        // group1 = role1, role2, ...
        // group2 = ...
        List<String> result = new LinkedList<>();
        for(String groupName: groups) {
            result.add(groupName + " = " +
                    StringUtils.join(groupInfo.get(groupName), ", "));
        }
        return result;
        */
    }

    void grantGroupsToRole(String roleName, String ...groups) {
        try {
            // TODO change grantRoleToGroups
            String joinedGroups = String.join(SentryShellCommon.GROUP_SPLIT_CHAR, groups);
            command.grantRoleToGroups(authUser, roleName, joinedGroups);
        } catch (SentryUserException e) {
            System.out.printf("Failed to gran role %s to groups: %s\n",
                    roleName, e.toString());
        }
    }

    void revokeGroupsFromRole(String roleName, String ...groups) {
        try {
            // TODO change revokeRoleFromGroups
            String joinedGroups = String.join(SentryShellCommon.GROUP_SPLIT_CHAR, groups);
            command.revokeRoleFromGroups(authUser, roleName, joinedGroups);
        } catch (SentryUserException e) {
            System.out.printf("Failed to revoke role %s to groups: %s\n",
                    roleName, e.toString());
        }
    }

    void grantPrivilegeToRole(String roleName, String privilege) {
        try {
            command.grantPrivilegeToRole(authUser, roleName, privilege);
        } catch (SentryUserException e) {
            System.out.println("Error granting privilege: " + e.toString());
        }
    }

    List<String> listPrivileges(String roleName) {
        try {
            return command.listPrivileges(authUser, roleName);
        } catch (SentryUserException e) {
            System.out.println("Failed to list privileges: " + e.toString());
            return Collections.emptyList();
        }
    }

    void revokePrivilegeFromRole(String roleName, String privilegeStr) {
        try {
            command.revokePrivilegeFromRole(authUser, roleName, privilegeStr);
        } catch (SentryUserException e) {
            System.out.println("failed to revoke privilege: " + e.toString());
        }
    }


}
