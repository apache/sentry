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

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.service.thrift.*;
import org.apache.sentry.service.thrift.ServiceConstants;

import java.util.*;

import static org.apache.sentry.service.thrift.SentryServiceUtil.convertTSentryPrivilegeToStr;
import static org.apache.sentry.service.thrift.SentryServiceUtil.convertToTSentryPrivilege;

/**
 * ShellUtil implements actual commands
 */
class ShellUtil {

    List<String> listRoles() {
        List<String> roles = null;
        try {
            return getRoles();
        } catch (SentryUserException e) {
            System.out.println("Error listing roles: " + e.toString());
        }
        return new LinkedList<>();
    }

    List<String> listRoles(String group) {
        Set<TSentryRole> roles = null;
        try {
            roles = sentryClient.listRolesByGroupName(authUser, group);
        } catch (SentryUserException e) {
            System.out.println("Error listing roles: " + e.toString());
        }
        List<String> result = new ArrayList<>();
        if (roles == null || roles.isEmpty()) {
            return result;
        }

        for(TSentryRole role: roles) {
            result.add(role.getRoleName());
        }

        Collections.sort(result);
        return result;
    }

    void createRoles(String ...roles) {
        for (String role: roles) {
            try {
                sentryClient.createRole(authUser, role);
            } catch (SentryUserException e) {
                System.out.printf("failed to create role %s: %s\n",
                        role, e.toString());
            }
        }
    }

    void removeRoles(String ...roles) {
        for (String role: roles) {
            try {
                sentryClient.dropRole(authUser, role);
            } catch (SentryUserException e) {
                System.out.printf("failed to remove role %s: %s\n",
                        role, e.toString());
            }
        }
    }

    List<String> listGroups() {
        Set<TSentryRole> roles = null;

        try {
            roles = sentryClient.listRoles(authUser);
        } catch (SentryUserException e) {
            System.out.println("Error reading roles: " + e.toString());
        }

        if (roles == null || roles.isEmpty()) {
            return new ArrayList<>();
        }

        // Set of all group names
        Set<String> groupNames = new HashSet<>();

        // Get all group names
        for (TSentryRole role: roles) {
            for (TSentryGroup group: role.getGroups()) {
                groupNames.add(group.getGroupName());
            }
        }

        List<String> result = new ArrayList<>(groupNames);

        Collections.sort(result);
        return result;
    }

    List<String> listGroupRoles() {
        Set<TSentryRole> roles = null;

        try {
            roles = sentryClient.listRoles(authUser);
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
    }

    void grantGroupsToRole(String roleName, String ...groups) {
        try {
            sentryClient.grantRoleToGroups(authUser, roleName, Sets.newHashSet(groups));
        } catch (SentryUserException e) {
            System.out.printf("Failed to gran role %s to groups: %s\n",
                    roleName, e.toString());
        }
    }

    void revokeGroupsFromRole(String roleName, String ...groups) {
        try {
            sentryClient.revokeRoleFromGroups(authUser, roleName, Sets.newHashSet(groups));
        } catch (SentryUserException e) {
            System.out.printf("Failed to revoke role %s to groups: %s\n",
                    roleName, e.toString());
        }
    }

    void grantPrivilegeToRole(String roleName, String privilege) {
        TSentryPrivilege tPriv = convertToTSentryPrivilege(privilege);
        boolean grantOption = tPriv.getGrantOption().equals(TSentryGrantOption.TRUE);
        try {
            if (ServiceConstants.PrivilegeScope.SERVER.toString().equals(tPriv.getPrivilegeScope())) {
                sentryClient.grantServerPrivilege(authUser, roleName, tPriv.getServerName(),
                        tPriv.getAction(), grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.DATABASE.toString().equals(tPriv.getPrivilegeScope())) {
                sentryClient.grantDatabasePrivilege(authUser, roleName, tPriv.getServerName(),
                        tPriv.getDbName(), tPriv.getAction(), grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.TABLE.toString().equals(tPriv.getPrivilegeScope())) {
                sentryClient.grantTablePrivilege(authUser, roleName, tPriv.getServerName(),
                        tPriv.getDbName(), tPriv.getTableName(),
                        tPriv.getAction(), grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.COLUMN.toString().equals(tPriv.getPrivilegeScope())) {
                sentryClient.grantColumnPrivilege(authUser, roleName, tPriv.getServerName(),
                        tPriv.getDbName(), tPriv.getTableName(),
                        tPriv.getColumnName(), tPriv.getAction(), grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.URI.toString().equals(tPriv.getPrivilegeScope())) {
                sentryClient.grantURIPrivilege(authUser, roleName, tPriv.getServerName(),
                        tPriv.getURI(), grantOption);
                return;
            }
        } catch (SentryUserException e) {
            System.out.println("Error granting privilege: " + e.toString());
        }
    }

    List<String> listPrivileges(String roleName) {
        Set<TSentryPrivilege> privileges = null;
        try {
            privileges = sentryClient
                    .listAllPrivilegesByRoleName(authUser, roleName);
        } catch (SentryUserException e) {
            System.out.println("Failed to list privileges: " + e.toString());
        }

        if (privileges == null || privileges.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> result = new LinkedList<>();
        for (TSentryPrivilege privilege : privileges) {
            String privilegeStr =  convertTSentryPrivilegeToStr(privilege);
            if (privilegeStr.isEmpty()) {
                continue;
            }
            result.add(privilegeStr);
        }
        return result;
    }

    /**
     * List all privileges
     * @return string with privilege info for all roles
     */
    String listPrivileges() {
        List<String> roles = null;
        try {
            roles = getRoles();
        } catch (SentryUserException e) {
            System.out.println("failed to get role names: " + e.toString());
        }

        if (roles == null || roles.isEmpty()) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        for (String role: roles) {
            List<String> privs = listPrivileges(role);
            if (privs.isEmpty()) {
                continue;
            }
            result.append(role).append(" = ");
            result.append(StringUtils.join(listPrivileges(role), ",\n\t"));
            result.append('\n');
        }
        return result.toString();
    }

    void revokePrivilegeFromRole(String roleName, String privilegeStr) {
        TSentryPrivilege tSentryPrivilege = convertToTSentryPrivilege(privilegeStr);
        boolean grantOption = tSentryPrivilege.getGrantOption().equals(TSentryGrantOption.TRUE) ? true : false;

        try {
            if (ServiceConstants.PrivilegeScope.SERVER.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
                sentryClient.revokeServerPrivilege(authUser, roleName, tSentryPrivilege.getServerName(),
                        grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.DATABASE.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
                sentryClient.revokeDatabasePrivilege(authUser, roleName, tSentryPrivilege.getServerName(),
                        tSentryPrivilege.getDbName(), tSentryPrivilege.getAction(), grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.TABLE.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
                sentryClient.revokeTablePrivilege(authUser, roleName, tSentryPrivilege.getServerName(),
                        tSentryPrivilege.getDbName(), tSentryPrivilege.getTableName(),
                        tSentryPrivilege.getAction(), grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.COLUMN.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
                sentryClient.revokeColumnPrivilege(authUser, roleName, tSentryPrivilege.getServerName(),
                        tSentryPrivilege.getDbName(), tSentryPrivilege.getTableName(),
                        tSentryPrivilege.getColumnName(), tSentryPrivilege.getAction(), grantOption);
                return;
            }
            if (ServiceConstants.PrivilegeScope.URI.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
                sentryClient.revokeURIPrivilege(authUser, roleName, tSentryPrivilege.getServerName(),
                        tSentryPrivilege.getURI(), grantOption);
                return;
            }
        } catch (SentryUserException e) {
            System.out.println("failed to revoke privilege: " + e.toString());
        }
    }


    private List<String>getRoles() throws SentryUserException {
        // Collect role names
        Set<TSentryRole> roles = null;
        roles = sentryClient.listRoles(authUser);
        List<String> roleNames = new ArrayList<>();
        for(TSentryRole role: roles) {
            roleNames.add(role.getRoleName());
        }

        Collections.sort(roleNames);
        return roleNames;
    }

    ShellUtil(SentryPolicyServiceClient sentryClient, String authUser) {
        this.sentryClient = sentryClient;
        this.authUser = authUser;
    }

    private final SentryPolicyServiceClient sentryClient;
    private final String authUser;

}
