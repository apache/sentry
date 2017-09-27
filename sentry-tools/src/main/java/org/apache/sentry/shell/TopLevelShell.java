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

package org.apache.sentry.shell;

import com.budhash.cliche.*;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;

import java.io.IOException;
import java.util.List;

/**
 * Top level commands
 */
public class TopLevelShell implements ShellDependent, Runnable {

    private final Shell topShell;
    private final ShellUtil tools;
    private Shell shell; // top level shell object

    private final String authUser;
    private final SentryPolicyServiceClient sentryClient;

    TopLevelShell(SentryPolicyServiceClient sentryClient,
                  String authUser) {
        this.authUser = authUser;
        this.sentryClient = sentryClient;
        this.tools = new ShellUtil(sentryClient, authUser);
        topShell = ShellFactory.createConsoleShell("sentry",
                "sentry shell\n" +
                "Enter ?l to list available commands.",
                this);
    }

    @Command(description="list, create and remove roles")
    public void roles() throws IOException {
        ShellFactory.createSubshell("roles", shell, "roles commands",
                new RolesShell(sentryClient, authUser)).commandLoop();
    }

    @Command(description = "list, create and remove groups")
    public void groups() throws IOException {
        ShellFactory.createSubshell("groups", shell, "groups commands",
                new GroupShell(sentryClient, authUser)).commandLoop();
    }

    @Command(description = "list, create and remove privileges")
    public void privileges() throws IOException {
        ShellFactory.createSubshell("privileges", shell, "privileges commands",
                new PrivsShell(sentryClient, authUser)).commandLoop();
    }

    @Command(description = "List sentry roles. shows all available roles.")
    public List<String> listRoles() {
        return tools.listRoles();
    }

    @Command(description = "List sentry roles by group")
    public List<String> listRoles(
            @Param(name = "groupName")
            String group) {
        return tools.listRoles(group);
    }

    @Command(abbrev = "lg", header = "[groups]",
             description = "list groups and their roles")
    public List<String> listGroups() {
        return tools.listGroupRoles();
    }

    @Command(description = "Grant role to groups")
    public void grantRole(
            @Param(name = "roleName")
            String roleName,
            @Param(name = "group...") String ...groups) {
        tools.grantGroupsToRole(roleName, groups);
    }

    @Command(abbrev = "grm",
            description = "Revoke role from groups")
    public void revokeRole(
            @Param(name = "roleName")
            String roleName,
            @Param(name = "group...")
            String ...groups) {
        tools.revokeGroupsFromRole(roleName, groups);
    }

    @Command(description = "Create Sentry role(s).")
    public void createRole(
            @Param(name = "roleName", description = "name of role to create")
                    String ...roles) {
        tools.createRoles(roles);
    }

    @Command(abbrev = "rm", description = "remove Sentry role(s).")
    public void removeRole(
            @Param(name = "roleName ...", description = "role names to remove")
                    String ...roles) {
        tools.removeRoles(roles);
    }

    @Command(description = "list Sentry privileges")
    public String listPrivileges() {
        return tools.listPrivileges();
    }

    @Command(description = "list Sentry privileges")
    public List<String> listPrivileges(
            @Param(name = "roleName")
            String roleName) {
        return tools.listPrivileges(roleName);
    }

    @Command(description = "Grant privilege to role")
    public void grantPrivilege(
            @Param(name = "roleName")
            String roleName,
            @Param(name = "privilege", description = "privilege string, e.g. server=s1->db=foo")
            String privilege) {
        tools.grantPrivilegeToRole(roleName, privilege);
    }

    @Command
    public void revokePrivilege(
            @Param(name = "roleName")
            String roleName,
            @Param(name = "privilege", description = "privilege string, e.g. server=s1->db=foo")
            String privilege) {
        tools.revokePrivilegeFromRole(roleName, privilege);
    }

    @Override
    public void cliSetShell(Shell theShell) {
        this.shell = theShell;
    }

    @Override
    public void run() {
        try {
            this.topShell.commandLoop();
        } catch (IOException e) {
            System.out.println("error: " + e.toString());
        }
    }
}
