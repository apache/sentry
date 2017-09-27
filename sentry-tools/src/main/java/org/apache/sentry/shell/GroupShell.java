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

import com.budhash.cliche.Command;
import com.budhash.cliche.Shell;
import com.budhash.cliche.ShellDependent;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;

import java.util.List;

/**
 * Sentry group manipulation for CLI
 */
public class GroupShell implements ShellDependent {
    @Command
    public List<String> list() {
        return tools.listGroups();
    }

    @Command(abbrev = "lr", header = "[groups]",
            description = "list groups and their roles")
    public List<String> listRoles() {
        return tools.listGroupRoles();
    }

    @Command(description = "Grant role to groups")
    public void grant(String roleName, String ...groups) {
        tools.grantGroupsToRole(roleName, groups);
    }

    @Command(description = "Revoke role from groups")
    public void revoke(String roleName, String ...groups) {
        tools.revokeGroupsFromRole(roleName, groups);
    }

    private final ShellUtil tools;
    Shell shell;


    public GroupShell(SentryPolicyServiceClient sentryClient, String authUser) {
        this.tools = new ShellUtil(sentryClient, authUser);
    }

    @Override
    public void cliSetShell(Shell theShell) {
        this.shell = theShell;
    }
}
