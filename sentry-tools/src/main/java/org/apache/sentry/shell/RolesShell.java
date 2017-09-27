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
import com.budhash.cliche.Param;
import com.budhash.cliche.Shell;
import com.budhash.cliche.ShellDependent;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;

import java.util.List;

/**
 * Sentry roles manipulation for CLI.
 */
public class RolesShell implements ShellDependent {
    @Command(description = "List sentry roles. shows all available roles.")
    public List<String> list() {
        return tools.listRoles();
    }

    @Command(description = "List sentry roles by group")
    public List<String> list(
            @Param(name = "groupName", description = "group name for roles")
            String group) {
        return tools.listRoles(group);
    }

    @Command(description = "Create Sentry role(s).")
    public void create(
            @Param(name = "roleName", description = "name of role to create")
            String ...roles) {
        tools.createRoles(roles);
    }

    @Command(description = "remove Sentry role(s).")
    public void remove(
            @Param(name = "roleName ...", description = "role names to remove")
            String ...roles) {
        tools.removeRoles(roles);
    }


    @Override
    public void cliSetShell(Shell theShell) {
        this.shell = theShell;
    }

    private final ShellUtil tools;
    Shell shell;

    public RolesShell(SentryPolicyServiceClient sentryClient, String authUser) {
        this.tools = new ShellUtil(sentryClient, authUser);
    }

}
