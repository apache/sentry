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

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.cli.tools.ShellCommand;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sentry group manipulation for CLI
 */
public class GroupShell implements ShellDependent {

  private final ShellCommand shellCommand;
  private final String authUser;
  Shell shell;

  public GroupShell(ShellCommand shellCommand, String authUser) {
    this.shellCommand = shellCommand;
    this.authUser = authUser;
  }

  @Command(abbrev = "lr", header = "[groups]",
          description = "list groups and their roles")
  public List<String> listRoles() {
    try {
      return shellCommand.listGroupRoles(authUser);
    } catch (SentryUserException e) {
      System.out.printf("failed to list the groups and roles: %s\n", e.toString());
      return Collections.emptyList();
    }
  }

  @Command(description = "Grant role to groups")
  public void grant(String roleName, String ...groups) {
    try {
      Set<String> groupsSet = new HashSet<>(Arrays.asList(groups));
      shellCommand.grantRoleToGroups(authUser, roleName, groupsSet);
    } catch (SentryUserException e) {
      System.out.printf("Failed to gran role %s to groups: %s\n",
              roleName, e.toString());
    }
  }

  @Command(description = "Revoke role from groups")
  public void revoke(String roleName, String ...groups) {
    try {
      Set<String> groupsSet = new HashSet<>(Arrays.asList(groups));
      shellCommand.revokeRoleFromGroups(authUser, roleName, groupsSet);
    } catch (SentryUserException e) {
      System.out.printf("Failed to revoke role %s to groups: %s\n",
              roleName, e.toString());
    }
  }

  @Override
  public void cliSetShell(Shell theShell) {
    this.shell = theShell;
  }
}
