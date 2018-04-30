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

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.cli.tools.ShellCommand;

import java.util.Collections;
import java.util.List;

/**
 * Sentry roles manipulation for CLI.
 */
public class RolesShell implements ShellDependent {
  private final ShellCommand shellCommand;
  private final String authUser;
  Shell shell;

  public RolesShell(ShellCommand shellCommand, String authUser) {
    this.shellCommand = shellCommand;
    this.authUser = authUser;
  }

  @Command(description = "List sentry roles. shows all available roles.")
  public List<String> list() {
    try {
      List<String> result = shellCommand.listRoles(authUser, null);
      Collections.sort(result);
      return result;
    } catch (SentryUserException e) {
      System.out.printf("failed to list roles: %s\n", e.toString());
      return Collections.emptyList();
    }
  }

  @Command(description = "List sentry roles by group")
  public List<String> list(
      @Param(name = "groupName", description = "group name for roles")
      String group) {
    try {
      List<String> result = shellCommand.listRoles(authUser, group);
      Collections.sort(result);
      return result;
    } catch (SentryUserException e) {
      System.out.printf("failed to list roles with group %s: %s\n",
          group, e.toString());
      return Collections.emptyList();
    }
  }

  @Command(description = "Create Sentry role(s).")
  public void create(
      @Param(name = "roleName", description = "name of role to create")
      String ...roles) {
    for (String role : roles) {
      try {
        shellCommand.createRole(authUser, role);
      } catch (SentryUserException e) {
        System.out.printf("failed to create role %s: %s\n",
            role, e.toString());
      }
    }
  }

  @Command(description = "drop Sentry role(s).")
  public void drop(
      @Param(name = "roleName ...", description = "role names to remove")
      String ...roles) {
    for (String role : roles) {
      try {
        shellCommand.dropRole(authUser, role);
      } catch (SentryUserException e) {
        System.out.printf("failed to drop role %s: %s\n",
            role, e.toString());
      }
    }
  }

  @Override
  public void cliSetShell(Shell theShell) {
    this.shell = theShell;
  }

}
