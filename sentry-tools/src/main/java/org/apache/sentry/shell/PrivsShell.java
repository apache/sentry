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

public class PrivsShell implements ShellDependent {
  private final ShellCommand shellCommand;
  private final String authUser;
  Shell shell;

  public PrivsShell(ShellCommand shellCommand, String authUser) {
    this.shellCommand = shellCommand;
    this.authUser = authUser;
  }

  @Command(description = "Grant privilege to role")
  public void grant(
      @Param(name = "roleName")
      String roleName,
      @Param(name = "privilege",
             description = "privilege string, e.g. server=s1->db=foo")
      String privilege) {
    try {
      shellCommand.grantPrivilegeToRole(authUser, roleName, privilege);
    } catch (SentryUserException e) {
      System.out.println("Error granting privilege: " + e.toString());
    }
  }

  @Command
  public List<String> list(
      @Param(name = "roleName")
      String roleName) {
    try {
      return shellCommand.listPrivileges(authUser, roleName);
    } catch (SentryUserException e) {
      System.out.println("Failed to list privileges: " + e.toString());
      return Collections.emptyList();
    }
  }

  @Command
  public void revoke(
      @Param(name = "roleName")
      String roleName,
      @Param(name = "privilege",
             description = "privilege string, e.g. server=s1->db=foo")
      String privilege) {
    try {
      shellCommand.revokePrivilegeFromRole(authUser, roleName, privilege);
    } catch (SentryUserException e) {
      System.out.println("failed to revoke privilege: " + e.toString());
    }
  }

  @Override
  public void cliSetShell(Shell theShell) {
    this.shell = theShell;
  }
}
