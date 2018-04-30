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

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.tools.GenericPrivilegeConverter;
import org.apache.sentry.cli.tools.command.GenericShellCommand;
import org.apache.sentry.provider.db.generic.tools.TSentryPrivilegeConverter;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.cli.tools.ShellCommand;
import org.apache.sentry.cli.tools.command.hive.HiveShellCommand;

import com.budhash.cliche.Command;
import com.budhash.cliche.Param;
import com.budhash.cliche.Shell;
import com.budhash.cliche.ShellDependent;
import com.budhash.cliche.ShellFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Top level commands
 */
public class TopLevelShell implements ShellDependent, Runnable {

  public enum TYPE { KAFKA, HIVE, SOLR, SQOOP };

  private final Shell topShell;
  private ShellCommand shellCommand;
  private Shell shell; // top level shell object

  private final String authUser;
  private final SentryPolicyServiceClient sentryClient;
  private final SentryGenericServiceClient sentryGenericClient;

  TopLevelShell(SentryPolicyServiceClient sentryClient,
      SentryGenericServiceClient sentryGenericClient,
      String authUser) {
    this.authUser = authUser;
    this.sentryClient = sentryClient;
    this.sentryGenericClient = sentryGenericClient;
    shellCommand = new HiveShellCommand(sentryClient);
    topShell = ShellFactory.createConsoleShell("sentry",
        "sentry shell\n" +
        "Enter ?l to list available commands.",
        this);
  }

  @Command(description="list, create and remove roles")
  public void roles() throws IOException {
    ShellFactory.createSubshell("roles", shell, "roles commands",
        new RolesShell(shellCommand, authUser)).commandLoop();
  }

  @Command(description = "list, create and remove groups")
  public void groups() throws IOException {
    ShellFactory.createSubshell("groups", shell, "groups commands",
        new GroupShell(shellCommand, authUser)).commandLoop();
  }

  @Command(description = "list, create and remove privileges")
  public void privileges() throws IOException {
    ShellFactory.createSubshell("privileges", shell, "privileges commands",
        new PrivsShell(shellCommand, authUser)).commandLoop();
  }

  @Command(description = "List sentry roles. shows all available roles.")
  public List<String> listRoles() {
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
  public List<String> listRoles(
      @Param(name = "groupName")
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

  @Command(abbrev = "lg", header = "[groups]",
    description = "list groups and their roles")
  public List<String> listGroups() {
    try {
      return shellCommand.listGroupRoles(authUser);
    } catch (SentryUserException e) {
      System.out.printf("failed to list the groups and roles: %s\n", e.toString());
      return Collections.emptyList();
    }
  }

  @Command(description = "Grant role to groups")
  public void grantRole(
      @Param(name = "roleName")
      String roleName,
      @Param(name = "group...") String ...groups) {
    try {
      Set<String> groupsSet = new HashSet<>(Arrays.asList(groups));
      shellCommand.grantRoleToGroups(authUser, roleName, groupsSet);
    } catch (SentryUserException e) {
      System.out.printf("Failed to gran role %s to groups: %s\n",
          roleName, e.toString());
    }
  }

  @Command(abbrev = "grm", description = "Revoke role from groups")
  public void revokeRole(
      @Param(name = "roleName")
      String roleName,
      @Param(name = "group...")
      String ...groups) {
    try {
      Set<String> groupsSet = new HashSet<>(Arrays.asList(groups));
      shellCommand.revokeRoleFromGroups(authUser, roleName, groupsSet);
    } catch (SentryUserException e) {
      System.out.printf("Failed to revoke role %s to groups: %s\n",
          roleName, e.toString());
    }
  }

  @Command(description = "Create Sentry role(s).")
  public void createRole(
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

  @Command(abbrev = "dr", description = "drop Sentry role(s).")
  public void dropRole(
      @Param(name = "roleName ...", description = "role names to drop")
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

  @Command(description = "list Sentry privileges")
  public List<String> listPrivileges(
      @Param(name = "roleName")
      String roleName) {
    try {
      return shellCommand.listPrivileges(authUser, roleName);
    } catch (SentryUserException e) {
      System.out.println("Failed to list privileges: " + e.toString());
      return Collections.emptyList();
    }
  }

  @Command(description = "Grant privilege to role")
  public void grantPrivilege(
      @Param(name = "roleName")
      String roleName,
      @Param(name = "privilege", description = "privilege string, e.g. server=s1->db=foo")
      String privilege) {
    try {
      shellCommand.grantPrivilegeToRole(authUser, roleName, privilege);
    } catch (SentryUserException e) {
      System.out.println("Error granting privilege: " + e.toString());
    }
  }

  @Command
  public void revokePrivilege(
      @Param(name = "roleName")
      String roleName,
      @Param(name = "privilege", description = "privilege string, e.g. server=s1->db=foo")
      String privilege) {
    try {
      shellCommand.revokePrivilegeFromRole(authUser, roleName, privilege);
    } catch (SentryUserException e) {
      System.out.println("failed to revoke privilege: " + e.toString());
    }
  }

  @Command(description = "Set the type: hive, kafka, sqoop, solr, etc.")
  public void type(
      @Param(name = "type", description = "the type to set: hive, kafka, sqoop, solr, etc.")
      String type) {
    // Check it's a valid type first
    try {
      TYPE parsedType = parseType(type);
      if (parsedType == TYPE.HIVE) {
        shellCommand = new HiveShellCommand(sentryClient);
      } else {
        String component = getComponent(parsedType);
        String service = getService(parsedType);
        TSentryPrivilegeConverter converter = new GenericPrivilegeConverter(component, service);
        shellCommand = new GenericShellCommand(sentryGenericClient, component, service, converter);
      }
    } catch (IllegalArgumentException ex) {
      System.out.printf("%s is not an accepted type value\n", type);
    }
  }

  @Command(description = "Set the type: hive, kafka, sqoop, solr, etc.")
  public void type(
      @Param(name = "type", description = "the type to set: hive, kafka, sqoop, solr, etc.")
      String type,
      @Param(name = "service", description = "the service name")
      String service) {
    try {
      // Check it's a valid type first
      TYPE parsedType = parseType(type);
      if (parsedType == TYPE.HIVE) {
        shellCommand = new HiveShellCommand(sentryClient);
      } else {
        String component = getComponent(parsedType);
        TSentryPrivilegeConverter converter = new GenericPrivilegeConverter(component, service);
        shellCommand = new GenericShellCommand(sentryGenericClient, component, service, converter);
      }
    } catch (IllegalArgumentException ex) {
      System.out.printf("%s is not an accepted type value\n", type);
    }
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

  private String getComponent(TYPE type) {
    switch (type) {
      case KAFKA:
        return AuthorizationComponent.KAFKA;
      case SOLR:
        return "SOLR";
      case SQOOP:
        return AuthorizationComponent.SQOOP;
      default:
        throw new IllegalArgumentException("Invalid type specified for SentryShellGeneric: " + type);
    }
  }

  private String getService(TYPE type) {
    switch (type) {
      case KAFKA:
        return AuthorizationComponent.KAFKA;
      case SOLR:
        return "service1";
      case SQOOP:
        return "sqoopServer1";
      default:
        throw new IllegalArgumentException("Invalid type specified for SentryShellGeneric: " + type);
    }
  }

  private TYPE parseType(String typeStr) {
    for (TYPE type : TYPE.values()) {
      if (type.name().equalsIgnoreCase(typeStr)) {
        return type;
      }
    }

    throw new IllegalArgumentException("Invalid type specified for SentryShellGeneric: " + typeStr);
  }

}
