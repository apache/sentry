/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.access.provider.file;

import java.util.EnumSet;

import org.apache.access.core.AccessConstants;
import org.apache.access.core.Action;
import org.apache.access.core.AuthorizationProvider;
import org.apache.access.core.Database;
import org.apache.access.core.Server;
import org.apache.access.core.ServerResource;
import org.apache.access.core.Subject;
import org.apache.access.core.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermissionResolver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

public class ResourceAuthorizationProvider implements AuthorizationProvider {

  private final GroupMappingService groupService;
  private final Policy policy;
  private final WildcardPermissionResolver permissionResolver;

  @VisibleForTesting
  public ResourceAuthorizationProvider(Policy policy,
      GroupMappingService groupService) {
    this.policy = policy;
    this.groupService = groupService;
    permissionResolver = new WildcardPermissionResolver();
  }

  public ResourceAuthorizationProvider(String resource) {
    this(new SimplePolicy(resource), new HadoopGroupMappingService(
        Groups.getUserToGroupsMappingService(new Configuration())));
  }

  @Override
  public boolean hasAccess(Subject subject, Server server, Database database,
      Table table, EnumSet<Action> actions) {
    Preconditions.checkNotNull(subject, "Subject cannot be null");
    Preconditions.checkNotNull(server, "Server cannot be null");
    Preconditions.checkNotNull(database, "Database cannot be null");
    Preconditions.checkNotNull(table, "Table cannot be null");
    Preconditions.checkNotNull(actions, "Actions cannot be null");
    Preconditions.checkArgument(!actions.isEmpty(), "Actions cannot be empty");
    return doHasAccess(subject, server, database, table, actions);
  }

  @Override
  public boolean hasAccess(Subject subject, Server server,
      ServerResource serverResource, EnumSet<Action> actions) {
    Preconditions.checkNotNull(subject, "Subject cannot be null");
    Preconditions.checkNotNull(server, "Server cannot be null");
    Preconditions.checkNotNull(actions, "Actions cannot be null");
    Preconditions.checkArgument(!actions.isEmpty(), "Actions cannot be empty");
    return doHasAccess(subject, server, serverResource, actions);
  }

  private boolean doHasAccess(Subject subject, Server server,
      Database database, Table table, EnumSet<Action> actions) {
    for (String group : groupService.getGroups(subject.getName())) {
      Iterable<Permission> permissions = getPermissionsForGroup(group);
      for (Action action : actions) {
        String requestedPermission = Joiner.on(":").join(
            returnWildcardOrKV("server", server.getName()),
            returnWildcardOrKV("db", database.getName()),
            returnWildcardOrKV("table", table.getName()), action.getValue());
        for (Permission permission : permissions) {
          if (permission.implies(permissionResolver
              .resolvePermission(requestedPermission))) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private boolean doHasAccess(Subject subject, Server server,
      ServerResource serverResource, EnumSet<Action> actions) {
    for (String group : groupService.getGroups(subject.getName())) {
      Iterable<Permission> permissions = getPermissionsForGroup(group);
      for (Action action : actions) {
        String requestedPermission = Joiner.on(":").join(
            returnWildcardOrKV("server", server.getName()),
            serverResource.name().toLowerCase(), action.getValue());
        for (Permission permission : permissions) {
          if (permission.implies(permissionResolver
              .resolvePermission(requestedPermission))) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private Iterable<Permission> getPermissionsForGroup(String group) {
    return Iterables.transform(policy.getPermissions(group),
        new Function<String, Permission>() {
      @Override
      public Permission apply(String permission) {
        return permissionResolver.resolvePermission(permission);
      }
    });
  }

  private String returnWildcardOrKV(String prefix, String value) {
    value = Strings.nullToEmpty(value).trim();
    if (value.isEmpty() || value.equals(AccessConstants.ALL)) {
      return AccessConstants.ALL;
    }
    return Joiner.on("=").join(prefix, value);
  }
}
