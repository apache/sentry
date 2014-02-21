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
package org.apache.sentry.provider.file;

import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.PRIVILEGE_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.policy.common.PermissionFactory;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.shiro.authz.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public abstract class ResourceAuthorizationProvider implements AuthorizationProvider {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ResourceAuthorizationProvider.class);
  private final GroupMappingService groupService;
  private final PolicyEngine policy;
  private final PermissionFactory permissionFactory;

  public ResourceAuthorizationProvider(PolicyEngine policy,
      GroupMappingService groupService) {
    this.policy = policy;
    this.groupService = groupService;
    this.permissionFactory = policy.getPermissionFactory();
  }

  /***
   * @param subject: UserID to validate privileges
   * @param authorizableHierarchy : List of object according to namespace hierarchy.
   *        eg. Server->Db->Table or Server->Function
   *        The privileges will be validated from the higher to lower scope
   * @param actions : Privileges to validate
   * @return
   *        True if the subject is authorized to perform requested action on the given object
   */
  @Override
  public boolean hasAccess(Subject subject, List<? extends Authorizable> authorizableHierarchy,
      Set<? extends Action> actions) {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Authorization Request for " + subject + " " +
          authorizableHierarchy + " and " + actions);
    }
    Preconditions.checkNotNull(subject, "Subject cannot be null");
    Preconditions.checkNotNull(authorizableHierarchy, "Authorizable cannot be null");
    Preconditions.checkArgument(!authorizableHierarchy.isEmpty(), "Authorizable cannot be empty");
    Preconditions.checkNotNull(actions, "Actions cannot be null");
    Preconditions.checkArgument(!actions.isEmpty(), "Actions cannot be empty");
    return doHasAccess(subject, authorizableHierarchy, actions);
  }

  private boolean doHasAccess(Subject subject,
      List<? extends Authorizable> authorizables, Set<? extends Action> actions) {
    List<String> groups = groupService.getGroups(subject.getName());
    List<String> hierarchy = new ArrayList<String>();
    for (Authorizable authorizable : authorizables) {
      hierarchy.add(KV_JOINER.join(authorizable.getTypeName(), authorizable.getName()));
    }
    Iterable<Permission> permissions = getPermissions(authorizables, groups);
    for (Action action : actions) {
      String requestPermission = AUTHORIZABLE_JOINER.join(hierarchy);
      requestPermission = AUTHORIZABLE_JOINER.join(requestPermission,
          KV_JOINER.join(PRIVILEGE_NAME, action.getValue()));
      for (Permission permission : permissions) {
        /*
         * Does the permission granted in the policy file imply the requested action?
         */
        boolean result = permission.implies(permissionFactory.createPermission(requestPermission));
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("FilePermission {}, RequestPermission {}, result {}",
              new Object[]{ permission, requestPermission, result});
        }
        if (result) {
          return true;
        }
      }
    }
    return false;
  }

  private Iterable<Permission> getPermissions(List<? extends Authorizable> authorizables, List<String> groups) {
    return Iterables.transform(policy.getPermissions(authorizables, groups).values(),
        new Function<String, Permission>() {
      @Override
      public Permission apply(String permission) {
        return permissionFactory.createPermission(permission);
      }
    });
  }

  @Override
  public GroupMappingService getGroupMapping() {
    return groupService;
  }
}
