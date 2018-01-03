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
package org.apache.sentry.provider.common;

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.core.common.utils.SentryConstants.KV_JOINER;
import static org.apache.sentry.core.common.utils.SentryConstants.PRIVILEGE_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.exception.SentryConfigurationException;
import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.policy.common.PrivilegeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public abstract class ResourceAuthorizationProvider implements AuthorizationProvider {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ResourceAuthorizationProvider.class);
  private final static ThreadLocal<List<String>> lastFailedPrivileges =
      new ThreadLocal<List<String>>() {
        @Override
        protected List<String> initialValue() {
          return new ArrayList<String>();
        }
      };

  private final GroupMappingService groupService;
  private final PolicyEngine policy;
  private final PrivilegeFactory privilegeFactory;
  private final Model model;

  public ResourceAuthorizationProvider(PolicyEngine policy,
      GroupMappingService groupService, Model model) {
    this.policy = policy;
    this.groupService = groupService;
    this.privilegeFactory = policy.getPrivilegeFactory();
    this.model = model;
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
      Set<? extends Action> actions, ActiveRoleSet roleSet) {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Authorization Request for " + subject + " " +
          authorizableHierarchy + " and " + actions);
    }
    Preconditions.checkNotNull(subject, "Subject cannot be null");
    Preconditions.checkNotNull(authorizableHierarchy, "Authorizable cannot be null");
    Preconditions.checkArgument(!authorizableHierarchy.isEmpty(), "Authorizable cannot be empty");
    Preconditions.checkNotNull(actions, "Actions cannot be null");
    Preconditions.checkArgument(!actions.isEmpty(), "Actions cannot be empty");
    Preconditions.checkNotNull(roleSet, "ActiveRoleSet cannot be null");
    boolean hasAccess = false;
    hasAccess = doHasAccess(subject, authorizableHierarchy, actions, roleSet);
    return hasAccess;
  }

  private boolean doHasAccess(Subject subject,
      List<? extends Authorizable> authorizables, Set<? extends Action> actions,
      ActiveRoleSet roleSet) {
    Set<String> groups;
    try {
      groups = getGroups(subject);
    } catch (SentryGroupNotFoundException e) {
      groups = Collections.emptySet();
      LOGGER.debug("Groups not found for " + subject);
    }
    Set<String> users = Sets.newHashSet(subject.getName());
    Set<String> hierarchy = new HashSet<String>();
    for (Authorizable authorizable : authorizables) {
      hierarchy.add(KV_JOINER.join(authorizable.getTypeName(), authorizable.getName()));
    }
    List<String> requestPrivileges = buildPermissions(authorizables, actions);
    Iterable<Privilege> privileges = getPrivileges(groups, users, roleSet,
        authorizables.toArray(new Authorizable[0]));
    lastFailedPrivileges.get().clear();

    for (String requestPrivilege : requestPrivileges) {
      Privilege priv = privilegeFactory.createPrivilege(requestPrivilege);
      for (Privilege permission : privileges) {
        /*
         * Does the permission granted in the policy file imply the requested action?
         */
        boolean result = permission.implies(priv, model);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("ProviderPrivilege {}, RequestPrivilege {}, RoleSet {}, Result {}",
              new Object[]{ permission, requestPrivilege, roleSet, result});
        }
        if (result) {
          return true;
        }
      }
    }

    lastFailedPrivileges.get().addAll(requestPrivileges);
    return false;
  }

  private Iterable<Privilege> getPrivileges(Set<String> groups, Set<String> users,
      ActiveRoleSet roleSet, Authorizable[] authorizables) {
    ImmutableSet<String> privileges = policy.getPrivileges(groups, users, roleSet, authorizables);
    return Iterables.transform(appendDefaultDBPriv(privileges, authorizables),
        new Function<String, Privilege>() {
      @Override
      public Privilege apply(String privilege) {
        return privilegeFactory.createPrivilege(privilege);
      }
    });
  }

  private ImmutableSet<String> appendDefaultDBPriv(ImmutableSet<String> privileges, Authorizable[] authorizables) {
    // Only for switch db
    if (authorizables != null && authorizables.length == 4 && authorizables[2].getName().equals("+")
      && privileges.size() == 1 && hasOnlyServerPrivilege(privileges.asList().get(0))) {
      // Assuming authorizable[0] will always be the server
      // This Code is only reachable only when user fires a 'use default'
      // and the user has a privilege on atleast 1 privilized Object
      String defaultPriv = "Server=" + authorizables[0].getName()
          + "->Db=default->Table=*->Column=*->action=select";
      Set<String> newPrivs = Sets.newHashSet(defaultPriv);
      return ImmutableSet.copyOf(newPrivs);
    }
    return privileges;
  }

  private boolean hasOnlyServerPrivilege(String priv) {
    ArrayList<String> l = Lists.newArrayList(AUTHORIZABLE_SPLITTER.split(priv));
    if (l.size() == 1 && l.get(0).toLowerCase().startsWith("server")) {
      return l.get(0).toLowerCase().split("=")[1].endsWith("+");
    }
    return false;
  }

  @Override
  public GroupMappingService getGroupMapping() {
    return groupService;
  }

  private Set<String> getGroups(Subject subject) throws SentryGroupNotFoundException {
    return groupService.getGroups(subject.getName());
  }

  @Override
  public void validateResource(boolean strictValidation) throws SentryConfigurationException {
    policy.validatePolicy(strictValidation);
  }

  @Override
  public Set<String> listPrivilegesForSubject(Subject subject)
      throws SentryConfigurationException, SentryGroupNotFoundException {
    return policy.getPrivileges(getGroups(subject), Sets.newHashSet(subject.getName()),
        ActiveRoleSet.ALL, (Authorizable[]) null);
  }

  @Override
  public Set<String> listPrivilegesForGroup(String groupName) throws SentryConfigurationException {
    return policy.getPrivileges(Sets.newHashSet(groupName), ActiveRoleSet.ALL);
  }

  @Override
  public List<String> getLastFailedPrivileges() {
    return lastFailedPrivileges.get();
  }

  @Override
  public void close() {
    if (policy != null) {
      policy.close();
    }
  }

  private List<String> buildPermissions(List<? extends Authorizable> authorizables,
      Set<? extends Action> actions) {
    List<String> hierarchy = new ArrayList<String>();
    List<String> requestedPermissions = new ArrayList<String>();

    for (Authorizable authorizable : authorizables) {
      hierarchy.add(KV_JOINER.join(authorizable.getTypeName(), authorizable.getName()));
    }

    for (Action action : actions) {
      String requestPermission = AUTHORIZABLE_JOINER.join(hierarchy);
      requestPermission = AUTHORIZABLE_JOINER.join(requestPermission,
          KV_JOINER.join(PRIVILEGE_NAME, action.getValue()));
      requestedPermissions.add(requestPermission);
    }
    return requestedPermissions;
  }

  @Override
  public PolicyEngine getPolicyEngine() {
    return policy;
  }
}
