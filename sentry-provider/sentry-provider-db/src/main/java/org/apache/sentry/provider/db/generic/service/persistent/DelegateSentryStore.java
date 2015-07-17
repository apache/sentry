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
package org.apache.sentry.provider.db.generic.service.persistent;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.persistent.CommitContext;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.SentryConfigurationException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * The DelegateSentryStore will supports the generic authorizable model. It stores the authorizables
 * into separated column. Take the authorizables:[DATABASE=db1,TABLE=tb1,COLUMN=cl1] for example,
 * The DATABASE,db1,TABLE,tb1,COLUMN and cl1 will be stored into the six columns(resourceName0=db1,resourceType0=DATABASE,
 * resourceName1=tb1,resourceType1=TABLE,
 * resourceName2=cl1,resourceType2=COLUMN ) of generic privilege table
 */
public class DelegateSentryStore implements SentryStoreLayer {
  private SentryStore delegate;
  private Configuration conf;
  private Set<String> adminGroups;
  private PrivilegeOperatePersistence privilegeOperator;

  public DelegateSentryStore(Configuration conf) throws SentryNoSuchObjectException,
      SentryAccessDeniedException, SentryConfigurationException, IOException {
    this.privilegeOperator = new PrivilegeOperatePersistence();
    // The generic model doesn't turn on the thread that cleans hive privileges
    conf.set(ServerConfig.SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL,"false");
    this.conf = conf;
    //delegated old sentryStore
    this.delegate = new SentryStore(conf);
    adminGroups = ImmutableSet.copyOf(toTrimedLower(Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
  }

  private PersistenceManager openTransaction() {
    return delegate.openTransaction();
  }

  private CommitContext commitUpdateTransaction(PersistenceManager pm) {
    return delegate.commitUpdateTransaction(pm);
  }

  private void rollbackTransaction(PersistenceManager pm) {
    delegate.rollbackTransaction(pm);
  }

  private void commitTransaction(PersistenceManager pm) {
    delegate.commitTransaction(pm);
  }

  private MSentryRole getRole(String roleName, PersistenceManager pm) {
    return delegate.getMSentryRole(pm, roleName);
  }

  @Override
  public CommitContext createRole(String component, String role,
      String requestor) throws SentryAlreadyExistsException {
    return delegate.createSentryRole(role);
  }

  /**
   * The role is global in the generic model, such as the role may be has more than one component
   * privileges, so delete role will remove all privileges related to it.
   */
  @Override
  public CommitContext dropRole(String component, String role, String requestor)
      throws SentryNoSuchObjectException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    role = toTrimedLower(role);
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryRole.class);
      query.setFilter("this.roleName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      MSentryRole sentryRole = (MSentryRole) query.execute(role);
      if (sentryRole == null) {
        throw new SentryNoSuchObjectException("Role " + role);
      } else {
        pm.retrieve(sentryRole);
        sentryRole.removeGMPrivileges();
        sentryRole.removePrivileges();
        pm.deletePersistent(sentryRole);
      }
      CommitContext commit = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commit;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext alterRoleAddGroups(String component, String role,
      Set<String> groups, String requestor) throws SentryNoSuchObjectException {
    return delegate.alterSentryRoleAddGroups(requestor, role, toTSentryGroups(groups));
  }

  @Override
  public CommitContext alterRoleDeleteGroups(String component, String role,
      Set<String> groups, String requestor) throws SentryNoSuchObjectException {
  //called to old sentryStore
    return delegate.alterSentryRoleDeleteGroups(role, toTSentryGroups(groups));
  }

  @Override
  public CommitContext alterRoleGrantPrivilege(String component, String role,
      PrivilegeObject privilege, String grantorPrincipal)
      throws SentryUserException {
    role = toTrimedLower(role);
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try{
      pm = openTransaction();
      MSentryRole mRole = getRole(role, pm);
      if (mRole == null) {
        throw new SentryNoSuchObjectException("role:" + role + " isn't exist");
      }
      /**
       * check with grant option
       */
      grantOptionCheck(privilege, grantorPrincipal, pm);

      privilegeOperator.grantPrivilege(privilege, mRole, pm);

      CommitContext commitContext = delegate.commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;

    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext alterRoleRevokePrivilege(String component,
      String role, PrivilegeObject privilege, String grantorPrincipal)
      throws SentryUserException {
    role = toTrimedLower(role);
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try{
      pm = openTransaction();
      MSentryRole mRole = getRole(role, pm);
      if (mRole == null) {
        throw new SentryNoSuchObjectException("role:" + role + " isn't exist");
      }
      /**
       * check with grant option
       */
      grantOptionCheck(privilege, grantorPrincipal, pm);

      privilegeOperator.revokePrivilege(privilege, mRole, pm);

      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;

    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext renamePrivilege(String component, String service,
      List<? extends Authorizable> oldAuthorizables,
      List<? extends Authorizable> newAuthorizables, String requestor)
      throws SentryUserException {
    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);
    Preconditions.checkNotNull(oldAuthorizables);
    Preconditions.checkNotNull(newAuthorizables);

    if (oldAuthorizables.size() != newAuthorizables.size()) {
      throw new SentryAccessDeniedException(
          "rename privilege denied: the size of oldAuthorizables must equals the newAuthorizables "
              + "oldAuthorizables:" + Arrays.toString(oldAuthorizables.toArray()) + " "
              + "newAuthorizables:" + Arrays.toString(newAuthorizables.toArray()));
    }

    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try {
      pm = openTransaction();

      privilegeOperator.renamePrivilege(toTrimedLower(component), toTrimedLower(service),
          oldAuthorizables, newAuthorizables, requestor, pm);

      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext dropPrivilege(String component,
      PrivilegeObject privilege, String requestor) throws SentryUserException {
    Preconditions.checkNotNull(requestor);

    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try {
      pm = openTransaction();

      privilegeOperator.dropPrivilege(privilege, pm);

      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  /**
   * Grant option check
   * @param component
   * @param pm
   * @param privilegeReader
   * @throws SentryUserException
   */
  private void grantOptionCheck(PrivilegeObject requestPrivilege, String grantorPrincipal,PersistenceManager pm)
      throws SentryUserException {

    if (Strings.isNullOrEmpty(grantorPrincipal)) {
      throw new SentryInvalidInputException("grantorPrincipal should not be null or empty");
    }

    Set<String> groups = getRequestorGroups(grantorPrincipal);
    if (groups == null || groups.isEmpty()) {
      throw new SentryGrantDeniedException(grantorPrincipal
          + " has no grant!");
    }
    //admin group check
    if (!Sets.intersection(adminGroups, toTrimedLower(groups)).isEmpty()) {
      return;
    }
    //privilege grant option check
    Set<MSentryRole> mRoles = delegate.getRolesForGroups(pm, groups);
    if (!privilegeOperator.checkPrivilegeOption(mRoles, requestPrivilege, pm)) {
      throw new SentryGrantDeniedException(grantorPrincipal
          + " has no grant!");
    }
  }

  @Override
  public Set<String> getRolesByGroups(String component, Set<String> groups)
      throws SentryUserException {
    Set<String> roles = Sets.newHashSet();
    if (groups == null) {
      return roles;
    }
    for (TSentryRole tSentryRole : delegate.getTSentryRolesByGroupName(groups, true)) {
      roles.add(tSentryRole.getRoleName());
    }
    return roles;
  }

  @Override
  public Set<String> getGroupsByRoles(String component, Set<String> roles)
      throws SentryUserException {
    roles = toTrimedLower(roles);
    Set<String> groupNames = Sets.newHashSet();
    if (roles.size() == 0) return groupNames;

    PersistenceManager pm = null;
    try{
      pm = openTransaction();
      //get groups by roles
      Query query = pm.newQuery(MSentryGroup.class);
      StringBuilder filters = new StringBuilder();
      query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
      List<String> rolesFiler = new LinkedList<String>();
      for (String role : roles) {
        rolesFiler.add("role.roleName == \"" + role + "\" ");
      }
      filters.append("roles.contains(role) " + "&& (" + Joiner.on(" || ").join(rolesFiler) + ")");
      query.setFilter(filters.toString());

      List<MSentryGroup> groups = (List<MSentryGroup>)query.execute();
      if (groups == null) {
        return groupNames;
      }
      for (MSentryGroup group : groups) {
        groupNames.add(group.getGroupName());
      }
      return groupNames;
    } finally {
      commitTransaction(pm);
    }
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByRole(String component,
      Set<String> roles) throws SentryUserException {
    Preconditions.checkNotNull(roles);
    Set<PrivilegeObject> privileges = Sets.newHashSet();
    if (roles.isEmpty()) return privileges;

    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Set<MSentryRole> mRoles = Sets.newHashSet();
      for (String role : roles) {
        MSentryRole mRole = getRole(toTrimedLower(role), pm);
        if (mRole != null) {
          mRoles.add(mRole);
        }
      }
      privileges.addAll(privilegeOperator.getPrivilegesByRole(mRoles, pm));
    } finally {
      commitTransaction(pm);
    }
    return privileges;
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByProvider(String component,
      String service, Set<String> roles, Set<String> groups,
      List<? extends Authorizable> authorizables) throws SentryUserException {
    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);

    component = toTrimedLower(component);
    service = toTrimedLower(service);

    Set<PrivilegeObject> privileges = Sets.newHashSet();
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      //CaseInsensitive roleNames
      roles = toTrimedLower(roles);

      if (groups != null) {
        roles.addAll(delegate.getRoleNamesForGroups(groups));
      }

      if (roles.size() == 0) {
        return privileges;
      }

      Set<MSentryRole> mRoles = Sets.newHashSet();
      for (String role : roles) {
        MSentryRole mRole = getRole(role, pm);
        if (mRole != null) {
          mRoles.add(mRole);
        }
      }
      //get the privileges
      privileges.addAll(privilegeOperator.getPrivilegesByProvider(component, service, mRoles, authorizables, pm));
    } finally {
      commitTransaction(pm);
    }
    return privileges;
  }

  @Override
  public void close() {
    delegate.stop();
  }

  private Set<TSentryGroup> toTSentryGroups(Set<String> groups) {
    Set<TSentryGroup> tSentryGroups = Sets.newHashSet();
    for (String group : toTrimedLower(groups)) {
      tSentryGroups.add(new TSentryGroup(group));
    }
    return tSentryGroups;
  }

  private Set<String> toTrimedLower(Set<String> s) {
    if (s == null) {
      return new HashSet<String>();
    }
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }

  private String toTrimedLower(String s) {
    if (s == null) {
      return "";
    }
    return s.trim().toLowerCase();
  }

  private Set<String> getRequestorGroups(String userName)
      throws SentryUserException {
    return SentryPolicyStoreProcessor.getGroupsFromUserName(this.conf, userName);
  }

  @VisibleForTesting
  void clearAllTables() {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      pm.newQuery(MSentryRole.class).deletePersistentAll();
      pm.newQuery(MSentryGroup.class).deletePersistentAll();
      pm.newQuery(MSentryGMPrivilege.class).deletePersistentAll();
      commitUpdateTransaction(pm);
      rollbackTransaction = false;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }
}
