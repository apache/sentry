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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.persistent.CommitContext;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.persistent.TransactionBlock;
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
  private static final UUID SERVER_UUID = UUID.randomUUID();
  private final SentryStore delegate;
  private final Configuration conf;
  private final Set<String> adminGroups;
  private final PrivilegeOperatePersistence privilegeOperator;

  public DelegateSentryStore(Configuration conf) throws Exception {
    this.privilegeOperator = new PrivilegeOperatePersistence(conf);
    // The generic model doesn't turn on the thread that cleans hive privileges
    conf.set(ServerConfig.SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL,"false");
    this.conf = conf;
    //delegated old sentryStore
    this.delegate = new SentryStore(conf);
    adminGroups = ImmutableSet.copyOf(toTrimmed(Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
  }

  private MSentryRole getRole(String roleName, PersistenceManager pm) {
    return delegate.getMSentryRole(pm, roleName);
  }

  @Override
  public CommitContext createRole(String component, String role,
      String requestor) throws Exception {
    return delegate.createSentryRole(role);
  }

  /**
   * The role is global in the generic model, such as the role may be has more than one component
   * privileges, so delete role will remove all privileges related to it.
   */
  @Override
  public CommitContext dropRole(final String component, final String role, final String requestor)
      throws Exception {
    return (CommitContext)delegate.getTransactionManager().executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRole = toTrimmedLower(role);
            Query query = pm.newQuery(MSentryRole.class);
            query.setFilter("this.roleName == t");
            query.declareParameters("java.lang.String t");
            query.setUnique(true);
            MSentryRole sentryRole = (MSentryRole) query.execute(trimmedRole);
            if (sentryRole == null) {
              throw new SentryNoSuchObjectException("Role: " + trimmedRole + " doesn't exist");
            } else {
              pm.retrieve(sentryRole);
              sentryRole.removeGMPrivileges();
              sentryRole.removePrivileges();
              pm.deletePersistent(sentryRole);
            }
            return new CommitContext(SERVER_UUID, 0l);
          }
        });
  }

  @Override
  public Set<String> getAllRoleNames() {
    return delegate.getAllRoleNames();
  }

  @Override
  public CommitContext alterRoleAddGroups(String component, String role,
      Set<String> groups, String requestor) throws Exception {
    return delegate.alterSentryRoleAddGroups(requestor, role, toTSentryGroups(groups));
  }

  @Override
  public CommitContext alterRoleDeleteGroups(String component, String role,
      Set<String> groups, String requestor) throws Exception {
  //called to old sentryStore
    return delegate.alterSentryRoleDeleteGroups(role, toTSentryGroups(groups));
  }

  @Override
  public CommitContext alterRoleGrantPrivilege(final String component, final String role,
      final PrivilegeObject privilege, final String grantorPrincipal) throws Exception {
    return (CommitContext)delegate.getTransactionManager().executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRole = toTrimmedLower(role);
            MSentryRole mRole = getRole(trimmedRole, pm);
            if (mRole == null) {
              throw new SentryNoSuchObjectException("Role: " + trimmedRole + " doesn't exist");
            }
            /**
             * check with grant option
             */
            grantOptionCheck(privilege, grantorPrincipal, pm);

            privilegeOperator.grantPrivilege(privilege, mRole, pm);
            return new CommitContext(SERVER_UUID, 0l);
          }
        });
  }

  @Override
  public CommitContext alterRoleRevokePrivilege(final String component,
      final String role, final PrivilegeObject privilege, final String grantorPrincipal)
      throws Exception {
    return (CommitContext)delegate.getTransactionManager().executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRole = toTrimmedLower(role);
            MSentryRole mRole = getRole(trimmedRole, pm);
            if (mRole == null) {
              throw new SentryNoSuchObjectException("Role: " + trimmedRole + " doesn't exist");
            }
            /**
             * check with grant option
             */
            grantOptionCheck(privilege, grantorPrincipal, pm);

            privilegeOperator.revokePrivilege(privilege, mRole, pm);
            return new CommitContext(SERVER_UUID, 0l);
          }
        });
  }

  @Override
  public CommitContext renamePrivilege(final String component, final String service,
      final List<? extends Authorizable> oldAuthorizables,
      final List<? extends Authorizable> newAuthorizables, final String requestor)
      throws Exception {
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

    return (CommitContext)delegate.getTransactionManager().executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            privilegeOperator.renamePrivilege(toTrimmedLower(component), toTrimmedLower(service),
                oldAuthorizables, newAuthorizables, requestor, pm);
            return new CommitContext(SERVER_UUID, 0l);
          }
        });
  }

  @Override
  public CommitContext dropPrivilege(final String component,
      final PrivilegeObject privilege, final String requestor) throws Exception {
    Preconditions.checkNotNull(requestor);
    return (CommitContext)delegate.getTransactionManager().executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            privilegeOperator.dropPrivilege(privilege, pm);
            return new CommitContext(SERVER_UUID, 0l);
          }
        });
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
    if (!Sets.intersection(adminGroups, toTrimmed(groups)).isEmpty()) {
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
  public Set<String> getRolesByGroups(String component, Set<String> groups) throws Exception {
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
  public Set<String> getGroupsByRoles(final String component, final Set<String> roles)
      throws Exception {
    final Set<String> trimmedRoles = toTrimmedLower(roles);
    final Set<String> groupNames = Sets.newHashSet();
    if (trimmedRoles.size() == 0) {
      return groupNames;
    }

    return (Set<String>) delegate.getTransactionManager().executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            //get groups by roles
            Query query = pm.newQuery(MSentryGroup.class);
            StringBuilder filters = new StringBuilder();
            query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
            List<String> rolesFiler = new LinkedList<String>();
            for (String role : trimmedRoles) {
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
          }
        });
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByRole(final String component,
      final Set<String> roles) throws Exception {
    Preconditions.checkNotNull(roles);
    final Set<PrivilegeObject> privileges = Sets.newHashSet();
    if (roles.isEmpty()) {
      return privileges;
    }

    return (Set<PrivilegeObject>) delegate.getTransactionManager().executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            Set<MSentryRole> mRoles = Sets.newHashSet();
            for (String role : roles) {
              MSentryRole mRole = getRole(toTrimmedLower(role), pm);
              if (mRole != null) {
                mRoles.add(mRole);
              }
            }
            privileges.addAll(privilegeOperator.getPrivilegesByRole(mRoles, pm));
            return privileges;
          }
        });
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByProvider(final String component,
      final String service, final Set<String> roles, final Set<String> groups,
      final List<? extends Authorizable> authorizables) throws Exception {
    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);

    return (Set<PrivilegeObject>) delegate.getTransactionManager().executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedComponent = toTrimmedLower(component);
            String trimmedService = toTrimmedLower(service);
            Set<PrivilegeObject> privileges = Sets.newHashSet();
            //CaseInsensitive roleNames
            Set<String> trimmedRoles = toTrimmedLower(roles);

            if (groups != null) {
              trimmedRoles.addAll(delegate.getRoleNamesForGroups(groups));
            }

            if (trimmedRoles.size() == 0) {
              return privileges;
            }

            Set<MSentryRole> mRoles = Sets.newHashSet();
            for (String role : trimmedRoles) {
              MSentryRole mRole = getRole(role, pm);
              if (mRole != null) {
                mRoles.add(mRole);
              }
            }
            //get the privileges
            privileges.addAll(privilegeOperator.getPrivilegesByProvider(trimmedComponent,
                trimmedService, mRoles, authorizables, pm));

            return privileges;
          }
        });
  }

  @Override
  public Set<MSentryGMPrivilege> getPrivilegesByAuthorizable(final String component,
      final String service, final Set<String> validActiveRoles,
      final List<? extends Authorizable> authorizables) throws Exception {
    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);

    final Set<MSentryGMPrivilege> privileges = Sets.newHashSet();

    if (validActiveRoles == null || validActiveRoles.isEmpty()) {
      return privileges;
    }

    return (Set<MSentryGMPrivilege>) delegate.getTransactionManager().executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String lComponent = toTrimmedLower(component);
            String lService = toTrimmedLower(service);

            Set<MSentryRole> mRoles = Sets.newHashSet();
            for (String role : validActiveRoles) {
              MSentryRole mRole = getRole(role, pm);
              if (mRole != null) {
                mRoles.add(mRole);
              }
            }

            //get the privileges
            Set<MSentryGMPrivilege> mSentryGMPrivileges =
                privilegeOperator.getPrivilegesByAuthorizable(lComponent, lService, mRoles, authorizables, pm);

            for (MSentryGMPrivilege mSentryGMPrivilege : mSentryGMPrivileges) {
              /**
               * force to load all roles related this privilege
               * avoid the lazy-loading
               */
              pm.retrieve(mSentryGMPrivilege);
              privileges.add(mSentryGMPrivilege);
            }

            return privileges;
          }
        });
  }

   @Override
  public void close() {
    delegate.stop();
  }

  private Set<TSentryGroup> toTSentryGroups(Set<String> groups) {
    Set<TSentryGroup> tSentryGroups = Sets.newHashSet();
    for (String group : groups) {
      tSentryGroups.add(new TSentryGroup(group));
    }
    return tSentryGroups;
  }

  private Set<String> toTrimmedLower(Set<String> s) {
    if (s == null) {
      return new HashSet<String>();
    }
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }

  private Set<String> toTrimmed(Set<String> s) {
    if (s == null) {
      return new HashSet<String>();
    }
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim());
    }
    return result;
  }

  private String toTrimmedLower(String s) {
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
  void clearAllTables() throws Exception {
    delegate.getTransactionManager().executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            pm.newQuery(MSentryRole.class).deletePersistentAll();
            pm.newQuery(MSentryGroup.class).deletePersistentAll();
            pm.newQuery(MSentryGMPrivilege.class).deletePersistentAll();
            return null;
          }
        });
  }
}
