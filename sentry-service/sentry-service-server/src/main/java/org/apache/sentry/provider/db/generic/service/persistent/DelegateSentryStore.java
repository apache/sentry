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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import javax.jdo.Query;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryGrantDeniedException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.persistent.QueryParamBuilder;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.api.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.api.service.thrift.TSentryGroup;
import org.apache.sentry.api.generic.thrift.TSentryRole;
import org.apache.sentry.provider.db.service.persistent.TransactionBlock;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;

import javax.jdo.PersistenceManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  public DelegateSentryStore(Configuration conf) throws Exception {
    this.privilegeOperator = new PrivilegeOperatePersistence(conf);
    this.conf = conf;
    //delegated old sentryStore
    this.delegate = new SentryStore(conf);
    adminGroups = ImmutableSet.copyOf(toTrimmed(Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
  }

  private MSentryRole getRole(String roleName, PersistenceManager pm) {
    return delegate.getRole(pm, roleName);
  }

  @Override
  public Object createRole(String component, String role,
      String requestor) throws Exception {
    delegate.createSentryRole(role);
    return null;
  }

  /**
   * The role is global in the generic model, such as the role may be has more than one component
   * privileges, so delete role will remove all privileges related to it.
   */
  @Override
  public Object dropRole(final String component, final String role, final String requestor)
      throws Exception {
    delegate.dropSentryRole(toTrimmedLower(role));
    return null;
  }

  @Override
  public Set<String> getAllRoleNames() throws Exception {
    return delegate.getAllRoleNames();
  }

  @Override
  public Object alterRoleAddGroups(String component, String role,
      Set<String> groups, String requestor) throws Exception {
    delegate.alterSentryRoleAddGroups(requestor, role, toTSentryGroups(groups));
    return null;
  }

  @Override
  public Object alterRoleDeleteGroups(String component, String role,
      Set<String> groups, String requestor) throws Exception {
    delegate.alterSentryRoleDeleteGroups(role, toTSentryGroups(groups));
    return null;
  }

  @Override
  public Object alterRoleGrantPrivilege(final String component, final String role,
      final PrivilegeObject privilege, final String grantorPrincipal)
      throws Exception {
    delegate.getTransactionManager().executeTransactionWithRetry(
            pm -> {
              pm.setDetachAllOnCommit(false); // No need to detach objects
              String trimmedRole = toTrimmedLower(role);
              MSentryRole mRole = getRole(trimmedRole, pm);
              if (mRole == null) {
                throw new SentryNoSuchObjectException("Role: " + trimmedRole);
              }

              // check with grant option
              grantOptionCheck(privilege, grantorPrincipal, pm);

              privilegeOperator.grantPrivilege(privilege, mRole, pm);
              return null;
            });
    return null;
  }

  @Override
  public Object alterRoleRevokePrivilege(final String component,
      final String role, final PrivilegeObject privilege, final String grantorPrincipal)
      throws Exception {
    delegate.getTransactionManager().executeTransactionWithRetry(
            pm -> {
              pm.setDetachAllOnCommit(false); // No need to detach objects
              String trimmedRole = toTrimmedLower(role);
              MSentryRole mRole = getRole(trimmedRole, pm);
              if (mRole == null) {
                throw new SentryNoSuchObjectException("Role: " + trimmedRole);
              }

              // check with grant option
              grantOptionCheck(privilege, grantorPrincipal, pm);

              privilegeOperator.revokePrivilege(privilege, mRole, pm);
              return null;
            });
    return null;
  }

  @Override
  public Object renamePrivilege(final String component, final String service,
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

    delegate.getTransactionManager().executeTransactionWithRetry(
            pm -> {
              pm.setDetachAllOnCommit(false); // No need to detach objects
              privilegeOperator.renamePrivilege(toTrimmedLower(component), toTrimmedLower(service),
                  oldAuthorizables, newAuthorizables, requestor, pm);
              return null;
            });
    return null;
  }

  @Override
  public Object dropPrivilege(final String component,
      final PrivilegeObject privilege, final String requestor) throws Exception {
    Preconditions.checkNotNull(requestor);

    delegate.getTransactionManager().executeTransactionWithRetry(
            pm -> {
              pm.setDetachAllOnCommit(false); // No need to detach objects
              privilegeOperator.dropPrivilege(privilege, pm);
              return null;
            });
    return null;
  }

  /**
   * Grant option check
   * @throws SentryUserException
   */
  private void grantOptionCheck(PrivilegeObject requestPrivilege,
                                String grantorPrincipal,PersistenceManager pm)
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
  public Set<String> getRolesByGroups(String component, Set<String> groups)
      throws Exception {
    if (groups == null || groups.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> roles = Sets.newHashSet();
    if(groups.contains(null)) {
      roles = delegate.getAllRoleNames();
    } else {
      roles = delegate.getRoleNamesForGroups(groups);
    }
    return roles;
  }

  @Override
  public Set<TSentryRole> getTSentryRolesByGroupName(String component, Set<String> groups)
      throws Exception {
    if (groups == null || groups.isEmpty()) {
      return Collections.emptySet();
    }

    return delegate.getTransactionManager().executeTransaction(
      new TransactionBlock<Set<TSentryRole>>() {
        public Set<TSentryRole> execute(PersistenceManager pm) throws Exception {
          Set<TSentryRole> tRoles = Sets.newHashSet();
          pm.setDetachAllOnCommit(false); // No need to detach objects
          Set<MSentryRole> mSentryRoles = Sets.newHashSet();
          if(groups.contains(null)) {
            mSentryRoles.addAll(delegate.getAllRoles(pm));
          } else {
            mSentryRoles = delegate.getRolesForGroups(pm, groups);
          }
          for(MSentryRole mSentryRole: mSentryRoles) {
            String roleName = mSentryRole.getRoleName().intern();
            Set<String> groupNames = Sets.newHashSet();
            Set<MSentryGroup> mSentryGroups = mSentryRole.getGroups();
            for(MSentryGroup mSentryGroup: mSentryGroups) {
              groupNames.add(mSentryGroup.getGroupName());
            }
            tRoles.add(new TSentryRole(roleName, groupNames));
          }

          return tRoles;
        }
      });
  }

  @Override
  public Set<String> getGroupsByRoles(final String component, final Set<String> roles)
      throws Exception {
    if (roles.isEmpty()) {
      return Collections.emptySet();
    }

    return delegate.getTransactionManager().executeTransaction(
      new TransactionBlock<Set<String>>() {
        public Set<String> execute(PersistenceManager pm) throws Exception {
          pm.setDetachAllOnCommit(false); // No need to detach objects
          Query query = pm.newQuery(MSentryGroup.class);
          // Find privileges matching all roles
          QueryParamBuilder paramBuilder = QueryParamBuilder.addRolesFilter(query, null,
              roles);
          query.setFilter(paramBuilder.toString());
          List<MSentryGroup> mGroups =
              (List<MSentryGroup>)query.executeWithMap(paramBuilder.getArguments());

          Set<String> groupNames = new HashSet<>();
          for(MSentryGroup g: mGroups) {
            groupNames.add(g.getGroupName());
          }
          return groupNames;
        }
      });
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByRole(final String component,
      final Set<String> roles) throws Exception {
    Preconditions.checkNotNull(roles);
    if (roles.isEmpty()) {
      return Collections.emptySet();
    }
    return delegate.getTransactionManager().executeTransaction(
            pm -> {
              pm.setDetachAllOnCommit(false); // No need to detach objects
              Set<MSentryRole> mRoles = new HashSet<>();
              for (String role : roles) {
                MSentryRole mRole = getRole(toTrimmedLower(role), pm);
                if (mRole != null) {
                  mRoles.add(mRole);
                }
              }
              return new HashSet<>(privilegeOperator.getPrivilegesByRole(mRoles, pm));
            });
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByProvider(final String component,
      final String service, final Set<String> roles, final Set<String> groups,
      final List<? extends Authorizable> authorizables) throws Exception {
    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);

    return delegate.getTransactionManager().executeTransaction(
            pm -> {
              pm.setDetachAllOnCommit(false); // No need to detach objects
              String trimmedComponent = toTrimmedLower(component);
              String trimmedService = toTrimmedLower(service);

              //CaseInsensitive roleNames
              Set<String> trimmedRoles = SentryStore.toTrimedLower(roles);

              if (groups != null) {
                trimmedRoles.addAll(delegate.getRoleNamesForGroups(groups));
              }

              if (trimmedRoles.isEmpty()) {
                return Collections.emptySet();
              }

              Set<MSentryRole> mRoles = new HashSet<>(trimmedRoles.size());
              for (String role : trimmedRoles) {
                MSentryRole mRole = getRole(role, pm);
                if (mRole != null) {
                  mRoles.add(mRole);
                }
              }
              //get the privileges
              Set<PrivilegeObject> privileges = new HashSet<>();
              privileges.addAll(privilegeOperator.
                      getPrivilegesByProvider(trimmedComponent,
                              trimmedService, mRoles, authorizables, pm));
              return privileges;
            });
  }

  @Override
  public Set<MSentryGMPrivilege> getPrivilegesByAuthorizable(final String component,
      final String service, final Set<String> validActiveRoles,
      final List<? extends Authorizable> authorizables) throws Exception {
    if (validActiveRoles == null || validActiveRoles.isEmpty()) {
      return Collections.emptySet();
    }

    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);

    return delegate.getTransactionManager().executeTransaction(
            pm -> {
              String lComponent = toTrimmedLower(component);
              String lService = toTrimmedLower(service);
              Set<MSentryRole> mRoles = new HashSet<>(validActiveRoles.size());
              for (String role : validActiveRoles) {
                MSentryRole mRole = getRole(role, pm);
                if (mRole != null) {
                  mRoles.add(mRole);
                }
              }

              //get the privileges
              Set<MSentryGMPrivilege> mSentryGMPrivileges =
                  privilegeOperator.getPrivilegesByAuthorizable(lComponent, lService,
                          mRoles, authorizables, pm);

              final Set<MSentryGMPrivilege> privileges =
                      new HashSet<>(mSentryGMPrivileges.size());
              for (MSentryGMPrivilege mSentryGMPrivilege : mSentryGMPrivileges) {
                /*
                 * force to load all roles related this privilege
                 * avoid the lazy-loading
                 */
                pm.retrieve(mSentryGMPrivilege);
                privileges.add(mSentryGMPrivilege);
              }
              return privileges;
            });
  }

   @Override
  public void close() {
    delegate.stop();
  }

  private Set<TSentryGroup> toTSentryGroups(Set<String> groups) {
    if (groups.isEmpty()) {
      return Collections.emptySet();
    }
    Set<TSentryGroup> tSentryGroups = new HashSet<>(groups.size());
    for (String group : groups) {
      tSentryGroups.add(new TSentryGroup(group));
    }
    return tSentryGroups;
  }

  private static Set<String> toTrimmed(Set<String> s) {
    if (s.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> result = new HashSet<>(s.size());
    for (String v : s) {
      result.add(v.trim());
    }
    return result;
  }

  private static String toTrimmedLower(String s) {
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
            pm -> {
              pm.newQuery(MSentryRole.class).deletePersistentAll();
              pm.newQuery(MSentryGroup.class).deletePersistentAll();
              pm.newQuery(MSentryGMPrivilege.class).deletePersistentAll();
              return null;
            });
  }
}
