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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;
import org.apache.sentry.core.model.kafka.KafkaActionFactory;
import org.apache.sentry.core.model.search.SearchActionFactory;
import org.apache.sentry.provider.db.generic.service.persistent.PrivilegeObject.Builder;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class used do some operations related privilege and make the results
 * persistence
 */
public class PrivilegeOperatePersistence {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrivilegeOperatePersistence.class);
  private static final Map<String, BitFieldActionFactory> actionFactories = Maps.newHashMap();
  static{
    actionFactories.put("solr", new SearchActionFactory());
    actionFactories.put("kafka", KafkaActionFactory.getInstance());
  }

  private final Configuration conf;

  public PrivilegeOperatePersistence(Configuration conf) {
    this.conf = conf;
  }

  public boolean checkPrivilegeOption(Set<MSentryRole> roles, PrivilegeObject privilege, PersistenceManager pm) {
    MSentryGMPrivilege requestPrivilege = convertToPrivilege(privilege);
    boolean hasGrant = false;
    //get persistent privileges by roles
    Query query = pm.newQuery(MSentryGMPrivilege.class);
    StringBuilder filters = new StringBuilder();
    if ((roles != null) && (roles.size() > 0)) {
      query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
      List<String> rolesFiler = new LinkedList<String>();
      for (MSentryRole role : roles) {
        rolesFiler.add("role.roleName == \"" + role.getRoleName() + "\" ");
      }
      filters.append("roles.contains(role) " + "&& (" + Joiner.on(" || ").join(rolesFiler) + ")");
    }
    query.setFilter(filters.toString());

    List<MSentryGMPrivilege> tPrivileges = (List<MSentryGMPrivilege>)query.execute();
    for (MSentryGMPrivilege tPrivilege : tPrivileges) {
      if (tPrivilege.getGrantOption() && tPrivilege.implies(requestPrivilege)) {
        hasGrant = true;
        break;
      }
    }
    return hasGrant;
  }
  public void grantPrivilege(PrivilegeObject privilege,MSentryRole role, PersistenceManager pm) throws SentryUserException {
    MSentryGMPrivilege mPrivilege = convertToPrivilege(privilege);
    grantRolePartial(mPrivilege, role, pm);
  }

  private void grantRolePartial(MSentryGMPrivilege grantPrivilege,
      MSentryRole role,PersistenceManager pm) {
    /**
     * If Grant is for ALL action and other actions belongs to ALL action already exists..
     * need to remove it and GRANT ALL action
     */
    String component = grantPrivilege.getComponentName();
    BitFieldAction action = getAction(component, grantPrivilege.getAction());
    BitFieldAction allAction = getAction(component, Action.ALL);

    if (action.implies(allAction)) {
      /**
       * ALL action is a multi-bit set action that includes some actions such as INSERT,SELECT and CREATE.
       */
      List<? extends BitFieldAction> actions = getActionFactory(component).getActionsByCode(allAction.getActionCode());
      for (BitFieldAction ac : actions) {
        grantPrivilege.setAction(ac.getValue());
        MSentryGMPrivilege existPriv = getPrivilege(grantPrivilege, pm);
        if ((existPriv != null) && (role.getGmPrivileges().contains(existPriv))) {
          /**
           * force to load all roles related this privilege
           * avoid the lazy-loading risk,such as:
           * if the roles field of privilege aren't loaded, then the roles is a empty set
           * privilege.removeRole(role) and pm.makePersistent(privilege)
           * will remove other roles that shouldn't been removed
           */
          pm.retrieve(existPriv);
          existPriv.removeRole(role);
          pm.makePersistent(existPriv);
        }
      }
    } else {
      /**
       * If ALL Action already exists..
       * do nothing.
       */
      grantPrivilege.setAction(allAction.getValue());
      MSentryGMPrivilege allPrivilege = getPrivilege(grantPrivilege, pm);
      if ((allPrivilege != null) && (role.getGmPrivileges().contains(allPrivilege))) {
        return;
      }
    }

    /**
     * restore the action
     */
    grantPrivilege.setAction(action.getValue());
    /**
     * check the privilege is exist or not
     */
    MSentryGMPrivilege mPrivilege = getPrivilege(grantPrivilege, pm);
    if (mPrivilege == null) {
      mPrivilege = grantPrivilege;
    }
    mPrivilege.appendRole(role);
    pm.makePersistent(mPrivilege);
  }


  public void revokePrivilege(PrivilegeObject privilege,MSentryRole role, PersistenceManager pm) throws SentryUserException {
    MSentryGMPrivilege mPrivilege = getPrivilege(convertToPrivilege(privilege), pm);
    if (mPrivilege == null) {
      mPrivilege = convertToPrivilege(privilege);
    } else {
      mPrivilege = (MSentryGMPrivilege) pm.detachCopy(mPrivilege);
    }

    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(Sets.newHashSet(role), mPrivilege, pm));

    /**
     * Get the privilege graph
     * populateIncludePrivileges will get the privileges that needed revoke
     */
    for (MSentryGMPrivilege persistedPriv : privilegeGraph) {
      /**
       * force to load all roles related this privilege
       * avoid the lazy-loading risk,such as:
       * if the roles field of privilege aren't loaded, then the roles is a empty set
       * privilege.removeRole(role) and pm.makePersistent(privilege)
       * will remove other roles that shouldn't been removed
       */
      revokeRolePartial(mPrivilege, persistedPriv, role, pm);
    }
    pm.makePersistent(role);
  }

  /**
   * Explore Privilege graph and collect privileges that are belong to the specific privilege
   */
  @SuppressWarnings("unchecked")
  private Set<MSentryGMPrivilege> populateIncludePrivileges(Set<MSentryRole> roles,
      MSentryGMPrivilege parent, PersistenceManager pm) {
    Set<MSentryGMPrivilege> childrens = Sets.newHashSet();

    Query query = pm.newQuery(MSentryGMPrivilege.class);
    StringBuilder filters = new StringBuilder();
    //add populateIncludePrivilegesQuery
    filters.append(MSentryGMPrivilege.populateIncludePrivilegesQuery(parent));
    // add filter for role names
    if ((roles != null) && (roles.size() > 0)) {
      query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
      List<String> rolesFiler = new LinkedList<String>();
      for (MSentryRole role : roles) {
        rolesFiler.add("role.roleName == \"" + role.getRoleName() + "\" ");
      }
      filters.append("&& roles.contains(role) " + "&& (" + Joiner.on(" || ").join(rolesFiler) + ")");
    }
    query.setFilter(filters.toString());

    List<MSentryGMPrivilege> privileges = (List<MSentryGMPrivilege>)query.execute();
    childrens.addAll(privileges);
    return childrens;
  }

  /**
   * Roles can be granted multi-bit set action like ALL action on resource object.
   * Take solr component for example, When a role has been granted ALL action but
   * QUERY or UPDATE or CREATE are revoked, we need to remove the ALL
   * privilege and add left privileges like UPDATE and CREATE(QUERY was revoked) or
   * QUERY and UPDATE(CREATEE was revoked).
   */
  private void revokeRolePartial(MSentryGMPrivilege revokePrivilege,
      MSentryGMPrivilege persistedPriv, MSentryRole role,
      PersistenceManager pm) {
    String component = revokePrivilege.getComponentName();
    BitFieldAction revokeaction = getAction(component, revokePrivilege.getAction());
    BitFieldAction persistedAction = getAction(component, persistedPriv.getAction());
    BitFieldAction allAction = getAction(component, Action.ALL);

    if (revokeaction.implies(allAction)) {
      /**
       * if revoke action is ALL, directly revoke its children privileges and itself
       */
      persistedPriv.removeRole(role);
      pm.makePersistent(persistedPriv);
    } else {
      /**
       * if persisted action is ALL, it only revoke the requested action and left partial actions
       * like the requested action is SELECT, the UPDATE and CREATE action are left
       */
      if (persistedAction.implies(allAction)) {
        /**
         * revoke the ALL privilege
         */
        persistedPriv.removeRole(role);
        pm.makePersistent(persistedPriv);

        List<? extends BitFieldAction> actions = getActionFactory(component).getActionsByCode(allAction.getActionCode());
        for (BitFieldAction ac: actions) {
          if (ac.getActionCode() != revokeaction.getActionCode()) {
            /**
             * grant the left privileges to role
             */
            MSentryGMPrivilege tmpPriv = new MSentryGMPrivilege(persistedPriv);
            tmpPriv.setAction(ac.getValue());
            MSentryGMPrivilege leftPersistedPriv = getPrivilege(tmpPriv, pm);
            if (leftPersistedPriv == null) {
              //leftPersistedPriv isn't exist
              leftPersistedPriv = tmpPriv;
              role.appendGMPrivilege(leftPersistedPriv);
            }
            leftPersistedPriv.appendRole(role);
            pm.makePersistent(leftPersistedPriv);
          }
        }
      } else if (revokeaction.implies(persistedAction)) {
        /**
         * if the revoke action is equal to the persisted action and they aren't ALL action
         * directly remove the role from privilege
         */
        persistedPriv.removeRole(role);
        pm.makePersistent(persistedPriv);
      } else {
        /**
         * if the revoke action is not equal to the persisted action,
         * do nothing
         */
      }
    }
  }

  /**
   * Drop any role related to the requested privilege and its children privileges
   */
  public void dropPrivilege(PrivilegeObject privilege,PersistenceManager pm) {
    MSentryGMPrivilege requestPrivilege = convertToPrivilege(privilege);

    if (Strings.isNullOrEmpty(privilege.getAction())) {
      requestPrivilege.setAction(getAction(privilege.getComponent(), Action.ALL).getValue());
    }
    /**
     * Get the privilege graph
     * populateIncludePrivileges will get the privileges that need dropped,
     */
    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(null, requestPrivilege, pm));

    for (MSentryGMPrivilege mPrivilege : privilegeGraph) {
      /**
       * force to load all roles related this privilege
       * avoid the lazy-loading
       */
      pm.retrieve(mPrivilege);
      Set<MSentryRole> roles = mPrivilege.getRoles();
      for (MSentryRole role : roles) {
        revokeRolePartial(requestPrivilege, mPrivilege, role, pm);
      }
    }
  }

  private MSentryGMPrivilege convertToPrivilege(PrivilegeObject privilege) {
    return new MSentryGMPrivilege(privilege.getComponent(),
        privilege.getService(), privilege.getAuthorizables(),
        privilege.getAction(), privilege.getGrantOption());
  }

  private MSentryGMPrivilege getPrivilege(MSentryGMPrivilege privilege, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryGMPrivilege.class);
    query.setFilter(MSentryGMPrivilege.toQuery(privilege));
    query.setUnique(true);
    return (MSentryGMPrivilege)query.execute();
  }

  @SuppressWarnings("unchecked")
  public Set<PrivilegeObject> getPrivilegesByRole(Set<MSentryRole> roles, PersistenceManager pm) {
    Set<PrivilegeObject> privileges = Sets.newHashSet();
    if ((roles == null) || (roles.size() == 0)) {
      return privileges;
    }
    Query query = pm.newQuery(MSentryGMPrivilege.class);
    StringBuilder filters = new StringBuilder();
    // add filter for role names
    query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
    List<String> rolesFiler = new LinkedList<String>();
    for (MSentryRole role : roles) {
      rolesFiler.add("role.roleName == \"" + role.getRoleName() + "\" ");
    }
    filters.append("roles.contains(role) " + "&& (" + Joiner.on(" || ").join(rolesFiler) + ")");

    query.setFilter(filters.toString());
    List<MSentryGMPrivilege> mPrivileges = (List<MSentryGMPrivilege>) query.execute();
    if ((mPrivileges == null) || (mPrivileges.size() ==0)) {
      return privileges;
    }
    for (MSentryGMPrivilege mPrivilege : mPrivileges) {
      privileges.add(new Builder()
                               .setComponent(mPrivilege.getComponentName())
                               .setService(mPrivilege.getServiceName())
                               .setAction(mPrivilege.getAction())
                               .setAuthorizables(mPrivilege.getAuthorizables())
                               .withGrantOption(mPrivilege.getGrantOption())
                               .build());
    }
    return privileges;
  }

  public Set<PrivilegeObject> getPrivilegesByProvider(String component,
      String service, Set<MSentryRole> roles,
      List<? extends Authorizable> authorizables, PersistenceManager pm) {
    Set<PrivilegeObject> privileges = Sets.newHashSet();
    if ((roles == null) || (roles.size() == 0)) return privileges;

    MSentryGMPrivilege parentPrivilege = new MSentryGMPrivilege(component, service, authorizables, null, null);
    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(roles, parentPrivilege, pm));

    for (MSentryGMPrivilege mPrivilege : privilegeGraph) {
      privileges.add(new Builder()
                               .setComponent(mPrivilege.getComponentName())
                               .setService(mPrivilege.getServiceName())
                               .setAction(mPrivilege.getAction())
                               .setAuthorizables(mPrivilege.getAuthorizables())
                               .withGrantOption(mPrivilege.getGrantOption())
                               .build());
    }
    return privileges;
  }

  public Set<MSentryGMPrivilege> getPrivilegesByAuthorizable(String component,
      String service, Set<MSentryRole> roles,
      List<? extends Authorizable> authorizables, PersistenceManager pm) {

    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();

    if (roles == null || roles.isEmpty()) {
      return privilegeGraph;
    }

    MSentryGMPrivilege parentPrivilege = new MSentryGMPrivilege(component, service, authorizables, null, null);
    privilegeGraph.addAll(populateIncludePrivileges(roles, parentPrivilege, pm));
    return privilegeGraph;
  }

  public void renamePrivilege(String component, String service,
      List<? extends Authorizable> oldAuthorizables, List<? extends Authorizable> newAuthorizables,
      String grantorPrincipal, PersistenceManager pm)
      throws SentryUserException {
    MSentryGMPrivilege oldPrivilege = new MSentryGMPrivilege(component, service, oldAuthorizables, null, null);
    oldPrivilege.setAction(getAction(component,Action.ALL).getValue());
    /**
     * Get the privilege graph
     * populateIncludePrivileges will get the old privileges that need dropped
     */
    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(null, oldPrivilege, pm));

    for (MSentryGMPrivilege dropPrivilege : privilegeGraph) {
      /**
       * construct the new privilege needed to add
       */
      List<Authorizable> authorizables = new ArrayList<Authorizable>(
          dropPrivilege.getAuthorizables());
      for (int i = 0; i < newAuthorizables.size(); i++) {
        authorizables.set(i, newAuthorizables.get(i));
      }
      MSentryGMPrivilege newPrivilge = new MSentryGMPrivilege(
          component,service, authorizables, dropPrivilege.getAction(),
          dropPrivilege.getGrantOption());

      /**
       * force to load all roles related this privilege
       * avoid the lazy-loading
       */
      pm.retrieve(dropPrivilege);

      Set<MSentryRole> roles = dropPrivilege.getRoles();
      for (MSentryRole role : roles) {
        revokeRolePartial(oldPrivilege, dropPrivilege, role, pm);
        grantRolePartial(newPrivilge, role, pm);
      }
    }
  }

  private BitFieldAction getAction(String component, String name) {
    BitFieldActionFactory actionFactory = getActionFactory(component);
    BitFieldAction action = actionFactory.getActionByName(name);
    if (action == null) {
      throw new RuntimeException("can't get BitFieldAction for name:" + name);
    }
    return action;
  }

  private BitFieldActionFactory getActionFactory(String component) {
    String caseInsensitiveComponent = component.toLowerCase();
    if (actionFactories.containsKey(caseInsensitiveComponent)) {
      return actionFactories.get(caseInsensitiveComponent);
    }
    BitFieldActionFactory actionFactory = createActionFactory(caseInsensitiveComponent);
    actionFactories.put(caseInsensitiveComponent, actionFactory);
    LOGGER.info("Action factory for component {} not found in cache. Loaded it from configuration as {}.",
                component, actionFactory.getClass().getName());
    return actionFactory;
  }

  private BitFieldActionFactory createActionFactory(String component) {
    String actionFactoryClassName =
      conf.get(String.format(ServiceConstants.ServerConfig.SENTRY_COMPONENT_ACTION_FACTORY_FORMAT, component));
    if (actionFactoryClassName == null) {
      throw new RuntimeException("ActionFactory not defined for component " + component +
                                   ". Please define the parameter " +
                                   "sentry." + component + ".action.factory in configuration");
    }
    Class<?> actionFactoryClass;
    try {
      actionFactoryClass = Class.forName(actionFactoryClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("ActionFactory class " + actionFactoryClassName + " not found.");
    }
    if (!BitFieldActionFactory.class.isAssignableFrom(actionFactoryClass)) {
      throw new RuntimeException("ActionFactory class " + actionFactoryClassName + " must extend "
                                   + BitFieldActionFactory.class.getName());
    }
    BitFieldActionFactory actionFactory;
    try {
      Constructor<?> actionFactoryConstructor = actionFactoryClass.getDeclaredConstructor();
      actionFactoryConstructor.setAccessible(true);
      actionFactory = (BitFieldActionFactory) actionFactoryClass.newInstance();
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Could not instantiate actionFactory " + actionFactoryClassName +
                                   " for component: " + component, e);
    }
    return actionFactory;
  }
}
