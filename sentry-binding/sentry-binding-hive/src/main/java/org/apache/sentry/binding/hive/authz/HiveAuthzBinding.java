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
package org.apache.sentry.binding.hive.authz;

import java.lang.reflect.Constructor;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.binding.hive.conf.InvalidConfigurationException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.cache.PrivilegeCache;
import org.apache.sentry.provider.cache.SimpleCacheProviderBackend;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class HiveAuthzBinding {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBinding.class);
  private static final AtomicInteger queryID = new AtomicInteger();
  private static final Splitter ROLE_SET_SPLITTER = Splitter.on(",").trimResults()
      .omitEmptyStrings();
  public static final String HIVE_BINDING_TAG = "hive.authz.bindings.tag";

  private final HiveConf hiveConf;
  private final Server authServer;
  private final AuthorizationProvider authProvider;
  private volatile boolean open;
  private ActiveRoleSet activeRoleSet;
  private HiveAuthzConf authzConf;

  public static enum HiveHook {
    HiveServer2,
    HiveMetaStore
    ;
  }

  public HiveAuthzBinding (HiveConf hiveConf, HiveAuthzConf authzConf) throws Exception {
    this(HiveHook.HiveServer2, hiveConf, authzConf);
  }

  public HiveAuthzBinding (HiveHook hiveHook, HiveConf hiveConf, HiveAuthzConf authzConf) throws Exception {
    validateHiveConfig(hiveHook, hiveConf, authzConf);
    this.hiveConf = hiveConf;
    this.authzConf = authzConf;
    this.authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
    this.authProvider = getAuthProvider(hiveConf, authzConf, authServer.getName());
    this.open = true;
    this.activeRoleSet = parseActiveRoleSet(hiveConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET,
        authzConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET, "")).trim());
  }

  public HiveAuthzBinding (HiveHook hiveHook, HiveConf hiveConf, HiveAuthzConf authzConf,
      PrivilegeCache privilegeCache) throws Exception {
    validateHiveConfig(hiveHook, hiveConf, authzConf);
    this.hiveConf = hiveConf;
    this.authzConf = authzConf;
    this.authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
    this.authProvider = getAuthProviderWithPrivilegeCache(authzConf, authServer.getName(), privilegeCache);
    this.open = true;
    this.activeRoleSet = parseActiveRoleSet(hiveConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET,
            authzConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET, "")).trim());
  }

  private static ActiveRoleSet parseActiveRoleSet(String name)
      throws SentryUserException {
    return parseActiveRoleSet(name, null);
  }

  private static ActiveRoleSet parseActiveRoleSet(String name,
      Set<TSentryRole> allowedRoles) throws SentryUserException {
    // if unset, then we choose the default of ALL
    if (name.isEmpty()) {
      return ActiveRoleSet.ALL;
    } else if (AccessConstants.NONE_ROLE.equalsIgnoreCase(name)) {
      return new ActiveRoleSet(new HashSet<String>());
    } else if (AccessConstants.ALL_ROLE.equalsIgnoreCase(name)) {
      return ActiveRoleSet.ALL;
    } else if (AccessConstants.RESERVED_ROLE_NAMES.contains(name.toUpperCase())) {
      String msg = "Role " + name + " is reserved";
      throw new IllegalArgumentException(msg);
    } else {
      if (allowedRoles != null) {
        // check if the user has been granted the role
        boolean foundRole = false;
        for (TSentryRole role : allowedRoles) {
          if (role.getRoleName().equalsIgnoreCase(name)) {
            foundRole = true;
            break;
          }
        }
        if (!foundRole) {
          //Set the reason for hive binding to pick up
          throw new SentryUserException("Not authorized to set role " + name, "Not authorized to set role " + name);

        }
      }
      return new ActiveRoleSet(Sets.newHashSet(ROLE_SET_SPLITTER.split(name)));
    }
  }

  private void validateHiveConfig(HiveHook hiveHook, HiveConf hiveConf, HiveAuthzConf authzConf)
      throws InvalidConfigurationException{
    if(hiveHook.equals(HiveHook.HiveMetaStore)) {
      validateHiveMetaStoreConfig(hiveConf, authzConf);
    }else if(hiveHook.equals(HiveHook.HiveServer2)) {
      validateHiveServer2Config(hiveConf, authzConf);
    }
  }

  private void validateHiveMetaStoreConfig(HiveConf hiveConf, HiveAuthzConf authzConf)
      throws InvalidConfigurationException{
    boolean isTestingMode = Boolean.parseBoolean(Strings.nullToEmpty(
        authzConf.get(AuthzConfVars.SENTRY_TESTING_MODE.getVar())).trim());
    LOG.debug("Testing mode is " + isTestingMode);
    if(!isTestingMode) {
      boolean sasl = hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
      if(!sasl) {
        throw new InvalidConfigurationException(
            ConfVars.METASTORE_USE_THRIFT_SASL + " can't be false in non-testing mode");
      }
    } else {
      boolean setUgi = hiveConf.getBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI);
      if(!setUgi) {
        throw new InvalidConfigurationException(
            ConfVars.METASTORE_EXECUTE_SET_UGI.toString() + " can't be false in non secure mode");
      }
    }
  }

  private void validateHiveServer2Config(HiveConf hiveConf, HiveAuthzConf authzConf)
      throws InvalidConfigurationException{
    boolean isTestingMode = Boolean.parseBoolean(Strings.nullToEmpty(
        authzConf.get(AuthzConfVars.SENTRY_TESTING_MODE.getVar())).trim());
    LOG.debug("Testing mode is " + isTestingMode);
    if(!isTestingMode) {
      String authMethod = Strings.nullToEmpty(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)).trim();
      if("none".equalsIgnoreCase(authMethod)) {
        throw new InvalidConfigurationException(ConfVars.HIVE_SERVER2_AUTHENTICATION +
            " can't be none in non-testing mode");
      }
      boolean impersonation = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS);
      boolean allowImpersonation = Boolean.parseBoolean(Strings.nullToEmpty(
          authzConf.get(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION.getVar())).trim());

      if(impersonation && !allowImpersonation) {
        LOG.error("Role based authorization does not work with HiveServer2 impersonation");
        throw new InvalidConfigurationException(ConfVars.HIVE_SERVER2_ENABLE_DOAS +
            " can't be set to true in non-testing mode");
      }
    }
    String defaultUmask = hiveConf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
    if("077".equalsIgnoreCase(defaultUmask)) {
      LOG.error("HiveServer2 required a default umask of 077");
      throw new InvalidConfigurationException(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY +
          " should be 077 in non-testing mode");
    }
  }

  // Instantiate the configured authz provider
  public static AuthorizationProvider getAuthProvider(HiveConf hiveConf, HiveAuthzConf authzConf,
        String serverName) throws Exception {
    // get the provider class and resources from the authz config
    String authProviderName = authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar());
    String resourceName =
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    String providerBackendName = authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar());
    String policyEngineName = authzConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar());

    LOG.debug("Using authorization provider " + authProviderName +
        " with resource " + resourceName + ", policy engine "
        + policyEngineName + ", provider backend " + providerBackendName);
      // load the provider backend class
      Constructor<?> providerBackendConstructor =
        Class.forName(providerBackendName).getDeclaredConstructor(Configuration.class, String.class);
      providerBackendConstructor.setAccessible(true);
    ProviderBackend providerBackend = (ProviderBackend) providerBackendConstructor.
        newInstance(new Object[] {authzConf, resourceName});

    // load the policy engine class
    Constructor<?> policyConstructor =
      Class.forName(policyEngineName).getDeclaredConstructor(String.class, ProviderBackend.class);
    policyConstructor.setAccessible(true);
    PolicyEngine policyEngine = (PolicyEngine) policyConstructor.
        newInstance(new Object[] {serverName, providerBackend});


    // load the authz provider class
    Constructor<?> constrctor =
      Class.forName(authProviderName).getDeclaredConstructor(String.class, PolicyEngine.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {resourceName, policyEngine});
  }

  // Instantiate the authz provider using PrivilegeCache, this method is used for metadata filter function.
  public static AuthorizationProvider getAuthProviderWithPrivilegeCache(HiveAuthzConf authzConf,
      String serverName, PrivilegeCache privilegeCache) throws Exception {
    // get the provider class and resources from the authz config
    String authProviderName = authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar());
    String resourceName =
            authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    String policyEngineName = authzConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar());

    LOG.debug("Using authorization provider " + authProviderName +
            " with resource " + resourceName + ", policy engine "
            + policyEngineName + ", provider backend SimpleCacheProviderBackend");

    ProviderBackend providerBackend = new SimpleCacheProviderBackend(authzConf, resourceName);
    ProviderBackendContext context = new ProviderBackendContext();
    context.setBindingHandle(privilegeCache);
    providerBackend.initialize(context);

    // load the policy engine class
    Constructor<?> policyConstructor =
            Class.forName(policyEngineName).getDeclaredConstructor(String.class, ProviderBackend.class);
    policyConstructor.setAccessible(true);
    PolicyEngine policyEngine = (PolicyEngine) policyConstructor.
            newInstance(new Object[] {serverName, providerBackend});

    // load the authz provider class
    Constructor<?> constrctor =
            Class.forName(authProviderName).getDeclaredConstructor(String.class, PolicyEngine.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {resourceName, policyEngine});
  }


  /**
   * Validate the privilege for the given operation for the given subject
   * @param hiveOp
   * @param stmtAuthPrivileges
   * @param subject
   * @param currDB
   * @param inputEntities
   * @param outputEntities
   * @throws AuthorizationException
   */
  public void authorize(HiveOperation hiveOp, HiveAuthzPrivileges stmtAuthPrivileges,
      Subject subject, List<List<DBModelAuthorizable>> inputHierarchyList,
      List<List<DBModelAuthorizable>> outputHierarchyList)
          throws AuthorizationException {
    if (!open) {
      throw new IllegalStateException("Binding has been closed");
    }
    boolean isDebug = LOG.isDebugEnabled();
    if(isDebug) {
      LOG.debug("Going to authorize statement " + hiveOp.name() +
          " for subject " + subject.getName());
    }

    /* for each read and write entity captured by the compiler -
     *    check if that object type is part of the input/output privilege list
     *    If it is, then validate the access.
     * Note the hive compiler gathers information on additional entities like partitions,
     * etc which are not of our interest at this point. Hence its very
     * much possible that the we won't be validating all the entities in the given list
     */

    // Check read entities
    Map<AuthorizableType, EnumSet<DBModelAction>> requiredInputPrivileges =
        stmtAuthPrivileges.getInputPrivileges();
    if(isDebug) {
      LOG.debug("requiredInputPrivileges = " + requiredInputPrivileges);
      LOG.debug("inputHierarchyList = " + inputHierarchyList);
    }
    Map<AuthorizableType, EnumSet<DBModelAction>> requiredOutputPrivileges =
        stmtAuthPrivileges.getOutputPrivileges();
    if(isDebug) {
      LOG.debug("requiredOuputPrivileges = " + requiredOutputPrivileges);
      LOG.debug("outputHierarchyList = " + outputHierarchyList);
    }

    boolean found = false;
    for(AuthorizableType key: requiredInputPrivileges.keySet()) {
      for (List<DBModelAuthorizable> inputHierarchy : inputHierarchyList) {
        if (getAuthzType(inputHierarchy).equals(key)) {
          found = true;
          if (!authProvider.hasAccess(subject, inputHierarchy, requiredInputPrivileges.get(key), activeRoleSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have privileges for " + hiveOp.name());
          }
        }
      }
      if(!found && !(key.equals(AuthorizableType.URI)) &&  !(hiveOp.equals(HiveOperation.QUERY))
          && !(hiveOp.equals(HiveOperation.CREATETABLE_AS_SELECT))) {
        //URI privileges are optional for some privileges: anyPrivilege, tableDDLAndOptionalUriPrivilege
        //Query can mean select/insert/analyze where all of them have different required privileges.
        //CreateAsSelect can has table/columns privileges with select.
        //For these alone we skip if there is no equivalent input privilege
        //TODO: Even this case should be handled to make sure we do not skip the privilege check if we did not build
        //the input privileges correctly
        throw new AuthorizationException("Required privilege( " + key.name() + ") not available in input privileges");
      }
      found = false;
    }

    for(AuthorizableType key: requiredOutputPrivileges.keySet()) {
      for (List<DBModelAuthorizable> outputHierarchy : outputHierarchyList) {
        if (getAuthzType(outputHierarchy).equals(key)) {
          found = true;
          if (!authProvider.hasAccess(subject, outputHierarchy, requiredOutputPrivileges.get(key), activeRoleSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have privileges for " + hiveOp.name());
          }
        }
      }
      if(!found && !(key.equals(AuthorizableType.URI)) &&  !(hiveOp.equals(HiveOperation.QUERY))) {
        //URI privileges are optional for some privileges: tableInsertPrivilege
        //Query can mean select/insert/analyze where all of them have different required privileges.
        //For these alone we skip if there is no equivalent output privilege
        //TODO: Even this case should be handled to make sure we do not skip the privilege check if we did not build
        //the output privileges correctly
        throw new AuthorizationException("Required privilege( " + key.name() + ") not available in output privileges");
      }
      found = false;
    }

  }

  public void setActiveRoleSet(String activeRoleSet,
      Set<TSentryRole> allowedRoles) throws SentryUserException {
    this.activeRoleSet = parseActiveRoleSet(activeRoleSet, allowedRoles);
    hiveConf.set(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET, activeRoleSet);
  }

  public ActiveRoleSet getActiveRoleSet() {
    return activeRoleSet;
  }

  public Set<String> getGroups(Subject subject) {
    return authProvider.getGroupMapping().getGroups(subject.getName());
  }

  public Server getAuthServer() {
    if (!open) {
      throw new IllegalStateException("Binding has been closed");
    }
    return authServer;
  }

  public HiveAuthzConf getAuthzConf() {
    return authzConf;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  private AuthorizableType getAuthzType (List<DBModelAuthorizable> hierarchy){
    return hierarchy.get(hierarchy.size() -1).getAuthzType();
  }

  public List<String> getLastQueryPrivilegeErrors() {
    if (!open) {
      throw new IllegalStateException("Binding has been closed");
    }
    return authProvider.getLastFailedPrivileges();
  }

  public void close() {
    authProvider.close();
  }

  public AuthorizationProvider getCurrentAuthProvider() {
    return authProvider;
  }
}
