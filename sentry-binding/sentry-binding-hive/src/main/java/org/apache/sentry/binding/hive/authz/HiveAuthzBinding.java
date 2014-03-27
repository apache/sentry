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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
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
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.NoAuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class HiveAuthzBinding {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBinding.class);
  private static final Map<String, HiveAuthzBinding> authzBindingMap =
      new ConcurrentHashMap<String, HiveAuthzBinding>();
  private static final AtomicInteger queryID = new AtomicInteger();
  private static final Splitter ROLE_SET_SPLITTER = Splitter.on(",").trimResults()
      .omitEmptyStrings();
  public static final String HIVE_BINDING_TAG = "hive.authz.bindings.tag";

  private final HiveConf hiveConf;
  private final Server authServer;
  private final AuthorizationProvider authProvider;
  private volatile boolean open;
  private ActiveRoleSet activeRoleSet;

  public HiveAuthzBinding (HiveConf hiveConf, HiveAuthzConf authzConf) throws Exception {
    this.hiveConf = hiveConf;
    this.authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
    this.authProvider = getAuthProvider(hiveConf, authzConf, authServer.getName());
    this.open = true;
    this.activeRoleSet = parseActiveRoleSet(hiveConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET,
        authzConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET, "")).trim());
  }

  private static ActiveRoleSet parseActiveRoleSet(String name) {
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
      return new ActiveRoleSet(Sets.newHashSet(ROLE_SET_SPLITTER.split(name)));
    }
  }

  /**
   * Retrieve the HiveAuthzBinding if the tag is saved in the given configuration
   * @param conf
   * @return HiveAuthzBinding or null
   */
  public static HiveAuthzBinding get(Configuration conf) {
    String tagName = conf.get(HIVE_BINDING_TAG);
    if (tagName == null) {
      return null;
    } else {
      return authzBindingMap.get(tagName);
    }
  }

  /**
   * store the HiveAuthzBinding in the authzBindingMap and save a tag in the given configuration
   * @param conf
   */
  public void set (Configuration conf) {
    if (!open) {
      throw new IllegalStateException("Binding has been closed");
    }
    String tagName = SessionState.get().getSessionId() + "_" + queryID.incrementAndGet();
    authzBindingMap.put(tagName, this);
    conf.set(HIVE_BINDING_TAG, tagName);
  }

  /**
   * remove the authzBindingMap entry for given tag
   * @param conf
   */
  public void clear(Configuration conf) {
    if (!open) {
      throw new IllegalStateException("Binding has been closed");
    }
    String tagName = conf.get(HIVE_BINDING_TAG);
    if (tagName != null) {
      authzBindingMap.remove(tagName);
    }
    open = false;
    authProvider.close();
  }

  // Instantiate the configured authz provider
  public static AuthorizationProvider getAuthProvider(HiveConf hiveConf, HiveAuthzConf authzConf,
        String serverName) throws Exception {
    boolean isTestingMode = Boolean.parseBoolean(Strings.nullToEmpty(
        authzConf.get(AuthzConfVars.SENTRY_TESTING_MODE.getVar())).trim());
    LOG.debug("Testing mode is " + isTestingMode);
    if(!isTestingMode) {
      String authMethod = Strings.nullToEmpty(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)).trim();
      if("none".equalsIgnoreCase(authMethod)) {
        throw new InvalidConfigurationException("Authentication can't be NONE in non-testing mode");
      }
      boolean impersonation = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS);
      boolean allowImpersonation = Boolean.parseBoolean(Strings.nullToEmpty(
          authzConf.get(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION.getVar())).trim());

      if(impersonation && !allowImpersonation) {
        LOG.error("Role based authorization does not work with HiveServer2 impersonation");
        return new NoAuthorizationProvider();
      }
    }
    String defaultUmask = hiveConf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
    if("077".equalsIgnoreCase(defaultUmask)) {
      LOG.error("HiveServer2 required a default umask of 077");
      return new NoAuthorizationProvider();
    }
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
      for (List<DBModelAuthorizable> inputHierarchy : inputHierarchyList) {
        if(isDebug) {
          LOG.debug("requiredInputPrivileges = " + requiredInputPrivileges);
          LOG.debug("inputHierarchy = " + inputHierarchy);
          LOG.debug("getAuthzType(inputHierarchy) = " + getAuthzType(inputHierarchy));
        }
        if (requiredInputPrivileges.containsKey(getAuthzType(inputHierarchy))) {
          EnumSet<DBModelAction> inputPrivSet =
            requiredInputPrivileges.get(getAuthzType(inputHierarchy));
          if (!authProvider.hasAccess(subject, inputHierarchy, inputPrivSet, activeRoleSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have privileges for " + hiveOp.name());
          }
        }
      }
      // Check write entities
      Map<AuthorizableType, EnumSet<DBModelAction>> requiredOutputPrivileges =
          stmtAuthPrivileges.getOutputPrivileges();
      for (List<DBModelAuthorizable> outputHierarchy : outputHierarchyList) {
        if(isDebug) {
          LOG.debug("requiredOutputPrivileges = " + requiredOutputPrivileges);
          LOG.debug("outputHierarchy = " + outputHierarchy);
          LOG.debug("getAuthzType(outputHierarchy) = " + getAuthzType(outputHierarchy));
        }
        if (requiredOutputPrivileges.containsKey(getAuthzType(outputHierarchy))) {
          EnumSet<DBModelAction> outputPrivSet =
            requiredOutputPrivileges.get(getAuthzType(outputHierarchy));
          if (!authProvider.hasAccess(subject, outputHierarchy, outputPrivSet, activeRoleSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have priviliedges for " + hiveOp.name());
          }
        }
      }
  }

  public void setActiveRoleSet(String activeRoleSet) {
    this.activeRoleSet = parseActiveRoleSet(activeRoleSet);
    hiveConf.set(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET, activeRoleSet);
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

  private AuthorizableType getAuthzType (List<DBModelAuthorizable> hierarchy){
    return hierarchy.get(hierarchy.size() -1).getAuthzType();
  }

  public List<String> getLastQueryPrivilegeErrors() {
    if (!open) {
      throw new IllegalStateException("Binding has been closed");
    }
    return authProvider.getLastFailedPrivileges();
  }
}
