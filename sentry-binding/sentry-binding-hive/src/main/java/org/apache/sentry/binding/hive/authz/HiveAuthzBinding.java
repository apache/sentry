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
import java.util.List;
import java.util.Map;
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
import org.apache.sentry.core.Action;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.AuthorizationProvider;
import org.apache.sentry.core.NoAuthorizationProvider;
import org.apache.sentry.core.Server;
import org.apache.sentry.core.Subject;
import org.apache.sentry.core.Authorizable.AuthorizableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.sentry.binding.hive.conf.InvalidConfigurationException;

import com.google.common.base.Strings;

public class HiveAuthzBinding {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBinding.class);
  private static final Map<String, HiveAuthzBinding> authzBindingMap =
      new ConcurrentHashMap<String, HiveAuthzBinding>();
  private static final AtomicInteger queryID = new AtomicInteger();
  public static final String HIVE_BINDING_TAG = "hive.authz.bindings.tag";

  private final HiveAuthzConf authzConf;
  private final Server authServer;
  private final AuthorizationProvider authProvider;

  public HiveAuthzBinding (HiveConf hiveConf, HiveAuthzConf authzConf) throws Exception {
    this.authzConf = authzConf;
    this.authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
    this.authProvider = getAuthProvider(hiveConf, authServer.getName());
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
    String tagName = SessionState.get().getSessionId() + "_" + queryID.incrementAndGet();
    authzBindingMap.put(tagName, this);
    conf.set(HIVE_BINDING_TAG, tagName);
  }

  /**
   * remove the authzBindingMap entry for given tag
   * @param conf
   */
  public void clear(Configuration conf) {
    String tagName = conf.get(HIVE_BINDING_TAG);
    if (tagName == null) {
      authzBindingMap.remove(tagName);
    }
  }

  // Instantiate the configured authz provider
  private AuthorizationProvider getAuthProvider(HiveConf hiveConf, String serverName) throws Exception {
    boolean isTestingMode = Boolean.parseBoolean(Strings.nullToEmpty(
        authzConf.get(AuthzConfVars.SENTRY_TESTING_MODE.getVar())).trim());
    LOG.debug("Testing mode is " + isTestingMode);
    if(!isTestingMode) {
      String authMethod = Strings.nullToEmpty(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)).trim();
      if("none".equalsIgnoreCase(authMethod)) {
        throw new InvalidConfigurationException("Authentication can't be NONE in non-testing mode");
      }
      boolean impersonation = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_KERBEROS_IMPERSONATION);
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
    LOG.debug("Using authorization provide " + authProviderName +
        " with resource " + resourceName);

    // load the authz provider class
    Constructor<?> constrctor =
        Class.forName(authProviderName).getDeclaredConstructor(String.class, String.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {resourceName, serverName});
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
      Subject subject, List<List<Authorizable>> inputHierarchyList, List<List<Authorizable>> outputHierarchyList )
          throws AuthorizationException {
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
      Map<AuthorizableType, EnumSet<Action>> requiredInputPrivileges =
          stmtAuthPrivileges.getInputPrivileges();
      for (List<Authorizable> inputHierarchy : inputHierarchyList) {
        if(isDebug) {
          LOG.debug("requiredInputPrivileges = " + requiredInputPrivileges);
          LOG.debug("inputHierarchy = " + inputHierarchy);
          LOG.debug("getAuthzType(inputHierarchy) = " + getAuthzType(inputHierarchy));
        }
        if (requiredInputPrivileges.containsKey(getAuthzType(inputHierarchy))) {
          EnumSet<Action> inputPrivSet =
            requiredInputPrivileges.get(getAuthzType(inputHierarchy));
          if (!authProvider.hasAccess(subject, inputHierarchy, inputPrivSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have privileges for " + hiveOp.name());
          }
        }
      }
      // Check write entities
      Map<AuthorizableType, EnumSet<Action>> requiredOutputPrivileges =
          stmtAuthPrivileges.getOutputPrivileges();
      for (List<Authorizable> outputHierarchy : outputHierarchyList) {
        if(isDebug) {
          LOG.debug("requiredOutputPrivileges = " + requiredOutputPrivileges);
          LOG.debug("outputHierarchy = " + outputHierarchy);
          LOG.debug("getAuthzType(outputHierarchy) = " + getAuthzType(outputHierarchy));
        }
        if (requiredOutputPrivileges.containsKey(getAuthzType(outputHierarchy))) {
          EnumSet<Action> outputPrivSet =
            requiredOutputPrivileges.get(getAuthzType(outputHierarchy));
          if (!authProvider.hasAccess(subject, outputHierarchy, outputPrivSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have priviliedges for " + hiveOp.name());
          }
        }
      }
  }

  public Server getAuthServer() {
    return authServer;
  }

  private AuthorizableType getAuthzType (List<Authorizable> hierarchy){
    return hierarchy.get(hierarchy.size() -1).getAuthzType();
  }
}
