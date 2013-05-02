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
package org.apache.access.binding.hive.authz;

import java.lang.reflect.Constructor;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.access.core.Action;
import org.apache.access.core.Authorizable;
import org.apache.access.core.Authorizable.AuthorizableType;
import org.apache.access.core.AuthorizationProvider;
import org.apache.access.core.NoAuthorizationProvider;
import org.apache.access.core.Server;
import org.apache.access.core.ServerResource;
import org.apache.access.core.Subject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.base.Strings;

public class HiveAuthzBinding {
  private static final Log LOG = LogFactory.getLog(HiveAuthzBinding.class.getName());
  private static final Map<String, HiveAuthzBinding> authzBindingMap =
      new ConcurrentHashMap<String, HiveAuthzBinding>();
  private static final AtomicInteger queryID = new AtomicInteger();
  public static final String HIVE_BINDING_TAG = "hive.authz.bindings.tag";

  private final HiveAuthzConf authzConf;
  private final Server authServer;
  private AuthorizationProvider authProvider;

  public HiveAuthzBinding (HiveAuthzConf authzConf) throws Exception {
    this.authzConf = authzConf;
    this.authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
    this.authProvider = getAuthProvider(authServer.getName());
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
  private AuthorizationProvider getAuthProvider(String serverName) throws Exception {
    boolean isTestingMode = Boolean.parseBoolean(Strings.nullToEmpty(
        authzConf.get(AuthzConfVars.ACCESS_TESTING_MODE.getVar())).trim());
    HiveConf hiveConf = new HiveConf();
    LOG.debug("Testing mode is " + isTestingMode);
    if(!isTestingMode) {
      String authMethod = Strings.nullToEmpty(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)).trim();
      if("none".equalsIgnoreCase(authMethod)) {
        LOG.error("HiveServer2 authentication method cannot be set to none unless testing mode is enabled");
        return new NoAuthorizationProvider();
      }
      boolean impersonation = Boolean.parseBoolean(Strings.nullToEmpty(
          hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_IMPERSONATION)).trim());
      if(impersonation) {
        LOG.error("HiveServer2 does not work with impersonation");
        return new NoAuthorizationProvider();
      }
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
    LOG.debug("Going to authorize statement " + hiveOp.name() + " for subject " + subject.getName());

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
        if (requiredInputPrivileges.containsKey(getAuthzType(inputHierarchy))) {
          EnumSet<Action> inputPrivSet =
            requiredInputPrivileges.get(getAuthzType(inputHierarchy));
          if (!authProvider.hasAccess(subject, inputHierarchy, inputPrivSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have priviliedges for " + hiveOp.name());
          }
        }
      }

      // Check write entities
      Map<AuthorizableType, EnumSet<Action>> requiredOutputPrivileges =
          stmtAuthPrivileges.getOutputPrivileges();
      for (List<Authorizable> outputHierarchy : outputHierarchyList) {
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

  // Extract server resource
  private ServerResource getResource(HiveOperation hiveOp) {
    switch (hiveOp) {
    case CREATEFUNCTION:
      return ServerResource.UDFS;
    default:
      throw new UnsupportedOperationException("Unsupported ServerResource type " +
          hiveOp.name());
    }
  }

  private String printEntities (Set<? extends Entity> entitySet) {
    StringBuilder st = new StringBuilder();
    for (Entity e: entitySet) {
      st.append(e.toString() + ", ");
    }
    return st.toString();
  }

  private AuthorizableType getAuthzType (List<Authorizable> hierarchy){
    return hierarchy.get(hierarchy.size() -1).getAuthzType();
  }
}
