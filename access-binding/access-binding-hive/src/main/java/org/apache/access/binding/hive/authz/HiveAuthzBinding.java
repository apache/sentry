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

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.access.core.Action;
import org.apache.access.core.Authorizable;
import org.apache.access.core.Authorizable.AuthorizableType;
import org.apache.access.core.AuthorizationProvider;
import org.apache.access.core.Server;
import org.apache.access.core.ServerResource;
import org.apache.access.core.Subject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

public class HiveAuthzBinding {
  static final private Log LOG = LogFactory.getLog(HiveAuthzBinding.class.getName());

  private final HiveAuthzConf authzConf;
  private final Server authServer;
  private AuthorizationProvider authProvider;

  public HiveAuthzBinding (HiveAuthzConf authzConf) throws Exception {
    this.authzConf = authzConf;
    this.authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
    this.authProvider = getAuthProvider(authServer.getName());
  }

  // Instantiate the configured authz provider
  private AuthorizationProvider getAuthProvider(String serverName) throws Exception {
    // get the provider class and resources from the authz config
    String authProviderName = authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar());
    String resourceName =
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    LOG.debug("Using autherization provide " + authProviderName +
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
