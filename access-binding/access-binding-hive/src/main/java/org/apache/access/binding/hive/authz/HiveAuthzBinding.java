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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.access.binding.hive.authz.HiveAuthzPrivileges.HiveObjectTypes;
import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.access.core.AccessURI;
import org.apache.access.core.Action;
import org.apache.access.core.Authorizable;
import org.apache.access.core.AuthorizationProvider;
import org.apache.access.core.Database;
import org.apache.access.core.Server;
import org.apache.access.core.ServerResource;
import org.apache.access.core.Subject;
import org.apache.access.core.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveAuthzBinding {
  static final private Log LOG = LogFactory.getLog(HiveAuthzBinding.class.getName());

  private final HiveAuthzConf authzConf;
  private final Server authServer;
  private AuthorizationProvider authProvider;

  public HiveAuthzBinding (HiveAuthzConf authzConf) throws Exception {
    this.authzConf = authzConf;
    this.authProvider = getAuthProvider();
    authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
  }

  // Instantiate the configured authz provider
  private AuthorizationProvider getAuthProvider() throws Exception {
    // get the provider class and resources from the authz config
    String authProviderName = authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar());
    String resourceName =
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    LOG.debug("Using autherization provide " + authProviderName +
        " with resource " + resourceName);

    // load the authz provider class
    Constructor<?> constrctor =
        Class.forName(authProviderName).getDeclaredConstructor(String.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {resourceName});
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
      Subject subject, Database currDB, Set<ReadEntity> inputEntities,
      Set<WriteEntity> outputEntities)
          throws AuthorizationException {
    LOG.debug("Going to authorize statement " + hiveOp.name() + " for subject " + subject.getName());
    LOG.debug("Input Entities " + printEntities(inputEntities));
    LOG.debug("Output Entities " + printEntities(outputEntities));
    List<Authorizable> inputHierarchy = null;
    List<Authorizable> outputHierarchy = null;

    switch (stmtAuthPrivileges.getOperationScope()) {
    case SERVER :
      // validate server level privileges if applicable. Eg create UDF,register jar etc ..
      inputHierarchy = new ArrayList<Authorizable>();
      inputHierarchy.add(authServer);
      if (!authProvider.hasAccess(subject, inputHierarchy,  EnumSet.of(Action.ALL))) {
        throw new AuthorizationException("User " + subject.getName() +
              " does not have priviliedges for " + stmtAuthPrivileges.getOperationType().name());
      }
      break;
    case DATABASE:
      /**
       * validate database privileges for level operations like create db, alter db etc
       * Currently hive compiler doesn't capture db name all these cases so use the one saved
       * by the pre-hook
       */
      inputHierarchy = new ArrayList<Authorizable>();
      inputHierarchy.add(authServer);
      inputHierarchy.add(currDB);
      if (!authProvider.hasAccess(subject, inputHierarchy,  EnumSet.of(Action.ALL))) {
        throw new AuthorizationException("User " + subject.getName() +
            " does not have priviliedges for " + stmtAuthPrivileges.getOperationType().name());
      }
      /**
       * The DB level operation could have additional input/output entities, eg URIs
       * Fall through !!
       */
    case TABLE:
      /* for each read and write entity captured by the compiler -
       *    check if that object type is part of the input/output privilege list
       *    If it is, then validate the access.
       * Note the hive compiler gathers information on additional entities like partitions,
       * HDFS directories etc which are not of our interest at this point. Hence its very
       * much possible that the we won't be validating all the entities in the given list
       */

      // Check read entities
      Map<HiveObjectTypes, EnumSet<Action>> requiredInputPrivileges =
      stmtAuthPrivileges.getInputPrivileges();
      for (ReadEntity readEntity: inputEntities) {
        // check if the current statement needs any objects to be validated
        if (requiredInputPrivileges.containsKey(
            HiveObjectTypes.convertHiveEntity(readEntity.getType()))) {
          EnumSet<Action> inputPrivSet =
              requiredInputPrivileges.get(HiveObjectTypes.
                  convertHiveEntity(readEntity.getType()));

          inputHierarchy = new ArrayList<Authorizable>();
          inputHierarchy.add(authServer);
          inputHierarchy.addAll(getAuthzHierarchyFromEntity(readEntity));
          if (!authProvider.hasAccess(subject, inputHierarchy, inputPrivSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have priviliedges for " + hiveOp.name());
          }
        }
      }

      // Check write entities
      Map<HiveObjectTypes, EnumSet<Action>> requiredOutputPrivileges =
          stmtAuthPrivileges.getOutputPrivileges();
      for (WriteEntity writeEntity: outputEntities) {
        // check if the statement needs any objects to be validated as per validation policy
        if (requiredOutputPrivileges.containsKey(
            HiveObjectTypes.convertHiveEntity(writeEntity.getType()))) {
          // check if we want to skip this entity verification
            if (filterWriteEntity(writeEntity)) {
              continue;
            }
          EnumSet<Action> outputPrivSet =
              requiredOutputPrivileges.get(HiveObjectTypes.
                  convertHiveEntity(writeEntity.getType()));
          outputHierarchy = new ArrayList<Authorizable>();
          outputHierarchy.add(authServer);
          outputHierarchy.addAll(getAuthzHierarchyFromEntity(writeEntity));
          if (!authProvider.hasAccess(subject, outputHierarchy, outputPrivSet)) {
            throw new AuthorizationException("User " + subject.getName() +
                " does not have priviliedges for " + hiveOp.name());
          }
        }
      }
      break;
    default:
      throw new AuthorizationException("Unknown operation scope type " +
          stmtAuthPrivileges.getOperationScope().toString());
    }
  }

// Check if this write entity needs to skipped
  private boolean filterWriteEntity(WriteEntity writeEntity) {
    // skip URI validation for session scratch file URIs
    try {
      if (writeEntity.getTyp().equals(Type.DFS_DIR) ||
          writeEntity.getTyp().equals(Type.LOCAL_DIR)) {
        HiveConf conf = SessionState.get().getConf();
        if (writeEntity.getLocation().getPath().
            startsWith(conf.getVar(HiveConf.ConfVars.SCRATCHDIR))) {
          return true;
        }
        if (writeEntity.getLocation().getPath().
            startsWith(conf.getVar(HiveConf.ConfVars.LOCALSCRATCHDIR))) {
          return true;
        }
      }
    } catch (Exception e) {
      throw new AuthorizationException("Failed to extract uri details", e);
    }
    return false;
  }

  // Build the hierarchy of authorizable object for the given entity type.
  private List<Authorizable> getAuthzHierarchyFromEntity(Entity entity) {
    List<Authorizable> objectHierarchy = new ArrayList<Authorizable>();
    switch (entity.getType()) {
    case TABLE:
      objectHierarchy.add(new Database(entity.getTable().getDbName()));
      objectHierarchy.add(new Table(entity.getTable().getTableName()));
      break;
    case PARTITION:
      objectHierarchy.add(new Database(entity.getPartition().getTable().getDbName()));
      objectHierarchy.add(new Table(entity.getPartition().getTable().getTableName()));
      break;
    case DFS_DIR:
    case LOCAL_DIR:
      try {
        objectHierarchy.add(new AccessURI(entity.toString()));
      } catch (Exception e) {
        throw new AuthorizationException("Failed to get File URI", e);
      }
      break;
    default:
      throw new UnsupportedOperationException("Unsupported entity type " +
          entity.getType().name());
    }
    return objectHierarchy;
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
}
