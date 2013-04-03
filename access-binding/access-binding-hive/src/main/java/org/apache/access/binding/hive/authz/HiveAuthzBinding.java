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
import java.util.Map;
import java.util.Set;

import org.apache.access.binding.hive.authz.HiveAuthzPrivileges.HiveObjectTypes;
import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.access.core.Action;
import org.apache.access.core.AuthorizationProvider;
import org.apache.access.core.Database;
import org.apache.access.core.Server;
import org.apache.access.core.ServerResource;
import org.apache.access.core.Subject;
import org.apache.access.core.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

public class HiveAuthzBinding {
  static final private Log LOG = LogFactory.getLog(HiveAuthzBinding.class.getName());

  // TODO: Push the wildcard Object to AuthorizationProvider, interface should provide way to pass implicit wildchar
  static Database anyDatabase = new Database("*");
  static Table anyTable = new Table("*");

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

    switch (stmtAuthPrivileges.getOperationScope()) {
    case SERVER :
      // validate server level privileges if applicable. Eg create UDF,register jar etc ..
      if (!authProvider.hasAccess(subject, authServer,
          getResoruce(hiveOp), EnumSet.of(Action.ALL))) {
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
      if (!authProvider.hasAccess(subject, authServer, currDB,
          anyTable, EnumSet.of(Action.ALL))) {
        throw new AuthorizationException("User " + subject.getName() +
            " does not have priviliedges for " + stmtAuthPrivileges.getOperationType().name());
      }
      break;
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

          if (!authProvider.hasAccess(subject, authServer,
              getDatabaseFromEntity(readEntity), getTableFromEntity(readEntity),
              inputPrivSet)) {
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
          EnumSet<Action> outputPrivSet =
              requiredOutputPrivileges.get(HiveObjectTypes.
                  convertHiveEntity(writeEntity.getType()));

          if (!authProvider.hasAccess(subject, authServer,
              getDatabaseFromEntity(writeEntity), getTableFromEntity(writeEntity),
              outputPrivSet)) {
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

  // Extract the DB from the hive entity
  private Database getDatabaseFromEntity(Entity entity) {
    switch (entity.getType()) {
    case TABLE:
      return new Database(entity.getTable().getDbName());
    case PARTITION:
      return new Database(entity.getPartition().getTable().getDbName());
    default:
      throw new UnsupportedOperationException("Unsupported entity type " +
          entity.getType().name());
    }
  }

  // Extract Table from the hive entity
  private Table getTableFromEntity(Entity entity) {
    switch (entity.getType()) {
    case TABLE:
      return new Table(entity.getTable().getTableName());
    case PARTITION:
      return new Table(entity.getPartition().getTable().getTableName());
    default:
      throw new UnsupportedOperationException("Unsupported entity type " +
          entity.getType().name());
    }
  }

  // Extract server resource
  private ServerResource getResoruce(HiveOperation hiveOp) {
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
