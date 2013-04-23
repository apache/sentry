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
package org.apache.access.binding.hive;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.Serializable;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.access.binding.hive.authz.HiveAuthzBinding;
import org.apache.access.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.access.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.core.AccessURI;
import org.apache.access.core.Authorizable;
import org.apache.access.core.Database;
import org.apache.access.core.Subject;
import org.apache.access.core.Table;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveAuthzBindingHook extends AbstractSemanticAnalyzerHook {

  private final HiveAuthzBinding hiveAuthzBinding;
  private final HiveAuthzConf authzConf;
  private Database currDB = Database.ALL;
  private Table currTab = null;
  private AccessURI udfURI = null;

  public HiveAuthzBindingHook() throws Exception {
    authzConf = new HiveAuthzConf();
    hiveAuthzBinding = new HiveAuthzBinding(authzConf);
  }

  /**
   * Pre-analyze hook called after compilation and before semantic analysis
   * We extract things for to Database and metadata level operations which are
   * not capture in the input/output entities during semantic analysis.
   * Ideally it should be handled in Hive. We need to move most of these into hive semantic analyzer
   * and then remove it from the access hook.
   */
  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {

    SessionState.get().getConf().setBoolVar(ConfVars.HIVE_ENITITY_CAPTURE_INPUT_URI, true);
    switch (ast.getToken().getType()) {
    // Hive parser doesn't capture the database name in output entity, so we store it here for now
    case HiveParser.TOK_CREATEDATABASE:
    case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
    case HiveParser.TOK_DROPDATABASE:
    case HiveParser.TOK_SWITCHDATABASE:
      currDB = new Database(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText()));
      break;
    case HiveParser.TOK_DESCDATABASE:
      currDB = new Database(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText()));
      break;
    case HiveParser.TOK_CREATEFUNCTION:
      String udfClassName = BaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());
      try {
        CodeSource udfSrc = Class.forName(udfClassName).getProtectionDomain().getCodeSource();
        if (udfSrc == null) {
          throw new SemanticException("Could not resolve the jar for UDF class " + udfClassName);
        }
        String udfJar = udfSrc.getLocation().getPath();
        if (udfJar == null || udfJar.isEmpty()) {
          throw new SemanticException("Could not find the jar for UDF class " + udfClassName +
              "to validate privileges");
        }
        udfURI = new AccessURI(udfJar);
      } catch (ClassNotFoundException e) {
        throw new SemanticException("Error retrieving current db", e);
      }
      // create/drop function is allowed with any database
      currDB = Database.ALL;
      break;
    case HiveParser.TOK_DROPFUNCTION:
      // create/drop function is allowed with any database
      currDB = Database.ALL;
      break;
    case HiveParser.TOK_DESCTABLE:
      // describe doesn't support db.table format. so just extract the table name here.
      currTab = new Table(BaseSemanticAnalyzer.
          unescapeIdentifier(ast.getChild(0).getChild(0).getText()));
      // *** FALL THROUGH ***
    default:
      try {
        currDB = new Database(Hive.get().getCurrentDatabase());
      } catch (HiveException e) {
        throw new SemanticException("Error retrieving current db", e);
      }
    }
    return ast;
  }

  /**
   * Post analyze hook that invokes hive auth bindings
   */
  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    HiveOperation stmtOperation = getCurrentHiveStmtOp();
    HiveAuthzPrivileges stmtAuthObject =
        HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(stmtOperation);
    if (stmtAuthObject == null) {
      // We don't handle authorizing this statement
      return;
    }
    try {
      authorizeWithHiveBindings(context, stmtAuthObject, stmtOperation);
    } catch (AuthorizationException e) {
      throw new SemanticException("No valid privileges", e);
    }
  }

  /**
   * Convert the input/output entities into authorizables.
   * generate authorizables for cases like Database and metadata operations where the compiler
   * doesn't capture entities.
   * invoke the hive binding to validate permissions
   * @param context
   * @param stmtAuthObject
   * @param stmtOperation
   * @throws AuthorizationException
   */
  private void authorizeWithHiveBindings(HiveSemanticAnalyzerHookContext context,
      HiveAuthzPrivileges stmtAuthObject, HiveOperation stmtOperation) throws  AuthorizationException {

    Set<ReadEntity> inputs = context.getInputs();
    Set<WriteEntity> outputs = context.getOutputs();
    List<List<Authorizable>> inputHierarchy = new ArrayList<List<Authorizable>> ();
    List<List<Authorizable>> outputHierarchy = new ArrayList<List<Authorizable>> ();

    switch (stmtAuthObject.getOperationScope()) {
      case SERVER :
        // validate server level privileges if applicable. Eg create UDF,register jar etc ..
        List<Authorizable> serverHierarchy = new ArrayList<Authorizable>();
        serverHierarchy.add(hiveAuthzBinding.getAuthServer());
        inputHierarchy.add(serverHierarchy);
        break;
      case DATABASE:
        // workaround for database scope statements (create/alter/drop db)
        List<Authorizable> dbHierarchy = new ArrayList<Authorizable>();
        dbHierarchy.add(hiveAuthzBinding.getAuthServer());
        dbHierarchy.add(currDB);
        inputHierarchy.add(dbHierarchy);
        outputHierarchy.add(dbHierarchy);
        break;
      case TABLE:
        for (ReadEntity readEntity: inputs) {
          List<Authorizable> entityHierarchy = new ArrayList<Authorizable>();
          entityHierarchy.add(hiveAuthzBinding.getAuthServer());
          entityHierarchy.addAll(getAuthzHierarchyFromEntity(readEntity));
          inputHierarchy.add(entityHierarchy);
        }
        for (WriteEntity writeEntity: outputs) {
          if (filterWriteEntity(writeEntity)) {
            continue;
          }
          List<Authorizable> entityHierarchy = new ArrayList<Authorizable>();
          entityHierarchy.add(hiveAuthzBinding.getAuthServer());
          entityHierarchy.addAll(getAuthzHierarchyFromEntity(writeEntity));
          outputHierarchy.add(entityHierarchy);
        }
        // workaround for metadata queries.
        // Capture the table name in pre-analyze and include that in the entity list
        if (currTab != null) {
          List<Authorizable> externalAuthorizableHierarchy = new ArrayList<Authorizable>();
          externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
          externalAuthorizableHierarchy.add(currDB);
          externalAuthorizableHierarchy.add(currTab);
          inputHierarchy.add(externalAuthorizableHierarchy);
        }
        break;
      case CONNECT:
        /* The 'CONNECT' is an implicit privilege scope currently used for
         *  - CREATE TEMP FUNCTION
         *  - DROP TEMP FUNCTION
         *  - USE <db>
         *  It's allowed when the user has any privilege on the current database. For application
         *  backward compatibility, we allow (optional) implicit connect permission on 'default' db.
         */
        List<Authorizable> connectHierarchy = new ArrayList<Authorizable>();
        connectHierarchy.add(hiveAuthzBinding.getAuthServer());
        // by default allow connect access to default db
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(currDB.getName()) &&
            "false".equalsIgnoreCase(authzConf.
                get(HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "false"))) {
          currDB = Database.ALL;
        }
        connectHierarchy.add(currDB);
        connectHierarchy.add(Table.ALL);

        inputHierarchy.add(connectHierarchy);
        // check if this is a create temp function and we need to validate URI
        if (udfURI != null) {
          List<Authorizable> udfUriHierarchy = new ArrayList<Authorizable>();
          udfUriHierarchy.add(hiveAuthzBinding.getAuthServer());
          udfUriHierarchy.add(udfURI);
          inputHierarchy.add(udfUriHierarchy);
        }

        outputHierarchy.add(connectHierarchy);
        break;

      default:
        throw new AuthorizationException("Unknown operation scope type " +
            stmtAuthObject.getOperationScope().toString());
    }

    // validate permission
    hiveAuthzBinding.authorize(stmtOperation, stmtAuthObject, getCurrentSubject(context),
          inputHierarchy, outputHierarchy);
  }

  private HiveOperation getCurrentHiveStmtOp () {
    SessionState sessState = SessionState.get();
    if (sessState == null) {
      // TODO: Warn
      return null;
    }
    return sessState.getHiveOperation();
  }

  private Subject getCurrentSubject(HiveSemanticAnalyzerHookContext context) {
    // Extract the username from the hook context
    return new Subject(context.getUserName());
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

//Check if this write entity needs to skipped
  private boolean filterWriteEntity(WriteEntity writeEntity)
      throws AuthorizationException {
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

}
