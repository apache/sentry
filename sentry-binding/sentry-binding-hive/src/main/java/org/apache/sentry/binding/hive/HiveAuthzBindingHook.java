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
package org.apache.sentry.binding.hive;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationType;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.cache.PrivilegeCache;
import org.apache.sentry.provider.cache.SimplePrivilegeCache;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class HiveAuthzBindingHook extends AbstractSemanticAnalyzerHook {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBindingHook.class);
  private final HiveAuthzBinding hiveAuthzBinding;
  private final HiveAuthzConf authzConf;
  private Database currDB = Database.ALL;
  private Table currTab;
  private AccessURI udfURI;
  private AccessURI partitionURI;
  private AccessURI indexURI;
  private Table currOutTab = null;
  private Database currOutDB = null;

  // True if this is a basic DESCRIBE <table> operation. False for other DESCRIBE variants
  // like DESCRIBE [FORMATTED|EXTENDED]. Required because Hive treats these stmts as the same
  // HiveOperationType, but we want to enforces different privileges on each statement.
  // Basic DESCRIBE <table> is allowed with only column-level privs, while the variants
  // require table-level privileges.
  public boolean isDescTableBasic = false;

  public HiveAuthzBindingHook() throws Exception {
    SessionState session = SessionState.get();
    if(session == null) {
      throw new IllegalStateException("Session has not been started");
    }
    // HACK: set a random classname to force the Auth V2 in Hive
    SessionState.get().setAuthorizer(null);

    HiveConf hiveConf = session.getConf();
    if(hiveConf == null) {
      throw new IllegalStateException("Session HiveConf is null");
    }
    authzConf = loadAuthzConf(hiveConf);
    hiveAuthzBinding = new HiveAuthzBinding(hiveConf, authzConf);

    FunctionRegistry.setupPermissionsForBuiltinUDFs("", HiveAuthzConf.HIVE_UDF_BLACK_LIST);
  }

  public static HiveAuthzConf loadAuthzConf(HiveConf hiveConf) {
    boolean depreicatedConfigFile = false;
    HiveAuthzConf newAuthzConf = null;
    String hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
    if(hiveAuthzConf == null || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
      hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_ACCESS_CONF_URL);
      depreicatedConfigFile = true;
    }

    if(hiveAuthzConf == null || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
      throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_SENTRY_CONF_URL
          + " value '" + hiveAuthzConf + "' is invalid.");
    }
    try {
      newAuthzConf = new HiveAuthzConf(new URL(hiveAuthzConf));
    } catch (MalformedURLException e) {
      if (depreicatedConfigFile) {
        throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_ACCESS_CONF_URL
            + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
      } else {
        throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_SENTRY_CONF_URL
            + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
      }
    }
    return newAuthzConf;
  }

  /**
   * Pre-analyze hook called after compilation and before semantic analysis We
   * extract things for to Database and metadata level operations which are not
   * capture in the input/output entities during semantic analysis. Ideally it
   * should be handled in Hive. We need to move most of these into hive semantic
   * analyzer and then remove it from the access hook.
   */
  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {

    switch (ast.getToken().getType()) {
    // Hive parser doesn't capture the database name in output entity, so we store it here for now
      case HiveParser.TOK_CREATEDATABASE:
      case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
      case HiveParser.TOK_DROPDATABASE:
      case HiveParser.TOK_SWITCHDATABASE:
      case HiveParser.TOK_DESCDATABASE:
        currDB = new Database(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText()));
        break;
      case HiveParser.TOK_CREATETABLE:
      case HiveParser.TOK_CREATEVIEW:
        /*
         * Compiler doesn't create read/write entities for create table.
         * Hence we need extract dbname from db.tab format, if applicable
         */
        currDB = extractDatabase((ASTNode)ast.getChild(0));
        break;
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_DROPVIEW:
      case HiveParser.TOK_SHOW_CREATETABLE:
      case HiveParser.TOK_ALTERTABLE_SERIALIZER:
      case HiveParser.TOK_ALTERVIEW_ADDPARTS:
      case HiveParser.TOK_ALTERVIEW_DROPPARTS:
      case HiveParser.TOK_ALTERVIEW_PROPERTIES:
      case HiveParser.TOK_ALTERVIEW_RENAME:
      case HiveParser.TOK_CREATEINDEX:
        indexURI = extractTableLocation(ast);//As index location is captured using token HiveParser.TOK_TABLELOCATION
        break;
      case HiveParser.TOK_DROPINDEX:
      case HiveParser.TOK_LOCKTABLE:
      case HiveParser.TOK_UNLOCKTABLE:
        currTab = extractTable((ASTNode)ast.getFirstChildWithType(HiveParser.TOK_TABNAME));
        currDB = extractDatabase((ASTNode) ast.getChild(0));
        break;
      case HiveParser.TOK_ALTERINDEX_REBUILD:
        currTab = extractTable((ASTNode)ast.getChild(0)); //type is not TOK_TABNAME
        currDB = extractDatabase((ASTNode) ast.getChild(0));
        break;
      case HiveParser.TOK_SHOW_TABLESTATUS:
        currDB = extractDatabase((ASTNode)ast.getChild(0));
        int children = ast.getChildCount();
        for (int i = 1; i < children; i++) {
          ASTNode child = (ASTNode) ast.getChild(i);
          if (child.getToken().getType() == HiveParser.Identifier) {
            currDB = new Database(child.getText());
            break;
          }
        }
        //loosing the requested privileges for possible wildcard tables, since
        //further authorization will be done at the filter step and those unwanted will
        //eventually be filtered out from the output
        currTab = Table.ALL;
        break;
      case HiveParser.TOK_ALTERTABLE_RENAME:
      case HiveParser.TOK_ALTERTABLE_PROPERTIES:
      case HiveParser.TOK_ALTERTABLE_DROPPARTS:
      case HiveParser.TOK_ALTERTABLE_RENAMECOL:
      case HiveParser.TOK_ALTERTABLE_ADDCOLS:
      case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
      case HiveParser.TOK_SHOW_TBLPROPERTIES:
      case HiveParser.TOK_SHOWINDEXES:
      case HiveParser.TOK_SHOWPARTITIONS:
        //token name TOK_TABNAME is not properly set in this case
        currTab = extractTable((ASTNode)ast.getChild(0));
        currDB = extractDatabase((ASTNode)ast.getChild(0));
        break;
      case HiveParser.TOK_MSCK:
        // token name TOK_TABNAME is not properly set in this case and child(0) does
        // not contain the table name.
        // TODO: Fix Hive to capture the table and DB name
        currOutTab = extractTable((ASTNode)ast.getChild(1));
        currOutDB  = extractDatabase((ASTNode)ast.getChild(0));
        break;
      case HiveParser.TOK_ALTERTABLE_ADDPARTS:
        /*
         * Compiler doesn't create read/write entities for create table.
         * Hence we need extract dbname from db.tab format, if applicable
         */
        currTab = extractTable((ASTNode)ast.getChild(0));
        currDB = extractDatabase((ASTNode)ast.getChild(0));
        partitionURI = extractPartition(ast);
        break;
      case HiveParser.TOK_CREATEFUNCTION:
        String udfClassName = BaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());
        try {
          CodeSource udfSrc =
              Class.forName(udfClassName, true, Utilities.getSessionSpecifiedClassLoader())
                  .getProtectionDomain().getCodeSource();
          if (udfSrc == null) {
            throw new SemanticException("Could not resolve the jar for UDF class " + udfClassName);
          }
          String udfJar = udfSrc.getLocation().getPath();
          if (udfJar == null || udfJar.isEmpty()) {
            throw new SemanticException("Could not find the jar for UDF class " + udfClassName +
                "to validate privileges");
          }
          udfURI = parseURI(udfSrc.getLocation().toString(), true);
        } catch (ClassNotFoundException e) {
          throw new SemanticException("Error retrieving udf class:" + e.getMessage(), e);
        }
        // create/drop function is allowed with any database
        currDB = Database.ALL;
        break;
      case HiveParser.TOK_DROPFUNCTION:
        // create/drop function is allowed with any database
        currDB = Database.ALL;
        break;

      case HiveParser.TOK_LOAD:
        String dbName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getChild(0).getChild(0).getText());
        currDB = new Database(dbName);
        break;
      case HiveParser.TOK_DESCTABLE:
        currDB = getCanonicalDb();
        // For DESCRIBE FORMATTED/EXTENDED ast will have an additional child node with value
        // "FORMATTED/EXTENDED".
        isDescTableBasic = (ast.getChildCount() == 1);
        break;
      case HiveParser.TOK_TRUNCATETABLE:
        // SENTRY-826:
        // Truncate empty partitioned table should throw SemanticException only if the
        // user does not have permission.
        // In postAnalyze, currOutDB and currOutTbl will be added into outputHierarchy
        // which will be validated in the hiveAuthzBinding.authorize method.
        Preconditions.checkArgument(ast.getChildCount() == 1);
        // childcount is 1 for table without partition, 2 for table with partitions
        Preconditions.checkArgument(ast.getChild(0).getChildCount() >= 1);
        Preconditions.checkArgument(ast.getChild(0).getChild(0).getChildCount() == 1);
        currOutDB = extractDatabase((ASTNode) ast.getChild(0));
        currOutTab = extractTable((ASTNode) ast.getChild(0).getChild(0).getChild(0));
        break;
      default:
        currDB = getCanonicalDb();
        break;
    }
    return ast;
  }

  // Find the current database for session
  private Database getCanonicalDb() {
    return new Database(SessionState.get().getCurrentDatabase());
  }

  private static AccessURI extractTableLocation(ASTNode ast) throws SemanticException {
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode)ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_TABLELOCATION ) {
        //We expect the child HiveParser.TOK_TABLELOCATION to contain the URI path
        if (child.getChildCount() == 1) {
          return parseURI(BaseSemanticAnalyzer.
                  unescapeSQLString(child.getChild(0).getText()));
        } else {
          LOG.error("Found Token HiveParser.TOK_TABLELOCATION, but was expecting the URI as its only child. " +
                  "This means it is possible that permissions on the URI are not checked for this command ");
        }
      }
    }
    LOG.info("Token HiveParser.TOK_TABLELOCATION not found in ast. This means command does not have a location clause");
    return null;
  }

  private Database extractDatabase(ASTNode ast) throws SemanticException {
    String tableName = BaseSemanticAnalyzer.getUnescapedName(ast);
    if (tableName.contains(".")) {
      return new Database((tableName.split("\\."))[0]);
    } else {
      return getCanonicalDb();
    }
  }
  private Table extractTable(ASTNode ast) throws SemanticException {
    String tableName = BaseSemanticAnalyzer.getUnescapedName(ast);
    if (tableName.contains(".")) {
      return new Table((tableName.split("\\."))[1]);
    } else {
      return new Table(tableName);
    }
  }

  @VisibleForTesting
  protected static AccessURI extractPartition(ASTNode ast) throws SemanticException {
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode)ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_PARTITIONLOCATION &&
          child.getChildCount() == 1) {
        return parseURI(BaseSemanticAnalyzer.
          unescapeSQLString(child.getChild(0).getText()));
      }
    }
    return null;
  }

  @VisibleForTesting
  protected static AccessURI parseURI(String uri) throws SemanticException {
    return parseURI(uri, false);
  }

  @VisibleForTesting
  protected static AccessURI parseURI(String uri, boolean isLocal)
      throws SemanticException {
    try {
      HiveConf conf = SessionState.get().getConf();
      String warehouseDir = conf.getVar(ConfVars.METASTOREWAREHOUSE);
      Path warehousePath = new Path(warehouseDir);

      // If warehousePath is an absolute path and a scheme is null and authority is null as well,
      // qualified it with default file system scheme and authority.
      if (warehousePath.isAbsoluteAndSchemeAuthorityNull()) {

        URI defaultUri = FileSystem.getDefaultUri(conf);
        warehousePath = warehousePath.makeQualified(defaultUri, warehousePath);
        warehouseDir = warehousePath.toUri().toString();
      }
      return new AccessURI(PathUtils.parseURI(warehouseDir, uri, isLocal));
    } catch (Exception e) {
      throw new SemanticException("Error parsing URI " + uri + ": " +
        e.getMessage(), e);
    }
  }

  /**
   * Post analyze hook that invokes hive auth bindings
   */
  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    HiveOperation stmtOperation = getCurrentHiveStmtOp();
    HiveAuthzPrivileges stmtAuthObject;

    stmtAuthObject = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(stmtOperation);

    // must occur above the null check on stmtAuthObject
    // since GRANT/REVOKE/etc are not authorized by binding layer at present
    Subject subject = getCurrentSubject(context);
    Set<String> subjectGroups = hiveAuthzBinding.getGroups(subject);
    for (Task<? extends Serializable> task : rootTasks) {
      if (task instanceof SentryGrantRevokeTask) {
        SentryGrantRevokeTask sentryTask = (SentryGrantRevokeTask)task;
        sentryTask.setHiveAuthzBinding(hiveAuthzBinding);
        sentryTask.setAuthzConf(authzConf);
        sentryTask.setSubject(subject);
        sentryTask.setSubjectGroups(subjectGroups);
        sentryTask.setIpAddress(context.getIpAddress());
        sentryTask.setOperation(stmtOperation);
      }
    }

    try {
      if (stmtAuthObject == null) {
        // We don't handle authorizing this statement
        return;
      }

      /**
       * Replace DDLTask using the SentryFilterDDLTask for protection,
       * such as "show column" only allow show some column that user can access to.
       * SENTRY-847
       */
      for (int i = 0; i < rootTasks.size(); i++) {
        Task<? extends Serializable> task = rootTasks.get(i);
        if (task instanceof DDLTask) {
          ShowColumnsDesc showCols = ((DDLTask) task).getWork().getShowColumnsDesc();
          if (showCols != null) {
            SentryFilterDDLTask filterTask =
                    new SentryFilterDDLTask(hiveAuthzBinding, subject, stmtOperation);
            filterTask.copyDDLTask((DDLTask) task);
            rootTasks.set(i, filterTask);
          }
        }
      }

      authorizeWithHiveBindings(context, stmtAuthObject, stmtOperation);
    } catch (AuthorizationException e) {
      executeOnFailureHooks(context, stmtOperation, e);
      String permsRequired = "";
      for (String perm : hiveAuthzBinding.getLastQueryPrivilegeErrors()) {
        permsRequired += perm + ";";
      }
      SessionState.get().getConf().set(HiveAuthzConf.HIVE_SENTRY_AUTH_ERRORS, permsRequired);
      String msgForLog = HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE
          + "\n Required privileges for this query: "
          + permsRequired;
      String msgForConsole = HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE + "\n "
          + e.getMessage()+ "\n The required privileges: " + permsRequired;
      // AuthorizationException is not a real exception, use the info level to record this.
      LOG.info(msgForLog);
      throw new SemanticException(msgForConsole, e);
    } finally {
      hiveAuthzBinding.close();
    }

    if ("true".equalsIgnoreCase(context.getConf().
        get(HiveAuthzConf.HIVE_SENTRY_MOCK_COMPILATION))) {
      throw new SemanticException(HiveAuthzConf.HIVE_SENTRY_MOCK_ERROR + " Mock query compilation aborted. Set " +
          HiveAuthzConf.HIVE_SENTRY_MOCK_COMPILATION + " to 'false' for normal query processing");
    }
  }

  private void executeOnFailureHooks(HiveSemanticAnalyzerHookContext context,
      HiveOperation hiveOp, AuthorizationException e) {
    SentryOnFailureHookContext hookCtx = new SentryOnFailureHookContextImpl(
        context.getCommand(), context.getInputs(), context.getOutputs(),
        hiveOp, currDB, currTab, udfURI, null, context.getUserName(),
        context.getIpAddress(), e, context.getConf());
    String csHooks = authzConf.get(
        HiveAuthzConf.AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(), "").trim();

    try {
      for (Hook aofh : getHooks(csHooks)) {
        ((SentryOnFailureHook)aofh).run(hookCtx);
      }
    } catch (Exception ex) {
      LOG.error("Error executing hook:", ex);
    }
  }

  public static void runFailureHook(SentryOnFailureHookContext hookContext,
      String csHooks) {
    try {
      for (Hook aofh : getHooks(csHooks)) {
        ((SentryOnFailureHook) aofh).run(hookContext);
      }
    } catch (Exception ex) {
      LOG.error("Error executing hook:", ex);
    }
  }
  /**
   * Convert the input/output entities into authorizables. generate
   * authorizables for cases like Database and metadata operations where the
   * compiler doesn't capture entities. invoke the hive binding to validate
   * permissions
   *
   * @param context
   * @param stmtAuthObject
   * @param stmtOperation
   * @throws AuthorizationException
   */
  private void authorizeWithHiveBindings(HiveSemanticAnalyzerHookContext context,
      HiveAuthzPrivileges stmtAuthObject, HiveOperation stmtOperation) throws  AuthorizationException {
    Set<ReadEntity> inputs = context.getInputs();
    Set<WriteEntity> outputs = context.getOutputs();
    List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
    List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();

    if(LOG.isDebugEnabled()) {
      LOG.debug("stmtAuthObject.getOperationScope() = " + stmtAuthObject.getOperationScope());
      LOG.debug("context.getInputs() = " + context.getInputs());
      LOG.debug("context.getOutputs() = " + context.getOutputs());
    }

    // Workaround to allow DESCRIBE <table> to be executed with only column-level privileges, while
    // still authorizing DESCRIBE [EXTENDED|FORMATTED] as table-level.
    // This is done by treating DESCRIBE <table> the same as SHOW COLUMNS, which only requires column
    // level privs.
    if (isDescTableBasic) {
      stmtAuthObject = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.SHOWCOLUMNS);
    }

    switch (stmtAuthObject.getOperationScope()) {

    case SERVER :
      // validate server level privileges if applicable. Eg create UDF,register jar etc ..
      List<DBModelAuthorizable> serverHierarchy = new ArrayList<DBModelAuthorizable>();
      serverHierarchy.add(hiveAuthzBinding.getAuthServer());
      inputHierarchy.add(serverHierarchy);
      break;
    case DATABASE:
      // workaround for database scope statements (create/alter/drop db)
      List<DBModelAuthorizable> dbHierarchy = new ArrayList<DBModelAuthorizable>();
      dbHierarchy.add(hiveAuthzBinding.getAuthServer());
      dbHierarchy.add(currDB);
      inputHierarchy.add(dbHierarchy);
      outputHierarchy.add(dbHierarchy);

      getInputHierarchyFromInputs(inputHierarchy, inputs);
      break;
    case TABLE:
      // workaround for add partitions
      if(partitionURI != null) {
        inputHierarchy.add(ImmutableList.of(hiveAuthzBinding.getAuthServer(), partitionURI));
      }
      if(indexURI != null) {
        outputHierarchy.add(ImmutableList.of(hiveAuthzBinding.getAuthServer(), indexURI));
      }

      getInputHierarchyFromInputs(inputHierarchy, inputs);
      for (WriteEntity writeEntity: outputs) {
        if (filterWriteEntity(writeEntity)) {
          continue;
        }
        List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
        entityHierarchy.add(hiveAuthzBinding.getAuthServer());
        entityHierarchy.addAll(getAuthzHierarchyFromEntity(writeEntity));
        outputHierarchy.add(entityHierarchy);
      }
      // workaround for metadata queries.
      // Capture the table name in pre-analyze and include that in the input entity list
      if (currTab != null) {
        List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
        externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
        externalAuthorizableHierarchy.add(currDB);
        externalAuthorizableHierarchy.add(currTab);
        inputHierarchy.add(externalAuthorizableHierarchy);
      }



      // workaround for DDL statements
      // Capture the table name in pre-analyze and include that in the output entity list
      if (currOutTab != null) {
        List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
        externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
        externalAuthorizableHierarchy.add(currOutDB);
        externalAuthorizableHierarchy.add(currOutTab);
        outputHierarchy.add(externalAuthorizableHierarchy);
      }
      break;
    case FUNCTION:
      /* The 'FUNCTION' privilege scope currently used for
       *  - CREATE TEMP FUNCTION
       *  - DROP TEMP FUNCTION.
       */
      if (udfURI != null) {
        List<DBModelAuthorizable> udfUriHierarchy = new ArrayList<DBModelAuthorizable>();
        udfUriHierarchy.add(hiveAuthzBinding.getAuthServer());
        udfUriHierarchy.add(udfURI);
        inputHierarchy.add(udfUriHierarchy);
        for (WriteEntity writeEntity : outputs) {
          List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
          entityHierarchy.add(hiveAuthzBinding.getAuthServer());
          entityHierarchy.addAll(getAuthzHierarchyFromEntity(writeEntity));
          outputHierarchy.add(entityHierarchy);
        }
      }
      break;
    case CONNECT:
      /* The 'CONNECT' is an implicit privilege scope currently used for
       *  - USE <db>
       *  It's allowed when the user has any privilege on the current database. For application
       *  backward compatibility, we allow (optional) implicit connect permission on 'default' db.
       */
      List<DBModelAuthorizable> connectHierarchy = new ArrayList<DBModelAuthorizable>();
      connectHierarchy.add(hiveAuthzBinding.getAuthServer());
      // by default allow connect access to default db
      Table currTbl = Table.ALL;
      Column currCol = Column.ALL;
      if ((DEFAULT_DATABASE_NAME.equalsIgnoreCase(currDB.getName()) &&
          "false".equalsIgnoreCase(authzConf.
              get(HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "false")))) {
        currDB = Database.ALL;
        currTbl = Table.SOME;
      }

      connectHierarchy.add(currDB);
      connectHierarchy.add(currTbl);
      connectHierarchy.add(currCol);

      inputHierarchy.add(connectHierarchy);
      outputHierarchy.add(connectHierarchy);
      break;
    case COLUMN:
      for (ReadEntity readEntity: inputs) {
        if (readEntity.getAccessedColumns() != null && !readEntity.getAccessedColumns().isEmpty()) {
          addColumnHierarchy(inputHierarchy, readEntity);
        } else {
          List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
          entityHierarchy.add(hiveAuthzBinding.getAuthServer());
          entityHierarchy.addAll(getAuthzHierarchyFromEntity(readEntity));
          entityHierarchy.add(Column.ALL);
          inputHierarchy.add(entityHierarchy);
        }
      }
      break;
    default:
      throw new AuthorizationException("Unknown operation scope type " +
          stmtAuthObject.getOperationScope().toString());
    }

    HiveAuthzBinding binding = null;
    try {
      binding = getHiveBindingWithPrivilegeCache(hiveAuthzBinding, context.getUserName());
    } catch (SemanticException e) {
      // Will use the original hiveAuthzBinding
      binding = hiveAuthzBinding;
    }
    // validate permission
    binding.authorize(stmtOperation, stmtAuthObject, getCurrentSubject(context), inputHierarchy,
        outputHierarchy);
  }

  private HiveOperation getCurrentHiveStmtOp() {
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
  private List<DBModelAuthorizable> getAuthzHierarchyFromEntity(Entity entity) {
    List<DBModelAuthorizable> objectHierarchy = new ArrayList<DBModelAuthorizable>();
    switch (entity.getType()) {
    case TABLE:
      objectHierarchy.add(new Database(entity.getTable().getDbName()));
      objectHierarchy.add(new Table(entity.getTable().getTableName()));
      break;
    case PARTITION:
    case DUMMYPARTITION:
      objectHierarchy.add(new Database(entity.getPartition().getTable().getDbName()));
      objectHierarchy.add(new Table(entity.getPartition().getTable().getTableName()));
      break;
    case DFS_DIR:
    case LOCAL_DIR:
      try {
        objectHierarchy.add(parseURI(entity.toString(),
            entity.getType().equals(Entity.Type.LOCAL_DIR)));
      } catch (Exception e) {
        throw new AuthorizationException("Failed to get File URI", e);
      }
      break;
    case DATABASE:
    case FUNCTION:
      // TODO use database entities from compiler instead of capturing from AST
      break;
    default:
      throw new UnsupportedOperationException("Unsupported entity type " +
          entity.getType().name());
    }
    return objectHierarchy;
  }

  /**
   * Add column level hierarchy to inputHierarchy
   *
   * @param inputHierarchy
   * @param entity
   * @param sentryContext
   */
  private void addColumnHierarchy(List<List<DBModelAuthorizable>> inputHierarchy,
      ReadEntity entity) {
    List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
    entityHierarchy.add(hiveAuthzBinding.getAuthServer());
    entityHierarchy.addAll(getAuthzHierarchyFromEntity(entity));

    switch (entity.getType()) {
    case TABLE:
    case PARTITION:
      List<String> cols = entity.getAccessedColumns();
      for (String col : cols) {
        List<DBModelAuthorizable> colHierarchy = new ArrayList<DBModelAuthorizable>(entityHierarchy);
        colHierarchy.add(new Column(col));
        inputHierarchy.add(colHierarchy);
      }
      break;
    default:
      inputHierarchy.add(entityHierarchy);
    }
  }

  /**
   * Get Authorizable from inputs and put into inputHierarchy
   *
   * @param inputHierarchy
   * @param entity
   * @param sentryContext
   */
  private void getInputHierarchyFromInputs(List<List<DBModelAuthorizable>> inputHierarchy,
      Set<ReadEntity> inputs) {
    for (ReadEntity readEntity: inputs) {
      // skip the tables/view that are part of expanded view definition
      // skip the Hive generated dummy entities created for queries like 'select <expr>'
      if (isChildTabForView(readEntity) || isDummyEntity(readEntity)) {
        continue;
      }
      if (readEntity.getAccessedColumns() != null && !readEntity.getAccessedColumns().isEmpty()) {
        addColumnHierarchy(inputHierarchy, readEntity);
      } else {
        List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
        entityHierarchy.add(hiveAuthzBinding.getAuthServer());
        entityHierarchy.addAll(getAuthzHierarchyFromEntity(readEntity));
        inputHierarchy.add(entityHierarchy);
      }
    }
  }

  // Check if this write entity needs to skipped
  private boolean filterWriteEntity(WriteEntity writeEntity)
      throws AuthorizationException {
    // skip URI validation for session scratch file URIs
    if (writeEntity.isTempURI()) {
      return true;
    }
    try {
      if (writeEntity.getTyp().equals(Type.DFS_DIR)
          || writeEntity.getTyp().equals(Type.LOCAL_DIR)) {
        HiveConf conf = SessionState.get().getConf();
        String warehouseDir = conf.getVar(ConfVars.METASTOREWAREHOUSE);
        URI scratchURI = new URI(PathUtils.parseDFSURI(warehouseDir,
          conf.getVar(HiveConf.ConfVars.SCRATCHDIR)));
        URI requestURI = new URI(PathUtils.parseDFSURI(warehouseDir,
          writeEntity.getLocation().getPath()));
        LOG.debug("scratchURI = " + scratchURI + ", requestURI = " + requestURI);
        if (PathUtils.impliesURI(scratchURI, requestURI)) {
          return true;
        }
        URI localScratchURI = new URI(PathUtils.parseLocalURI(conf.getVar(HiveConf.ConfVars.LOCALSCRATCHDIR)));
        URI localRequestURI = new URI(PathUtils.parseLocalURI(writeEntity.getLocation().getPath()));
        LOG.debug("localScratchURI = " + localScratchURI + ", localRequestURI = " + localRequestURI);
        if (PathUtils.impliesURI(localScratchURI, localRequestURI)) {
          return true;
        }
      }
    } catch (Exception e) {
      throw new AuthorizationException("Failed to extract uri details", e);
    }
    return false;
  }

  public static List<String> filterShowTables(
      HiveAuthzBinding hiveAuthzBinding, List<String> queryResult,
      HiveOperation operation, String userName, String dbName)
          throws SemanticException {
    List<String> filteredResult = new ArrayList<String>();
    Subject subject = new Subject(userName);
    HiveAuthzPrivileges tableMetaDataPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Column, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.INFO).
        build();

    HiveAuthzBinding hiveBindingWithPrivilegeCache = getHiveBindingWithPrivilegeCache(hiveAuthzBinding, userName);

    for (String tableName : queryResult) {
      // if user has privileges on table, add to filtered list, else discard
      Table table = new Table(tableName);
      Database database;
      database = new Database(dbName);

      List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
      externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
      externalAuthorizableHierarchy.add(database);
      externalAuthorizableHierarchy.add(table);
      externalAuthorizableHierarchy.add(Column.ALL);
      inputHierarchy.add(externalAuthorizableHierarchy);

      try {
        // do the authorization by new HiveAuthzBinding with PrivilegeCache
        hiveBindingWithPrivilegeCache.authorize(operation, tableMetaDataPrivilege, subject,
            inputHierarchy, outputHierarchy);
        filteredResult.add(table.getName());
      } catch (AuthorizationException e) {
        // squash the exception, user doesn't have privileges, so the table is
        // not added to
        // filtered list.
        ;
      }
    }
    return filteredResult;
  }

  public static List<FieldSchema> filterShowColumns(
      HiveAuthzBinding hiveAuthzBinding, List<FieldSchema> cols,
      HiveOperation operation, String userName, String tableName, String dbName)
          throws SemanticException {
    List<FieldSchema> filteredResult = new ArrayList<FieldSchema>();
    Subject subject = new Subject(userName);
    HiveAuthzPrivileges columnMetaDataPrivilege =
        HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.SHOWCOLUMNS);
    HiveAuthzBinding hiveBindingWithPrivilegeCache = getHiveBindingWithPrivilegeCache(hiveAuthzBinding, userName);

    Database database = new Database(dbName);
    Table table = new Table(tableName);
    for (FieldSchema col : cols) {
      // if user has privileges on column, add to filtered list, else discard
      List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
      externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
      externalAuthorizableHierarchy.add(database);
      externalAuthorizableHierarchy.add(table);
      externalAuthorizableHierarchy.add(new Column(col.getName()));
      inputHierarchy.add(externalAuthorizableHierarchy);

      try {
        // do the authorization by new HiveAuthzBinding with PrivilegeCache
        hiveBindingWithPrivilegeCache.authorize(operation, columnMetaDataPrivilege, subject,
            inputHierarchy, outputHierarchy);
        filteredResult.add(col);
      } catch (AuthorizationException e) {
        // squash the exception, user doesn't have privileges, so the column is
        // not added to
        // filtered list.
        ;
      }
    }
    return filteredResult;
  }

  public static List<String> filterShowDatabases(
      HiveAuthzBinding hiveAuthzBinding, List<String> queryResult,
      HiveOperation operation, String userName) throws SemanticException {
    List<String> filteredResult = new ArrayList<String>();
    Subject subject = new Subject(userName);
    HiveAuthzBinding hiveBindingWithPrivilegeCache = getHiveBindingWithPrivilegeCache(hiveAuthzBinding, userName);

    HiveAuthzPrivileges anyPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Column, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
        setOperationScope(HiveOperationScope.CONNECT).
        setOperationType(HiveOperationType.QUERY).
        build();

    for (String dbName:queryResult) {
      // if user has privileges on database, add to filtered list, else discard
      Database database = null;

      // if default is not restricted, continue
      if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(dbName) && "false".equalsIgnoreCase(
        hiveAuthzBinding.getAuthzConf().get(
              HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(),
              "false"))) {
        filteredResult.add(DEFAULT_DATABASE_NAME);
        continue;
      }

      database = new Database(dbName);

      List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
      externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
      externalAuthorizableHierarchy.add(database);
      externalAuthorizableHierarchy.add(Table.ALL);
      externalAuthorizableHierarchy.add(Column.ALL);
      inputHierarchy.add(externalAuthorizableHierarchy);

      try {
        // do the authorization by new HiveAuthzBinding with PrivilegeCache
        hiveBindingWithPrivilegeCache.authorize(operation, anyPrivilege, subject,
            inputHierarchy, outputHierarchy);
        filteredResult.add(database.getName());
      } catch (AuthorizationException e) {
        // squash the exception, user doesn't have privileges, so the table is
        // not added to
        // filtered list.
        ;
      }
    }

    return filteredResult;
  }

  /**
   * Check if the given read entity is a table that has parents of type Table
   * Hive compiler performs a query rewrite by replacing view with its definition. In the process, tt captures both
   * the original view and the tables/view that it selects from .
   * The access authorization is only interested in the top level views and not the underlying tables.
   * @param readEntity
   * @return
   */
  private boolean isChildTabForView(ReadEntity readEntity) {
    // If this is a table added for view, then we need to skip that
    if (!readEntity.getType().equals(Type.TABLE) && !readEntity.getType().equals(Type.PARTITION)) {
      return false;
    }
    if ((readEntity.getParents() != null) && (readEntity.getParents().size() > 0)) {
      for (ReadEntity parentEntity : readEntity.getParents()) {
        if (!parentEntity.getType().equals(Type.TABLE)) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns a set of hooks specified in a configuration variable.
   *
   * See getHooks(HiveAuthzConf.AuthzConfVars hookConfVar, Class<T> clazz)
   * @param hookConfVar
   * @return
   * @throws Exception
   */
  private static List<Hook> getHooks(String csHooks) throws Exception {
    return getHooks(csHooks, Hook.class);
  }

  /**
   * Returns the hooks specified in a configuration variable.  The hooks are returned in a list in
   * the order they were specified in the configuration variable.
   *
   * @param hookConfVar The configuration variable specifying a comma separated list of the hook
   *                    class names.
   * @param clazz       The super type of the hooks.
   * @return            A list of the hooks cast as the type specified in clazz, in the order
   *                    they are listed in the value of hookConfVar
   * @throws Exception
   */
  private static <T extends Hook> List<T> getHooks(String csHooks,
      Class<T> clazz)
      throws Exception {

    List<T> hooks = new ArrayList<T>();
    if (csHooks.isEmpty()) {
      return hooks;
    }
    for (String hookClass : Splitter.on(",").omitEmptyStrings().trimResults().split(csHooks)) {
      try {
        @SuppressWarnings("unchecked")
        T hook =
            (T) Class.forName(hookClass, true, JavaUtils.getClassLoader()).newInstance();
        hooks.add(hook);
      } catch (ClassNotFoundException e) {
        LOG.error(hookClass + " Class not found:" + e.getMessage());
        throw e;
      }
    }

    return hooks;
  }

  // Check if the given entity is identified as dummy by Hive compilers.
  private boolean isDummyEntity(Entity entity) {
    return entity.isDummy();
  }

  // create hiveBinding with PrivilegeCache
  private static HiveAuthzBinding getHiveBindingWithPrivilegeCache(HiveAuthzBinding hiveAuthzBinding,
      String userName) throws SemanticException {
    // get the original HiveAuthzBinding, and get the user's privileges by AuthorizationProvider
    AuthorizationProvider authProvider = hiveAuthzBinding.getCurrentAuthProvider();
    Set<String> userPrivileges = authProvider.getPolicyEngine().getPrivileges(
            authProvider.getGroupMapping().getGroups(userName), hiveAuthzBinding.getActiveRoleSet(),
            hiveAuthzBinding.getAuthServer());

    // create PrivilegeCache using user's privileges
    PrivilegeCache privilegeCache = new SimplePrivilegeCache(userPrivileges);
    try {
      // create new instance of HiveAuthzBinding whose backend provider should be SimpleCacheProviderBackend
      return new HiveAuthzBinding(HiveAuthzBinding.HiveHook.HiveServer2, hiveAuthzBinding.getHiveConf(),
              hiveAuthzBinding.getAuthzConf(), privilegeCache);
    } catch (Exception e) {
      LOG.error("Can not create HiveAuthzBinding with privilege cache.");
      throw new SemanticException(e);
    }
  }
}
