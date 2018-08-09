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

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.CodeSource;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.SentryFilterDDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.authz.HiveAuthzBindingHookBase;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HiveAuthzBindingHook extends HiveAuthzBindingHookBase {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBindingHook.class);

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

        for (Node childNode : ast.getChildren()) {
          ASTNode childASTNode = (ASTNode) childNode;
          if ("TOK_TABLESERIALIZER".equals(childASTNode.getText())) {
            ASTNode serdeNode = (ASTNode)childASTNode.getChild(0);
            String serdeClassName = BaseSemanticAnalyzer.unescapeSQLString(serdeNode.getChild(0).getText());
            setSerdeURI(serdeClassName);
          }
        }
      /* FALLTHROUGH */
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
      case HiveParser.TOK_ALTERVIEW:
      case HiveParser.TOK_CREATEINDEX:
      case HiveParser.TOK_DROPINDEX:
      case HiveParser.TOK_LOCKTABLE:
      case HiveParser.TOK_UNLOCKTABLE:
        currTab = extractTable((ASTNode)ast.getFirstChildWithType(HiveParser.TOK_TABNAME));
        currDB = extractDatabase((ASTNode) ast.getChild(0));
        indexURI = extractTableLocation(ast);//As index location is captured using token HiveParser.TOK_TABLELOCATION
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
        extractDbTableNameFromTOKTABLE((ASTNode) ast.getChild(1));
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
          udfURIs.add(parseURI(udfSrc.getLocation().toString(), true));
        } catch (ClassNotFoundException e) {
          List<String> functionJars = getFunctionJars(ast);
          if (functionJars.isEmpty()) {
            throw new SemanticException("Error retrieving udf class:" + e.getMessage(), e);
          } else {
            // Add the jars from the command "Create function using jar" to the access list
            // Defer to hive to check if the class is in the jars
            for(String jar : functionJars) {
              udfURIs.add(parseURI(jar, false));
            }
          }
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
        ASTNode tableTok = (ASTNode) ast.getChild(0).getChild(0);
        Preconditions.checkArgument(tableTok.getChildCount() >= 1);
        if (tableTok.getChildCount() == 1) {
          // If tableTok chilcount is 1, tableTok does not has database information, use current working DB
          currOutDB = extractDatabase((ASTNode) ast.getChild(0));
          currOutTab = extractTable((ASTNode) tableTok.getChild(0));
        } else {
          // If tableTok has fully-qualified name(childcount is 2),
          // get the db and table information from tableTok.
          extractDbTableNameFromTOKTABLE(tableTok);
        }
        break;
    case HiveParser.TOK_ALTERTABLE:
      currDB = getCanonicalDb();
      for (Node childNode : ast.getChildren()) {
        ASTNode childASTNode = (ASTNode) childNode;
        if ("TOK_ALTERTABLE_SERIALIZER".equals(childASTNode.getText())) {
          ASTNode serdeNode = (ASTNode)childASTNode.getChild(0);
          String serdeClassName = BaseSemanticAnalyzer.unescapeSQLString(serdeNode.getText());
          setSerdeURI(serdeClassName);
        }
        if ("TOK_ALTERTABLE_RENAME".equals(childASTNode.getText())) {
          currDB = extractDatabase((ASTNode)ast.getChild(0));
          ASTNode newTableNode = (ASTNode)childASTNode.getChild(0);
          currOutDB = extractDatabase(newTableNode);
        }
      }

      break;

    case HiveParser.TOK_ALTERDATABASE_OWNER:
      currDB = currOutDB = new Database(ast.getChild(0).getText());
      break;
    default:
        currDB = getCanonicalDb();
        break;
    }
    return ast;
  }

  /**
   * Post analyze hook that invokes hive auth bindings
   */
  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    HiveOperation stmtOperation = context.getHiveOperation();
    HiveAuthzPrivileges stmtAuthObject;

    stmtAuthObject = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(stmtOperation);

    Subject subject = getCurrentSubject(context);

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
      StringBuilder permsBuilder = new StringBuilder();
      for (String perm : hiveAuthzBinding.getLastQueryPrivilegeErrors()) {
        permsBuilder.append(perm);
        permsBuilder.append(";");
      }
      String permsRequired = permsBuilder.toString();
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

}
