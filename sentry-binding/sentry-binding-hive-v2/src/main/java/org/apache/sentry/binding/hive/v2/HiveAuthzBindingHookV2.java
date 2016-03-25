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
package org.apache.sentry.binding.hive.v2;

import java.io.Serializable;
import java.security.CodeSource;
import java.util.List;

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
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.HiveAuthzBindingHookBase;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveAuthzBindingHookV2 extends HiveAuthzBindingHookBase {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBindingHookV2.class);

  public HiveAuthzBindingHookV2() throws Exception {
    super();
  }

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
    switch (ast.getToken().getType()) {
    // Hive parser doesn't capture the database name in output entity, so we store it here for now
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
          throw new SemanticException("Error retrieving udf class", e);
        }
        // create/drop function is allowed with any database
        currDB = Database.ALL;
        break;
      case HiveParser.TOK_DROPFUNCTION:
        // create/drop function is allowed with any database
        currDB = Database.ALL;
        break;
      case HiveParser.TOK_CREATETABLE:
        for (Node childNode : ast.getChildren()) {
          ASTNode childASTNode = (ASTNode) childNode;
          if ("TOK_TABLESERIALIZER".equals(childASTNode.getText())) {
            ASTNode serdeNode = (ASTNode) childASTNode.getChild(0);
            String serdeClassName =
                BaseSemanticAnalyzer.unescapeSQLString(serdeNode.getChild(0).getText());
            setSerdeURI(serdeClassName);
          }
        }
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
    HiveOperation stmtOperation = getCurrentHiveStmtOp();
    Subject subject = new Subject(context.getUserName());
    for (int i = 0; i < rootTasks.size(); i++) {
      Task<? extends Serializable> task = rootTasks.get(i);
      if (task instanceof DDLTask) {
        SentryFilterDDLTask filterTask =
            new SentryFilterDDLTask(hiveAuthzBinding, subject, stmtOperation);
        filterTask.setWork((DDLWork)task.getWork());
        rootTasks.set(i, filterTask);
      }
    }
    HiveAuthzPrivileges stmtAuthObject = HiveAuthzPrivilegesMapV2.getHiveAuthzPrivileges(stmtOperation);
    if (stmtOperation.equals(HiveOperation.CREATEFUNCTION)
        || stmtOperation.equals(HiveOperation.DROPFUNCTION)
        || stmtOperation.equals(HiveOperation.CREATETABLE)) {
      try {
        if (stmtAuthObject == null) {
          // We don't handle authorizing this statement
          return;
        }

        authorizeWithHiveBindings(context, stmtAuthObject, stmtOperation);
      } catch (AuthorizationException e) {
        executeOnFailureHooks(context, stmtOperation, e);
        String permsRequired = "";
        for (String perm : hiveAuthzBinding.getLastQueryPrivilegeErrors()) {
          permsRequired += perm + ";";
        }
        SessionState.get().getConf().set(HiveAuthzConf.HIVE_SENTRY_AUTH_ERRORS, permsRequired);
        String msgForLog =
            HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE
                + "\n Required privileges for this query: " + permsRequired;
        String msgForConsole =
            HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE + "\n " + e.getMessage();
        // AuthorizationException is not a real exception, use the info level to record this.
        LOG.info(msgForLog);
        throw new SemanticException(msgForConsole, e);
      } finally {
        hiveAuthzBinding.close();
      }
    }
  }

}
