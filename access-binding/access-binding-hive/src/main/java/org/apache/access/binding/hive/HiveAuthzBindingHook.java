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

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.access.binding.hive.authz.HiveAuthzBinding;
import org.apache.access.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.access.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.core.Database;
import org.apache.access.core.Subject;
import org.apache.access.core.Table;
import org.apache.hadoop.hive.ql.exec.Task;
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
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveAuthzBindingHook extends AbstractSemanticAnalyzerHook {
  // TODO: Push the wildcard Object to AuthorizationProvider, interface should provide way to pass implicit wildchar
  static Table anyTable = new Table("*");
  static Database anyDatabase = new Database("*");

  private final HiveAuthzBinding hiveAuthzBinding;
  private final HiveAuthzConf authzConf;
  private Database currDB = anyDatabase;

  public HiveAuthzBindingHook() throws Exception {
    authzConf = new HiveAuthzConf();
    hiveAuthzBinding = new HiveAuthzBinding(authzConf);
  }

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {

    switch (ast.getToken().getType()) {
    // Hive parser doesn't capture the database name in output entity, so we store it here for now
    case HiveParser.TOK_CREATEDATABASE:
    case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
      currDB = new Database(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText()));
      break;
    default:
      break;
    }
    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    Set<ReadEntity> inputs = context.getInputs();
    Set<WriteEntity> outputs = context.getOutputs();
    HiveOperation stmtOperation = getCurrentHiveStmtOp();

    HiveAuthzPrivileges stmtAuthObject =
        HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(stmtOperation);
    if (stmtAuthObject == null) {
      // We don't handle authorizing this statement
      return;
    }

    // validate permission
    try {
      hiveAuthzBinding.authorize(stmtOperation, stmtAuthObject, getCurrentSubject(),
          currDB, inputs, outputs);
    } catch (AuthorizationException e) {
      throw new SemanticException("No valid privileges", e);
    }
  }

  private HiveOperation getCurrentHiveStmtOp () {
    SessionState sessState = SessionState.get();
    if (sessState == null) {
      // TODO: Warn
      return null;
    }
    return sessState.getHiveOperation();
  }

  private Subject getCurrentSubject() {
    // TODO: Need to pass extract the username from Driver or session to this hook
    return new Subject("foo");
  }

}
