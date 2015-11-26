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
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.SentryFilterDDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.HiveAuthzBindingHook;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveAuthzBindingHookV2 extends AbstractSemanticAnalyzerHook {
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAuthzBindingHookV2.class);
  private final HiveAuthzBinding hiveAuthzBinding;
  private final HiveAuthzConf authzConf;

  public HiveAuthzBindingHookV2() throws Exception {
    SessionState session = SessionState.get();
    if(session == null) {
      throw new IllegalStateException("Session has not been started");
    }

    HiveConf hiveConf = session.getConf();
    if(hiveConf == null) {
      throw new IllegalStateException("Session HiveConf is null");
    }
    authzConf = HiveAuthzBindingHook.loadAuthzConf(hiveConf);
    hiveAuthzBinding = new HiveAuthzBinding(hiveConf, authzConf);
  }

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
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
  }

  private HiveOperation getCurrentHiveStmtOp() {
    SessionState sessState = SessionState.get();
    if (sessState == null) {
      LOG.warn("SessionState is null");
      return null;
    }
    return sessState.getHiveOperation();
  }

}
