/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.binding.hive.v2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SentryHivePrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactoryImpl;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;

public class SentryHiveAuthorizationTaskFactoryImplV2 extends HiveAuthorizationTaskFactoryImpl {

  public SentryHiveAuthorizationTaskFactoryImplV2(HiveConf conf, Hive db) {
    super(conf, db);
  }

  @Override
  protected PrivilegeObjectDesc parsePrivObject(ASTNode ast) throws SemanticException {
    SentryHivePrivilegeObjectDesc subject = new SentryHivePrivilegeObjectDesc();
    ASTNode child = (ASTNode) ast.getChild(0);
    ASTNode gchild = (ASTNode) child.getChild(0);
    if (child.getType() == HiveParser.TOK_TABLE_TYPE) {
      subject.setTable(true);
      String[] qualified = BaseSemanticAnalyzer.getQualifiedTableName(gchild);
      subject.setObject(BaseSemanticAnalyzer.getDotName(qualified));
    } else if (child.getType() == HiveParser.TOK_URI_TYPE) {
      subject.setUri(true);
      subject.setObject(gchild.getText());
    } else if (child.getType() == HiveParser.TOK_SERVER_TYPE) {
      subject.setServer(true);
      subject.setObject(gchild.getText());
    } else {
      subject.setTable(false);
      subject.setObject(BaseSemanticAnalyzer.unescapeIdentifier(gchild.getText()));
    }
    // if partition spec node is present, set partition spec
    for (int i = 1; i < child.getChildCount(); i++) {
      gchild = (ASTNode) child.getChild(i);
      if (gchild.getType() == HiveParser.TOK_PARTSPEC) {
        subject.setPartSpec(DDLSemanticAnalyzer.getPartSpec(gchild));
      } else if (gchild.getType() == HiveParser.TOK_TABCOLNAME) {
        subject.setColumns(BaseSemanticAnalyzer.getColumnNames(gchild));
      }
    }
    return subject;
  }
}
