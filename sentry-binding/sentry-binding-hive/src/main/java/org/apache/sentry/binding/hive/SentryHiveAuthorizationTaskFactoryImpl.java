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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.exec.SentryGrantRevokeTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactory;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.RevokeDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.ShowGrantDesc;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeRegistry;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.core.model.db.AccessConstants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SentryHiveAuthorizationTaskFactoryImpl implements HiveAuthorizationTaskFactory {


  public SentryHiveAuthorizationTaskFactoryImpl(HiveConf conf, Hive db) {

  }

  @Override
  public Task<? extends Serializable> createCreateRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    String roleName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
    if (AccessConstants.RESERVED_ROLE_NAMES.contains(roleName.toUpperCase())) {
      String msg = "Roles cannot be one of the reserved roles: " + AccessConstants.RESERVED_ROLE_NAMES;
      throw new SemanticException(msg);
    }
    RoleDDLDesc roleDesc = new RoleDDLDesc(roleName, RoleDDLDesc.RoleOperation.CREATE_ROLE);
    return createTask(new DDLWork(inputs, outputs, roleDesc));
  }
  @Override
  public Task<? extends Serializable> createDropRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    String roleName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
    if (AccessConstants.RESERVED_ROLE_NAMES.contains(roleName.toUpperCase())) {
      String msg = "Roles cannot be one of the reserved roles: " + AccessConstants.RESERVED_ROLE_NAMES;
      throw new SemanticException(msg);
    }
    RoleDDLDesc roleDesc = new RoleDDLDesc(roleName, RoleDDLDesc.RoleOperation.DROP_ROLE);
    return createTask(new DDLWork(inputs, outputs, roleDesc));
  }
  @Override
  public Task<? extends Serializable> createShowRoleGrantTask(ASTNode ast, Path resultFile,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) throws SemanticException {
    ASTNode child = (ASTNode) ast.getChild(0);
    PrincipalType principalType = PrincipalType.USER;
    switch (child.getType()) {
    case HiveParser.TOK_USER:
      principalType = PrincipalType.USER;
      break;
    case HiveParser.TOK_GROUP:
      principalType = PrincipalType.GROUP;
      break;
    case HiveParser.TOK_ROLE:
      principalType = PrincipalType.ROLE;
      break;
    }
    if (principalType != PrincipalType.GROUP) {
      String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principalType;
      throw new SemanticException(msg);
    }
    String principalName = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
    RoleDDLDesc roleDesc = new RoleDDLDesc(principalName, principalType,
        RoleDDLDesc.RoleOperation.SHOW_ROLE_GRANT, null);
    roleDesc.setResFile(resultFile.toString());
    return createTask(new DDLWork(inputs, outputs,  roleDesc));
  }

  @Override
  public Task<? extends Serializable> createGrantTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    List<PrivilegeDesc> privilegeDesc = analyzePrivilegeListDef(
        (ASTNode) ast.getChild(0));
    List<PrincipalDesc> principalDesc = analyzePrincipalListDef(
        (ASTNode) ast.getChild(1));
    PrivilegeObjectDesc privilegeObj = null;

    if (ast.getChildCount() > 2) {
      for (int i = 2; i < ast.getChildCount(); i++) {
        ASTNode astChild = (ASTNode) ast.getChild(i);
        if (astChild.getType() == HiveParser.TOK_GRANT_WITH_OPTION) {
          throw new SemanticException(SentryHiveConstants.GRANT_OPTION_NOT_SUPPORTED);
        } else if (astChild.getType() == HiveParser.TOK_PRIV_OBJECT) {
          privilegeObj = analyzePrivilegeObject(astChild);
        }
      }
    }
    String userName = null;
    if (SessionState.get() != null
        && SessionState.get().getAuthenticator() != null) {
      userName = SessionState.get().getAuthenticator().getUserName();
    }
    Preconditions.checkNotNull(privilegeObj, "privilegeObj is null for " + ast.dump());
    if (privilegeObj.getPartSpec() != null) {
      throw new SemanticException(SentryHiveConstants.PARTITION_PRIVS_NOT_SUPPORTED);
    }
    for (PrivilegeDesc privDesc : privilegeDesc) {
      List<String> columns = privDesc.getColumns();
      if (columns != null && !columns.isEmpty()) {
        throw new SemanticException(SentryHiveConstants.COLUMN_PRIVS_NOT_SUPPORTED);
      }
    }
    for (PrincipalDesc princ : principalDesc) {
      if (princ.getType() != PrincipalType.ROLE) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + princ.getType();
        throw new SemanticException(msg);
      }
    }
    GrantDesc grantDesc = new GrantDesc(privilegeObj, privilegeDesc,
        principalDesc, userName, PrincipalType.USER, false);
    return createTask(new DDLWork(inputs, outputs, grantDesc));
  }
  @Override
  public Task<? extends Serializable> createRevokeTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    List<PrivilegeDesc> privilegeDesc = analyzePrivilegeListDef((ASTNode) ast.getChild(0));
    List<PrincipalDesc> principalDesc = analyzePrincipalListDef((ASTNode) ast.getChild(1));
    PrivilegeObjectDesc privilegeObj = null;
    if (ast.getChildCount() > 2) {
      ASTNode astChild = (ASTNode) ast.getChild(2);
      privilegeObj = analyzePrivilegeObject(astChild);
    }
    if (privilegeObj.getPartSpec() != null) {
      throw new SemanticException(SentryHiveConstants.PARTITION_PRIVS_NOT_SUPPORTED);
    }
    for (PrivilegeDesc privDesc : privilegeDesc) {
      List<String> columns = privDesc.getColumns();
      if (columns != null && !columns.isEmpty()) {
        throw new SemanticException(SentryHiveConstants.COLUMN_PRIVS_NOT_SUPPORTED);
      }
    }
    for (PrincipalDesc princ : principalDesc) {
      if (princ.getType() != PrincipalType.ROLE) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + princ.getType();
        throw new SemanticException(msg);
      }
    }
    RevokeDesc revokeDesc = new RevokeDesc(privilegeDesc, principalDesc, privilegeObj);
    return createTask(new DDLWork(inputs, outputs, revokeDesc));
  }

  @Override
  public Task<? extends Serializable> createGrantRoleTask(ASTNode ast, HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs)
      throws SemanticException {
    return analyzeGrantRevokeRole(true, ast, inputs, outputs);
  }

  @Override
  public Task<? extends Serializable> createShowGrantTask(ASTNode ast, Path resultFile, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    PrivilegeObjectDesc privHiveObj = null;

    ASTNode principal = (ASTNode) ast.getChild(0);
    PrincipalType type = PrincipalType.USER;
    switch (principal.getType()) {
    case HiveParser.TOK_USER:
      type = PrincipalType.USER;
      break;
    case HiveParser.TOK_GROUP:
      type = PrincipalType.GROUP;
      break;
    case HiveParser.TOK_ROLE:
      type = PrincipalType.ROLE;
      break;
    }
    if (type != PrincipalType.ROLE) {
      String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + type;
      throw new SemanticException(msg);
    }
    String principalName = BaseSemanticAnalyzer.unescapeIdentifier(principal.getChild(0).getText());
    PrincipalDesc principalDesc = new PrincipalDesc(principalName, type);
    if (ast.getChildCount() > 1) {
      ASTNode child = (ASTNode) ast.getChild(1);
      if (child.getToken().getType() == HiveParser.TOK_PRIV_OBJECT_COL) {
        privHiveObj = new PrivilegeObjectDesc();
        privHiveObj.setObject(BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText()));
        if (child.getChildCount() > 1) {
          for (int i = 1; i < child.getChildCount(); i++) {
            ASTNode grandChild = (ASTNode) child.getChild(i);
            if (grandChild.getToken().getType() == HiveParser.TOK_PARTSPEC) {
              throw new SemanticException(SentryHiveConstants.PARTITION_PRIVS_NOT_SUPPORTED);
            } else if (grandChild.getToken().getType() == HiveParser.TOK_TABCOLNAME) {
              throw new SemanticException(SentryHiveConstants.COLUMN_PRIVS_NOT_SUPPORTED);
            } else {
              privHiveObj.setTable(child.getChild(i) != null);
            }
          }
        }
      }
    }

    if (privHiveObj == null) {
      throw new SemanticException(SentryHiveConstants.COLUMN_PRIVS_NOT_SUPPORTED);
    }

    ShowGrantDesc showGrant = new ShowGrantDesc(resultFile.toString(),
        principalDesc, privHiveObj, null);
    return createTask(new DDLWork(inputs, outputs, showGrant));
  }

  @Override
  public Task<? extends Serializable> createRevokeRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    return analyzeGrantRevokeRole(false, ast, inputs, outputs);
  }

  private Task<? extends Serializable> analyzeGrantRevokeRole(boolean isGrant, ASTNode ast,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) throws SemanticException {
    List<PrincipalDesc> principalDesc = analyzePrincipalListDef(
        (ASTNode) ast.getChild(0));
    List<String> roles = new ArrayList<String>();
    for (int i = 1; i < ast.getChildCount(); i++) {
      roles.add(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(i).getText()));
    }
    String roleOwnerName = "";
    if (SessionState.get() != null
        && SessionState.get().getAuthenticator() != null) {
      roleOwnerName = SessionState.get().getAuthenticator().getUserName();
    }
    for (PrincipalDesc princ : principalDesc) {
      if (princ.getType() != PrincipalType.GROUP) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_ON_OBJECT + princ.getType();
        throw new SemanticException(msg);
      }
    }
    GrantRevokeRoleDDL grantRevokeRoleDDL = new GrantRevokeRoleDDL(isGrant,
        roles, principalDesc, roleOwnerName, PrincipalType.USER, false);
    return createTask(new DDLWork(inputs, outputs, grantRevokeRoleDDL));
  }

  @Override
  public Task<? extends Serializable> createSetRoleTask(String role, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    RoleDDLDesc roleDesc = new RoleDDLDesc(role, RoleDDLDesc.RoleOperation.SET_ROLE);
    return createTask(new DDLWork(inputs, outputs, roleDesc));
  }

  @Override
  public Task<? extends Serializable> createShowCurrentRoleTask(HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs, Path resultFile) throws SemanticException {
    RoleDDLDesc ddlDesc = new RoleDDLDesc(null, RoleDDLDesc.RoleOperation.SHOW_CURRENT_ROLE);
    ddlDesc.setResFile(resultFile.toString());
    return createTask(new DDLWork(inputs, outputs, ddlDesc));
  }

  @Override
  public Task<? extends Serializable> createShowRolesTask(ASTNode ast, Path resFile,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) throws SemanticException {
    RoleDDLDesc showRolesDesc = new RoleDDLDesc(null, null, RoleDDLDesc.RoleOperation.SHOW_ROLES,
        null);
    showRolesDesc.setResFile(resFile.toString());
    return createTask(new DDLWork(inputs, outputs, showRolesDesc));
  }

  private PrivilegeObjectDesc analyzePrivilegeObject(ASTNode ast)
      throws SemanticException {
    PrivilegeObjectDesc subject = new PrivilegeObjectDesc();
    subject.setObject(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText()));
    if (ast.getChildCount() > 1) {
      for (int i = 0; i < ast.getChildCount(); i++) {
        ASTNode astChild = (ASTNode) ast.getChild(i);
        if (astChild.getToken().getType() == HiveParser.TOK_PARTSPEC) {
          throw new SemanticException(SentryHiveConstants.PARTITION_PRIVS_NOT_SUPPORTED);
        } else {
          subject.setTable(ast.getChild(0) != null);
        }
      }
    }
    return subject;
  }

  private List<PrincipalDesc> analyzePrincipalListDef(ASTNode node) {
    List<PrincipalDesc> principalList = new ArrayList<PrincipalDesc>();
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      PrincipalType type = null;
      switch (child.getType()) {
      case HiveParser.TOK_USER:
        type = PrincipalType.USER;
        break;
      case HiveParser.TOK_GROUP:
        type = PrincipalType.GROUP;
        break;
      case HiveParser.TOK_ROLE:
        type = PrincipalType.ROLE;
        break;
      }
      String principalName = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
      PrincipalDesc principalDesc = new PrincipalDesc(principalName, type);
      principalList.add(principalDesc);
    }
    return principalList;
  }

  private List<PrivilegeDesc> analyzePrivilegeListDef(ASTNode node)
      throws SemanticException {
    List<PrivilegeDesc> ret = new ArrayList<PrivilegeDesc>();
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode privilegeDef = (ASTNode) node.getChild(i);
      ASTNode privilegeType = (ASTNode) privilegeDef.getChild(0);
      Privilege privObj = PrivilegeRegistry.getPrivilege(privilegeType.getType());
      if (privObj == null) {
        throw new SemanticException("undefined privilege " + privilegeType.getType());
      }
      if (!SentryHiveConstants.ALLOWED_PRIVS.contains(privObj.getPriv())) {
        String msg = SentryHiveConstants.PRIVILEGE_NOT_SUPPORTED + privObj.getPriv();
        throw new SemanticException(msg);
      }
      if (privilegeDef.getChildCount() > 1) {
        throw new SemanticException(SentryHiveConstants.COLUMN_PRIVS_NOT_SUPPORTED);
      }
      PrivilegeDesc privilegeDesc = new PrivilegeDesc(privObj, null);
      ret.add(privilegeDesc);
    }
    return ret;
  }

  private static Task<? extends Serializable> createTask(DDLWork work) {
    SentryGrantRevokeTask task = new SentryGrantRevokeTask();
    task.setId("Stage-" + Integer.toString(TaskFactory.getAndIncrementId()));
    task.setWork(work);
    return task;
  }


}
