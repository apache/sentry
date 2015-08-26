package org.apache.hadoop.hive.ql.exec;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.RevokeDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.ShowGrantDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.HiveAuthzBindingHook;
import org.apache.sentry.binding.hive.SentryOnFailureHookContext;
import org.apache.sentry.binding.hive.SentryOnFailureHookContextImpl;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class SentryGrantRevokeTask extends Task<DDLWork> implements Serializable {
  private static final Logger LOG = LoggerFactory
      .getLogger(SentryGrantRevokeTask.class);
  private static final int RETURN_CODE_SUCCESS = 0;
  private static final int RETURN_CODE_FAILURE = 1;
  private static final Splitter DB_TBL_SPLITTER = Splitter.on(".").omitEmptyStrings().trimResults();
  private static final int separator = Utilities.tabCode;
  private static final int terminator = Utilities.newLineCode;
  private static final long serialVersionUID = -7625118066790571999L;

  private SentryPolicyServiceClient sentryClient;
  private HiveConf conf;
  private HiveAuthzBinding hiveAuthzBinding;
  private HiveAuthzConf authzConf;
  private String server;
  private Subject subject;
  private Set<String> subjectGroups;
  private String ipAddress;
  private HiveOperation stmtOperation;

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, driverContext);
    this.conf = conf;
  }

  @Override
  public int execute(DriverContext driverContext) {
    try {
      try {
        this.sentryClient = SentryServiceClientFactory.create(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      Preconditions.checkNotNull(hiveAuthzBinding, "HiveAuthzBinding cannot be null");
      Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
      Preconditions.checkNotNull(subject, "Subject cannot be null");
      server = Preconditions.checkNotNull(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()),
          "Config " + AuthzConfVars.AUTHZ_SERVER_NAME.getVar() + " is required");
      try {
        if (work.getRoleDDLDesc() != null) {
          return processRoleDDL(conf, console, sentryClient, subject.getName(),
              hiveAuthzBinding, work.getRoleDDLDesc());
        }
        if (work.getGrantDesc() != null) {
          return processGrantDDL(conf, console, sentryClient,
              subject.getName(), server, work.getGrantDesc());
        }
        if (work.getRevokeDesc() != null) {
          return processRevokeDDL(conf, console, sentryClient,
              subject.getName(), server, work.getRevokeDesc());
        }
        if (work.getShowGrantDesc() != null) {
          return processShowGrantDDL(conf, console, sentryClient, subject.getName(), server,
              work.getShowGrantDesc());
        }
        if (work.getGrantRevokeRoleDDL() != null) {
          return processGrantRevokeRoleDDL(conf, console, sentryClient,
              subject.getName(), work.getGrantRevokeRoleDDL());
        }
        throw new AssertionError(
            "Unknown command passed to Sentry Grant/Revoke Task");
      } catch (SentryAccessDeniedException e) {
        String csHooks = authzConf.get(
            HiveAuthzConf.AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(), "")
            .trim();
        SentryOnFailureHookContext hookContext = new SentryOnFailureHookContextImpl(
            queryPlan.getQueryString(), new HashSet<ReadEntity>(),
            new HashSet<WriteEntity>(), stmtOperation,
            null, null, null, null, subject.getName(), ipAddress,
            new AuthorizationException(e), conf);
        HiveAuthzBindingHook.runFailureHook(hookContext, csHooks);
        throw e; // rethrow the exception for logging
      }
    } catch(SentryUserException e) {
      setException(new Exception(e.getClass().getSimpleName() + ": " + e.getReason(), e));
      String msg = "Error processing Sentry command: " + e.getReason() + ".";
      if (e instanceof SentryAccessDeniedException) {
        msg += "Please grant admin privilege to " + subject.getName() + ".";
      }
      LOG.error(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } catch(Throwable e) {
      setException(e);
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
      if (hiveAuthzBinding != null) {
        hiveAuthzBinding.close();
      }
    }
  }

  public void setAuthzConf(HiveAuthzConf authzConf) {
    Preconditions.checkState(this.authzConf == null,
        "setAuthzConf should only be called once: " + this.authzConf);
    this.authzConf = authzConf;
  }
  public void setHiveAuthzBinding(HiveAuthzBinding hiveAuthzBinding) {
    Preconditions.checkState(this.hiveAuthzBinding == null,
        "setHiveAuthzBinding should only be called once: " + this.hiveAuthzBinding);
    this.hiveAuthzBinding = hiveAuthzBinding;
  }
  public void setSubject(Subject subject) {
    Preconditions.checkState(this.subject == null,
        "setSubject should only be called once: " + this.subject);
    this.subject = subject;
  }
  public void setSubjectGroups(Set<String> subjectGroups) {
    Preconditions.checkState(this.subjectGroups == null,
        "setSubjectGroups should only be called once: " + this.subjectGroups);
    this.subjectGroups = subjectGroups;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  public void setOperation(HiveOperation stmtOperation) {
    this.stmtOperation = stmtOperation;
  }

  private int processRoleDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      HiveAuthzBinding hiveAuthzBinding, RoleDDLDesc desc)
          throws SentryUserException {
    RoleDDLDesc.RoleOperation operation = desc.getOperation();
    DataOutputStream outStream = null;
    String name = desc.getName();
    try {
      if (operation.equals(RoleDDLDesc.RoleOperation.SET_ROLE)) {
        hiveAuthzBinding.setActiveRoleSet(name, sentryClient.listUserRoles(subject));
        return RETURN_CODE_SUCCESS;
      } else if (operation.equals(RoleDDLDesc.RoleOperation.CREATE_ROLE)) {
        sentryClient.createRole(subject, name);
        return RETURN_CODE_SUCCESS;
      } else if (operation.equals(RoleDDLDesc.RoleOperation.DROP_ROLE)) {
        sentryClient.dropRole(subject, name);
        return RETURN_CODE_SUCCESS;
      } else if (operation.equals(RoleDDLDesc.RoleOperation.SHOW_ROLE_GRANT)) {
        Set<TSentryRole> roles;
        PrincipalType principalType = desc.getPrincipalType();
        if (principalType != PrincipalType.GROUP) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principalType;
          throw new HiveException(msg);
        }
        roles = sentryClient.listRolesByGroupName(subject, desc.getName() );
        writeToFile(writeRoleGrantsInfo(roles), desc.getResFile());
        return RETURN_CODE_SUCCESS;
      } else if(operation.equals(RoleDDLDesc.RoleOperation.SHOW_ROLES)) {
        Set<TSentryRole> roles = sentryClient.listRoles(subject);
        writeToFile(writeRolesInfo(roles), desc.getResFile());
        return RETURN_CODE_SUCCESS;
      } else if(operation.equals(RoleDDLDesc.RoleOperation.SHOW_CURRENT_ROLE)) {
        ActiveRoleSet roleSet = hiveAuthzBinding.getActiveRoleSet();
        if( roleSet.isAll()) {
          Set<TSentryRole> roles = sentryClient.listUserRoles(subject);
          writeToFile(writeRolesInfo(roles), desc.getResFile());
          return RETURN_CODE_SUCCESS;
        } else {
          Set<String> roles = roleSet.getRoles();
          writeToFile(writeActiveRolesInfo(roles), desc.getResFile());
          return RETURN_CODE_SUCCESS;
        }
      } else {
        throw new HiveException("Unknown role operation "
            + operation.getOperationName());
      }
    } catch (HiveException e) {
      String msg = "Error in role operation "
          + operation.getOperationName() + " on role name "
          + name + ", error message " + e.getMessage();
      LOG.warn(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } catch (IOException e) {
      String msg = "IO Error in role operation " + e.getMessage();
      LOG.info(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } finally {
      closeQuiet(outStream);
    }
  }

  private int processGrantDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      String server, GrantDesc desc) throws SentryUserException {
    return processGrantRevokeDDL(console, sentryClient, subject,
        server, true, desc.getPrincipals(), desc.getPrivileges(),
        desc.getPrivilegeSubjectDesc(), desc.isGrantOption());
  }

  // For grant option, we use null to stand for revoke the privilege ignore the grant option
  private int processRevokeDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      String server, RevokeDesc desc) throws SentryUserException {
    return processGrantRevokeDDL(console, sentryClient, subject,
        server, false, desc.getPrincipals(), desc.getPrivileges(),
        desc.getPrivilegeSubjectDesc(), null);
  }

  private int processShowGrantDDL(HiveConf conf, LogHelper console, SentryPolicyServiceClient sentryClient,
      String subject, String server, ShowGrantDesc desc) throws SentryUserException{
    PrincipalDesc principalDesc = desc.getPrincipalDesc();
    PrivilegeObjectDesc hiveObjectDesc = desc.getHiveObj();
    String principalName = principalDesc.getName();
    Set<TSentryPrivilege> privileges;

    try {
      if (principalDesc.getType() != PrincipalType.ROLE) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principalDesc.getType();
        throw new HiveException(msg);
      }

      if (hiveObjectDesc == null) {
        privileges = sentryClient.listPrivilegesByRoleName(subject, principalName, null);
      } else {
        SentryHivePrivilegeObjectDesc privSubjectDesc = toSentryHivePrivilegeObjectDesc(hiveObjectDesc);
        List<Authorizable> authorizableHeirarchy = toAuthorizable(privSubjectDesc);
        if (privSubjectDesc.getColumns() != null && !privSubjectDesc.getColumns().isEmpty()) {
          List<List<Authorizable>> ps = parseColumnToAuthorizable(authorizableHeirarchy, privSubjectDesc);
          ImmutableSet.Builder<TSentryPrivilege> pbuilder = new ImmutableSet.Builder<TSentryPrivilege>();
          for (List<Authorizable> p : ps) {
            pbuilder.addAll(sentryClient.listPrivilegesByRoleName(subject, principalName, p));
          }
          privileges = pbuilder.build();
        } else {
          privileges = sentryClient.listPrivilegesByRoleName(subject, principalName, authorizableHeirarchy);
        }
      }
      writeToFile(writeGrantInfo(privileges, principalName), desc.getResFile());
      return RETURN_CODE_SUCCESS;
    } catch (IOException e) {
      String msg = "IO Error in show grant " + e.getMessage();
      LOG.info(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } catch (HiveException e) {
      String msg = "Error in show grant operation, error message " + e.getMessage();
      LOG.warn(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    }
  }

  private List<Authorizable> toAuthorizable(SentryHivePrivilegeObjectDesc privSubjectDesc) throws HiveException{
    List<Authorizable> authorizableHeirarchy = new ArrayList<Authorizable>();
    authorizableHeirarchy.add(new Server(server));
    String dbName = null;
    if (privSubjectDesc.getTable()) {
      DatabaseTable dbTable = parseDBTable(privSubjectDesc.getObject());
      dbName = dbTable.getDatabase();
      String tableName = dbTable.getTable();
      authorizableHeirarchy.add(new Table(tableName));
      authorizableHeirarchy.add(new Database(dbName));
    } else if (privSubjectDesc.getUri()) {
      String uriPath = privSubjectDesc.getObject();
      String warehouseDir = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
      try {
      authorizableHeirarchy.add(new AccessURI(PathUtils.parseDFSURI(warehouseDir, uriPath)));
      } catch(URISyntaxException e) {
        throw new HiveException(e.getMessage());
      }
    } else {
      dbName = privSubjectDesc.getObject();
      authorizableHeirarchy.add(new Database(dbName));
    }
    return authorizableHeirarchy;
  }

  private List<List<Authorizable>> parseColumnToAuthorizable(List<Authorizable> authorizableHeirarchy,
      SentryHivePrivilegeObjectDesc privSubjectDesc) {
    ImmutableList.Builder<List<Authorizable>> listsBuilder = ImmutableList.builder();
    List<String> cols = privSubjectDesc.getColumns();
    if ( cols != null && !cols.isEmpty() ) {
      for ( String col : cols ) {
        ImmutableList.Builder<Authorizable> listBuilder = ImmutableList.builder();
        listBuilder.addAll(authorizableHeirarchy);
        listBuilder.add(new Column(col));
        listsBuilder.add(listBuilder.build());
      }
    }
    return listsBuilder.build();
  }

  private void writeToFile(String data, String file) throws IOException {
    Path resFile = new Path(file);
    FileSystem fs = resFile.getFileSystem(conf);
    FSDataOutputStream out = fs.create(resFile);
    try {
      if (data != null && !data.isEmpty()) {
        OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
        writer.write(data);
        writer.write((char) terminator);
        writer.flush();
      }
    } finally {
      closeQuiet(out);
    }
  }

  private int processGrantRevokeRoleDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      GrantRevokeRoleDDL desc) throws SentryUserException {
    try {
      boolean grantRole = desc.getGrant();
      List<PrincipalDesc> principals = desc.getPrincipalDesc();
      List<String> roles = desc.getRoles();
      // get principals
      Set<String> groups = Sets.newHashSet();
      for (PrincipalDesc principal : principals) {
        if (principal.getType() != PrincipalType.GROUP) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL +
              principal.getType();
          throw new HiveException(msg);
        }
        groups.add(principal.getName());
      }

      // grant/revoke role to/from principals
      for (String roleName : roles) {
        if (grantRole) {
          sentryClient.grantRoleToGroups(subject, roleName, groups);
        } else {
          sentryClient.revokeRoleFromGroups(subject, roleName, groups);
        }
      }

    } catch (HiveException e) {
      String msg = "Error in grant/revoke operation, error message " + e.getMessage();
      LOG.warn(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    }
    return RETURN_CODE_SUCCESS;
  }

  static String writeGrantInfo(Set<TSentryPrivilege> privileges, String roleName) {
    if (privileges == null || privileges.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();

    for (TSentryPrivilege privilege : privileges) {

      if (PrivilegeScope.URI.name().equalsIgnoreCase(
          privilege.getPrivilegeScope())) {
        appendNonNull(builder, privilege.getURI(), true);
      } else if(PrivilegeScope.SERVER.name().equalsIgnoreCase(
          privilege.getPrivilegeScope())) {
        appendNonNull(builder, "*", true);//Db column would show * if it is a server level privilege
      } else {
        appendNonNull(builder, privilege.getDbName(), true);
      }
      appendNonNull(builder, privilege.getTableName());
      appendNonNull(builder, null);//getPartValues()
      appendNonNull(builder, privilege.getColumnName());//getColumnName()
      appendNonNull(builder, roleName);//getPrincipalName()
      appendNonNull(builder, "ROLE");//getPrincipalType()
      appendNonNull(builder, privilege.getAction());
      appendNonNull(builder,
          TSentryGrantOption.TRUE.equals(privilege.getGrantOption()));
      appendNonNull(builder, privilege.getCreateTime() * 1000L);
      appendNonNull(builder, "--");
    }
    LOG.info("builder.toString(): " + builder.toString());
    return builder.toString();
  }

  static String writeRoleGrantsInfo(Set<TSentryRole> roleGrants) {
    if (roleGrants == null || roleGrants.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (TSentryRole roleGrant : roleGrants) {
      appendNonNull(builder, roleGrant.getRoleName(), true);
      appendNonNull(builder, false);//isGrantOption()
      appendNonNull(builder, null);//roleGrant.getGrantTime() * 1000L
      appendNonNull(builder, "--");
    }
    return builder.toString();
  }

  static String writeRolesInfo(Set<TSentryRole> roles) {
    if (roles == null || roles.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (TSentryRole roleGrant : roles) {
      appendNonNull(builder, roleGrant.getRoleName(), true);
    }
    return builder.toString();
  }

  static String writeActiveRolesInfo(Set<String> roles) {
    if (roles == null || roles.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (String role : roles) {
      appendNonNull(builder, role, true);
    }
    return builder.toString();
  }

  static StringBuilder appendNonNull(StringBuilder builder, Object value) {
    return appendNonNull(builder, value, false);
  }

  static StringBuilder appendNonNull(StringBuilder builder, Object value, boolean firstColumn) {
    if (!firstColumn) {
      builder.append((char)separator);
    } else if (builder.length() > 0) {
      builder.append((char)terminator);
    }
    if (value != null) {
      builder.append(value);
    }
    return builder;
  }

  private static int processGrantRevokeDDL(LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject, String server,
      boolean isGrant, List<PrincipalDesc> principals,
      List<PrivilegeDesc> privileges, PrivilegeObjectDesc privSubjectObjDesc,
      Boolean grantOption) throws SentryUserException {
    if (privileges == null || privileges.size() == 0) {
      console.printError("No privilege found.");
      return RETURN_CODE_FAILURE;
    }

    String dbName = null;
    String tableName = null;
    List<String> columnNames = null;
    String uriPath = null;
    String serverName = null;
    try {
      SentryHivePrivilegeObjectDesc privSubjectDesc = toSentryHivePrivilegeObjectDesc(privSubjectObjDesc);

      if (privSubjectDesc == null) {
        throw new HiveException("Privilege subject cannot be null");
      }
      if (privSubjectDesc.getPartSpec() != null) {
        throw new HiveException(SentryHiveConstants.PARTITION_PRIVS_NOT_SUPPORTED);
      }
      String obj = privSubjectDesc.getObject();
      if (privSubjectDesc.getTable()) {
        DatabaseTable dbTable = parseDBTable(obj);
        dbName = dbTable.getDatabase();
        tableName = dbTable.getTable();
      } else if (privSubjectDesc.getUri()) {
        uriPath = privSubjectDesc.getObject();
      } else if (privSubjectDesc.getServer()) {
        serverName = privSubjectDesc.getObject();
      } else {
        dbName = privSubjectDesc.getObject();
      }
      for (PrivilegeDesc privDesc : privileges) {
        List<String> columns = privDesc.getColumns();
        if (columns != null && !columns.isEmpty()) {
          columnNames = columns;
        }
        if (!SentryHiveConstants.ALLOWED_PRIVS.contains(privDesc.getPrivilege().getPriv())) {
          String msg = SentryHiveConstants.PRIVILEGE_NOT_SUPPORTED + privDesc.getPrivilege().getPriv();
          throw new HiveException(msg);
        }
        if (columnNames != null && (privDesc.getPrivilege().getPriv().equals(PrivilegeType.INSERT)
            || privDesc.getPrivilege().getPriv().equals(PrivilegeType.ALL))) {
          String msg = SentryHiveConstants.PRIVILEGE_NOT_SUPPORTED
              + privDesc.getPrivilege().getPriv() + " on Column";
          throw new SemanticException(msg);
        }
      }
      for (PrincipalDesc princ : principals) {
        if (princ.getType() != PrincipalType.ROLE) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + princ.getType();
          throw new HiveException(msg);
        }
        for (PrivilegeDesc privDesc : privileges) {
          if (isGrant) {
            if (serverName != null) {
              sentryClient.grantServerPrivilege(subject, princ.getName(), serverName,
                  toSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            } else if (uriPath != null) {
              sentryClient.grantURIPrivilege(subject, princ.getName(), server, uriPath, grantOption);
            } else if (tableName == null) {
              sentryClient.grantDatabasePrivilege(subject, princ.getName(), server, dbName,
                  toDbSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            } else if (columnNames == null) {
              sentryClient.grantTablePrivilege(subject, princ.getName(), server, dbName,
                  tableName, toSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            } else {
              sentryClient.grantColumnsPrivileges(subject, princ.getName(), server, dbName,
                  tableName, columnNames, toSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            }
          } else {
            if (serverName != null) {
              sentryClient.revokeServerPrivilege(subject, princ.getName(), serverName,
                toSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            } else if (uriPath != null) {
              sentryClient.revokeURIPrivilege(subject, princ.getName(), server, uriPath, grantOption);
            } else if (tableName == null) {
              sentryClient.revokeDatabasePrivilege(subject, princ.getName(), server, dbName,
                  toDbSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            } else if (columnNames == null) {
              sentryClient.revokeTablePrivilege(subject, princ.getName(), server, dbName,
                  tableName, toSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            } else {
              sentryClient.revokeColumnsPrivilege(subject, princ.getName(), server, dbName,
                  tableName, columnNames, toSentryAction(privDesc.getPrivilege().getPriv()), grantOption);
            }
          }
        }
      }
      return RETURN_CODE_SUCCESS;
    } catch (HiveException e) {
      String msg = "Error in grant/revoke operation, error message " + e.getMessage();
      LOG.warn(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    }
  }

  private static String toDbSentryAction(PrivilegeType privilegeType) throws SentryUserException{
    switch(privilegeType) {
      case ALL:
        return AccessConstants.ALL;
      case SELECT:
        return AccessConstants.SELECT;
      case INSERT:
        return AccessConstants.INSERT;
      case CREATE:
        return AccessConstants.CREATE;
      case DROP:
        return AccessConstants.DROP;
      case ALTER_METADATA:
        return AccessConstants.ALTER;
      case INDEX:
        return AccessConstants.INDEX;
      case LOCK:
        return AccessConstants.LOCK;
      default:
        throw new SentryUserException("Unknown privilege type: " + privilegeType);
        //Exception is thrown here only for development purposes.
      }
  }

  private static SentryHivePrivilegeObjectDesc toSentryHivePrivilegeObjectDesc(PrivilegeObjectDesc privSubjectObjDesc)
    throws HiveException{
    if (!(privSubjectObjDesc instanceof SentryHivePrivilegeObjectDesc)) {
      throw new HiveException(
          "Privilege subject not parsed correctly by Sentry");
    }
    return (SentryHivePrivilegeObjectDesc) privSubjectObjDesc;
  }

  private static String toSentryAction(PrivilegeType privilegeType) {
    if (PrivilegeType.ALL.equals(privilegeType)) {
      return AccessConstants.ALL;
    } else {
      return privilegeType.toString();
    }
  }

  private static DatabaseTable parseDBTable(String obj) throws HiveException {
    String[] dbTab = Iterables.toArray(DB_TBL_SPLITTER.split(obj), String.class);
    if (dbTab.length == 2) {
      return new DatabaseTable(dbTab[0], dbTab[1]);
    } else if (dbTab.length == 1){
      return new DatabaseTable(SessionState.get().getCurrentDatabase(), obj);
    } else {
      String msg = "Malformed database.table '" + obj + "'";
      throw new HiveException(msg);
    }
  }

  private static class DatabaseTable {
    private final String database;
    private final String table;
    public DatabaseTable(String database, String table) {
      this.database = database;
      this.table = table;
    }
    public String getDatabase() {
      return database;
    }
    public String getTable() {
      return table;
    }
  }

  /**
   * Close to be used in the try block of a try-catch-finally
   * statement. Returns null so the close/set to null idiom can be
   * completed in a single line.
   */
  private static DataOutputStream close(DataOutputStream out)
      throws IOException {
    if (out != null) {
      out.close();
    }
    return null;
  }
  /**
   * Close to be used in the finally block of a try-catch-finally
   * statement.
   */
  private static void closeQuiet(DataOutputStream out) {
    try {
      close(out);
    } catch (IOException e) {
      LOG.warn("Error closing output stream", e);
    }
  }

  @Override
  public boolean requireLock() {
    return false;
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return "SENTRY";
  }
}
