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
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.RevokeDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.ShowGrantDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

// TODO remove this suppress
@SuppressWarnings("unused")
public class SentryGrantRevokeTask extends Task<DDLWork> implements Serializable {
  private static final Logger LOG = LoggerFactory
      .getLogger(SentryGrantRevokeTask.class);
  private static final int RETURN_CODE_SUCCESS = 0;
  private static final int RETURN_CODE_FAILURE = 1;
  private static final Splitter DB_TBL_SPLITTER = Splitter.on(".").omitEmptyStrings().trimResults();
  private static final int separator = Utilities.tabCode;
  private static final int terminator = Utilities.newLineCode;
  private static final long serialVersionUID = -7625118066790571999L;

  private SentryServiceClientFactory sentryClientFactory;
  private SentryPolicyServiceClient sentryClient;
  private HiveConf conf;
  private HiveAuthzConf authzConf;
  private String server;
  private Subject subject;
  private Set<String> subjectGroups;


  public SentryGrantRevokeTask() {
    this(new SentryServiceClientFactory());
  }
  public SentryGrantRevokeTask(SentryServiceClientFactory sentryClientFactory) {
    super();
    this.sentryClientFactory = sentryClientFactory;
  }


  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, driverContext);
    this.conf = conf;
  }

  @Override
  public int execute(DriverContext driverContext) {
    try {
      try {
        this.sentryClient = sentryClientFactory.create(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client: " + e.getMessage();
        LOG.error(msg, e);
        throw new RuntimeException(msg, e);
      }
      Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
      Preconditions.checkNotNull(subject, "Subject cannot be null");
      Preconditions.checkNotNull(subjectGroups, "Subject Groups cannot be null");
      server = Preconditions.checkNotNull(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()),
          "Config " + AuthzConfVars.AUTHZ_SERVER_NAME.getVar() + " is required");
      if (work.getRoleDDLDesc() != null) {
        return processRoleDDL(conf, console, sentryClient, subject.getName(), subjectGroups,
            work.getRoleDDLDesc());
      }
      if (work.getGrantDesc() != null) {
        return processGrantDDL(conf, console, sentryClient, subject.getName(), subjectGroups,
            server, work.getGrantDesc());
      }
      if (work.getRevokeDesc() != null) {
        return processRevokeDDL(conf, console, sentryClient, subject.getName(), subjectGroups,
            server, work.getRevokeDesc());
      }
      if (work.getShowGrantDesc() != null) {
        return processShowGrantDDL(conf, console, subject.getName(), subjectGroups,
            work.getShowGrantDesc());
      }
      if (work.getGrantRevokeRoleDDL() != null) {
        return processGrantRevokeRoleDDL(conf, console, sentryClient, subject.getName(), subjectGroups,
            work.getGrantRevokeRoleDDL());
      }
      throw new AssertionError("Unknown command passed to Sentry Grant/Revoke Task");
    } catch(Throwable throwable) {
      setException(throwable);
      String msg = "Error processing Sentry command: " + throwable.getMessage();
      LOG.error(msg, throwable);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  public void setAuthzConf(HiveAuthzConf authzConf) {
    Preconditions.checkState(this.authzConf == null,
        "setAuthzConf should only be called once: " + this.authzConf);
    this.authzConf = authzConf;
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

  @VisibleForTesting
  static int processRoleDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      Set<String> subjectGroups, RoleDDLDesc desc) throws SentryUserException {
    RoleDDLDesc.RoleOperation operation = desc.getOperation();
    DataOutputStream outStream = null;
    String name = desc.getName();
    try {
      if (operation.equals(RoleDDLDesc.RoleOperation.CREATE_ROLE)) {
        SessionState.get().getAuthenticator();
        sentryClient.createRole(subject, subjectGroups, name);
        return RETURN_CODE_SUCCESS;
      } else if (operation.equals(RoleDDLDesc.RoleOperation.DROP_ROLE)) {
        sentryClient.dropRole(subject, subjectGroups, name);
        return RETURN_CODE_SUCCESS;
      } else if (operation.equals(RoleDDLDesc.RoleOperation.SHOW_ROLE_GRANT)) {
        PrincipalType principalType = desc.getPrincipalType();
        if (principalType != PrincipalType.GROUP) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principalType;
          throw new HiveException(msg);
        }
        throw new AssertionError("TODO");
        // TODO once retrieval API is implemented this can be implemented
//        List<String> roles = sentryClient.getRoles(name);
//        if (!roles.isEmpty()) {
//          Path resFile = new Path(desc.getResFile());
//          FileSystem fs = resFile.getFileSystem(conf);
//          outStream = fs.create(resFile);
//          for (String role : roles) {
//            outStream.writeBytes("role name:" + role);
//            outStream.write(terminator);
//          }
//          outStream = close(outStream);
//        }
//        return RETURN_CODE_SUCCESS;
      } else {
        throw new HiveException("Unkown role operation "
            + operation.getOperationName());
      }
    } catch (HiveException e) {
      String msg = "Error in role operation "
          + operation.getOperationName() + " on role name "
          + name + ", error message " + e.getMessage();
      LOG.warn(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
//    } catch (IOException e) {
//      String msg = "IO Error in role operation " + e.getMessage();
//      LOG.info(msg, e);
//      console.printError(msg);
//      return RETURN_CODE_FAILURE;
    } finally {
      closeQuiet(outStream);
    }
  }

  @VisibleForTesting
  static int processGrantDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      Set<String> subjectGroups, String server, GrantDesc desc) throws SentryUserException {
    return processGrantRevokeDDL(console, sentryClient, subject, subjectGroups,
        server, true, desc.getPrincipals(), desc.getPrivileges(), desc.getPrivilegeSubjectDesc());
  }

  @VisibleForTesting
  static int processRevokeDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      Set<String> subjectGroups, String server, RevokeDesc desc) throws SentryUserException {
    return processGrantRevokeDDL(console, sentryClient, subject, subjectGroups,
        server, false, desc.getPrincipals(), desc.getPrivileges(),
        desc.getPrivilegeSubjectDesc());
  }

  @VisibleForTesting
  static int processShowGrantDDL(HiveConf conf, LogHelper console, String subject,
      Set<String> subjectGroups, ShowGrantDesc desc) {
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(desc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      PrincipalDesc principalDesc = desc.getPrincipalDesc();
      PrivilegeObjectDesc hiveObjectDesc = desc.getHiveObj();
      String principalName = principalDesc.getName();
      List<String> columns = desc.getColumns();
      if (columns != null && !columns.isEmpty()) {
        throw new HiveException(SentryHiveConstants.COLUMN_PRIVS_NOT_SUPPORTED);
      }
      if (hiveObjectDesc == null) {
        // TDOD get users from somewhere?
        List<String> users = Collections.emptyList();
        if (users != null && users.size() > 0) {
          boolean first = true;
          Collections.sort(users);
          for (String usr : users) {
            if (!first) {
              outStream.write(terminator);
            } else {
              first = false;
            }
            // TODO write grant info
          }
        }
      } else {
        if (hiveObjectDesc.getPartSpec() != null) {
          throw new HiveException(SentryHiveConstants.PARTITION_PRIVS_NOT_SUPPORTED);
        }
        String obj = hiveObjectDesc.getObject();
        String dbName = null;
        String tableName = null;
        if (hiveObjectDesc.getTable()) {
          DatabaseTable dbTable = parseDBTable(obj);
          dbName = dbTable.getDatabase();
          tableName = dbTable.getTable();
        } else {
          dbName = hiveObjectDesc.getObject();
        }
        if (hiveObjectDesc.getTable()) {
          // show table level privileges
          // TODO
          List<String> tbls = Collections.emptyList();
          if (tbls != null && tbls.size() > 0) {
            boolean first = true;
            Collections.sort(tbls);
            for (String tbl : tbls) {
              if (!first) {
                outStream.write(terminator);
              } else {
                first = false;
              }
              // TODO write grant info
            }
          }
        } else {
          // show database level privileges
          // TODO
          List<String> dbs = Collections.emptyList();
          if (dbs != null && dbs.size() > 0) {
            boolean first = true;
            Collections.sort(dbs);
            for (String db : dbs) {
              if (!first) {
                outStream.write(terminator);
              } else {
                first = false;
              }
              // TODO write grant info
            }
          }
        }
      }
      outStream = close(outStream);
    } catch (HiveException e) {
      String msg = "Error in show grant operation " + e.getMessage();
      LOG.warn(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } catch (IOException e) {
      String msg = "IO Error in show grant " + e.getMessage();
      LOG.info(msg, e);
      console.printError(msg);
      return RETURN_CODE_FAILURE;
    } finally {
      closeQuiet(outStream);
    }
    return RETURN_CODE_SUCCESS;
  }

  @VisibleForTesting
  static int processGrantRevokeRoleDDL(HiveConf conf, LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject, Set<String> subjectGroups,
      GrantRevokeRoleDDL desc) throws SentryUserException {
    try {
      boolean grantRole = desc.getGrant();
      List<PrincipalDesc> principals = desc.getPrincipalDesc();
      List<String> roles = desc.getRoles();
      for (PrincipalDesc principal : principals) {
        if (principal.getType() != PrincipalType.GROUP) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL +
              principal.getType();
          throw new HiveException(msg);
        }
        String groupName = principal.getName();
        for (String roleName : roles) {
          if (grantRole) {
            sentryClient.grantRoleToGroup(subject, subjectGroups, groupName, roleName);
          } else {
            sentryClient.revokeRoleFromGroup(subject, subjectGroups, groupName, roleName);
          }
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

  private static int processGrantRevokeDDL(LogHelper console,
      SentryPolicyServiceClient sentryClient, String subject,
      Set<String> subjectGroups, String server,
      boolean isGrant, List<PrincipalDesc> principals,
      List<PrivilegeDesc> privileges, PrivilegeObjectDesc privSubjectDesc) throws SentryUserException {
    if (privileges == null || privileges.size() == 0) {
      console.printError("No privilege found.");
      return RETURN_CODE_FAILURE;
    }
    String dbName = null;
    String tableName = null;
    try {
      if (privSubjectDesc == null) {
        throw new HiveException("Privilege subject cannot be null");
      }
      if (privSubjectDesc.getPartSpec() != null) {
        throw new HiveException(SentryHiveConstants.PARTITION_PRIVS_NOT_SUPPORTED);
      }
      // TODO how to grant all on server
      String obj = privSubjectDesc.getObject();
      if (privSubjectDesc.getTable()) {
        DatabaseTable dbTable = parseDBTable(obj);
        dbName = dbTable.getDatabase();
        tableName = dbTable.getTable();
      } else {
        dbName = privSubjectDesc.getObject();
      }
      for (PrivilegeDesc privDesc : privileges) {
        List<String> columns = privDesc.getColumns();
        if (columns != null && !columns.isEmpty()) {
          throw new HiveException(SentryHiveConstants.COLUMN_PRIVS_NOT_SUPPORTED);
        }
        if (!SentryHiveConstants.ALLOWED_PRIVS.contains(privDesc.getPrivilege().getPriv())) {
          String msg = SentryHiveConstants.PRIVILEGE_NOT_SUPPORTED + privDesc.getPrivilege().getPriv();
          throw new HiveException(msg);
        }
      }
      for (PrincipalDesc princ : principals) {
        if (princ.getType() != PrincipalType.ROLE) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + princ.getType();
          throw new HiveException(msg);
        }
        for (PrivilegeDesc privDesc : privileges) {
          if (isGrant) {
            if (tableName == null) {
              sentryClient.grantDatabasePrivilege(subject, subjectGroups, princ.getName(), server, dbName);
            } else {
              sentryClient.grantTablePrivilege(subject, subjectGroups, princ.getName(), server, dbName,
                  tableName, privDesc.getPrivilege().getPriv().name());
            }
          } else {
            if (tableName == null) {
              sentryClient.revokeDatabasePrivilege(subject, subjectGroups, princ.getName(), server, dbName);
            } else {
              sentryClient.revokeTablePrivilege(subject, subjectGroups, princ.getName(), server, dbName,
                  tableName, privDesc.getPrivilege().getPriv().name());
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