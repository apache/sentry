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
package org.apache.sentry.binding.hive.authz;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.SentryOnFailureHookContext;
import org.apache.sentry.binding.hive.SentryOnFailureHookContextImpl;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding.HiveHook;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.util.SentryAuthorizerUtil;
import org.apache.sentry.binding.util.SimpleSemanticAnalyzer;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class used to do authorization. Check if current user has privileges to do the operation.
 */
public class DefaultSentryValidator extends SentryHiveAuthorizationValidator {

  public static final Logger LOG = LoggerFactory.getLogger(DefaultSentryValidator.class);

  protected HiveConf conf;
  protected HiveAuthzConf authzConf;
  protected HiveAuthenticationProvider authenticator;

  public DefaultSentryValidator(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    initilize(conf, authzConf, authenticator);
    this.hiveHook = HiveHook.HiveServer2;
  }

  public DefaultSentryValidator(HiveHook hiveHook, HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    initilize(conf, authzConf, authenticator);
    this.hiveHook = hiveHook;
  }

  /**
   * initialize authenticator and hiveAuthzBinding.
   */
  protected void initilize(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    Preconditions.checkNotNull(conf, "HiveConf cannot be null");
    Preconditions.checkNotNull(authzConf, "HiveAuthzConf cannot be null");
    Preconditions.checkNotNull(authenticator, "Hive authenticator provider cannot be null");
    this.conf = conf;
    this.authzConf = authzConf;
    this.authenticator = authenticator;
  }

  private HiveHook hiveHook;

  // all operations need to extend at DB scope
  private static final Set<HiveOperation> EX_DB_ALL = Sets.newHashSet(HiveOperation.DROPDATABASE,
      HiveOperation.CREATETABLE, HiveOperation.IMPORT, HiveOperation.DESCDATABASE,
      HiveOperation.ALTERTABLE_RENAME, HiveOperation.LOCKDB, HiveOperation.UNLOCKDB);
  // input operations need to extend at DB scope
  private static final Set<HiveOperation> EX_DB_INPUT = Sets.newHashSet(HiveOperation.DROPDATABASE,
      HiveOperation.DESCDATABASE, HiveOperation.ALTERTABLE_RENAME, HiveOperation.LOCKDB,
      HiveOperation.UNLOCKDB);

  // all operations need to extend at Table scope
  private static final Set<HiveOperation> EX_TB_ALL = Sets.newHashSet(HiveOperation.DROPTABLE,
      HiveOperation.DROPVIEW, HiveOperation.DESCTABLE, HiveOperation.SHOW_TBLPROPERTIES,
      HiveOperation.SHOWINDEXES, HiveOperation.ALTERTABLE_PROPERTIES,
      HiveOperation.ALTERTABLE_SERDEPROPERTIES, HiveOperation.ALTERTABLE_CLUSTER_SORT,
      HiveOperation.ALTERTABLE_FILEFORMAT, HiveOperation.ALTERTABLE_TOUCH,
      HiveOperation.ALTERTABLE_ADDCOLS, HiveOperation.ALTERTABLE_REPLACECOLS,
      HiveOperation.ALTERTABLE_RENAMEPART, HiveOperation.ALTERTABLE_ARCHIVE,
      HiveOperation.ALTERTABLE_UNARCHIVE, HiveOperation.ALTERTABLE_SERIALIZER,
      HiveOperation.ALTERTABLE_MERGEFILES, HiveOperation.ALTERTABLE_SKEWED,
      HiveOperation.ALTERTABLE_DROPPARTS, HiveOperation.ALTERTABLE_ADDPARTS,
      HiveOperation.ALTERTABLE_RENAME, HiveOperation.ALTERTABLE_LOCATION,
      HiveOperation.ALTERVIEW_PROPERTIES, HiveOperation.ALTERPARTITION_FILEFORMAT,
      HiveOperation.ALTERPARTITION_SERIALIZER, HiveOperation.ALTERPARTITION_MERGEFILES,
      HiveOperation.ALTERPARTITION_LOCATION, HiveOperation.ALTERTBLPART_SKEWED_LOCATION,
      HiveOperation.MSCK, HiveOperation.ALTERINDEX_REBUILD, HiveOperation.LOCKTABLE,
      HiveOperation.UNLOCKTABLE, HiveOperation.SHOWCOLUMNS, HiveOperation.SHOW_TABLESTATUS,
      HiveOperation.LOAD, HiveOperation.TRUNCATETABLE);
  // input operations need to extend at Table scope
  private static final Set<HiveOperation> EX_TB_INPUT = Sets.newHashSet(HiveOperation.DROPTABLE,
      HiveOperation.DROPVIEW, HiveOperation.SHOW_TBLPROPERTIES, HiveOperation.SHOWINDEXES,
      HiveOperation.ALTERINDEX_REBUILD, HiveOperation.LOCKTABLE, HiveOperation.UNLOCKTABLE,
      HiveOperation.SHOW_TABLESTATUS);
  private static final Set<HiveOperation> META_TB_INPUT = Sets.newHashSet(HiveOperation.DESCTABLE,
      HiveOperation.SHOWCOLUMNS);

  /**
   * Check if current user has privileges to perform given operation type hiveOpType on the given
   * input and output objects
   *
   * @param hiveOpType
   * @param inputHObjs
   * @param outputHObjs
   * @param context
   * @throws
   */
  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    if (LOG.isDebugEnabled()) {
      String msg =
          "Checking privileges for operation " + hiveOpType + " by user "
              + authenticator.getUserName() + " on " + " input objects " + inputHObjs
              + " and output objects " + outputHObjs + ". Context Info: " + context;
      LOG.debug(msg);
    }

    HiveOperation hiveOp = SentryAuthorizerUtil.convert2HiveOperation(hiveOpType.name());
    HiveAuthzPrivileges stmtAuthPrivileges = null;
    if (HiveOperation.DESCTABLE.equals(hiveOp) &&
        !(context.getCommandString().contains("EXTENDED") || context.getCommandString().contains("FORMATTED")) ) {
      stmtAuthPrivileges = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.SHOWCOLUMNS);
    } else {
      stmtAuthPrivileges = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(hiveOp);
    }

    HiveAuthzBinding hiveAuthzBinding = null;
    try {
      hiveAuthzBinding = getAuthzBinding();
      if (stmtAuthPrivileges == null) {
        // We don't handle authorizing this statement
        return;
      }

      List<List<DBModelAuthorizable>> inputHierarchyList =
          SentryAuthorizerUtil.convert2SentryPrivilegeList(hiveAuthzBinding.getAuthServer(),
              inputHObjs);
      List<List<DBModelAuthorizable>> outputHierarchyList =
          SentryAuthorizerUtil.convert2SentryPrivilegeList(hiveAuthzBinding.getAuthServer(),
              outputHObjs);

      // Workaround for metadata queries
      addExtendHierarchy(hiveOp, stmtAuthPrivileges, inputHierarchyList, outputHierarchyList,
          context.getCommandString(), hiveAuthzBinding);

      hiveAuthzBinding.authorize(hiveOp, stmtAuthPrivileges,
          new Subject(authenticator.getUserName()), inputHierarchyList, outputHierarchyList);
    } catch (AuthorizationException e) {
      Database db = null;
      Table tab = null;
      if (outputHObjs != null) {
        for (HivePrivilegeObject obj : outputHObjs) {
          switch (obj.getType()) {
            case DATABASE:
              db = new Database(obj.getObjectName());
              break;
            case TABLE_OR_VIEW:
              db = new Database(obj.getDbname());
              tab = new Table(obj.getObjectName());
              break;
            case PARTITION:
              db = new Database(obj.getDbname());
              tab = new Table(obj.getObjectName());
            case LOCAL_URI:
            case DFS_URI:
          }
        }
      }
      SentryOnFailureHookContext hookCtx =
          new SentryOnFailureHookContextImpl(context.getCommandString(), null, null, hiveOp, db,
              tab, Collections.<AccessURI>emptyList(), null,
                  authenticator.getUserName(), context.getIpAddress(), e, authzConf);
      SentryAuthorizerUtil.executeOnFailureHooks(hookCtx, authzConf);
      StringBuilder permsRequired = new StringBuilder();
      for (String perm : hiveAuthzBinding.getLastQueryPrivilegeErrors()) {
        permsRequired.append(perm).append(";");
      }
      String permsRequiredStr = permsRequired.toString();
      SessionState.get().getConf().set(HiveAuthzConf.HIVE_SENTRY_AUTH_ERRORS, permsRequiredStr);
      String msg =
          HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE
              + "\n Required privileges for this query: " + permsRequiredStr;
      throw new HiveAccessControlException(msg, e);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e.getClass()+ ": " + e.getMessage(), e);
    } finally {
      if (hiveAuthzBinding != null) {
        hiveAuthzBinding.close();
      }
    }

    if ("true".equalsIgnoreCase(SessionState.get().getConf()
        .get(HiveAuthzConf.HIVE_SENTRY_MOCK_COMPILATION))) {
      throw new HiveAccessControlException(HiveAuthzConf.HIVE_SENTRY_MOCK_ERROR
          + " Mock query compilation aborted. Set " + HiveAuthzConf.HIVE_SENTRY_MOCK_COMPILATION
          + " to 'false' for normal query processing");
    }
  }

  @VisibleForTesting
  public HiveAuthzBinding getAuthzBinding() throws Exception {
    return new HiveAuthzBinding(hiveHook, conf, authzConf);
  }

  private void addExtendHierarchy(HiveOperation hiveOp, HiveAuthzPrivileges stmtAuthPrivileges,
      List<List<DBModelAuthorizable>> inputHierarchyList,
      List<List<DBModelAuthorizable>> outputHierarchyList, String command,
      HiveAuthzBinding hiveAuthzBinding) throws HiveAuthzPluginException,
      HiveAccessControlException {
    String currDatabase = null;
    switch (stmtAuthPrivileges.getOperationScope()) {
      case SERVER:
        // validate server level privileges if applicable. Eg create UDF,register jar etc ..
        List<DBModelAuthorizable> serverHierarchy = new ArrayList<DBModelAuthorizable>();
        serverHierarchy.add(hiveAuthzBinding.getAuthServer());
        inputHierarchyList.add(serverHierarchy);
        break;
      case DATABASE:
        // workaround for metadata queries.
        if (EX_DB_ALL.contains(hiveOp)) {
          SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(hiveOp, command);
          currDatabase = analyzer.getCurrentDb();

          List<DBModelAuthorizable> externalAuthorizableHierarchy =
              new ArrayList<DBModelAuthorizable>();
          externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
          externalAuthorizableHierarchy.add(new Database(currDatabase));

          if (EX_DB_INPUT.contains(hiveOp)) {
            inputHierarchyList.add(externalAuthorizableHierarchy);
          } else {
            outputHierarchyList.add(externalAuthorizableHierarchy);
          }
        }
        break;
      case TABLE:
      case COLUMN:
        // workaround for drop table/view.
        if (EX_TB_ALL.contains(hiveOp)) {
          SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(hiveOp, command);
          currDatabase = analyzer.getCurrentDb();
          String currTable = analyzer.getCurrentTb();

          List<DBModelAuthorizable> externalAuthorizableHierarchy =
              new ArrayList<DBModelAuthorizable>();
          externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
          externalAuthorizableHierarchy.add(new Database(currDatabase));
          externalAuthorizableHierarchy.add(new Table(currTable));

          if (EX_TB_INPUT.contains(hiveOp)) {
            inputHierarchyList.add(externalAuthorizableHierarchy);
          } else if (META_TB_INPUT.contains(hiveOp)) {
            externalAuthorizableHierarchy.add(Column.SOME);
            inputHierarchyList.add(externalAuthorizableHierarchy);
          } else {
            outputHierarchyList.add(externalAuthorizableHierarchy);
          }
        }
        break;
      case FUNCTION:
        if (hiveOp.equals(HiveOperation.CREATEFUNCTION)) {
          SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(hiveOp, command);
          currDatabase = analyzer.getCurrentDb();
          String udfClassName = analyzer.getCurrentTb();
          try {
            CodeSource udfSrc = Class.forName(udfClassName).getProtectionDomain().getCodeSource();
            if (udfSrc == null) {
              throw new HiveAuthzPluginException("Could not resolve the jar for UDF class "
                  + udfClassName);
            }
            String udfJar = udfSrc.getLocation().getPath();
            if (udfJar == null || udfJar.isEmpty()) {
              throw new HiveAuthzPluginException("Could not find the jar for UDF class "
                  + udfClassName + "to validate privileges");
            }
            AccessURI udfURI = SentryAuthorizerUtil.parseURI(udfSrc.getLocation().toString(), true);
            List<DBModelAuthorizable> udfUriHierarchy = new ArrayList<DBModelAuthorizable>();
            udfUriHierarchy.add(hiveAuthzBinding.getAuthServer());
            udfUriHierarchy.add(udfURI);
            inputHierarchyList.add(udfUriHierarchy);
          } catch (Exception e) {
            throw new HiveAuthzPluginException("Error retrieving udf class", e);
          }
        }
        break;
      case CONNECT:
        /*
         * The 'CONNECT' is an implicit privilege scope currently used for - USE <db> It's allowed
         * when the user has any privilege on the current database. For application backward
         * compatibility, we allow (optional) implicit connect permission on 'default' db.
         */
        List<DBModelAuthorizable> connectHierarchy = new ArrayList<DBModelAuthorizable>();
        connectHierarchy.add(hiveAuthzBinding.getAuthServer());
        if (hiveOp.equals(HiveOperation.SWITCHDATABASE)) {
          currDatabase = command.split(" ")[1];
        }
        // by default allow connect access to default db
        Table currTbl = Table.ALL;
        Database currDB = new Database(currDatabase);
        Column currCol = Column.ALL;
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(currDatabase) && "false"
            .equalsIgnoreCase(authzConf.get(
                HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "false"))) {
          currDB = Database.ALL;
          currTbl = Table.SOME;
        }

        connectHierarchy.add(currDB);
        connectHierarchy.add(currTbl);
        connectHierarchy.add(currCol);

        inputHierarchyList.add(connectHierarchy);
        break;
    }
  }

  @Override
  public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
      HiveAuthzContext context) {
    if (listObjs != null && listObjs.size() >= 1) {
      HivePrivilegeObjectType pType = listObjs.get(0).getType();
      HiveAuthzBinding hiveAuthzBinding = null;
      try {
        switch (pType) {
          case DATABASE:
            hiveAuthzBinding = getAuthzBinding();
            listObjs = filterShowDatabases(listObjs, authenticator.getUserName(), hiveAuthzBinding);
            break;
          case TABLE_OR_VIEW:
            hiveAuthzBinding = getAuthzBinding();
            listObjs = filterShowTables(listObjs, authenticator.getUserName(), hiveAuthzBinding);
            break;
        }
      } catch (Exception e) {
        LOG.warn(e.getMessage(),e);
      } finally {
        if (hiveAuthzBinding != null) {
          hiveAuthzBinding.close();
        }
      }
    }
    return listObjs;
  }

  @Override
  public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext hiveAuthzContext,
      List<HivePrivilegeObject> list) throws SemanticException {
    // Sentry does not support this feature yet. Returning null is enough to let Hive
    // that no row filtering nor column masking will be applied.
    return null;
  }

  @Override
  public boolean needTransform() {
    // Hive uses this value to know whether a Hive query must be transformed if row filtering
    // or column masking is applied. Sentry does not support such feature yet, so returning
    // false is enough to let Hive know that the query is not required to be transformed.
    return false;
  }

  private List<HivePrivilegeObject> filterShowTables(List<HivePrivilegeObject> listObjs,
      String userName, HiveAuthzBinding hiveAuthzBinding) {
    List<HivePrivilegeObject> filteredResult = new ArrayList<HivePrivilegeObject>();
    Subject subject = new Subject(userName);
    HiveAuthzPrivileges tableMetaDataPrivilege =
        new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
            .addInputObjectPriviledge(AuthorizableType.Column,
                EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT))
            .setOperationScope(HiveOperationScope.TABLE)
            .setOperationType(
                HiveAuthzPrivileges.HiveOperationType.INFO)
            .build();

    for (HivePrivilegeObject obj : listObjs) {
      // if user has privileges on table, add to filtered list, else discard
      Table table = new Table(obj.getObjectName());
      Database database;
      database = new Database(obj.getDbname());

      List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<DBModelAuthorizable> externalAuthorizableHierarchy =
          new ArrayList<DBModelAuthorizable>();
      externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
      externalAuthorizableHierarchy.add(database);
      externalAuthorizableHierarchy.add(table);
      externalAuthorizableHierarchy.add(Column.ALL);
      inputHierarchy.add(externalAuthorizableHierarchy);

      try {
        hiveAuthzBinding.authorize(HiveOperation.SHOWTABLES, tableMetaDataPrivilege, subject,
            inputHierarchy, outputHierarchy);
        filteredResult.add(obj);
      } catch (AuthorizationException e) {
        // squash the exception, user doesn't have privileges, so the table is
        // not added to
        // filtered list.
      }
    }
    return filteredResult;
  }

  private List<HivePrivilegeObject> filterShowDatabases(List<HivePrivilegeObject> listObjs,
      String userName, HiveAuthzBinding hiveAuthzBinding) {
    List<HivePrivilegeObject> filteredResult = new ArrayList<HivePrivilegeObject>();
    Subject subject = new Subject(userName);
    HiveAuthzPrivileges anyPrivilege =
        new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
            .addInputObjectPriviledge(
                AuthorizableType.Column,
                EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT, DBModelAction.ALTER,
                    DBModelAction.CREATE, DBModelAction.DROP, DBModelAction.INDEX,
                    DBModelAction.LOCK))
            .setOperationScope(HiveOperationScope.CONNECT)
            .setOperationType(
                HiveAuthzPrivileges.HiveOperationType.QUERY)
            .build();

    for (HivePrivilegeObject obj : listObjs) {
      // if user has privileges on database, add to filtered list, else discard
      Database database = null;

      // if default is not restricted, continue
      if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(obj.getObjectName())
          && "false".equalsIgnoreCase(hiveAuthzBinding.getAuthzConf().get(
              HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "false"))) {
        filteredResult.add(obj);
        continue;
      }

      database = new Database(obj.getObjectName());

      List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<DBModelAuthorizable> externalAuthorizableHierarchy =
          new ArrayList<DBModelAuthorizable>();
      externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
      externalAuthorizableHierarchy.add(database);
      externalAuthorizableHierarchy.add(Table.ALL);
      externalAuthorizableHierarchy.add(Column.ALL);
      inputHierarchy.add(externalAuthorizableHierarchy);

      try {
        hiveAuthzBinding.authorize(HiveOperation.SHOWDATABASES, anyPrivilege, subject,
            inputHierarchy, outputHierarchy);
        filteredResult.add(obj);
      } catch (AuthorizationException e) {
        // squash the exception, user doesn't have privileges, so the table is
        // not added to
        // filtered list.
      }
    }
    return filteredResult;
  }
}
