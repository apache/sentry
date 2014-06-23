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
package org.apache.sentry.binding.hive.authz;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveExtendedOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationType;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;

public class HiveAuthzPrivilegesMap {
  private static final Map <HiveOperation, HiveAuthzPrivileges> hiveAuthzStmtPrivMap =
    new HashMap<HiveOperation, HiveAuthzPrivileges>();
  private static final Map <HiveExtendedOperation, HiveAuthzPrivileges> hiveAuthzExtendedPrivMap =
    new HashMap<HiveExtendedOperation, HiveAuthzPrivileges>();
  static {
    HiveAuthzPrivileges tableDDLPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges tableDDLAndUriPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges tableDDLAndOptionalUriPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).//TODO: make it optional
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();

    /* Currently Hive treats select/insert/analyze as Query
     * select = select on table
     * insert = insert on table /all on uri
     * analyze = select + insert on table
     */
    HiveAuthzPrivileges tableQueryPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.INSERT)).
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.QUERY).
        build();

    HiveAuthzPrivileges tableLoadPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DATA_LOAD).
        build();

    HiveAuthzPrivileges tableExportPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DATA_UNLOAD).
        build();

    HiveAuthzPrivileges tableMetaDataPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.INFO).
        build();

    HiveAuthzPrivileges dbDDLPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges createTablePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).//TODO: make it optional
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges dbImportPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges createViewPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
    addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
    addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
    addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).//TODO: This should not be required
    setOperationScope(HiveOperationScope.DATABASE).
    setOperationType(HiveOperationType.DDL).
    build();

    HiveAuthzPrivileges dbMetaDataPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
      addInputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
      setOperationScope(HiveOperationScope.DATABASE).
      setOperationType(HiveOperationType.INFO).
      build();

    HiveAuthzPrivileges tableDMLPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DML).
        build();
    HiveAuthzPrivileges serverPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Server, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.SERVER).
        setOperationType(HiveOperationType.DDL).
        build();


    HiveAuthzPrivileges anyPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)). //TODO: make them ||
        setOperationScope(HiveOperationScope.CONNECT).
        setOperationType(HiveOperationType.QUERY).
        build();

    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEDATABASE, serverPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPDATABASE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATETABLE, createTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPTABLE, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEVIEW, createViewPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPVIEW, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEINDEX, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPINDEX, tableDDLPrivilege);


    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAME, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_PROPERTIES, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SERDEPROPERTIES, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_CLUSTER_SORT, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_FILEFORMAT, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_TOUCH, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_PROTECTMODE, tableDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAMECOL, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ADDCOLS, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_REPLACECOLS, tableDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ADDPARTS, tableDDLAndOptionalUriPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAMEPART, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_DROPPARTS, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ARCHIVE, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_UNARCHIVE, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_FILEFORMAT, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_PROTECTMODE, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_SERDEPROPERTIES, tableDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SERIALIZER, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_MERGEFILES, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SKEWED, tableDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_SERIALIZER, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_MERGEFILES, tableDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERINDEX_PROPS, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERINDEX_REBUILD, tableDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERVIEW_PROPERTIES, tableDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_LOCATION, tableDDLAndUriPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_LOCATION, tableDDLAndUriPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTBLPART_SKEWED_LOCATION, tableDDLAndUriPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERDATABASE, dbDDLPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ANALYZE_TABLE, tableQueryPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.SWITCHDATABASE, anyPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEFUNCTION, anyPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPFUNCTION, anyPrivilege);

    // SHOWDATABASES
    // SHOWTABLES
    hiveAuthzStmtPrivMap.put(HiveOperation.SHOWCOLUMNS, tableMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.SHOW_TABLESTATUS, tableMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.SHOW_TBLPROPERTIES, tableMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.SHOW_CREATETABLE, tableMetaDataPrivilege);
    // SHOWFUNCTIONS
    hiveAuthzStmtPrivMap.put(HiveOperation.SHOWINDEXES, tableMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.SHOWPARTITIONS, tableMetaDataPrivilege);
    // SHOWLOCKS
    hiveAuthzStmtPrivMap.put(HiveOperation.EXPORT, tableExportPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.IMPORT, dbImportPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.LOAD, tableLoadPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.LOCKTABLE, tableDMLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.UNLOCKTABLE, tableDMLPrivilege);
    // CREATEROLE
    // DROPROLE
    // GRANT_PRIVILEGE
    // REVOKE_PRIVILEGE
    // SHOW_GRANT
    // GRANT_ROLE
    // REVOKE_ROLE
    // SHOW_ROLE_GRANT
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATETABLE_AS_SELECT,
        new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build());
    hiveAuthzStmtPrivMap.put(HiveOperation.QUERY, tableQueryPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DESCDATABASE, dbMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DESCTABLE, tableMetaDataPrivilege);

    hiveAuthzExtendedPrivMap.put(HiveExtendedOperation.TRANSFORM, serverPrivilege);
  }

  public static HiveAuthzPrivileges getHiveAuthzPrivileges(HiveOperation hiveStmtOp) {
    return hiveAuthzStmtPrivMap.get(hiveStmtOp);
  }

  public static HiveAuthzPrivileges getHiveExtendedAuthzPrivileges(HiveExtendedOperation hiveExtOp) {
    return hiveAuthzExtendedPrivMap.get(hiveExtOp);
  }
}
