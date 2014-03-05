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
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();
    /* Currently Hive treats both select and insert as Query
     * The difference is that the insert also has output table entities
     */
    HiveAuthzPrivileges tableQueryPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.INSERT)).
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.QUERY).
        build();
    HiveAuthzPrivileges tableLoadPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DATA_LOAD).
        build();

    HiveAuthzPrivileges tableExportPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DATA_UNLOAD).
        build();

    HiveAuthzPrivileges tableMetaDataPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.INFO).
        build();

    HiveAuthzPrivileges dbDDLPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges dbImportPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges createViewPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
    addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
    addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
    addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
    setOperationScope(HiveOperationScope.DATABASE).
    setOperationType(HiveOperationType.DDL).
    build();

    HiveAuthzPrivileges dbMetaDataPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
      addInputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.SELECT)).
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
        addOutputObjectPriviledge(AuthorizableType.Server, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.SERVER).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges anyPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
        setOperationScope(HiveOperationScope.CONNECT).
        setOperationType(HiveOperationType.QUERY).
        build();

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ADDCOLS, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_REPLACECOLS, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAMECOL, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAMEPART, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAME, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_DROPPARTS, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ADDPARTS, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_TOUCH, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ARCHIVE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_UNARCHIVE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SERIALIZER, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_PROPERTIES, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_SERIALIZER, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SERDEPROPERTIES, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_SERDEPROPERTIES, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_CLUSTER_SORT, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ANALYZE_TABLE, tableQueryPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.SWITCHDATABASE, anyPrivilege);
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
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEFUNCTION, anyPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPFUNCTION, anyPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEVIEW, createViewPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPVIEW, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEINDEX, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPINDEX, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPDATABASE, serverPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPTABLE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.EXPORT, tableExportPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.IMPORT, dbImportPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.LOAD, tableLoadPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERINDEX_REBUILD, tableDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERVIEW_PROPERTIES, tableDDLPrivilege);
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
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_PROTECTMODE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_PROTECTMODE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_FILEFORMAT, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_FILEFORMAT, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_LOCATION, serverPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_LOCATION, serverPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEDATABASE, serverPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATETABLE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATETABLE_AS_SELECT,
        new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build());
    hiveAuthzStmtPrivMap.put(HiveOperation.QUERY, tableQueryPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERINDEX_PROPS, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERDATABASE, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DESCDATABASE, dbMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DESCTABLE, tableMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_MERGEFILES, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_MERGEFILES, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SKEWED, dbDDLPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTBLPART_SKEWED_LOCATION, dbDDLPrivilege);

    hiveAuthzExtendedPrivMap.put(HiveExtendedOperation.TRANSFORM, serverPrivilege);
  }

  public static HiveAuthzPrivileges getHiveAuthzPrivileges(HiveOperation hiveStmtOp) {
    return hiveAuthzStmtPrivMap.get(hiveStmtOp);
  }

  public static HiveAuthzPrivileges getHiveExtendedAuthzPrivileges(HiveExtendedOperation hiveExtOp) {
    return hiveAuthzExtendedPrivMap.get(hiveExtOp);
  }
}
