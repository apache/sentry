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
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationType;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;

public class HiveAuthzPrivilegesMap {
  private static final Map <HiveOperation, HiveAuthzPrivileges> hiveAuthzStmtPrivMap =
    new HashMap<HiveOperation, HiveAuthzPrivileges>();
  static {
    HiveAuthzPrivileges createServerPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Server, EnumSet.of(DBModelAction.CREATE)).
        setOperationScope(HiveOperationScope.SERVER).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges macroCreatePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.CREATE)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges dropMacroPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.DROP)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges tableCreatePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.CREATE)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).//TODO: make it optional
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges dropDbPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.DROP)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges alterDbPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.ALTER)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges alterTablePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALTER)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges dropTablePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.DROP)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges indexTablePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.INDEX)).
        //Only used for create index location
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges alterTableAndUriPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALTER)).
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges addPartitionPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALTER)).
        //TODO: Uncomment this if we want to make it more restrictive
        //addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.CREATE)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).//TODO: make it optional
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();
    HiveAuthzPrivileges dropPartitionPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALTER)).
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.DROP)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges alterTableRenamePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.DROP)).
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.CREATE)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    // input required privilege from Hive: SELECT on column level and DELETE on table level
    // output required privilege from Hive: INSERT on table level
    // Sentry makes it more restrictive, and requires ALL at input, INSERT and ALTER at output
    HiveAuthzPrivileges alterTableExchangePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALL)).
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.INSERT, DBModelAction.ALTER)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges alterPartPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.ALTER)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.INFO).
        build();

    /* Currently Hive treats select/insert/analyze as Query
     * select = select on table
     * insert = insert on table /all on uri
     * analyze = select + insert on table
     */
    HiveAuthzPrivileges tableQueryPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        addInputObjectPriviledge(AuthorizableType.Column, EnumSet.of(DBModelAction.SELECT)).
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

    // Metadata statements which only require column-level privileges.
    HiveAuthzPrivileges columnMetaDataPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Column, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.COLUMN).
        setOperationType(HiveOperationType.INFO).
        build();

    HiveAuthzPrivileges dbImportPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.CREATE)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build();

    HiveAuthzPrivileges createViewPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
    addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.CREATE)).
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

    HiveAuthzPrivileges tableLockPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
        .addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.LOCK)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DML).
        build();

    HiveAuthzPrivileges dbLockPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
        .addInputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.LOCK))
        .setOperationScope(HiveOperationScope.DATABASE).setOperationType(HiveOperationType.DML)
        .build();

    HiveAuthzPrivileges functionPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        addOutputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.ALL)).
        setOperationScope(HiveOperationScope.FUNCTION).
        setOperationType(HiveOperationType.DATA_LOAD).
        build();

    HiveAuthzPrivileges anyPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Column, EnumSet.of(DBModelAction.SELECT,
            DBModelAction.INSERT, DBModelAction.ALTER, DBModelAction.CREATE, DBModelAction.DROP,
            DBModelAction.INDEX, DBModelAction.LOCK)).
        setOperationScope(HiveOperationScope.CONNECT).
        setOperationType(HiveOperationType.QUERY).
        build();

    HiveAuthzPrivileges truncateTablePrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addOutputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.DROP)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.DDL).
        build();

    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEDATABASE, createServerPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPDATABASE, dropDbPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATETABLE, tableCreatePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERDATABASE, alterDbPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERDATABASE_OWNER, alterDbPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEMACRO, macroCreatePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPMACRO, dropMacroPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.DROPTABLE, dropTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEVIEW, createViewPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPVIEW, dropTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEINDEX, indexTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPINDEX, indexTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERINDEX_PROPS, indexTablePrivilege);//TODO: Needs test case
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERINDEX_REBUILD, indexTablePrivilege);


    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_PROPERTIES, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SERDEPROPERTIES, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_CLUSTER_SORT, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_FILEFORMAT, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_TOUCH, alterTablePrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAMECOL, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ADDCOLS, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_REPLACECOLS, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_PARTCOLTYPE, alterPartPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_EXCHANGEPARTITION, alterTableExchangePrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_BUCKETNUM, alterPartPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_BUCKETNUM, alterPartPrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAMEPART, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ARCHIVE, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_UNARCHIVE, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_FILEFORMAT, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_SERDEPROPERTIES, alterTablePrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_MERGEFILES, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SKEWED, alterTablePrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_SERIALIZER, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_MERGEFILES, alterTablePrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERVIEW_PROPERTIES, alterTablePrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERVIEW_AS, createViewPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERVIEW_RENAME, alterTableRenamePrivilege);


    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_DROPPARTS, dropPartitionPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_ADDPARTS, addPartitionPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_RENAME, alterTableRenamePrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_SERIALIZER, alterTableAndUriPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTABLE_LOCATION, alterTableAndUriPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERPARTITION_LOCATION, alterTableAndUriPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.ALTERTBLPART_SKEWED_LOCATION, alterTableAndUriPrivilege);//TODO: Needs test case

    // MSCK REPAIR TABLE <table name> / ALTER TABLE RECOVER PARTITIONS <tableName>
    hiveAuthzStmtPrivMap.put(HiveOperation.MSCK, alterTablePrivilege);

    hiveAuthzStmtPrivMap.put(HiveOperation.ANALYZE_TABLE, tableQueryPrivilege);

    // SWITCHDATABASE
    hiveAuthzStmtPrivMap.put(HiveOperation.SWITCHDATABASE, anyPrivilege);

    // CREATEFUNCTION
    // DROPFUNCTION
    hiveAuthzStmtPrivMap.put(HiveOperation.CREATEFUNCTION, functionPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DROPFUNCTION, functionPrivilege);

    // SHOWCOLUMNS
    hiveAuthzStmtPrivMap.put(HiveOperation.SHOWCOLUMNS, columnMetaDataPrivilege);

    // SHOWDATABASES
    // SHOWTABLES
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
    hiveAuthzStmtPrivMap.put(HiveOperation.LOCKTABLE, tableLockPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.UNLOCKTABLE, tableLockPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.LOCKDB, dbLockPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.UNLOCKDB, dbLockPrivilege);
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
        addInputObjectPriviledge(AuthorizableType.Column, EnumSet.of(DBModelAction.SELECT)).
        addInputObjectPriviledge(AuthorizableType.URI,EnumSet.of(DBModelAction.ALL)).
        addOutputObjectPriviledge(AuthorizableType.Db, EnumSet.of(DBModelAction.CREATE)).
        setOperationScope(HiveOperationScope.DATABASE).
        setOperationType(HiveOperationType.DDL).
        build());
    hiveAuthzStmtPrivMap.put(HiveOperation.QUERY, tableQueryPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DESCDATABASE, dbMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.DESCTABLE, tableMetaDataPrivilege);
    hiveAuthzStmtPrivMap.put(HiveOperation.TRUNCATETABLE, truncateTablePrivilege);
  }

  public static HiveAuthzPrivileges getHiveAuthzPrivileges(HiveOperation hiveStmtOp) {
    return hiveAuthzStmtPrivMap.get(hiveStmtOp);
  }

  private HiveAuthzPrivilegesMap() {
    // Make constructor private to avoid instantiation
  }
}
