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

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationType;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;

/**
 * This class uses Sentry authorization to filter a list of HMS metadata objects (or authorization
 * objects) based on the Sentry privileges that a user is part of. The methods are commonly used
 * by the Sentry/HMS binding implementations to get a list of objects that a user is allowed to
 * see.
 */
public class MetastoreAuthzObjectFilter<T> {
  /**
   * This interface is used to extract information of an object to be filtered.
   * @param <T>
   */
  public interface ObjectExtractor<T> {
    String getDatabaseName(T t);
    String getTableName(T t);

  }

  private static final HiveAuthzPrivileges LIST_DATABASES_PRIVILEGES = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
    .addInputObjectPriviledge(
      AuthorizableType.Column,
      EnumSet.of(
        DBModelAction.SELECT, DBModelAction.INSERT, DBModelAction.ALTER,
        DBModelAction.CREATE, DBModelAction.DROP, DBModelAction.INDEX,
        DBModelAction.LOCK))
    .addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT))
    .setOperationScope(HiveOperationScope.CONNECT)
    .setOperationType(HiveOperationType.QUERY)
    .build();

  private static final HiveAuthzPrivileges LIST_TABLES_PRIVILEGES = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
    .addInputObjectPriviledge(
      AuthorizableType.Column,
      EnumSet.of(
        DBModelAction.SELECT, DBModelAction.INSERT, DBModelAction.ALTER,
        DBModelAction.CREATE, DBModelAction.DROP, DBModelAction.INDEX, DBModelAction.LOCK))
    .setOperationScope(HiveOperationScope.TABLE)
    .setOperationType(
      HiveAuthzPrivileges.HiveOperationType.INFO)
    .build();

  private final boolean DEFAULT_DATABASE_RESTRICTED;
  private final DBModelAuthorizable AUTH_SERVER;
  private HiveAuthzBinding authzBinding;
  private ObjectExtractor extractor;

  public MetastoreAuthzObjectFilter(HiveAuthzBinding authzBinding, ObjectExtractor extractor) {
    this.authzBinding = authzBinding;
    this.extractor = extractor;
    this.AUTH_SERVER = authzBinding.getAuthServer();
    this.DEFAULT_DATABASE_RESTRICTED = authzBinding.getAuthzConf()
      .getBoolean(HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), false);
  }

  /**
   * Return the required privileges for listing databases on a server
   * @return the required privileges for authorizing listing databases on a server
   */
  public static HiveAuthzPrivileges getListDatabasesPrivileges() {
    return LIST_DATABASES_PRIVILEGES;
  }

  /**
   * Return the required privileges for listing tables in a database
   * @return the required privileges for authorizing listing tables in a database
   */
  public static HiveAuthzPrivileges getListTablePrivileges() {
    return LIST_TABLES_PRIVILEGES;
  }

  /**
   * Filter a list of {@code dbNames} objects based on the authorization privileges of {@code subject}.
   *
   * @param username The username to request authorization from.
   * @param dbNames A list of databases that must be filtered based on the user privileges.
   * @return A list of database objects filtered by the privileges of the user. If a null value is
   * passed as {@code dbNames}, then an empty list is returned.
   */
  public List<T> filterDatabases(String username, List<T> dbNames) {
    if (dbNames == null) {
      return Collections.emptyList();
    }

    List<T> filteredDatabases = Lists.newArrayList();
    for (T dbName : dbNames) {
      String objName = extractor.getDatabaseName(dbName);
      if (Strings.isEmpty(objName) || authorizeDatabase(username, objName)) {
        filteredDatabases.add(dbName);
      }
    }

    return filteredDatabases;
  }

  /**
   * Filter a list of {@code tableNames} objects based on the authorization privileges of {@code subject}.
   *
   * @param username The username to request authorization from.
   * @param tables A list of tables that must be filtered based on the user privileges.
   * @return A list of tables objects filtered by the privileges of the user. If a null value is
   * passed as {@code tableNames}, then an empty list is returned.
   */
  public List<T> filterTables(String username, List<T> tables) {
    if (tables == null) {
      return Collections.emptyList();
    }

    List<T> filteredTables = Lists.newArrayList();
    for (T table : tables) {
      String dbName = extractor.getDatabaseName(table);
      String tableName = extractor.getTableName(table);
      if (Strings.isEmpty(dbName) || authorizeTable(username, dbName, tableName)) {
        filteredTables.add(table);
      }
    }

    return filteredTables;
  }

  /**
   * Checks if a database is authorized to be accessed by the specific user.
   * @return True if it is authorized, false otherwise.
   */
  private boolean authorizeDatabase(String username, String dbName) {
    if (!DEFAULT_DATABASE_RESTRICTED && dbName.equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
      return true;
    }

    Database database = new Database(dbName);
    List<DBModelAuthorizable> authorizable = Arrays.asList(
      AUTH_SERVER, database, Table.ALL, Column.ALL
    );

    return authorize(HiveOperation.SHOWDATABASES, LIST_DATABASES_PRIVILEGES, username, authorizable);
  }

  /**
   * Checks if a table is authorized to be accessed by the specific user.
   * @return True if it is authorized, false otherwise.
   */
  private boolean authorizeTable(String username, String dbName, String tableName) {
    Database database = new Database(dbName);
    Table table = new Table(tableName);

    List<DBModelAuthorizable> authorizable = Arrays.asList(
      AUTH_SERVER, database, table, Column.ALL
    );

    return authorize(HiveOperation.SHOWTABLES, LIST_TABLES_PRIVILEGES, username, authorizable);
  }

  /**
   * Calls the authorization method of Sentry to check the access to a specific authorizable.
   * @return True if it is authorized, false otherwise.
   */
  private boolean authorize(HiveOperation op, HiveAuthzPrivileges privs, String username, List<DBModelAuthorizable> authorizable) {
    try {
      authzBinding.authorize(op, privs, new Subject(username),
        Collections.singleton(authorizable), Collections.emptySet());
    } catch (AuthorizationException e) {
      return false;
    }

    return true;
  }
}
