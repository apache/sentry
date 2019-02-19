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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationType;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter.ObjectExtractor;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.assertj.core.api.iterable.Extractor;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMetastoreAuthzObjectFilter {
  // Mock the HiveAuthzBinding to avoid making real connections to a Sentry server
  private HiveAuthzBinding mockBinding = Mockito.mock(HiveAuthzBinding.class);

  private HiveAuthzConf authzConf = new HiveAuthzConf();
  private final Server SERVER1 = new Server("server1");

  private final MetastoreAuthzObjectFilter.ObjectExtractor<String> DB_NAME_EXTRACTOR =
    new ObjectExtractor<String>() {
      @Override
      public String getDatabaseName(String s) {
        return s;
      }

      @Override
      public String getTableName(String s) {
        return null;
      }
    };

  private final MetastoreAuthzObjectFilter.ObjectExtractor<HivePrivilegeObject> HIVE_OBJECT_EXTRACTOR =
    new ObjectExtractor<HivePrivilegeObject>() {
      @Override
      public String getDatabaseName(HivePrivilegeObject o) {
        return (o != null) ? o.getDbname() : null;
      }

      @Override
      public String getTableName(HivePrivilegeObject o) {
        return (o != null) ? o.getObjectName() : null;
      }
    };

  public static final HiveAuthzPrivileges LIST_DATABASES_PRIVILEGES = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
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

  public static final HiveAuthzPrivileges LIST_TABLES_PRIVILEGES = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
      .addInputObjectPriviledge(
          AuthorizableType.Column,
          EnumSet.of(
              DBModelAction.SELECT, DBModelAction.INSERT, DBModelAction.ALTER,
              DBModelAction.CREATE, DBModelAction.DROP, DBModelAction.INDEX, DBModelAction.LOCK))
      .setOperationScope(HiveOperationScope.TABLE)
      .setOperationType(
          HiveAuthzPrivileges.HiveOperationType.INFO)
      .build();

  /**
   * Internal class used by AssertJ extract() method to extract the object name of
   * a HivePrivilegeObject
   */
  static class HiveObjectExtractor implements Extractor<HivePrivilegeObject, String> {
    private HiveObjectExtractor() {}

    public static Extractor<HivePrivilegeObject, String> objectName() {
      return new HiveObjectExtractor();
    }

    @Override
    public String extract(HivePrivilegeObject input) {
      return input.getObjectName();
    }
  }

  private void restrictDefaultDatabase(boolean b) {
    authzConf.setBoolean(HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), b);
  }

  private MetastoreAuthzObjectFilter.ObjectExtractor<String> createTableStringExtractor(String dbName) {
    return new ObjectExtractor<String>() {
      @Override
      public String getDatabaseName(String s) {
        return dbName;
      }

      @Override
      public String getTableName(String s) {
        return s;
      }
    };
  }

  // Mock the authorize() method to throw an exception for the following list of databases
  private void restrictDatabaseNamesOnBinding(String username, List<String> dbNames) {
    for (String dbName : dbNames) {
      Database database = new Database(dbName);
      List<DBModelAuthorizable> authorizable = Arrays.asList(
        SERVER1, database, Table.ALL, Column.ALL
      );

      Mockito.doThrow(new AuthorizationException())
        .when(mockBinding).authorize(HiveOperation.SHOWDATABASES,
        LIST_DATABASES_PRIVILEGES, new Subject(username), Collections.singleton(authorizable), Collections.emptySet());
    }
  }

  // Mock the authorize() method to throw an exception for the following list of databases
  private void restrictTablesNamesOnBinding(String username, String dbName, List<String> tableNames) {
    Database database = new Database(dbName);

    for (String tableName : tableNames) {
      Table table = new Table(tableName);
      List<DBModelAuthorizable> authorizable = Arrays.asList(
        SERVER1, database, table, Column.ALL
      );

      Mockito.doThrow(new AuthorizationException())
        .when(mockBinding).authorize(HiveOperation.SHOWTABLES,
        LIST_TABLES_PRIVILEGES, new Subject(username), Collections.singleton(authorizable), Collections.emptySet());
    }
  }

  // Converts a list of database HivePrivilegeObject to a HivePrivilegeObject list
  private List<HivePrivilegeObject> createHivePrivilegeDatabaseList(String ... dbNames) {
    List<HivePrivilegeObject> hiveObjects = Lists.newArrayList();
    for (String dbName : dbNames) {
      hiveObjects.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, dbName, dbName));
    }

    return hiveObjects;
  }

  // Converts a list of table HivePrivilegeObject to a HivePrivilegeObject list
  private List<HivePrivilegeObject> createHivePrivilegeTableList(String dbName, String ... tableNames) {
    List<HivePrivilegeObject> hiveObjects = Lists.newArrayList();
    for (String tableName : tableNames) {
      hiveObjects.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, dbName, tableName));
    }

    return hiveObjects;
  }

  @Before
  public void setup() {
    // Reset the mocks in case it was modified on the test methods
    Mockito.reset(mockBinding);

    Mockito.when(mockBinding.getAuthServer()).thenReturn(SERVER1);
    Mockito.when(mockBinding.getAuthzConf()).thenReturn(authzConf);

    // Do not restrict the 'default' database by default
    restrictDefaultDatabase(false);
  }

  @Test
  public void testFilterDatabaseStrings() {
    final String USER1 = "user1";

    MetastoreAuthzObjectFilter filter = new MetastoreAuthzObjectFilter(mockBinding, DB_NAME_EXTRACTOR);
    assertThat(filter).isNotNull();

    // Verify that null or empty lists do return an empty list (not null values to avoid NPE)
    assertThat(filter.filterDatabases(USER1, null)).isNotNull().isEmpty();
    assertThat(filter.filterDatabases(USER1, Collections.emptyList())).isNotNull().isEmpty();

    // Verify that null or empty items in the list are not filtered out and not cause exceptions
    assertThat(filter.filterDatabases(USER1, Arrays.asList(null, "", null, null, "")))
      .containsExactly(null, "", null, null, "");

    // Verify restricted databases db1,db3,db5 are filtered out
    restrictDatabaseNamesOnBinding(USER1, Arrays.asList("db1", "db3", "db5"));
    assertThat(filter.filterDatabases(USER1, Arrays.asList("db1", "db2", "db3", "db4", "db5")))
      .containsExactly("db2", "db4");
  }

  @Test
  public void testFilterDatabaseHiveObjects() {
    final String USER1 = "user1";

    MetastoreAuthzObjectFilter filter =
      new MetastoreAuthzObjectFilter(mockBinding, HIVE_OBJECT_EXTRACTOR);
    assertThat(filter).isNotNull();

    // Verify that null or empty lists do return an empty list (not null values to avoid NPE)
    assertThat(filter.filterDatabases(USER1, null)).isNotNull().isEmpty();
    assertThat(filter.filterDatabases(USER1, Collections.emptyList())).isNotNull().isEmpty();

    // Verify that null or empty items in the list are not filtered out and not cause exceptions
    assertThat(filter.filterDatabases(USER1, Arrays.asList(null, null)))
      .containsExactly(null, null);

    // Verify restricted databases db1,db3,db5 are filtered out
    restrictDatabaseNamesOnBinding(USER1, Arrays.asList("db1", "db3", "db5"));
    assertThat(filter.filterDatabases(USER1, createHivePrivilegeDatabaseList("db1", "db2", "db3", "db4", "db5")))
      .extracting(HiveObjectExtractor.objectName()).containsExactly("db2", "db4");
  }

  @Test
  public void testFilterDatabasesRestrictDefaultDatabase() {
    final String USER1 = "user1";
    MetastoreAuthzObjectFilter filter;

    restrictDatabaseNamesOnBinding(USER1, Arrays.asList("db1", "default"));

    // Verify the default database is restricted when filtering strings
    restrictDefaultDatabase(true);
    filter = new MetastoreAuthzObjectFilter<String>(mockBinding, DB_NAME_EXTRACTOR);
    assertThat(filter.filterDatabases(USER1, Arrays.asList("db1", "default", "db4")))
      .containsExactly("db4");

    // Verify the default database is not restricted when filtering strings
    restrictDefaultDatabase(false);
    filter = new MetastoreAuthzObjectFilter<String>(mockBinding, DB_NAME_EXTRACTOR);
    assertThat(filter.filterDatabases(USER1, Arrays.asList("db1", "default", "db4")))
      .containsExactly("default", "db4");

    // Verify the default database is restricted when filtering Hive Privilege Objects
    restrictDefaultDatabase(true);
    filter = new MetastoreAuthzObjectFilter(mockBinding, HIVE_OBJECT_EXTRACTOR);
    assertThat(filter.filterDatabases(USER1, createHivePrivilegeDatabaseList("db1", "default", "db4")))
      .extracting(HiveObjectExtractor.objectName()).containsExactly("db4");

    // Verify the default database is not restricted when filtering Hive Privilege Objects
    restrictDefaultDatabase(false);
    filter = new MetastoreAuthzObjectFilter(mockBinding, HIVE_OBJECT_EXTRACTOR);
    assertThat(filter.filterDatabases(USER1, createHivePrivilegeDatabaseList("db1", "default", "db4")))
      .extracting(HiveObjectExtractor.objectName()).containsExactly("default", "db4");
  }

  @Test
  public void testFilterTableStrings() {
    final String USER1 = "user1";
    final String DB1 = "db1";

    MetastoreAuthzObjectFilter filter =
      new MetastoreAuthzObjectFilter(mockBinding, createTableStringExtractor(DB1));
    assertThat(filter).isNotNull();

    // Verify that null or empty lists do return an empty list (not null values to avoid NPE)
    assertThat(filter.filterTables(USER1, null)).isNotNull().isEmpty();
    assertThat(filter.filterTables(USER1, null)).isNotNull().isEmpty();
    assertThat(filter.filterTables(USER1, Collections.emptyList())).isNotNull().isEmpty();

    // Verify that null or empty items in the list are not filtered out and not cause exceptions
    assertThat(filter.filterTables(USER1, Arrays.asList(null, "", null, null, "")))
      .containsExactly(null, "", null, null, "");

    // Verify restricted databases t1,t3,t5 are filtered out
    restrictTablesNamesOnBinding(USER1, DB1, Arrays.asList("t1", "t3", "t5"));
    assertThat(filter.filterTables(USER1, Arrays.asList("t1", "t2", "t3", "t4", "t5")))
      .containsExactly("t2", "t4");
  }

  @Test
  public void testFilterTableHiveObjects() {
    final String USER1 = "user1";
    final String DB1 = "db1";

    MetastoreAuthzObjectFilter<HivePrivilegeObject>
      filter = new MetastoreAuthzObjectFilter(mockBinding, HIVE_OBJECT_EXTRACTOR);
    assertThat(filter).isNotNull();

    // Verify that null or empty lists do return an empty list (not null values to avoid NPE)
    assertThat(filter.filterTables(USER1, null)).isNotNull().isEmpty();
    assertThat(filter.filterTables(USER1, null)).isNotNull().isEmpty();
    assertThat(filter.filterTables(USER1, Collections.emptyList())).isNotNull().isEmpty();

    // Verify that null or empty items in the list are not filtered out and not cause exceptions
    assertThat(filter.filterTables(USER1, Arrays.asList(null, null)))
      .containsExactly(null, null);

    // Verify restricted databases t1,t3,t5 are filtered out
    restrictTablesNamesOnBinding(USER1, DB1, Arrays.asList("t1", "t3", "t5"));
    assertThat(filter.filterTables(USER1, createHivePrivilegeTableList(DB1, "t1", "t2", "t3", "t4", "t5")))
      .extracting(HiveObjectExtractor.objectName()).containsExactly("t2", "t4");
  }
}
