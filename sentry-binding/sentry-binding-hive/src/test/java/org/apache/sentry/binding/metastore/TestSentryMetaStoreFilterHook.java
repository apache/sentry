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
package org.apache.sentry.binding.metastore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationType;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Testing classes for the {@code }SentryMetaStoreFilterHook}. {@code} SentryMetaStoreFilterHook}
 * may be used by the HMS server to filter databases, tables and partitions that are authorized
 * to be seen by a user making the HMS request.
 */
public class TestSentryMetaStoreFilterHook {
  // Mock the HiveAuthzBinding to avoid making real connections to a Sentry server
  private HiveAuthzBinding mockBinding = Mockito.mock(HiveAuthzBinding.class);
  private final HiveAuthzPrivileges LIST_DATABASES_PRIVILEGES = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
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

  private final HiveAuthzPrivileges LIST_TABLES_PRIVILEGES = new HiveAuthzPrivileges.AuthzPrivilegeBuilder()
    .addInputObjectPriviledge(
      AuthorizableType.Column,
      EnumSet.of(
        DBModelAction.SELECT, DBModelAction.INSERT, DBModelAction.ALTER,
        DBModelAction.DROP, DBModelAction.INDEX, DBModelAction.LOCK))
    .setOperationScope(HiveOperationScope.TABLE)
    .setOperationType(
      HiveAuthzPrivileges.HiveOperationType.INFO)
    .build();

  private HiveAuthzConf authzConf = new HiveAuthzConf();
  private final Server SERVER1 = new Server("server1");

  // Mock the authorize() method to throw an exception for the following list of databases
  private void restrictDatabaseNamesOnBinding(String username, List<String> dbNames) {
    for (String dbName : dbNames) {
      Database database = new Database(dbName);
      List<DBModelAuthorizable> authorizable = Arrays.asList(
        SERVER1, database, Table.ALL, Column.ALL
      );

      Mockito.doThrow(new AuthorizationException())
        .when(mockBinding).authorize(HiveOperation.SHOWDATABASES, LIST_DATABASES_PRIVILEGES, new Subject(username),
        Collections.singleton(authorizable), Collections.emptySet());
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

  // Returns a mock of the HiveAuthzBindingFactory with the userName wrapped inside.
  private HiveAuthzBindingFactory getMockBinding(String userName) {
    return new HiveAuthzBindingFactory() {
      @Override
      public HiveAuthzBinding fromMetaStoreConf(HiveConf hiveConf, HiveAuthzConf authzConf) throws Exception {
        return mockBinding;
      }

      @Override
      public String getUserName() {
        return userName;
      }
    };
  }

  private org.apache.hadoop.hive.metastore.api.Database newHmsDatabase(String dbName) {
    org.apache.hadoop.hive.metastore.api.Database db =
      new org.apache.hadoop.hive.metastore.api.Database();

    db.setName(dbName);
    return db;
  }

  private org.apache.hadoop.hive.metastore.api.Table newHmsTable(String dbName, String tableName) {
    org.apache.hadoop.hive.metastore.api.Table table =
      new org.apache.hadoop.hive.metastore.api.Table();

    table.setDbName(dbName);
    table.setTableName(tableName);
    return table;
  }

  @Before
  public void setup() {
    // Reset the mocks in case it was modified on the test methods
    Mockito.reset(mockBinding);

    Mockito.when(mockBinding.getAuthServer()).thenReturn(SERVER1);
    Mockito.when(mockBinding.getAuthzConf()).thenReturn(authzConf);
  }

  @Test
  public void testFilterListOfDatabases() {
    final String USER1 = "user1";
    SentryMetaStoreFilterHook filterHook = new SentryMetaStoreFilterHook(null, authzConf,
      getMockBinding(USER1));

    // Verify that only db2 is returned by the filter
    restrictDatabaseNamesOnBinding(USER1, Arrays.asList("db1", "db3"));
    assertThat(filterHook.filterDatabases(Arrays.asList("db1", "db2", "db3"))).containsExactly("db2");
  }

  @Test
  public void testFilterSingleDatabase() throws NoSuchObjectException {
    final String USER1 = "user1";
    SentryMetaStoreFilterHook filterHook = new SentryMetaStoreFilterHook(null, authzConf,
      getMockBinding(USER1));

    restrictDatabaseNamesOnBinding(USER1, Arrays.asList("db1", "db3"));

    // db1 and db3 must be denied
    assertThatExceptionOfType(NoSuchObjectException.class)
      .isThrownBy(() -> filterHook.filterDatabase(newHmsDatabase("db1")));
    assertThatExceptionOfType(NoSuchObjectException.class)
      .isThrownBy(() -> filterHook.filterDatabase(newHmsDatabase("db3")));

    // db2 must be allowed
    assertThat(filterHook.filterDatabase(newHmsDatabase("db2")))
      .isEqualTo(newHmsDatabase("db2"));
  }

  @Test
  public void testFilterListOfTables() {
    final String USER1 = "user1";
    final String DB1 = "db1";
    SentryMetaStoreFilterHook filterHook = new SentryMetaStoreFilterHook(null, authzConf,
      getMockBinding(USER1));

    // Verify that only db2 is returned by the filter
    restrictTablesNamesOnBinding(USER1, DB1, Arrays.asList("t1", "t3"));
    assertThat(filterHook.filterTableNames(DB1, Arrays.asList("t1", "t2", "t3"))).containsExactly("t2");
    assertThat(filterHook.filterTables(Arrays.asList(
      newHmsTable(DB1, "t1"),
      newHmsTable(DB1, "t2"),
      newHmsTable(DB1, "t3")
    ))).containsExactly(newHmsTable(DB1, "t2"));
  }

  @Test
  public void testFilterSingleTable() throws NoSuchObjectException {
    final String USER1 = "user1";
    final String DB1 = "db1";
    SentryMetaStoreFilterHook filterHook = new SentryMetaStoreFilterHook(null, authzConf,
      getMockBinding(USER1));

    restrictTablesNamesOnBinding(USER1, DB1, Arrays.asList("t1", "t3"));

    // t1 and t3 must be denied
    assertThatExceptionOfType(NoSuchObjectException.class)
      .isThrownBy(() -> filterHook.filterTable(newHmsTable(DB1, "t1")));
    assertThatExceptionOfType(NoSuchObjectException.class)
      .isThrownBy(() -> filterHook.filterTable(newHmsTable(DB1, "t3")));

    // t2 must be allowed
    assertThat(filterHook.filterTable(newHmsTable(DB1, "t2")))
      .isEqualTo(newHmsTable(DB1, "t2"));
  }
}
