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

package org.apache.sentry.service.thrift;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hive.hcatalog.messaging.MessageDeserializer;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAddPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAlterPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAlterTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONCreateDatabaseMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONCreateTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropDatabaseMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageDeserializer;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hive.hcatalog.messaging.HCatEventMessage.EventType.*;
import static org.junit.Assert.*;

public class TestFullUpdateModifier {
  private static final String SERVER = "s";
  private static final String PRINCIPAL = "p";
  private static final String DB = "Db1";
  private static final String TABLE = "Tab1";
  private static final String AUTH = DB.toLowerCase() + "." + TABLE.toLowerCase();
  private static final String PATH = "foo/bar";
  private static final String LOCATION = uri(PATH);

  /**
   * Convert path to HDFS URI
   */
  private static final String uri(String path) {
    return "hdfs:///" + path;
  }

  /**
   * Test create database event. It should add database and its location.
   * As a result we should have entry {"db1": {foo/bar}}
   * @throws Exception
   */
  @Test
  public void testCreateDatabase() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    NotificationEvent event = new NotificationEvent(0, 0, CREATE_DATABASE.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONCreateDatabaseMessage message =
            new SentryJSONCreateDatabaseMessage(SERVER, PRINCIPAL, DB, 0L, LOCATION);
    Mockito.when(deserializer.getCreateDatabaseMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    Map<String, Set<String>> expected = new HashMap<>();
    expected.put(DB.toLowerCase(), Collections.singleton(PATH));
    assertEquals(expected, update);
  }

  /**
   * Test drop database event. It should drop database record.
   * @throws Exception
   */
  @Test
  public void testDropDatabase() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(DB.toLowerCase(), Collections.singleton(PATH));
    NotificationEvent event = new NotificationEvent(0, 0, DROP_DATABASE.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONDropDatabaseMessage message =
            new SentryJSONDropDatabaseMessage(SERVER, PRINCIPAL, DB, 0L, LOCATION);
    Mockito.when(deserializer.getDropDatabaseMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    assertTrue(update.isEmpty());
  }

  /**
   * Test drop database event when dropped database location doesn't
   * match original database location. Should leave update intact.
   * @throws Exception
   */
  @Test
  public void testDropDatabaseWrongLocation() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(DB.toLowerCase(), Collections.singleton(PATH));

    NotificationEvent event = new NotificationEvent(0, 0, DROP_DATABASE.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONDropDatabaseMessage message =
            new SentryJSONDropDatabaseMessage(SERVER, PRINCIPAL, DB, 0L,
                    "hdfs:///bad/location");
    Mockito.when(deserializer.getDropDatabaseMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    // DB should stay
    Map<String, Set<String>> expected = new HashMap<>();
    expected.put(DB.toLowerCase(), Collections.singleton(PATH));
    assertEquals(expected, update);
  }

  /**
   * Test drop database which has tables/partitions.
   * Should drop all reated database records but leave unrelated records in place.
   * @throws Exception
   */
  @Test
  public void testDropDatabaseWithTables() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(DB.toLowerCase(), Collections.singleton(PATH));
    update.put(AUTH, Collections.singleton(PATH));
    update.put("unrelated", Collections.singleton(PATH));
    NotificationEvent event = new NotificationEvent(0, 0, DROP_DATABASE.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONDropDatabaseMessage message =
            new SentryJSONDropDatabaseMessage(SERVER, PRINCIPAL, DB, 0L, LOCATION);
    Mockito.when(deserializer.getDropDatabaseMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    Map<String, Set<String>> expected = new HashMap<>();
    expected.put("unrelated", Collections.singleton(PATH));
    assertEquals(expected, update);
  }

  /**
   * Test create table event. It should add table and its location.
   * As a result we should have entry {"db1.tab1": {foo/bar}}
   * @throws Exception
   */
  @Test
  public void testCreateTable() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    NotificationEvent event = new NotificationEvent(0, 0, CREATE_TABLE.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONCreateTableMessage message =
            new SentryJSONCreateTableMessage(SERVER, PRINCIPAL, DB, TABLE, 0L, LOCATION);
    Mockito.when(deserializer.getCreateTableMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    Map<String, Set<String>> expected = new HashMap<>();
    expected.put(AUTH, Collections.singleton(PATH));
    assertEquals(expected, update);
  }

  /**
   * Test drop table event. It should drop table record.
   * @throws Exception
   */
  @Test
  public void testDropTable() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(AUTH, Collections.singleton(PATH));
    NotificationEvent event = new NotificationEvent(0, 0, DROP_TABLE.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONDropTableMessage message =
            new SentryJSONDropTableMessage(SERVER, PRINCIPAL, DB, TABLE, 0L, LOCATION);
    Mockito.when(deserializer.getDropTableMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    assertTrue(update.isEmpty());
  }

  /**
   * Test drop table event. It should drop table record.
   * @throws Exception
   */
  @Test
  public void testDropTableWrongLocation() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(AUTH, Collections.singleton(PATH));
    NotificationEvent event = new NotificationEvent(0, 0, DROP_TABLE.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONDropTableMessage message =
            new SentryJSONDropTableMessage(SERVER, PRINCIPAL, DB, TABLE, 0L,
                    "hdfs:///bad/location");
    Mockito.when(deserializer.getDropTableMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    // DB should stay
    assertEquals(Collections.singleton(PATH), update.get(AUTH));
    assertEquals(1, update.size());
  }

  /**
   * Test add partition event. It should add table and its location.
   * As a result we should have entry {"db1.tab1": {foo/bar, hello/world}}
   * @throws Exception
   */
  @Test
  public void testAddPartition() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    Set<String> locations = new HashSet<>();
    locations.add(PATH);
    update.put(AUTH, locations);

    NotificationEvent event = new NotificationEvent(0, 0, ADD_PARTITION.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    String partPath = "hello/world";
    String partLocation = uri(partPath);

    SentryJSONAddPartitionMessage message =
            new SentryJSONAddPartitionMessage(SERVER, PRINCIPAL, DB, TABLE,
                    Collections.<Map<String,String>>emptyList(), 0L,
                    Collections.singletonList(partLocation));
    Mockito.when(deserializer.getAddPartitionMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    Set<String> expected = new HashSet<>(2);
    expected.add(PATH);
    expected.add(partPath);
    assertEquals(expected, update.get(AUTH));
  }

  /**
   * Test drop partition event. It should drop partition info from the list of locations.
   * @throws Exception
   */
  @Test
  public void testDropPartitions() throws Exception {
    String partPath = "hello/world";
    String partLocation = uri(partPath);
    Map<String, Collection<String>> update = new HashMap<>();
    Set<String> locations = new HashSet<>();
    locations.add(PATH);
    locations.add(partPath);
    update.put(AUTH, locations);

    NotificationEvent event = new NotificationEvent(0, 0, DROP_PARTITION.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONDropPartitionMessage message =
            new SentryJSONDropPartitionMessage(SERVER, PRINCIPAL, DB, TABLE,
                    Collections.<Map<String,String>>emptyList(), 0L, Collections.singletonList(partLocation));
    Mockito.when(deserializer.getDropPartitionMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    assertEquals(Collections.singleton(PATH), update.get(AUTH));
  }

  /**
   * Test alter partition event. It should change partition location
   * @throws Exception
   */
  @Test
  public void testAlterPartition() throws Exception {
    String partPath = "hello/world";
    String partLocation = uri(partPath);

    String newPath = "better/world";
    String newLocation = uri(newPath);

    Map<String, Collection<String>> update = new HashMap<>();
    Set<String> locations = new HashSet<>();
    locations.add(PATH);
    locations.add(partPath);
    update.put(AUTH, locations);

    NotificationEvent event = new NotificationEvent(0, 0, ALTER_PARTITION.toString(), "");
    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONAlterPartitionMessage message =
            new SentryJSONAlterPartitionMessage(SERVER, PRINCIPAL, DB, TABLE,
                    0L, partLocation, newLocation);

    Mockito.when(deserializer.getAlterPartitionMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);

    Set<String> expected = new HashSet<>(2);
    expected.add(PATH);
    expected.add(newPath);
    assertEquals(expected, update.get(AUTH));
  }

  /**
   * Test alter table  event that changes database name when there are no tables.
   * @throws Exception
   */
  @Test
  public void testAlterTableChangeDbNameNoTables() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(DB.toLowerCase(), Collections.singleton(PATH));
    String newDbName = "Db2";

    NotificationEvent event = new NotificationEvent(0, 0, ALTER_TABLE.toString(), "");
    event.setDbName(newDbName);
    event.setTableName(TABLE);

    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONAlterTableMessage message =
            new SentryJSONAlterTableMessage(SERVER, PRINCIPAL, DB, TABLE, 0L,
                    LOCATION, LOCATION);

    Mockito.when(deserializer.getAlterTableMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    assertEquals(Collections.singleton(PATH), update.get(newDbName.toLowerCase()));
    assertFalse(update.containsKey(DB.toLowerCase()));
  }

  @Test
  /**
   * Test alter table  event that changes database name when there are tables.
   * All entries like "dbName.tableName" should have dbName changed to the new name.
   * @throws Exception
   */
  public void testAlterTableChangeDbNameWithTables() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(DB.toLowerCase(), Collections.singleton(PATH));
    Set<String> locations = new HashSet<>(1);
    locations.add(PATH);
    update.put(AUTH, locations);

    String newDbName = "Db2";
    String newAuth = newDbName.toLowerCase() + "." + TABLE.toLowerCase();

    NotificationEvent event = new NotificationEvent(0, 0, ALTER_TABLE.toString(), "");
    event.setDbName(newDbName);
    event.setTableName(TABLE);

    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONAlterTableMessage message =
            new SentryJSONAlterTableMessage(SERVER, PRINCIPAL, DB, TABLE, 0L,
                    LOCATION, LOCATION);

    Mockito.when(deserializer.getAlterTableMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    Map<String, Set<String>> expected = new HashMap<>(2);
    expected.put(newDbName.toLowerCase(), Collections.singleton(PATH));
    expected.put(newAuth, Collections.singleton(PATH));
    assertEquals(expected, update);
  }

  /**
   * Test alter table event that changes table name.
   * @throws Exception
   */
  @Test
  public void testAlterTableChangeTableName() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(DB.toLowerCase(), Collections.singleton(PATH));
    Set<String> locations = new HashSet<>(1);
    locations.add(PATH);
    update.put(AUTH, locations);

    String newTableName = "Table2";
    String newAuth = DB.toLowerCase() + "." + newTableName.toLowerCase();

    NotificationEvent event = new NotificationEvent(0, 0, ALTER_TABLE.toString(), "");
    event.setDbName(DB);
    event.setTableName(newTableName);

    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONAlterTableMessage message =
            new SentryJSONAlterTableMessage(SERVER, PRINCIPAL, DB, TABLE, 0L,
                    LOCATION, LOCATION);

    Mockito.when(deserializer.getAlterTableMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    Map<String, Set<String>> expected = new HashMap<>(2);
    expected.put(DB.toLowerCase(), Collections.singleton(PATH));
    expected.put(newAuth, Collections.singleton(PATH));
    assertEquals(expected, update);
  }

  /**
   * Test alter table event that changes object location.
   * @throws Exception
   */
  @Test
  public void testAlterTableChangeLocation() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(DB.toLowerCase(), Collections.singleton(PATH));
    Set<String> locations = new HashSet<>(1);
    locations.add(PATH);
    update.put(AUTH, locations);

    NotificationEvent event = new NotificationEvent(0, 0, ALTER_TABLE.toString(), "");
    event.setDbName(DB);
    event.setTableName(TABLE);

    String newPath = "hello/world";
    String newLocation = uri(newPath);

    MessageDeserializer deserializer = Mockito.mock(SentryJSONMessageDeserializer.class);

    SentryJSONAlterTableMessage message =
            new SentryJSONAlterTableMessage(SERVER, PRINCIPAL, DB, TABLE, 0L,
                    LOCATION, newLocation);

    Mockito.when(deserializer.getAlterTableMessage("")).thenReturn(message);
    FullUpdateModifier.applyEvent(update, event, deserializer);
    Map<String, Set<String>> expected = new HashMap<>(2);
    expected.put(DB.toLowerCase(), Collections.singleton(PATH));
    expected.put(AUTH.toLowerCase(), Collections.singleton(newPath));
    assertEquals(expected, update);
  }

  /**
   * Test renamePrefixKeys function.
   * We ask to rename "foo.bar" key to "foo.baz" key.
   * @throws Exception
   */
  @Test
  public void testRenamePrefixKeys() throws Exception {
    String oldKey = "foo.";
    String newKey = "baz.";
    String postfix = "bar";
    Map<String, Collection<String>> update = new HashMap<>();
    update.put(oldKey + postfix , Collections.<String>emptySet());
    FullUpdateModifier.renamePrefixKeys(update, oldKey, newKey);
    assertEquals(1, update.size());
    assertTrue(update.containsKey(newKey + postfix));
  }

  /**
   * Test renamePostfixKeys and RenamePrefixKeys functions mwhen the destination keys exist.
   * Should nto change anything.
   * We ask to rename "foo.bar" key to "baz.bar" key.
   * @throws Exception
   */
  @Test
  public void testRenameKeysWithConflicts() throws Exception {
    Map<String, Collection<String>> update = new HashMap<>();
    update.put("foo.bar", Collections.<String>emptySet());
    update.put("baz.bar", Collections.<String>emptySet());
    Map<String, Collection<String>> expected = new HashMap<>(update);

    FullUpdateModifier.renamePrefixKeys(update, "foo.", "baz.");
    assertEquals(update, expected);
  }
}