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
package org.apache.sentry.service.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.HCatEventMessage.EventType;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory;
import org.apache.sentry.hdfs.Updateable;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;

import java.util.Arrays;

import org.junit.Test;
import org.junit.Ignore;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.reset;

public class TestHMSFollower {
  SentryJSONMessageFactory messageFactory = new SentryJSONMessageFactory();
  SentryStore sentryStore = Mockito.mock(SentryStore.class);
  final static String hiveInstance = "server2";

  @Test
  public void testCreateDatabase() throws Exception {
    String dbName = "db1";

    // Create notification events
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.CREATE_DATABASE.toString(),
      messageFactory.buildCreateDatabaseMessage(new Database(dbName, null, "hdfs:///db1", null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");

    verify(sentryStore, times(0)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable));
  }

  @Test
  public void testDropDatabase() throws Exception {
    String dbName = "db1";

    // Create notification events
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.DROP_DATABASE.toString(),
      messageFactory.buildDropDatabaseMessage(new Database(dbName, null, "hdfs:///db1", null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");

    verify(sentryStore, times(1)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable));
  }

  @Test
  public void testCreateTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.CREATE_TABLE.toString(),
      messageFactory.buildCreateTableMessage(new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    verify(sentryStore, times(0)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable));
  }

  @Test
  public void testDropTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.DROP_TABLE.toString(),
      messageFactory.buildDropTableMessage(new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    verify(sentryStore, times(1)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable));
  }

  @Test
  public void testRenameTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    String newDbName = "db1";
    String newTableName = "table2";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.ALTER_TABLE.toString(),
      messageFactory.buildAlterTableMessage(
        new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
        new Table(newTableName, newDbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    notificationEvent.setDbName(newDbName);
    notificationEvent.setTableName(newTableName);
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);

    verify(sentryStore, times(1)).renamePrivilege(authorizable, newAuthorizable, HMSFollower.onRenameSentryPrivilege(authorizable, newAuthorizable));
  }


  @Ignore
  @Test
  public void testAlterPartitionWithInvalidEvent() throws Exception {
    String dbName = "db1";
    String tableName1 = "table1";
    String tableName2 = "table2";
    long inputEventId = 1;
    List<NotificationEvent> events = new ArrayList<>();
    NotificationEvent notificationEvent = null;
    List<FieldSchema> partCols;
    StorageDescriptor sd = null;
    Mockito.doNothing().when(sentryStore).persistLastProcessedNotificationID(Mockito.anyLong());
    Mockito.doNothing().when(sentryStore).addAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(), Mockito.any(Updateable.Update.class));

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table1");
    partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName1, dbName, null, 0, 0, 0, sd, partCols, null, null, null, null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
      HCatEventMessage.EventType.CREATE_TABLE.toString(),
      messageFactory.buildCreateTableMessage(table).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();

    // Create a partition
    List<Partition> partitions = new ArrayList<>();
    StorageDescriptor invalidSd = new StorageDescriptor();
    invalidSd.setLocation(null);
    Partition partition = new Partition(Arrays.asList("today"), dbName, tableName1,
      0, 0, sd, null);
    partitions.add(partition);
    notificationEvent = new NotificationEvent(inputEventId, 0, EventType.ADD_PARTITION.toString(),
      messageFactory.buildAddPartitionMessage(table, partitions).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    //Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle ADD_PARTITION notification
    // and persistLastProcessedNotificationID was not invoked.
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();

    // Create a alter notification with out actually changing anything.
    // This is an invalid event and should be processed by sentry store.
    // Event Id should be explicitly persisted using persistLastProcessedNotificationID
    notificationEvent = new NotificationEvent(inputEventId, 0, EventType.ALTER_PARTITION.toString(),
      messageFactory.buildAlterPartitionMessage(partition, partition).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that persistLastProcessedNotificationID is invoked explicitly.
    verify(sentryStore, times(1)).persistLastProcessedNotificationID(inputEventId - 1);
    reset(sentryStore);
    events.clear();

    // Create a alter notification with some actual change.
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://user/hive/wareshouse/db1.db/table1");
    Partition updatedPartition = new Partition(partition);
    updatedPartition.setSd(sd);
    notificationEvent = new NotificationEvent(inputEventId, 0, EventType.ALTER_PARTITION.toString(),
      messageFactory.buildAlterPartitionMessage(partition, updatedPartition).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that updateAuthzPathsMapping was invoked once to handle ALTER_PARTITION
    // notification and persistLastProcessedNotificationID was not invoked.
    verify(sentryStore, times(1)).updateAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyString(), Mockito.anyString(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(inputEventId - 1);
    reset(sentryStore);
    events.clear();

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table2");
    partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table1 = new Table(tableName2, dbName, null, 0, 0, 0, sd, partCols, null, null, null, null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
      HCatEventMessage.EventType.CREATE_TABLE.toString(),
      messageFactory.buildCreateTableMessage(table1).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName2);
    events.add(notificationEvent);
    // Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
  }

  @Test
  public void testAlterTableWithInvalidEvent() throws Exception {
    String dbName = "db1";
    String tableName1 = "table1";
    String tableName2 = "table2";
    long inputEventId = 1;
    List<NotificationEvent> events = new ArrayList<>();
    NotificationEvent notificationEvent = null;
    List<FieldSchema> partCols;
    StorageDescriptor sd = null;
    Mockito.doNothing().when(sentryStore).persistLastProcessedNotificationID(Mockito.anyLong());
    Mockito.doNothing().when(sentryStore).addAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(), Mockito.any(Updateable.Update.class));

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table1");
    partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName1, dbName, null, 0, 0, 0, sd, partCols, null, null, null, null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
      HCatEventMessage.EventType.CREATE_TABLE.toString(),
      messageFactory.buildCreateTableMessage(table).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();


    // Create alter table notification with actuall changing anything.
    // This notification should not be processed by sentry server
    // Notification should be persisted explicitly
    notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.ALTER_TABLE.toString(),
      messageFactory.buildAlterTableMessage(
        new Table(tableName1, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
        new Table(tableName1, dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events = new ArrayList<>();
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that renameAuthzObj and deleteAuthzPathsMapping were  not invoked
    // to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID is explicitly invoked
    verify(sentryStore, times(0)).renameAuthzObj(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).deleteAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(),  Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(1)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table2");
    partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table1 = new Table(tableName2, dbName, null, 0, 0, 0, sd, partCols, null, null, null, null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
      HCatEventMessage.EventType.CREATE_TABLE.toString(),
      messageFactory.buildCreateTableMessage(table1).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName2);
    events.add(notificationEvent);
    // Process the notification
    hmsFollower.processNotificationEvents(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
      Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
  }
}
