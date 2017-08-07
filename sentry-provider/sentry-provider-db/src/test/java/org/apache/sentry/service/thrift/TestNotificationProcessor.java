/*
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

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory;
import org.apache.sentry.hdfs.Updateable;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

// TODO 1. More tests should be added here.
// TODO 2. Tests using actual sentry store where.
@SuppressWarnings("unused")
public class TestNotificationProcessor {

  private static final SentryStore sentryStore = Mockito.mock(SentryStore.class);
  private final static String hiveInstance = "server2";
  private final static Configuration conf = new Configuration();
  private final SentryJSONMessageFactory messageFactory = new SentryJSONMessageFactory();
  private NotificationProcessor notificationProcessor;

  @BeforeClass
  public static void setup() {
    conf.set("sentry.hive.sync.create", "true");
    conf.set("sentry.hive.sync.drop", "true");

    // enable HDFS sync, so perm and path changes will be saved into DB
    conf.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    conf.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");
  }

  @After
  public void resetConf() {
    conf.set("sentry.hive.sync.create", "true");
    conf.set("sentry.hive.sync.drop", "true");
    reset(sentryStore);
  }

  @Test
  /*
    Makes sure that appropriate sentry store methods are invoked when create database event is
    processed.

    Also, checks the hive sync configuration.
   */
  public void testCreateDatabase() throws Exception {
    long seqNum = 1;
    String dbName = "db1";
    String uriPrefix = "hdfs:///";
    String location = "user/hive/warehouse";
    NotificationEvent notificationEvent;
    TSentryAuthorizable authorizable;
    notificationProcessor = new NotificationProcessor(sentryStore,
        hiveInstance, conf);

    // Create notification event
    notificationEvent = new NotificationEvent(seqNum, 0,
        HCatEventMessage.EventType.CREATE_DATABASE.toString(),
        messageFactory.buildCreateDatabaseMessage(new Database(dbName,
            null, uriPrefix + location, null)).toString());

    notificationProcessor.processNotificationEvent(notificationEvent);

    authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));

    verify(sentryStore, times(1)).dropPrivilege(authorizable,
        NotificationProcessor.getPermUpdatableOnDrop(authorizable));
    reset(sentryStore);

    //Change the configuration and make sure that exiting privileges are not dropped
    notificationProcessor.setSyncStoreOnCreate(false);
    dbName = "db2";
    notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.CREATE_DATABASE.toString(),
        messageFactory.buildCreateDatabaseMessage(new Database(dbName,
            null, "hdfs:///db2", null)).toString());

    notificationProcessor.processNotificationEvent(notificationEvent);

    authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);

    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    //making sure that privileges are not dropped
    verify(sentryStore, times(0)).dropPrivilege(authorizable,
        NotificationProcessor.getPermUpdatableOnDrop(authorizable));

  }

  @Test
  /*
    Makes sure that appropriate sentry store methods are invoked when drop database event is
    processed.

    Also, checks the hive sync configuration.
   */
  public void testDropDatabase() throws Exception {
    String dbName = "db1";

    notificationProcessor = new NotificationProcessor(sentryStore,
        hiveInstance, conf);

    // Create notification event
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.DROP_DATABASE.toString(),
        messageFactory.buildDropDatabaseMessage(new Database(dbName, null,
            "hdfs:///db1", null)).toString());

    notificationProcessor.processNotificationEvent(notificationEvent);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");

    //noinspection unchecked
    verify(sentryStore, times(1)).deleteAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(1)).dropPrivilege(authorizable,
        NotificationProcessor.getPermUpdatableOnDrop(authorizable));
    reset(sentryStore);

    // Change the configuration and make sure that exiting privileges are not dropped
    notificationProcessor.setSyncStoreOnDrop(false);
    dbName = "db2";
    // Create notification event
    notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.DROP_DATABASE.toString(),
        messageFactory.buildDropDatabaseMessage(new Database(dbName, null,
            "hdfs:///db2", null)).toString());

    notificationProcessor.processNotificationEvent(notificationEvent);

    authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);

    //noinspection unchecked
    verify(sentryStore, times(1)).deleteAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    verify(sentryStore, times(0)).dropPrivilege(authorizable,
        NotificationProcessor.getPermUpdatableOnDrop(authorizable));
  }

  @Test
  /*
    Makes sure that appropriate sentry store methods are invoked when create table event is
    processed.

    Also, checks the hive sync configuration.
   */
  public void testCreateTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    notificationProcessor = new NotificationProcessor(sentryStore,
        hiveInstance, conf);

    // Create notification event
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent =
        new NotificationEvent(1, 0, HCatEventMessage.EventType.CREATE_TABLE.toString(),
            messageFactory.buildCreateTableMessage(new Table(tableName,
                dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());

    notificationProcessor.processNotificationEvent(notificationEvent);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));

    verify(sentryStore, times(1)).dropPrivilege(authorizable,
        NotificationProcessor.getPermUpdatableOnDrop(authorizable));
    reset(sentryStore);

    // Change the configuration and make sure that existing privileges are not dropped
    notificationProcessor.setSyncStoreOnCreate(false);

    // Create notification event
    dbName = "db2";
    tableName = "table2";
    sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table2");
    notificationEvent =
        new NotificationEvent(1, 0, HCatEventMessage.EventType.CREATE_TABLE.toString(),
            messageFactory.buildCreateTableMessage(new Table(tableName,
                dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());

    notificationProcessor.processNotificationEvent(notificationEvent);

    authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    // Making sure that privileges are not dropped
    verify(sentryStore, times(0)).dropPrivilege(authorizable,
        NotificationProcessor.getPermUpdatableOnDrop(authorizable));
  }

  @Test
  /*
    Makes sure that appropriate sentry store methods are invoked when drop table event is
    processed.

    Also, checks the hive sync configuration.
   */
  public void testDropTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    Configuration authConf = new Configuration();
    // enable HDFS sync, so perm and path changes will be saved into DB
    authConf.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    authConf.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");

    notificationProcessor = new NotificationProcessor(sentryStore,
        hiveInstance, authConf);

    // Create notification event
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.DROP_TABLE.toString(),
        messageFactory.buildDropTableMessage(new Table(tableName,
            dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());

    notificationProcessor.processNotificationEvent(notificationEvent);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    verify(sentryStore, times(1)).deleteAllAuthzPathsMapping(Mockito.anyString(),
        Mockito.any(Updateable.Update.class));

    verify(sentryStore, times(1)).dropPrivilege(authorizable,
        NotificationProcessor.getPermUpdatableOnDrop(authorizable));
  }

  @Test
  /*
    Makes sure that appropriate sentry store methods are invoked when alter tables event is
    processed.
   */
  public void testAlterTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    String newDbName = "db1";
    String newTableName = "table2";

    Configuration authConf = new Configuration();
    // enable HDFS sync, so perm and path changes will be saved into DB
    authConf.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    authConf.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");

    notificationProcessor = new NotificationProcessor(sentryStore,
        hiveInstance, authConf);

    // Create notification event
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.ALTER_TABLE.toString(),
        messageFactory.buildAlterTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
            new Table(newTableName, newDbName, null, 0, 0, 0, sd, null, null, null, null, null))
            .toString());
    notificationEvent.setDbName(newDbName);
    notificationEvent.setTableName(newTableName);

    notificationProcessor.processNotificationEvent(notificationEvent);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);

    verify(sentryStore, times(1)).renameAuthzObj(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(Updateable.Update.class));

    verify(sentryStore, times(1)).renamePrivilege(authorizable, newAuthorizable,
        NotificationProcessor.getPermUpdatableOnRename(authorizable, newAuthorizable));
  }

  @Test
  /*
    Makes sure that appropriate sentry store methods are invoked when alter tables event is
    processed.
   */
  public void testRenameTableWithLocationUpdate() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    String newDbName = "db1";
    String newTableName = "table2";

    Configuration authConf = new Configuration();
    // enable HDFS sync, so perm and path changes will be saved into DB
    authConf.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    authConf.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");

    notificationProcessor = new NotificationProcessor(sentryStore,
        hiveInstance, authConf);

    // Create notification event
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    StorageDescriptor new_sd = new StorageDescriptor();
    new_sd.setLocation("hdfs:///db1.db/table2");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.ALTER_TABLE.toString(),
        messageFactory.buildAlterTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
            new Table(newTableName, newDbName, null, 0, 0, 0, new_sd, null, null, null, null, null))
            .toString());
    notificationEvent.setDbName(newDbName);
    notificationEvent.setTableName(newTableName);

    notificationProcessor.processNotificationEvent(notificationEvent);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);

    verify(sentryStore, times(1)).renameAuthzPathsMapping(Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyString(), Mockito.any(Updateable.Update.class));

    verify(sentryStore, times(1)).renamePrivilege(authorizable, newAuthorizable,
        NotificationProcessor.getPermUpdatableOnRename(authorizable, newAuthorizable));
  }

  @Test
  /*
    Test to made sure that sentry store is not invoked when invalid alter table event is
    processed.
   */
  public void testAlterTableWithInvalidEvent() throws Exception {
    String dbName = "db1";
    String tableName1 = "table1";
    String tableName2 = "table2";
    long inputEventId = 1;
    NotificationEvent notificationEvent;
    List<FieldSchema> partCols;
    StorageDescriptor sd;
    Mockito.doNothing().when(sentryStore).persistLastProcessedNotificationID(Mockito.anyLong());
    //noinspection unchecked
    Mockito.doNothing().when(sentryStore).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));

    Configuration authConf = new Configuration();
    // enable HDFS sync, so perm and path changes will be saved into DB
    authConf.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    authConf.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");

    notificationProcessor = new NotificationProcessor(sentryStore,
        hiveInstance, authConf);

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table1");
    partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName1, dbName, null, 0, 0, 0, sd, partCols,
        null, null, null, null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(table).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    inputEventId += 1;
    // Process the notification
    notificationProcessor.processNotificationEvent(notificationEvent);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    reset(sentryStore);

    // Create alter table notification with out actually changing anything.
    // This notification should not be processed by sentry server
    // Notification should be persisted explicitly
    notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.ALTER_TABLE.toString(),
        messageFactory.buildAlterTableMessage(
            new Table(tableName1, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
            new Table(tableName1, dbName, null, 0, 0, 0, sd, null,
                null, null, null, null)).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    inputEventId += 1;
    // Process the notification
    notificationProcessor.processNotificationEvent(notificationEvent);
    // Make sure that renameAuthzObj and deleteAuthzPathsMapping were  not invoked
    // to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID is explicitly invoked
    verify(sentryStore, times(0)).renameAuthzObj(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(Updateable.Update.class));
    //noinspection unchecked
    verify(sentryStore, times(0)).deleteAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
    reset(sentryStore);

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table2");
    partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table1 = new Table(tableName2, dbName, null, 0, 0, 0, sd,
        partCols, null, null, null, null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(table1).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName2);
    // Process the notification
    notificationProcessor.processNotificationEvent(notificationEvent);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(Updateable.Update.class));
  }
}
