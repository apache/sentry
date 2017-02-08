/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.tests.e2e.metastore;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hive.hcatalog.messaging.CreateDatabaseMessage;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.MessageDeserializer;
import org.apache.hive.hcatalog.messaging.MessageFactory;
import org.apache.hive.hcatalog.messaging.CreateTableMessage;
import org.apache.hive.hcatalog.messaging.DropTableMessage;
import org.apache.hive.hcatalog.messaging.AlterTableMessage;
import org.apache.hive.hcatalog.messaging.AlterPartitionMessage;
import org.apache.hive.hcatalog.messaging.DropDatabaseMessage;
import org.apache.hive.hcatalog.messaging.AddPartitionMessage;
import org.apache.hive.hcatalog.messaging.DropPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAlterPartitionMessage;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.hamcrest.text.IsEqualIgnoringCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

/**
 * Make sure NotificationLog is capturing the information correctly for the commands which change <Obj,Location> mapping
 * This test class is using Hive's default JSONMessageFactory and Hive's Notification log JSON deserializer.
 */

public class TestDefaultMessageFactoryInBuiltDeserializer extends AbstractMetastoreTestWithStaticConfiguration {

  protected static HiveMetaStoreClient client;
  protected static MessageDeserializer deserializer;
  protected static Random random = new Random();
  private static String testDB;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    setMetastoreListener = true;
    useDefaultMessageFactory = true;
    useDbNotificationListener = true;
    beforeClass();
  }

  protected static void beforeClass() throws Exception {
    AbstractMetastoreTestWithStaticConfiguration.setupTestStaticConfiguration();
    client = context.getMetaStoreClient(ADMIN1);
    deserializer = MessageFactory.getDeserializer("json", "");
    writePolicyFile(setAdminOnServer1(ADMINGROUP).setUserGroupMapping(StaticUserGroup.getStaticMapping()));
  }

  @AfterClass
  public static void cleanupAfterClass() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @After
  public void dropDBAfterTest() throws Exception {
    if(client != null && testDB != null) {
      dropMetastoreDBIfExists(client, testDB);
    }
  }

  @Test
  public void testCreateDropDatabase() throws Exception {
    CurrentNotificationEventId latestID, previousID;
    NotificationEventResponse response;

    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);

    // Create database
    // We need:
    // - Dbname
    // - location
    createMetastoreDB(client, testDB);
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    CreateDatabaseMessage createDatabaseMessage = deserializer.getCreateDatabaseMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.CREATE_DATABASE, createDatabaseMessage.getEventType()); //Validate EventType
    assertEquals(testDB, createDatabaseMessage.getDB()); //dbName
    //Location information is not available

    //Alter database location and rename are not supported. See HIVE-4847

    //Drop database
    // We need:
    // - dbName
    // - location
    client.dropDatabase(testDB);
    previousID = latestID;
    latestID = client.getCurrentNotificationEventId();
    assertEquals(previousID.getEventId() + 1, latestID.getEventId()); //Validate monotonically increasing eventID
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    DropDatabaseMessage dropDatabaseMessage = deserializer.getDropDatabaseMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_DATABASE, dropDatabaseMessage.getEventType()); //Event type
    assertThat(dropDatabaseMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB)); // dbName
    //Location information is not available, but we might not really need it as we can drop all paths associated with
    //the object when we drop
  }

  @Test
  public void testCreateDropTableWithPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);

    NotificationEventResponse response;
    CurrentNotificationEventId latestID, previousID;
    // Create database
    createMetastoreDB(client, testDB);

    // Create table with partition
    // We need:
    // - dbname
    // - tablename
    // - location
    createMetastoreTableWithPartition(client, testDB,
        testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    CreateTableMessage createTableMessage = deserializer.getCreateTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.CREATE_TABLE, createTableMessage.getEventType());
    assertEquals(testDB, createTableMessage.getDB()); //dbName
    assertEquals(testTable, createTableMessage.getTable()); //tableName
    //Location information is not available

    //Drop table
    // We need:
    // - dbName
    // - tableName
    // - location
    client.dropTable(testDB, testTable);
    previousID = latestID;
    latestID = client.getCurrentNotificationEventId();
    assertEquals(previousID.getEventId() + 1, latestID.getEventId());
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    DropTableMessage dropTableMessage = deserializer.getDropTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_TABLE, dropTableMessage.getEventType());
    assertThat(dropTableMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));//dbName
    assertThat(dropTableMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));//tableName
    //Location information is not available, but we might not really need it as we can drop all paths associated with
    //the object when we drop
  }

  @Test
  public void testCreateDropTableWithoutPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);

    NotificationEventResponse response;
    CurrentNotificationEventId latestID, previousID;
    // Create database
    createMetastoreDB(client, testDB);

    // Create table with partition
    // We need:
    // - dbname
    // - tablename
    // - location
    createMetastoreTable(client, testDB, testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")));
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    CreateTableMessage createTableMessage = deserializer.getCreateTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.CREATE_TABLE, createTableMessage.getEventType());
    assertEquals(testDB, createTableMessage.getDB()); //dbName
    assertEquals(testTable, createTableMessage.getTable()); //tableName
    //Location information is not available

    //Drop table
    // We need:
    // - dbName
    // - tableName
    // - location
    client.dropTable(testDB, testTable);
    previousID = latestID;
    latestID = client.getCurrentNotificationEventId();
    assertEquals(previousID.getEventId() + 1, latestID.getEventId());
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    DropTableMessage dropTableMessage = deserializer.getDropTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_TABLE, dropTableMessage.getEventType());
    assertThat(dropTableMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));//dbName
    assertThat(dropTableMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));//tableName
    //Location information is not available, but we might not really need it as we can drop all paths associated with
    //the object when we drop
  }

  @Test
  public void testAddDropPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);

    NotificationEventResponse response;
    CurrentNotificationEventId latestID, previousID;
    // Create database and table
    createMetastoreDB(client, testDB);
    Table tbl1 = createMetastoreTableWithPartition(client, testDB, testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));

    ArrayList<String> partVals1 = Lists.newArrayList("part1");

    //Add partition
    // We need:
    // - dbName
    // - tableName
    // - partition location
    addPartition(client, testDB, testTable, partVals1, tbl1);
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    AddPartitionMessage addPartitionMessage = deserializer.getAddPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ADD_PARTITION, addPartitionMessage.getEventType());
    assertThat(addPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));// dbName (returns lowered version)
    assertThat(addPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));// tableName (returns lowered version)
    //Location information is not available

    //Drop partition
    // We need:
    // - dbName
    // - tableName
    // - partition location
    dropPartition(client, testDB, testTable, partVals1);
    previousID = latestID;
    latestID = client.getCurrentNotificationEventId();
    assertEquals(previousID.getEventId() + 1, latestID.getEventId());
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    DropPartitionMessage dropPartitionMessage = deserializer.getDropPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_PARTITION, dropPartitionMessage.getEventType());
    assertThat(dropPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB)); //dbName
    assertThat(dropPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable)); //tableName
    //Location information is not available

  }

  @Ignore("Needs Hive >= 1.1.2")
  @Test
  public void testAlterTableWithPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);

    NotificationEventResponse response;
    CurrentNotificationEventId latestID, previousID;
    // Create database
    createMetastoreDB(client, testDB);

    // Create table with partition
    Table tbl1 = createMetastoreTableWithPartition(client, testDB,
        testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));

    //Alter table location
    // We need:
    // - dbName
    // - tableName
    // - old location
    // - new location
    String tabDir1 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
        + File.separator + random.nextInt(Integer.SIZE - 1);
    alterTableWithLocation(client, tbl1, tabDir1);
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId()-1, 1, null);
    AlterTableMessage alterTableMessage = deserializer.getAlterTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ALTER_TABLE, alterTableMessage.getEventType());
    assertThat(alterTableMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));//dbName
    assertThat(alterTableMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));//tableName
    //Old location is not available: This information is lost if not captured at the time of event.
    //New location is not available

    //Alter table rename managed table - location also changes
    // We need:
    // - oldDbName
    // - newDbName
    // - oldTableName
    // - newTableName
    // - old location
    // - new location
    String newDBName = testDB + random.nextInt(Integer.SIZE - 1);
    String newTableName = testTable + random.nextInt(Integer.SIZE - 1);
    String newLocation = tabDir1 + random.nextInt(Integer.SIZE - 1);
    createMetastoreDB(client, newDBName);
    alterTableRename(client, tbl1, newDBName, newTableName, newLocation);
    previousID = latestID;
    latestID = client.getCurrentNotificationEventId();
    assertEquals(previousID.getEventId() + 1, latestID.getEventId());
    response = client.getNextNotification(latestID.getEventId()-1, 1, null);
    alterTableMessage = deserializer.getAlterTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ALTER_TABLE, alterTableMessage.getEventType());
    assertThat(alterTableMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));//oldDbName
    assertThat(alterTableMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));//oldTableName
    assertThat(response.getEvents().get(0).getDbName(), IsEqualIgnoringCase.equalToIgnoringCase(newDBName));//newDbName
    assertThat(response.getEvents().get(0).getTableName(), IsEqualIgnoringCase.equalToIgnoringCase(newTableName));//newTableName
    //Old location: This information is lost if not captured at the time of event.
    //New location: Not sure how can we get this? Refresh all paths for every alter table add partition?
  }

  @Ignore("Needs Hive >= 1.1.2")
  @Test
  public void testAlterPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);

    NotificationEventResponse response;
    CurrentNotificationEventId latestID;
    // Create database
    createMetastoreDB(client, testDB);

    // Create table with partition
    Table tbl1 = createMetastoreTableWithPartition(client, testDB,
        testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    ArrayList<String> partVals1 = Lists.newArrayList("part1");
    Partition partition = addPartition(client, testDB, testTable, partVals1, tbl1);


    String warehouseDir = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR);
    //Alter partition with location
    // We need:
    // - dbName
    // - tableName
    // - partition location
    alterPartitionWithLocation(client, partition, warehouseDir + File.separator + "newpart");
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId()-1, 1, null);
    AlterPartitionMessage alterPartitionMessage = deserializer.getAlterPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ALTER_PARTITION, alterPartitionMessage.getEventType());
    assertThat(alterPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));// dbName
    assertThat(alterPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));// tableName
    assertEquals(partVals1, alterPartitionMessage.getKeyValues());
    if (alterPartitionMessage instanceof SentryJSONAlterPartitionMessage) {
      SentryJSONAlterPartitionMessage sjAlterPartitionMessage = (SentryJSONAlterPartitionMessage) alterPartitionMessage;
      assertEquals(partVals1, sjAlterPartitionMessage.getNewKeyValues());
    }

    Partition newPartition = partition.deepCopy();
    ArrayList<String> partVals2 = Lists.newArrayList("part2");
    newPartition.setValues(partVals2);
    renamePartition(client, partition, newPartition);
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId()-1, 1, null);
    alterPartitionMessage = deserializer.getAlterPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ALTER_PARTITION, alterPartitionMessage.getEventType());
    assertThat(alterPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));// dbName
    assertThat(alterPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));// tableName
    assertEquals(partVals1, alterPartitionMessage.getKeyValues());
    if (alterPartitionMessage instanceof SentryJSONAlterPartitionMessage) {
      SentryJSONAlterPartitionMessage sjAlterPartitionMessage = (SentryJSONAlterPartitionMessage) alterPartitionMessage;
      assertEquals(partVals2, sjAlterPartitionMessage.getNewKeyValues());
    }
  }
}

