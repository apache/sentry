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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.sentry.binding.metastore.messaging.json.*;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.hamcrest.text.IsEqualIgnoringCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * Make sure we are able to capture all HMS object and path changes using Sentry's SentryJSONMessageFactory
 * and Sentry Notification log deserializer.
 */
public class TestSentryMessageFactorySentryDeserializer extends AbstractMetastoreTestWithStaticConfiguration {

  protected static HiveMetaStoreClient client;
  protected static SentryJSONMessageDeserializer deserializer;
  protected static Random random = new Random();
  private static String warehouseDir;
  private static String testDB;


  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    setMetastoreListener = true;
    enableNotificationLog = true;
    AbstractMetastoreTestWithStaticConfiguration.setupTestStaticConfiguration();
    setupClass();
  }

  protected static void setupClass() throws Exception{
    client = context.getMetaStoreClient(ADMIN1);
    deserializer = new SentryJSONMessageDeserializer();
    warehouseDir = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR);
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
    SentryJSONCreateDatabaseMessage createDatabaseMessage = deserializer.getCreateDatabaseMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.CREATE_DATABASE, createDatabaseMessage.getEventType()); //Validate EventType
    assertEquals(testDB, createDatabaseMessage.getDB()); //dbName
    String expectedLocation = warehouseDir + "/" + testDB + ".db";
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(expectedLocation.toLowerCase(), createDatabaseMessage.getLocation());
    }

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
    SentryJSONDropDatabaseMessage dropDatabaseMessage = deserializer.getDropDatabaseMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_DATABASE, dropDatabaseMessage.getEventType()); //Event type
    assertThat(dropDatabaseMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB)); // dbName
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(expectedLocation.toLowerCase(), dropDatabaseMessage.getLocation()); //location
    }
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
    SentryJSONCreateTableMessage createTableMessage = deserializer.getCreateTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.CREATE_TABLE, createTableMessage.getEventType());
    assertEquals(testDB, createTableMessage.getDB()); //dbName
    assertEquals(testTable, createTableMessage.getTable()); //tableName
    String expectedLocation = warehouseDir + "/" + testDB + ".db/" + testTable;
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(expectedLocation.toLowerCase(), createTableMessage.getLocation());
    }


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
    SentryJSONDropTableMessage dropTableMessage = deserializer.getDropTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_TABLE, dropTableMessage.getEventType());
    assertThat(dropTableMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));//dbName
    assertThat(dropTableMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));//tableName
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(expectedLocation.toLowerCase(), dropTableMessage.getLocation()); //location
    }
  }

  @Test
  public void testCreateDropTableWithoutPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);

    NotificationEventResponse response;
    CurrentNotificationEventId latestID, previousID;
    // Create database
    createMetastoreDB(client, testDB);

    // Create table without partition
    // We need:
    // - dbname
    // - tablename
    // - location
    createMetastoreTable(client, testDB, testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")));
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    SentryJSONCreateTableMessage createTableMessage = deserializer.getCreateTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.CREATE_TABLE, createTableMessage.getEventType());
    assertEquals(testDB, createTableMessage.getDB()); //dbName
    assertEquals(testTable, createTableMessage.getTable()); //tableName
    String expectedLocation = warehouseDir + "/" + testDB + ".db/" + testTable;
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(expectedLocation.toLowerCase(), createTableMessage.getLocation());
    }

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
    SentryJSONDropTableMessage dropTableMessage = deserializer.getDropTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_TABLE, dropTableMessage.getEventType());
    assertThat(dropTableMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));//dbName
    assertThat(dropTableMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));//tableName
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(expectedLocation.toLowerCase(), dropTableMessage.getLocation()); //location
    }
  }

  @Test
  public void testAddDropPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);
    String partColName = "part_col1";
    String partColValue = "part1";

    NotificationEventResponse response;
    CurrentNotificationEventId latestID, previousID;
    // Create database and table
    createMetastoreDB(client, testDB);
    Table tbl1 = createMetastoreTableWithPartition(client, testDB, testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema(partColName, "string", "")));

    ArrayList<String> partVals1 = Lists.newArrayList(partColValue);

    //Add partition
    // We need:
    // - dbName
    // - tableName
    // - partition location
    addPartition(client, testDB, testTable, partVals1, tbl1);
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    SentryJSONAddPartitionMessage addPartitionMessage = deserializer.getAddPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ADD_PARTITION, addPartitionMessage.getEventType());
    assertThat(addPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));// dbName (returns lowered version)
    assertThat(addPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));// tableName (returns lowered version)
    String expectedLocation = warehouseDir + "/" + testDB + ".db/" + testTable + "/" + partColName + "=" + partColValue;
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(expectedLocation.toLowerCase(), addPartitionMessage.getLocations().get(0));
    }

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
    SentryJSONDropPartitionMessage dropPartitionMessage = deserializer.getDropPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.DROP_PARTITION, dropPartitionMessage.getEventType());
    assertThat(dropPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB)); //dbName
    assertThat(dropPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable)); //tableName
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(Arrays.asList(expectedLocation.toLowerCase()), dropPartitionMessage.getLocations());
    }
  }

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
    String oldLocation = tbl1.getSd().getLocation();

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
    SentryJSONAlterTableMessage alterTableMessage = deserializer.getAlterTableMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ALTER_TABLE, alterTableMessage.getEventType());
    assertThat(alterTableMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));//dbName
    assertThat(alterTableMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));//tableName
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(oldLocation, alterTableMessage.getOldLocation()); //oldLocation
      Assert.assertEquals(tbl1.getSd().getLocation(), alterTableMessage.getNewLocation()); //newLocation
    }

    //Alter table rename managed table - location also changes
    // We need:
    // - oldDbName
    // - newDbName
    // - oldTableName
    // - newTableName
    // - old location
    // - new location
    oldLocation = tbl1.getSd().getLocation();
    String newDBName = testDB + random.nextInt(Integer.SIZE - 1);
    String newTableName = testTable + random.nextInt(Integer.SIZE - 1);
    String newLocation = tabDir1 + random.nextInt(Integer.SIZE - 1);
    createMetastoreDB(client, newDBName);
    previousID = latestID;
    latestID = client.getCurrentNotificationEventId();
    assertEquals(previousID.getEventId() + 1, latestID.getEventId());
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
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(oldLocation, alterTableMessage.getOldLocation()); //oldLocation
      Assert.assertEquals(tbl1.getSd().getLocation(), alterTableMessage.getNewLocation()); //newLocation
    }
  }

  @Test
  public void testAlterPartition() throws Exception {
    testDB = "N_db" + random.nextInt(Integer.SIZE - 1);
    String testTable = "N_table" + random.nextInt(Integer.SIZE - 1);
    String partColName = "part_col1";
    String partColValue = "part1";

    NotificationEventResponse response;
    CurrentNotificationEventId latestID;
    // Create database
    createMetastoreDB(client, testDB);

    // Create table with partition
    Table tbl1 = createMetastoreTableWithPartition(client, testDB,
        testTable, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema(partColName, "string", "")));
    Map<String, String> partKeyVals1 = ImmutableMap.of(partColName, partColValue);
    ArrayList<String> partVals1 = Lists.newArrayList(partColValue);

    Partition partition = addPartition(client, testDB, testTable, partVals1, tbl1);

    //Alter partition with location
    // We need:
    // - dbName
    // - tableName
    // - partition location
    String oldLocation = tbl1.getSd().getLocation()  + "/" + partColName + "=" + partColValue;
    String newLocation = warehouseDir + File.separator + "newpart";
    alterPartitionWithLocation(client, partition, newLocation);
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId()-1, 1, null);
    SentryJSONAlterPartitionMessage alterPartitionMessage = deserializer.getAlterPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ALTER_PARTITION, alterPartitionMessage.getEventType());
    assertThat(alterPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));// dbName
    assertThat(alterPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));// tableName
    if(!useDefaultMessageFactory) {
      Assert.assertEquals(oldLocation.toLowerCase(), alterPartitionMessage.getOldLocation());
      Assert.assertEquals(newLocation.toLowerCase(), alterPartitionMessage.getNewLocation());
      assertEquals(partKeyVals1, alterPartitionMessage.getKeyValues());
      assertEquals(partKeyVals1, alterPartitionMessage.getNewKeyValues());
    }

    Partition newPartition = partition.deepCopy();
    Map<String, String> partKeyVals2 = ImmutableMap.of(partColName, "part2");
    ArrayList<String> partVals2 = Lists.newArrayList("part2");
    newPartition.setValues(partVals2);
    renamePartition(client, partition, newPartition);
    latestID = client.getCurrentNotificationEventId();
    response = client.getNextNotification(latestID.getEventId() - 1, 1, null);
    alterPartitionMessage = deserializer.getAlterPartitionMessage(response.getEvents().get(0).getMessage());
    assertEquals(HCatEventMessage.EventType.ALTER_PARTITION, alterPartitionMessage.getEventType());
    assertThat(alterPartitionMessage.getDB(), IsEqualIgnoringCase.equalToIgnoringCase(testDB));// dbName
    assertThat(alterPartitionMessage.getTable(), IsEqualIgnoringCase.equalToIgnoringCase(testTable));// tableName
    if(!useDbNotificationListener) {
      assertEquals(partKeyVals1, alterPartitionMessage.getKeyValues());
      assertEquals(partKeyVals2, alterPartitionMessage.getNewKeyValues());
    }
  }
}

