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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.security.auth.login.LoginException;

/**
 * Test mocks HiveMetaStoreClient class and tests SentryHMSClient.
 */
public class TestSentryHMSClient {

  private static final Configuration conf = new Configuration();
  private static SentryHMSClient client;
  private static MockHMSClientFactory hiveConnectionFactory;

  /**
   * Create mock database with the given name
   *
   * @param name Database name
   * @return Mock database object
   */
  private static Database makeDb(String name) {
    Database db = Mockito.mock(Database.class);
    Mockito.when(db.getName()).thenReturn(name);
    Mockito.when(db.getLocationUri()).thenReturn("hdfs:///" + name);
    return db;
  }

  /**
   * Create mock table
   *
   * @param dbName db for this table
   * @param tableName name of the table
   * @return mock table object
   */
  private static Table makeTable(String dbName, String tableName) {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.getDbName()).thenReturn(dbName);
    Mockito.when(table.getTableName()).thenReturn(tableName);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Mockito.when(sd.getLocation()).thenReturn(
        String.format("hdfs:///%s/%s", dbName, tableName));
    Mockito.when(table.getSd()).thenReturn(sd);
    return table;
  }

  /**
   * Create mock partition
   *
   * @param dbName database for this partition
   * @param tableName table for this partition
   * @param partName partition name
   * @return mock partition object
   */
  private static Partition makePartition(String dbName, String tableName, String partName) {
    Partition partition = Mockito.mock(Partition.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Mockito.when(sd.getLocation()).thenReturn(
        String.format("hdfs:///%s/%s/%s", dbName, tableName, partName));
    Mockito.when(partition.getSd()).thenReturn(sd);
    return partition;
  }

  @BeforeClass
  static public void initialize() throws IOException, LoginException {
    hiveConnectionFactory = new MockHMSClientFactory();
    client = new SentryHMSClient(conf, (HiveConnectionFactory)hiveConnectionFactory);
  }

  /**
   * Creating snapshot when SentryHMSClient is not connected to HMS
   */
  @Test
  public void testSnapshotCreationWithOutClientConnected() throws Exception {
    // Make sure that client is not connected
    Assert.assertFalse(client.isConnected());
    PathsImage snapshotInfo = client.getFullSnapshot();
    Assert.assertTrue(snapshotInfo.getPathImage().isEmpty());
  }

  /**
   * Creating snapshot when HMS doesn't have any data
   */
  @Test
  public void testSnapshotCreationWithNoHmsData() throws Exception {
    MockClient mockClient = new MockClient(new HiveSnapshot(), 1);
    client.setClient(mockClient.client);
    // Make sure that client is connected
    Assert.assertTrue(client.isConnected());
    PathsImage snapshotInfo = client.getFullSnapshot();
    Assert.assertTrue(snapshotInfo.getPathImage().isEmpty());
  }

  /**
   * Creating a snapshot when there is data but there are updates to HMS data mean while
   */
  @Test
  public void testSnapshotCreationWhenDataIsActivelyUpdated() throws Exception {
    HiveTable tab21 = new HiveTable("tab21");
    HiveTable tab31 = new HiveTable("tab31").add("part311").add("part312");
    HiveDb db3 = new HiveDb("db3", Lists.newArrayList(tab31));
    HiveDb db2 = new HiveDb("db2", Lists.newArrayList(tab21));
    HiveDb db1 = new HiveDb("db1");
    HiveSnapshot snap = new HiveSnapshot().add(db1).add(db2).add(db3);
    final MockClient mockClient = new MockClient(snap, 1);

    client.setClient(mockClient.client);
    hiveConnectionFactory.setClient(mockClient);
    // Make sure that client is connected
    Assert.assertTrue(client.isConnected());
    PathsImage snapshotInfo = client.getFullSnapshot();
    // Make sure that snapshot is not empty
    Assert.assertTrue(!snapshotInfo.getPathImage().isEmpty());

    Mockito.when(mockClient.client.getCurrentNotificationEventId()).
        thenAnswer(new Answer<CurrentNotificationEventId>() {
          @Override
          public CurrentNotificationEventId answer(InvocationOnMock invocation)
              throws Throwable {
            return new CurrentNotificationEventId(mockClient.incrementNotificationEventId());
          }

        });

    snapshotInfo = client.getFullSnapshot();
    Assert.assertTrue(snapshotInfo.getPathImage().isEmpty());
  }

  /**
   * Creating a snapshot when there is data in HMS.
   */
  @Test
  public void testSnapshotCreationSuccess() throws Exception {
    HiveTable tab21 = new HiveTable("tab21");
    HiveTable tab31 = new HiveTable("tab31");
    HiveDb db3 = new HiveDb("db3", Lists.newArrayList(tab31));
    HiveDb db2 = new HiveDb("db2", Lists.newArrayList(tab21));
    HiveDb db1 = new HiveDb("db1");
    HiveSnapshot snap = new HiveSnapshot().add(db1).add(db2).add(db3);
    MockClient mockClient = new MockClient(snap, 1);
    Mockito.when(mockClient.client.getCurrentNotificationEventId()).
        thenReturn(new CurrentNotificationEventId(mockClient.eventId));
    client.setClient(mockClient.client);
    hiveConnectionFactory.setClient(mockClient);
    // Make sure that client is connected
    Assert.assertTrue(client.isConnected());

    PathsImage snapshotInfo = client.getFullSnapshot();
    Assert.assertEquals(5, snapshotInfo.getPathImage().size());
    Assert.assertEquals(Sets.newHashSet("db1"), snapshotInfo.getPathImage().get("db1"));
    Assert.assertEquals(Sets.newHashSet("db2"), snapshotInfo.getPathImage().get("db2"));
    Assert.assertEquals(Sets.newHashSet("db3"), snapshotInfo.getPathImage().get("db3"));
    Assert.assertEquals(Sets.newHashSet("db2/tab21"),
        snapshotInfo.getPathImage().get("db2.tab21"));
    Assert.assertEquals(Sets.newHashSet("db3/tab31"), snapshotInfo.getPathImage().get("db3.tab31"));
    Assert.assertEquals(snapshotInfo.getId(), mockClient.eventId);

  }

  /**
   * Representation of a Hive table. A table has a name and a list of partitions.
   */
  private static class HiveTable {

    private final String name;
    private final List<String> partitions;

    HiveTable(String name) {
      this.name = name;
      this.partitions = new ArrayList<>();
    }

    HiveTable add(String partition) {
      partitions.add(partition);
      return this;
    }
  }

  /**
   * Representation of a Hive database. A database has a name and a list of tables
   */
  private static class HiveDb {

    final String name;
    Collection<HiveTable> tables;

    @SuppressWarnings("SameParameterValue")
    HiveDb(String name) {
      this.name = name;
      tables = new ArrayList<>();
    }

    HiveDb(String name, Collection<HiveTable> tables) {
      this.name = name;
      this.tables = tables;
      if (this.tables == null) {
        this.tables = new ArrayList<>();
      }
    }

    void add(HiveTable table) {
      this.tables.add(table);
    }
  }

  /**
   * Representation of a full Hive snapshot. A snapshot is collection of databases
   */
  private static class HiveSnapshot {

    final List<HiveDb> databases = new ArrayList<>();

    HiveSnapshot() {
    }

    HiveSnapshot(Collection<HiveDb> dblist) {
      if (dblist != null) {
        databases.addAll(dblist);
      }
    }

    HiveSnapshot add(HiveDb db) {
      this.databases.add(db);
      return this;
    }
  }

  /**
   * Mock for HMSClientFactory
   */
  private static class MockHMSClientFactory implements HiveConnectionFactory {

    private HiveMetaStoreClient mClient;

    public MockHMSClientFactory() {
      mClient  = null;
    }

    void setClient(MockClient mockClient) {
      this.mClient = mockClient.client;
    }
    @Override
    public HMSClient connect() throws IOException, InterruptedException, MetaException {
      return new HMSClient(mClient);
    }

    @Override
    public void close() throws Exception {
    }
  }

  /**
   * Convert Hive snapshot to mock client that will return proper values
   * for the snapshot.
   */
  private static class MockClient {

    public HiveMetaStoreClient client;
    public long eventId;

    MockClient(HiveSnapshot snapshot, long eventId) throws TException {
      this.eventId = eventId;
      client = Mockito.mock(HiveMetaStoreClient.class);
      List<String> dbNames = new ArrayList<>(snapshot.databases.size());
      // Walk over all databases and mock appropriate objects
      for (HiveDb mdb : snapshot.databases) {
        String dbName = mdb.name;
        dbNames.add(dbName);
        Database db = makeDb(dbName);
        Mockito.when(client.getDatabase(dbName)).thenReturn(db);
        List<String> tableNames = new ArrayList<>(mdb.tables.size());
        // Walk over all tables for the database and mock appropriate objects
        for (HiveTable table : mdb.tables) {
          String tableName = table.name;
          tableNames.add(tableName);
          Table mockTable = makeTable(dbName, tableName);
          Mockito.when(client.getTableObjectsByName(dbName,
              Lists.newArrayList(tableName)))
              .thenReturn(Lists.newArrayList(mockTable));
          Mockito.when(client.listPartitionNames(dbName, tableName, (short) -1))
              .thenReturn(table.partitions);
          // Walk across all partitions and mock appropriate objects
          for (String partName : table.partitions) {
            Partition p = makePartition(dbName, tableName, partName);
            Mockito.when(client.getPartitionsByNames(dbName, tableName,
                Lists.<String>newArrayList(partName)))
                .thenReturn(Lists.<Partition>newArrayList(p));
          }
        }
        Mockito.when(client.getAllTables(dbName)).thenReturn(tableNames);
      }
      // Return all database names
      Mockito.when(client.getAllDatabases()).thenReturn(dbNames);
      Mockito.when(client.getCurrentNotificationEventId()).
          thenReturn(new CurrentNotificationEventId(eventId));

    }

    public Long incrementNotificationEventId() {
      eventId = eventId + 1;
      return eventId;
    }
  }
}
