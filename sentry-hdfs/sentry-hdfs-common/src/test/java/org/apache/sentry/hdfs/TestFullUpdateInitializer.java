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
package org.apache.sentry.hdfs;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestFullUpdateInitializer {

  private static Configuration conf = new Configuration();

  static {
    conf.setInt(ServiceConstants.ServerConfig
            .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC, 1);
    conf.setInt(ServiceConstants.ServerConfig
            .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC, 1);
    conf.setInt(ServiceConstants.ServerConfig
            .SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS, 8);
  }

  /**
   * Representation of a Hive table. A table has a name and a list of partitions.
   */
  private static class HiveTable {
    String name;
    List<String> partitions;

    HiveTable(String name) {
      this.name = name;
      this.partitions = new ArrayList<>();
    }

    HiveTable(String name, List<String> partitions) {
      this.name = name;
      this.partitions = partitions;
      if (this.partitions == null) {
        this.partitions = new ArrayList<>();
      }
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
    String name;
    Collection<HiveTable> tables;

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
    List<HiveDb> databases = new ArrayList<>();

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
   * Convert Hive snapshot to mock client that will return proper values
   * for the snapshot.
   */
  private static class MockClient {
    HiveMetaStoreClient client;

    MockClient(HiveSnapshot snapshot) throws TException {
      client = Mockito.mock(HiveMetaStoreClient.class);
      List<String> dbNames = new ArrayList<>(snapshot.databases.size());
      // Walk over all databases and mock appropriate objects
      for (HiveDb mdb: snapshot.databases) {
        String dbName = mdb.name;
        dbNames.add(dbName);
        Database db = makeDb(dbName);
        Mockito.when(client.getDatabase(dbName)).thenReturn(db);
        List<String> tableNames = new ArrayList<>(mdb.tables.size());
        // Walk over all tables for the database and mock appropriate objects
        for (HiveTable table: mdb.tables) {
          String tableName = table.name;
          tableNames.add(tableName);
          Table mockTable = makeTable(dbName, tableName);
          Mockito.when(client.getTableObjectsByName(dbName,
                  Lists.newArrayList(tableName)))
                  .thenReturn(Lists.newArrayList(mockTable));
          Mockito.when(client.listPartitionNames(dbName, tableName, (short) -1))
                  .thenReturn(table.partitions);
          // Walk across all partitions and mock appropriate objects
          for (String partName: table.partitions) {
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
    }
  }

  /**
   * Create mock database with the given name
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

  @Test
  // Test basic operation with small database
  public void testSimple() throws Exception {
    HiveTable tab21 = new HiveTable("tab21");
    HiveTable tab31 = new HiveTable("tab31").add("part311").add("part312");
    HiveDb db3 = new HiveDb("db3", Lists.newArrayList(tab31));
    HiveDb db2 = new HiveDb("db2", Lists.newArrayList(tab21));
    HiveDb db1 = new HiveDb("db1");
    HiveSnapshot snap = new HiveSnapshot().add(db1).add(db2).add(db3);
    MockClient c = new MockClient(snap);

    Map<String, Set<String>> update;
    try(FullUpdateInitializer cacheInitializer = new FullUpdateInitializer(c.client, conf)) {
      update = cacheInitializer.getFullHMSSnapshot();
    }
    Assert.assertEquals(5, update.size());
    Assert.assertEquals(Sets.newHashSet("db1"), update.get("db1"));
    Assert.assertEquals(Sets.newHashSet("db2"), update.get("db2"));
    Assert.assertEquals(Sets.newHashSet("db3"), update.get("db3"));
    Assert.assertEquals(Sets.newHashSet("db2/tab21"), update.get("db2.tab21"));
    Assert.assertEquals(Sets.newHashSet("db3/tab31",
            "db3/tab31/part311", "db3/tab31/part312"), update.get("db3.tab31"));
  }

  @Test
  // Test that invalid paths are handled correctly
  public void testInvalidPaths() throws Exception {
    //Set up mocks: db1.tb1, with tb1 returning a wrong dbname (db2)
    Database db1 = makeDb("db1");

    Table tab1 = Mockito.mock(Table.class);
    //Return a wrong db name, so that this triggers an exception
    Mockito.when(tab1.getDbName()).thenReturn("db2");
    Mockito.when(tab1.getTableName()).thenReturn("tab1");

    HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.when(client.getAllDatabases()).thenReturn(Lists.newArrayList("db1"));
    Mockito.when(client.getDatabase("db1")).thenReturn(db1);

    Table tab12 = Mockito.mock(Table.class);
    Mockito.when(tab12.getDbName()).thenReturn("db1");
    Mockito.when(tab12.getTableName()).thenReturn("tab21");
    StorageDescriptor sd21 = Mockito.mock(StorageDescriptor.class);
    Mockito.when(sd21.getLocation()).thenReturn("hdfs:///db1/tab21");
    Mockito.when(tab12.getSd()).thenReturn(sd21);

    Mockito.when(client.getTableObjectsByName("db1",
            Lists.newArrayList("tab1"))).thenReturn(Lists.newArrayList(tab1));
    Mockito.when(client.getTableObjectsByName("db1",
            Lists.newArrayList("tab12"))).thenReturn(Lists.newArrayList(tab12));
    Mockito.when(client.getAllTables("db1")).
            thenReturn(Lists.newArrayList("tab1", "tab12"));


    Map<String, Set<String>> update;
    try(FullUpdateInitializer cacheInitializer = new FullUpdateInitializer(client, conf)) {
      update = cacheInitializer.getFullHMSSnapshot();
    }
    Assert.assertEquals(2, update.size());
    Assert.assertEquals(Sets.newHashSet("db1"), update.get("db1"));
    Assert.assertEquals(Sets.newHashSet("db1/tab21"), update.get("db1.tab21"));
  }

  @Test
  // Test handling of a big tables and partitions
  public void testBig() throws Exception {
    int ndbs = 3;
    int ntables = 51;
    int nparts = 131;

    HiveSnapshot snap = new HiveSnapshot();

    for (int i = 0; i < ndbs; i++) {
      HiveDb db = new HiveDb("db" + i);
      for (int j = 0; j < ntables; j++) {
        HiveTable table = new HiveTable("table" + i + j);
        for (int k = 0; k < nparts; k++) {
          table.add("part" + i + j + k);
        }
        db.add(table);
      }
      snap.add(db);
    }
    MockClient c = new MockClient(snap);
    Map<String, Set<String>> update;
    try(FullUpdateInitializer cacheInitializer = new FullUpdateInitializer(c.client, conf)) {
      update = cacheInitializer.getFullHMSSnapshot();
    }
    Assert.assertEquals((ntables * ndbs) + ndbs, update.size());
    for (int i = 0; i < ndbs; i++) {
      String dbName = "db" + i;
      Assert.assertEquals(Sets.newHashSet(dbName), update.get(dbName));

      for (int j = 0; j < ntables; j++) {
        String tableName = "table" + i + j;
        Set<String> values = new HashSet<>();
        values.add(String.format("%s/%s", dbName, tableName));
        for (int k = 0; k < nparts; k++) {
          String partName = "part" + i + j + k;
          values.add(String.format("%s/%s/%s", dbName, tableName, partName));
        }
        String authz = dbName + "." + tableName;
        Assert.assertEquals(values, update.get(authz));
      }
    }
  }

}
