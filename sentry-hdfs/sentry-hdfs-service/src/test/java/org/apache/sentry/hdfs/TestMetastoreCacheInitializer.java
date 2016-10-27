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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class TestMetastoreCacheInitializer {

  private Configuration setConf() {
    Configuration conf = new Configuration();
    conf.setInt(ServiceConstants.ServerConfig
            .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC, 1);
    conf.setInt(ServiceConstants.ServerConfig
            .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC, 1);
    conf.setInt(ServiceConstants.ServerConfig
            .SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS, 1);
    return conf;

  }

  @Test
  public void testInitializer() throws Exception {

    Database db1 = Mockito.mock(Database.class);
    Mockito.when(db1.getName()).thenReturn("db1");
    Mockito.when(db1.getLocationUri()).thenReturn("hdfs:///db1");
    Database db2 = Mockito.mock(Database.class);
    Mockito.when(db2.getName()).thenReturn("db2");
    Mockito.when(db2.getLocationUri()).thenReturn("hdfs:///db2");
    Database db3 = Mockito.mock(Database.class);
    Mockito.when(db3.getName()).thenReturn("db3");
    Mockito.when(db3.getLocationUri()).thenReturn("hdfs:///db3");

    Table tab21 = Mockito.mock(Table.class);
    Mockito.when(tab21.getDbName()).thenReturn("db2");
    Mockito.when(tab21.getTableName()).thenReturn("tab21");
    StorageDescriptor sd21 = Mockito.mock(StorageDescriptor.class);
    Mockito.when(sd21.getLocation()).thenReturn("hdfs:///db2/tab21");
    Mockito.when(tab21.getSd()).thenReturn(sd21);

    Table tab31 = Mockito.mock(Table.class);
    Mockito.when(tab31.getDbName()).thenReturn("db3");
    Mockito.when(tab31.getTableName()).thenReturn("tab31");
    StorageDescriptor sd31 = Mockito.mock(StorageDescriptor.class);
    Mockito.when(sd31.getLocation()).thenReturn("hdfs:///db3/tab31");
    Mockito.when(tab31.getSd()).thenReturn(sd31);

    Partition part311 = Mockito.mock(Partition.class);
    StorageDescriptor sd311 = Mockito.mock(StorageDescriptor.class);
    Mockito.when(sd311.getLocation()).thenReturn("hdfs:///db3/tab31/part311");
    Mockito.when(part311.getSd()).thenReturn(sd311);

    Partition part312 = Mockito.mock(Partition.class);
    StorageDescriptor sd312 = Mockito.mock(StorageDescriptor.class);
    Mockito.when(sd312.getLocation()).thenReturn("hdfs:///db3/tab31/part312");
    Mockito.when(part312.getSd()).thenReturn(sd312);

    IHMSHandler hmsHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(hmsHandler.get_all_databases()).thenReturn(Lists
            .newArrayList("db1", "db2", "db3"));
    Mockito.when(hmsHandler.get_database("db1")).thenReturn(db1);
    Mockito.when(hmsHandler.get_all_tables("db1")).thenReturn(new
            ArrayList<String>());

    Mockito.when(hmsHandler.get_database("db2")).thenReturn(db2);
    Mockito.when(hmsHandler.get_all_tables("db2")).thenReturn(Lists
            .newArrayList("tab21"));
    Mockito.when(hmsHandler.get_table_objects_by_name("db2",
            Lists.newArrayList("tab21")))
            .thenReturn(Lists.newArrayList(tab21));
    Mockito.when(hmsHandler.get_partition_names("db2", "tab21", (short) -1))
            .thenReturn(new ArrayList<String>());

    Mockito.when(hmsHandler.get_database("db3")).thenReturn(db3);
    Mockito.when(hmsHandler.get_all_tables("db3")).thenReturn(Lists
            .newArrayList("tab31"));
    Mockito.when(hmsHandler.get_table_objects_by_name("db3",
            Lists.newArrayList("tab31")))
            .thenReturn(Lists.newArrayList(tab31));
    Mockito.when(hmsHandler.get_partition_names("db3", "tab31", (short) -1))
            .thenReturn(Lists.newArrayList("part311", "part312"));

    Mockito.when(hmsHandler.get_partitions_by_names("db3", "tab31",
            Lists.newArrayList("part311")))
            .thenReturn(Lists.newArrayList(part311));
    Mockito.when(hmsHandler.get_partitions_by_names("db3", "tab31",
            Lists.newArrayList("part312")))
            .thenReturn(Lists.newArrayList(part312));

    MetastoreCacheInitializer cacheInitializer = new
            MetastoreCacheInitializer(hmsHandler, setConf());
    UpdateableAuthzPaths update = cacheInitializer.createInitialUpdate();

    Assert.assertEquals(new HashSet<String>(Arrays.asList("db1")), update.findAuthzObjectExactMatches(new
            String[]{"db1"}));
    Assert.assertEquals(new HashSet<String>(Arrays.asList("db2")), update.findAuthzObjectExactMatches(new
            String[]{"db2"}));
    Assert.assertEquals(new HashSet<String>(Arrays.asList("db2.tab21")), update.findAuthzObjectExactMatches(new
            String[]{"db2", "tab21"}));
    Assert.assertEquals(new HashSet<String>(Arrays.asList("db3")), update.findAuthzObjectExactMatches(new
            String[]{"db3"}));
    Assert.assertEquals(new HashSet<String>(Arrays.asList("db3.tab31")), update.findAuthzObjectExactMatches(new
            String[]{"db3", "tab31"}));
    Assert.assertEquals(new HashSet<String>(Arrays.asList("db3.tab31")), update.findAuthzObjectExactMatches(new
            String[]{"db3", "tab31", "part311"}));
    Assert.assertEquals(new HashSet<String>(Arrays.asList("db3.tab31")), update.findAuthzObjectExactMatches(new
            String[]{"db3", "tab31", "part312"}));
    cacheInitializer.close();

  }

  // Make sure exceptions in initializer parallel tasks are propagated well
  @Test
  public void testExceptionInTask() throws Exception {
    //Set up mocks: db1.tb1, with tb1 returning a wrong dbname (db2)
    Database db1 = Mockito.mock(Database.class);
    Mockito.when(db1.getName()).thenReturn("db1");
    Mockito.when(db1.getLocationUri()).thenReturn("hdfs:///db1");

    Table tab1 = Mockito.mock(Table.class);
    //Return a wrong db name, so that this triggers an exception
    Mockito.when(tab1.getDbName()).thenReturn("db2");
    Mockito.when(tab1.getTableName()).thenReturn("tab1");

    IHMSHandler hmsHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(hmsHandler.get_all_databases()).thenReturn(Lists
        .newArrayList("db1"));
    Mockito.when(hmsHandler.get_database("db1")).thenReturn(db1);
    Mockito.when(hmsHandler.get_table_objects_by_name("db1",
        Lists.newArrayList("tab1")))
        .thenReturn(Lists.newArrayList(tab1));
    Mockito.when(hmsHandler.get_all_tables("db1")).thenReturn(Lists
        .newArrayList("tab1"));

    try {
      MetastoreCacheInitializer cacheInitializer = new
          MetastoreCacheInitializer(hmsHandler, setConf());
      cacheInitializer.createInitialUpdate();
      Assert.fail("Expected cacheInitializer to fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }

  }

  @Test(expected = SentryMalformedPathException.class)
  public void testSentryMalFormedExceptionInDbTask() throws Exception {
    //Set up mocks: db1 with malformed paths
    Database db1 = Mockito.mock(Database.class);
    Mockito.when(db1.getName()).thenReturn("db1");
    Mockito.when(db1.getLocationUri()).thenReturn("hdfs://db1");

    IHMSHandler hmsHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(hmsHandler.get_all_databases()).thenReturn(Lists
            .newArrayList("db1"));
    Mockito.when(hmsHandler.get_database("db1")).thenReturn(db1);


    MetastoreCacheInitializer cacheInitializer = new MetastoreCacheInitializer(hmsHandler, setConf());
    cacheInitializer.createInitialUpdate();
    Assert.fail("Expected cacheInitializer to fail");
  }

  @Test(expected = SentryMalformedPathException.class)
  public void testSentryMalFormedExceptionInTableTask() throws Exception {
    //Set up mocks: db1 and tb1 with wrong location
    Database db1 = Mockito.mock(Database.class);
    Mockito.when(db1.getName()).thenReturn("db1");
    IHMSHandler hmsHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(hmsHandler.get_all_databases()).thenReturn(Lists
            .newArrayList("db1"));
    Mockito.when(hmsHandler.get_database("db1")).thenReturn(db1);

    Table tab1 = Mockito.mock(Table.class);
    Mockito.when(tab1.getDbName()).thenReturn("db1");
    Mockito.when(tab1.getTableName()).thenReturn("tab1");
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Mockito.when(tab1.getSd()).thenReturn(sd);
    Mockito.when(tab1.getSd().getLocation()).thenReturn("hdfs://db1");

    Mockito.when(hmsHandler.get_table_objects_by_name("db1",
            Lists.newArrayList("tab1")))
            .thenReturn(Lists.newArrayList(tab1));
    Mockito.when(hmsHandler.get_all_tables("db1")).thenReturn(Lists
            .newArrayList("tab1"));

    MetastoreCacheInitializer cacheInitializer = new MetastoreCacheInitializer(hmsHandler, setConf());
    cacheInitializer.createInitialUpdate();
    Assert.fail("Expected cacheInitializer to fail");

  }

  @Test(expected = SentryMalformedPathException.class)
  public void testSentryMalFormedExceptionInPartitionTask() throws Exception {
    //Set up mocks: db1,tb1 and partition with wrong location
    Database db1 = Mockito.mock(Database.class);
    Mockito.when(db1.getName()).thenReturn("db1");
    IHMSHandler hmsHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(hmsHandler.get_all_databases()).thenReturn(Lists
            .newArrayList("db1"));
    Mockito.when(hmsHandler.get_database("db1")).thenReturn(db1);

    Table tab1 = Mockito.mock(Table.class);
    StorageDescriptor tableSd = Mockito.mock(StorageDescriptor.class);
    Mockito.when(tab1.getDbName()).thenReturn("db1");
    Mockito.when(tab1.getTableName()).thenReturn("tab1");
    Mockito.when(tab1.getSd()).thenReturn(tableSd);
    Mockito.when(tab1.getSd().getLocation()).thenReturn("hdfs://hostname/db1/tab1");

    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Partition partition = Mockito.mock(Partition.class);
    Mockito.when(partition.getSd()).thenReturn(sd);
    Mockito.when(partition.getSd().getLocation()).thenReturn("hdfs://db1");

    Mockito.when(hmsHandler.get_table_objects_by_name("db1",
            Lists.newArrayList("tab1")))
            .thenReturn(Lists.newArrayList(tab1));
    Mockito.when(hmsHandler.get_all_tables("db1")).thenReturn(Lists
            .newArrayList("tab1"));
    List<String> partnames = new ArrayList<>();
    partnames.add("part1");
    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition);
    Mockito.when(hmsHandler.get_partition_names("db1", "tab1", (short) -1)).thenReturn(partnames);
    Mockito.when(hmsHandler.get_partitions_by_names("db1", "tab1", partnames)).thenReturn(partitions);

    MetastoreCacheInitializer cacheInitializer = new MetastoreCacheInitializer(hmsHandler, setConf());
    cacheInitializer.createInitialUpdate();
    Assert.fail("Expected cacheInitializer to fail");

  }
}
