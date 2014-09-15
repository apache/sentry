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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.MetastoreClient;

public class DummyHMSClient implements MetastoreClient {

  private HashMap<Database, HashMap<Table, HashSet<Partition>>> hmsData =
      new HashMap<Database, HashMap<Table, HashSet<Partition>>>();

  @Override
  public List<Database> getAllDatabases() {
    return new ArrayList<Database>(hmsData.keySet());
  }

  @Override
  public List<Table> getAllTablesOfDatabase(Database db) {
    if (hmsData.containsKey(db)) {
      return new ArrayList<Table>(hmsData.get(db).keySet());
    }
    return new ArrayList<Table>();
  }

  @Override
  public List<Partition> listAllPartitions(Database db, Table tbl) {
    if (hmsData.containsKey(db)) {
      if (hmsData.get(db).containsKey(tbl)) {
        return new ArrayList<Partition>(hmsData.get(db).get(tbl));
      }
    }
    return new ArrayList<Partition>();
  }

  public Database addDb(String dbName, String location) {
    Database db = new Database(dbName, null, location, null);
    hmsData.put(db, new HashMap<Table, HashSet<Partition>>());
    return db;
  }

  public Table addTable(Database db, String tblName, String location) {
    Table tbl = 
        new Table(tblName, db.getName(), null, 0, 0, 0, 
            new StorageDescriptor(null, location, null, null, false, 0, null, null, null, null),
            null, null, null, null, null);
    hmsData.get(db).put(tbl, new HashSet<Partition>());
    return tbl;
  }
  
  public void addPartition(Database db, Table tbl, String partitionPath) {
    Partition part = new Partition(null, db.getName(), tbl.getTableName(), 0, 0,
        new StorageDescriptor(null, partitionPath, null, null, false, 0, null, null, null, null), null);
    hmsData.get(db).get(tbl).add(part);
  }
}
