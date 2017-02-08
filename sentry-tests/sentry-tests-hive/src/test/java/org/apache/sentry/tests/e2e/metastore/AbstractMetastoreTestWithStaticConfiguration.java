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
package org.apache.sentry.tests.e2e.metastore;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.PigServer;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory.HiveServer2Type;
import org.junit.After;
import org.junit.BeforeClass;

import com.google.common.collect.Maps;

public abstract class AbstractMetastoreTestWithStaticConfiguration extends
    AbstractTestWithStaticConfiguration {

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    clearDbPerTest = false;
    testServerType = HiveServer2Type.InternalMetastore.name();
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  protected static void writePolicyFile(PolicyFile policyFile) throws Exception {
    policyFile.write(context.getPolicyFile());
  }

  public static PolicyFile setAdminOnServer1(String adminGroup)
      throws Exception {
    return SentryPolicyProviderForDb.setAdminOnServer1(adminGroup,
        getSentryClient());
  }
  /**
   * create a metastore table using the given attributes
   * @param client
   * @param dbName
   * @param tabName
   * @param cols
   * @return
   * @throws Exception
   */
  public Table createMetastoreTable(HiveMetaStoreClient client, String dbName,
      String tabName, List<FieldSchema> cols) throws Exception {

    Table tbl = makeMetastoreTableObject(client, dbName, tabName, cols);
    client.createTable(tbl);
    return tbl;
  }

  public Table createMetastoreTableWithLocation(HiveMetaStoreClient client,
      String dbName, String tabName, List<FieldSchema> cols, String location)
      throws Exception {
    Table tbl = makeMetastoreTableObject(client, dbName, tabName, cols);
    tbl.getSd().setLocation(location);
    client.createTable(tbl);
    return tbl;

  }

  public void alterTableWithLocation(HiveMetaStoreClient client,
                                                Table table, String location)
          throws Exception {
    table.getSd().setLocation(location);
    client.alter_table(table.getDbName(), table.getTableName(), table);
  }

  public void alterTableRename(HiveMetaStoreClient client,
                                     Table table, String newDBName, String newTableName, String newLocation)
          throws Exception {
    String dbName = table.getDbName();
    String tableName = table.getTableName();
    table.setDbName(newDBName);
    table.setTableName(newTableName);
    if( newLocation != null ) {
      table.getSd().setLocation(newLocation);
    }
    client.alter_table(dbName, tableName, table);
  }

  public Table createMetastoreTableWithPartition(HiveMetaStoreClient client,
      String dbName, String tabName, List<FieldSchema> cols,
      List<FieldSchema> partionVals) throws Exception {
    Table tbl = makeMetastoreTableObject(client, dbName, tabName, cols);
    tbl.setPartitionKeys(partionVals);
    client.createTable(tbl);
    return client.getTable(dbName, tabName);
  }

  public Partition addPartition(HiveMetaStoreClient client, String dbName,
      String tblName, List<String> ptnVals, Table tbl) throws Exception {
    Partition part = makeMetastorePartitionObject(dbName, tblName, ptnVals, tbl);
    return client.add_partition(part);
  }

  public void alterPartitionWithLocation(HiveMetaStoreClient client, Partition partition, String location) throws Exception {
    partition.getSd().setLocation(location);
    client.alter_partition(partition.getDbName(), partition.getTableName(), partition);
  }

  public void renamePartition(HiveMetaStoreClient client, Partition partition, Partition newPartition) throws Exception {
    client.renamePartition(partition.getDbName(), partition.getTableName(), partition.getValues(),
        newPartition);
  }

  public void dropPartition(HiveMetaStoreClient client, String dbName,
                           String tblName, List<String> ptnVals) throws Exception {
    client.dropPartition(dbName, tblName, ptnVals);
  }

  public void addPartitionWithLocation(HiveMetaStoreClient client,
      String dbName, String tblName, List<String> ptnVals, Table tbl,
      String location) throws Exception {
    Partition part = makeMetastorePartitionObject(dbName, tblName, ptnVals,
        tbl, location);
    client.add_partition(part);
  }

  public Table makeMetastoreTableObject(HiveMetaStoreClient client,
      String dbName, String tabName, List<FieldSchema> cols) throws Exception {
    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tabName);
    StorageDescriptor sd = new StorageDescriptor();
    tbl.setSd(sd);
    tbl.setParameters(new HashMap<String, String>());
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    return tbl;
  }

  public Partition makeMetastorePartitionObject(String dbName, String tblName,
      List<String> ptnVals, Table tbl, String partitionLocation) {
    Partition part = makeMetastoreBasePartitionObject(dbName, tblName, ptnVals,
        tbl);
    part.getSd().setLocation(partitionLocation);
    return part;
  }

  public Partition makeMetastorePartitionObject(String dbName, String tblName,
      List<String> ptnVals, Table tbl) {
    Partition part = makeMetastoreBasePartitionObject(dbName, tblName, ptnVals,
        tbl);
    return part;
  }

  private Partition makeMetastoreBasePartitionObject(String dbName,
      String tblName, List<String> ptnVals, Table tbl) {
    Partition part4 = new Partition();
    part4.setDbName(dbName);
    part4.setTableName(tblName);
    part4.setValues(ptnVals);
    part4.setParameters(new HashMap<String, String>());
    part4.setSd(tbl.getSd().deepCopy());
    part4.getSd().setLocation(null);
    part4.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo().deepCopy());
    part4.setParameters(new HashMap<String, String>());
    return part4;
  }

  public void createMetastoreDB(HiveMetaStoreClient client, String dbName)
      throws Exception {
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
  }

  public void dropMetastoreDBIfExists(HiveMetaStoreClient client, String dbName)
      throws Exception {
    client.dropDatabase(dbName, true, true, true);
  }

  public void execHiveSQLwithOverlay(final String sqlStmt,
      final String userName, Map<String, String> overLay) throws Exception {
    final HiveConf hiveConf = new HiveConf();
    for (Map.Entry<String, String> entry : overLay.entrySet()) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }
    UserGroupInformation clientUgi = UserGroupInformation
        .createRemoteUser(userName);
    clientUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Void run() throws Exception {
        Driver driver = new Driver(hiveConf, userName);
        SessionState.start(new CliSessionState(hiveConf));
        CommandProcessorResponse cpr = driver.run(sqlStmt);
        if (cpr.getResponseCode() != 0) {
          throw new IOException("Failed to execute \"" + sqlStmt
              + "\". Driver returned " + cpr.getResponseCode() + " Error: "
              + cpr.getErrorMessage());
        }
        driver.close();
        SessionState.get().close();
        return null;
      }
    });
  }


  public void execHiveSQL(String sqlStmt, String userName) throws Exception {
    execHiveSQLwithOverlay(sqlStmt, userName, new HashMap<String, String>());
  }

  public void execPigLatin(String userName, final PigServer pigServer,
      final String pigLatin) throws Exception {
    UserGroupInformation clientUgi = UserGroupInformation
        .createRemoteUser(userName);
    clientUgi.doAs(
        new PrivilegedExceptionAction<Object>() {
          @Override
          public Void run() throws Exception {
            pigServer.registerQuery(pigLatin);
            return null;
          }
        });
  }

}
