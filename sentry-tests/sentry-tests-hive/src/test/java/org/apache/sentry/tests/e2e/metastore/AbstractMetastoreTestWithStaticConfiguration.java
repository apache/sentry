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

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.PigServer;
import org.apache.sentry.tests.e2e.dbprovider.PolicyProviderForTest;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory.HiveServer2Type;
import org.junit.BeforeClass;

public abstract class AbstractMetastoreTestWithStaticConfiguration extends
    AbstractTestWithStaticConfiguration {

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    testServerType = HiveServer2Type.InternalMetastore.name();
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
    PolicyProviderForTest.setSentryClient(AbstractTestWithStaticConfiguration
        .getSentryClient());
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

  public Table createMetastoreTableWithPartition(HiveMetaStoreClient client,
      String dbName, String tabName, List<FieldSchema> cols,
      List<FieldSchema> partionVals) throws Exception {
    Table tbl = makeMetastoreTableObject(client, dbName, tabName, cols);
    tbl.setPartitionKeys(partionVals);
    client.createTable(tbl);
    return client.getTable(dbName, tabName);
  }

  public void addPartition(HiveMetaStoreClient client, String dbName,
      String tblName, List<String> ptnVals, Table tbl) throws Exception {
    Partition part = makeMetastorePartitionObject(dbName, tblName, ptnVals, tbl);
    Partition retp = client.add_partition(part);
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
    part.getSd().setLocation("");
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

  public void execHiveSQL(String sqlStmt, String userName) throws Exception {
    HiveConf hiveConf = new HiveConf();
    Driver driver = new Driver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    driver.run(sqlStmt);
    driver.close();
    SessionState.get().close();
  }

  public void execPigLatin(String userName, final PigServer pigServer,
      final String pigLatin) throws Exception {
    UserGroupInformation clientUgi = UserGroupInformation
        .createRemoteUser(userName);
    ShimLoader.getHadoopShims().doAs(clientUgi,
        new PrivilegedExceptionAction<Object>() {
          @Override
          public Void run() throws Exception {
            pigServer.registerQuery(pigLatin);
            return null;
          }
        });
  }

}
