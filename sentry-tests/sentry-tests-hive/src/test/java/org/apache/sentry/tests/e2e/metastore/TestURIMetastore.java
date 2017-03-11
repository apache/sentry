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

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.Context;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;

import static org.junit.Assert.fail;

/**
 * This clase is to test URI privileges usability for Hive MetaStore binding.
 */
public class TestURIMetastore extends
    AbstractMetastoreTestWithStaticConfiguration {

  private PolicyFile policyFile;
  private File dataFile;
  private static final String dbName = "db_1";
  private static final String db_all_role = "all_db1";
  private static final String uri_role = "uri_role";


  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    setMetastoreListener = false;
    defaultFSOnHdfs = true;
    AbstractMetastoreTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {

    policyFile = setAdminOnServer1(ADMINGROUP);
    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
    super.setup();

    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    client.dropDatabase(dbName, true, true, true);
    createMetastoreDB(client, dbName);
    client.close();

    policyFile
            .addRolesToGroup(USERGROUP1, db_all_role)
            .addPermissionsToRole(db_all_role, "server=server1->db=" + dbName)
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /**
   * Verify URI privileges for create table DDL without scheme and authority
   * @throws Exception
   */
  @Test
  public void testCreateTableWithURINoSchemeAndAuthority() throws Exception {

    String tabName1 = "tab1";
    String tabDir1 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR) + File.separator + tabName1;

    // URI location that contains no scheme and authority.
    String tabDir1Path = new URI(tabDir1).getPath();

    policyFile.addRolesToGroup(USERGROUP1, uri_role)
    .addRolesToGroup(USERGROUP2, db_all_role)
    .addPermissionsToRole(uri_role, "server=server1->URI=" + tabDir1);
    writePolicyFile(policyFile);

    // user with URI privileges should be able to add table to URI locations which has no authority and scheme.
    HiveMetaStoreClient client = context.getMetaStoreClient(USER1_1);
    createMetastoreTableWithLocation(client, dbName, tabName1, Lists.newArrayList(new FieldSchema("col1", "int", "")), tabDir1Path);

    client.close();

    // user without URI privileges should be NOT able to create table to a specific location
    client = context.getMetaStoreClient(USER2_1);
    try {
      createMetastoreTableWithLocation(client, dbName, "fooTab", Lists.newArrayList(new FieldSchema("col1", "int", "")), tabDir1Path);
      fail("Create table with location should fail without URI privilege");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();
  }

  /**
   * Verify URI privileges for alter table DDL without scheme and authority
   * @throws Exception
   */
  @Test
  public void testAlterTableWithURINoSchemeAndAuthority() throws Exception {
    String tabName1 = "tab1";
    String newPath1 = "fooTab1";
    String newPath2 = "fooTab2";
    ArrayList<String> partVals1 = Lists.newArrayList("part1");
    ArrayList<String> partVals2 = Lists.newArrayList("part2");
    ArrayList<String> partVals3 = Lists.newArrayList("part3");

    String tabDir1 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
        + File.separator + tabName1 + newPath1;
    String tabDir2 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
    + File.separator + tabName1 + newPath2;
    // URI location that contains no scheme and authority.
    String tabDir1Path = new URI(tabDir1).getPath();
    String tabDir2Path = new URI(tabDir2).getPath();

    policyFile.addRolesToGroup(USERGROUP1, uri_role)
    .addRolesToGroup(USERGROUP2, db_all_role)
    .addPermissionsToRole(uri_role, "server=server1->URI=" + tabDir1);
    writePolicyFile(policyFile);

    // user with URI privileges should be able to alter partition to set that specific location
    HiveMetaStoreClient client = context.getMetaStoreClient(USER1_1);
    Table tbl1 = createMetastoreTableWithPartition(client, dbName, tabName1,
        Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    addPartition(client, dbName, tabName1, partVals1, tbl1);
    addPartitionWithLocation(client, dbName, tabName1, partVals2, tbl1, tabDir1Path);
    client.close();

    // user without URI privileges should be NOT able to alter partition to set
    // that specific location
    client = context.getMetaStoreClient(USER2_1);
    try {
      tbl1 = client.getTable(dbName, tabName1);
      addPartitionWithLocation(client, dbName, tabName1, partVals3, tbl1, tabDir2Path);
      fail("Add partition with location should have failed");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();
  }
}
