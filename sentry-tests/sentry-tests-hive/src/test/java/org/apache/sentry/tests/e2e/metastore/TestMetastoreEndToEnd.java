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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.Context;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class TestMetastoreEndToEnd extends
    AbstractMetastoreTestWithStaticConfiguration {

  private PolicyFile policyFile;
  private File dataFile;
  private static final String dbName = "db_1";
  private static final String db_all_role = "all_db1";
  private static final String uri_role = "uri_role";
  private static final String tab1_all_role = "tab1_all_role";
  private static final String tab1_read_role = "tab1_read_role";
  private static final String tab2_all_role = "tab2_all_role";
  private static final String tab2_read_role = "tab2_read_role";
  private static final String tabName1 = "tab1";
  private static final String tabName2 = "tab2";

  @Before
  public void setup() throws Exception {
    policyFile = setAdminOnServer1(ADMINGROUP); // PolicyFile.setAdminOnServer1(ADMINGROUP);
    policyFile
        .addRolesToGroup(USERGROUP1, db_all_role)
        .addRolesToGroup(USERGROUP2, "read_db_role")
        .addRolesToGroup(USERGROUP2, tab1_all_role)
        .addRolesToGroup(USERGROUP2, tab2_all_role)
        .addRolesToGroup(USERGROUP3, tab1_read_role)
        .addRolesToGroup(USERGROUP3, tab2_read_role)
        .addPermissionsToRole(db_all_role, "server=server1->db=" + dbName)
        .addPermissionsToRole("read_db_role",
            "server=server1->db=" + dbName + "->table=*->action=SELECT")
        .addPermissionsToRole(tab1_all_role,
            "server=server1->db=" + dbName + "->table=" + tabName1)
        .addPermissionsToRole(tab2_all_role,
            "server=server1->db=" + dbName + "->table=" + tabName2)
        .addPermissionsToRole(tab1_read_role,
            "server=server1->db=" + dbName + "->table=" + tabName1 + "->action=SELECT")
        .addPermissionsToRole(tab2_read_role,
            "server=server1->db=" + dbName + "->table=" + tabName2 + "->action=SELECT")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    client.dropDatabase(dbName, true, true, true);
    createMetastoreDB(client, dbName);
    client.close();

  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /**
   * Setup admin privileges for user ADMIN1 verify user can create DB and tables
   * @throws Exception
   */
  @Test
  public void testServerPrivileges() throws Exception {
    String tabName = "tab1";
    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    client.dropDatabase(dbName, true, true, true);

    createMetastoreDB(client, dbName);
    createMetastoreTable(client, dbName, tabName,
        Lists.newArrayList(new FieldSchema("col1", "int", "")));
    assertEquals(1, client.getTables(dbName, tabName).size());
    client.dropTable(dbName, tabName);
    client.dropDatabase(dbName, true, true, true);
  }

  /**
   * verify non-admin user can not create or drop DB
   * @throws Exception
   */
  @Test
  public void testNegativeServerPrivileges() throws Exception {
    HiveMetaStoreClient client = context.getMetaStoreClient(USER1_1);
    try {
      createMetastoreDB(client, "fooDb");
      fail("Creat db should have failed for non-admin user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    try {
      createMetastoreDB(client, "barDb");
      fail("create db should have failed for non-admin user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
  }

  /**
   * Verify the user with DB permission can create table in that db Verify the
   * user can't create table in DB where he doesn't have ALL permissions
   * @throws Exception
   */
  @Test
  public void testTablePrivileges() throws Exception {
    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    createMetastoreTable(client, dbName, tabName1,
        Lists.newArrayList(new FieldSchema("col1", "int", "")));
    client.close();

    client = context.getMetaStoreClient(USER1_1);
    createMetastoreTable(client, dbName, tabName2,
        Lists.newArrayList(new FieldSchema("col1", "int", "")));
    assertEquals(1, client.getTables(dbName, tabName2).size());
    client.dropTable(dbName, tabName1);
    createMetastoreTable(client, dbName, tabName1,
        Lists.newArrayList(new FieldSchema("col1", "int", "")));
    client.close();

    // group2 users can't create the table, but can drop it
    client = context.getMetaStoreClient(USER2_1);
    try {
      createMetastoreTable(client, dbName, "barTab",
          Lists.newArrayList(new FieldSchema("col1", "int", "")));
      fail("Create table should have failed for non-privilege user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.dropTable(dbName, tabName2);
    client.close();

    // group3 users can't create or drop it
    client = context.getMetaStoreClient(USER3_1);
    try {
      createMetastoreTable(client, dbName, "barTab",
          Lists.newArrayList(new FieldSchema("col1", "int", "")));
      fail("Create table should have failed for non-privilege user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }

    try {
      client.dropTable(dbName, tabName1);
      fail("drop table should have failed for non-privilege user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();
  }

  /**
   * Verify alter table privileges
   * @throws Exception
   */
  @Test
  public void testAlterTablePrivileges() throws Exception {

    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    createMetastoreTable(client, dbName, tabName1,
        Lists.newArrayList(new FieldSchema("col1", "int", "")));
    client.close();

    // verify group1 users with DDL privileges can alter tables in db_1
    client = context.getMetaStoreClient(USER1_1);
    Table metaTable2 = client.getTable(dbName, tabName1);
    metaTable2.getSd().setCols(
        Lists.newArrayList(new FieldSchema("col2", "double", "")));
    client.alter_table(dbName, tabName1, metaTable2);
    Table metaTable3 = client.getTable(dbName, tabName1);
    assertEquals(metaTable2.getSd().getCols(), metaTable3.getSd().getCols());

    // verify group1 users with DDL privileges can alter tables in db_1
    client = context.getMetaStoreClient(USER2_1);
    metaTable2 = client.getTable(dbName, tabName1);
    metaTable2.getSd().setCols(
        Lists.newArrayList(new FieldSchema("col3", "string", "")));
    client.alter_table(dbName, tabName1, metaTable2);
    metaTable3 = client.getTable(dbName, tabName1);
    assertEquals(metaTable2.getSd().getCols(), metaTable3.getSd().getCols());

    // verify group3 users can't alter tables in db_1
    client = context.getMetaStoreClient(USER3_1);
    metaTable2 = client.getTable(dbName, tabName1);
    metaTable2.getSd().setCols(
        Lists.newArrayList(new FieldSchema("col3", "string", "")));
    try {
      client.alter_table(dbName, tabName1, metaTable2);
      fail("alter table should have failed for non-privilege user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();
  }

  /**
   * Verify add partition privileges
   * @throws Exception
   */
  @Test
  public void testAddPartitionPrivileges() throws Exception {
    ArrayList<String> partVals1 = Lists.newArrayList("part1");
    ArrayList<String> partVals2 = Lists.newArrayList("part2");
    ArrayList<String> partVals3 = Lists.newArrayList("part3");
    ArrayList<String> partVals4 = Lists.newArrayList("part4");

    // user with ALL on DB should be able to add partition
    HiveMetaStoreClient client = context.getMetaStoreClient(USER1_1);
    Table tbl1 = createMetastoreTableWithPartition(client, dbName, tabName1,
        Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    assertEquals(1, client.getTables(dbName, tabName1).size());
    addPartition(client, dbName, tabName1, partVals1, tbl1);
    addPartition(client, dbName, tabName1, partVals2, tbl1);
    client.close();

    // user with ALL on Table should be able to add partition
    client = context.getMetaStoreClient(USER2_1);
    tbl1 = client.getTable(dbName, tabName1);
    addPartition(client, dbName, tabName1, partVals3, tbl1);
    client.close();

    // user without ALL on DB or Table should NOT be able to add partition
    client = context.getMetaStoreClient(USER3_1);
    tbl1 = client.getTable(dbName, tabName1);
    try {
      addPartition(client, dbName, tabName1, partVals4, tbl1);
      fail("Add partition should have failed for non-admin user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();

    // user with ALL on DB should be able to drop partition
    client = context.getMetaStoreClient(USER1_1);
    tbl1 = client.getTable(dbName, tabName1);
    client.dropPartition(dbName, tabName1, partVals1, true);
    client.close();

    // user with ALL on Table should be able to drop partition
    client = context.getMetaStoreClient(USER2_1);
    tbl1 = client.getTable(dbName, tabName1);
    client.dropPartition(dbName, tabName1, partVals2, true);
    client.close();

    // user without ALL on DB or Table should NOT be able to drop partition
    client = context.getMetaStoreClient(USER3_1);
    try {
      addPartition(client, dbName, tabName1, partVals3, tbl1);
      fail("Drop partition should have failed for non-admin user");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
  }

  /**
   * Verify URI privileges for alter table table
   * @throws Exception
   */
  @Test
  public void testUriTablePrivileges() throws Exception {
    String newPath1 = "fooTab1";
    String newPath2 = "fooTab2";

    String tabDir1 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
        + File.separator + newPath1;
    String tabDir2 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
        + File.separator + newPath2;
    policyFile.addRolesToGroup(USERGROUP1, uri_role)
        .addRolesToGroup(USERGROUP2, uri_role)
        .addRolesToGroup(USERGROUP3, db_all_role)
        .addPermissionsToRole(uri_role, "server=server1->URI=" + tabDir1)
        .addPermissionsToRole(uri_role, "server=server1->URI=" + tabDir2);
    writePolicyFile(policyFile);

    // user with URI privileges should be able to create table with that
    // specific location
    HiveMetaStoreClient client = context.getMetaStoreClient(USER1_1);
    createMetastoreTableWithLocation(client, dbName, tabName1,
        Lists.newArrayList(new FieldSchema("col1", "int", "")), tabDir1);

    createMetastoreTableWithLocation(client, dbName, tabName2,
        Lists.newArrayList(new FieldSchema("col1", "int", "")), tabDir2);
    client.close();

    // user without URI privileges should be NOT able to create table with that specific location
    client = context.getMetaStoreClient(USER3_1);
    try {
      createMetastoreTableWithLocation(client, dbName, "fooTab",
          Lists.newArrayList(new FieldSchema("col1", "int", "")), tabDir2);
      fail("Create table with location should fail without URI privilege");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();

    // user with URI privileges should be able to alter table to set that specific location
    client = context.getMetaStoreClient(USER1_1);
    Table metaTable1 = client.getTable(dbName, tabName1);
    metaTable1.getSd().setLocation(tabDir2);
    client.alter_table(dbName, tabName1, metaTable1);
    client.close();

    // user with URI privileges and table all should be able to alter table to
    // set that specific location
    client = context.getMetaStoreClient(USER2_1);
    metaTable1 = client.getTable(dbName, tabName2);
    metaTable1.getSd().setLocation(tabDir1);
    client.alter_table(dbName, tabName2, metaTable1);
    client.close();

    // user without URI privileges should be NOT able to alter table to set that
    // specific location
    client = context.getMetaStoreClient(USER3_1);
    Table metaTable2 = client.getTable(dbName, tabName2);
    metaTable2.getSd().setLocation(tabDir2);
    try {
      client.alter_table(dbName, tabName2, metaTable2);
      fail("Alter table with location should fail without URI privilege");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();
  }

  /**
   * Verify URI privileges for alter table table
   * @throws Exception
   */
  @Test
  public void testUriPartitionPrivileges() throws Exception {
    String tabName1 = "tab1";
    String newPath1 = "fooTab1";
    String newPath2 = "fooTab2";
    ArrayList<String> partVals1 = Lists.newArrayList("part1");
    ArrayList<String> partVals2 = Lists.newArrayList("part2");
    ArrayList<String> partVals3 = Lists.newArrayList("part2");

    String tabDir1 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
        + File.separator + newPath1;
    String tabDir2 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
        + File.separator + newPath2;
    policyFile.addRolesToGroup(USERGROUP1, uri_role)
        .addRolesToGroup(USERGROUP2, db_all_role)
        .addPermissionsToRole(uri_role, "server=server1->URI=" + tabDir1)
        .addPermissionsToRole(uri_role, "server=server1->URI=" + tabDir2);
    writePolicyFile(policyFile);

    // user with URI privileges should be able to alter partition to set that specific location
    HiveMetaStoreClient client = context.getMetaStoreClient(USER1_1);
    Table tbl1 = createMetastoreTableWithPartition(client, dbName,
        tabName1, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    addPartition(client, dbName, tabName1, partVals1, tbl1);
    addPartitionWithLocation(client, dbName, tabName1, partVals2, tbl1,
        tabDir1);
    client.close();

    // user without URI privileges should be NOT able to alter partition to set
    // that specific location
    client = context.getMetaStoreClient(USER2_1);
    try {
      tbl1 = client.getTable(dbName, tabName1);
      addPartitionWithLocation(client, dbName, tabName1, partVals3,
          tbl1, tabDir2);
      fail("Add partition with location should have failed");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();
  }

  /**
   * Verify alter partion privileges
   * TODO: We seem to have a bit inconsistency with Alter partition. It's only
   * allowed with SERVER privilege. If we allow add/drop partition with DB
   * level privilege, then this should also be at the same level.
   * @throws Exception
   */
  @Test
  public void testAlterSetLocationPrivileges() throws Exception {
    String newPath1 = "fooTab1";
    ArrayList<String> partVals1 = Lists.newArrayList("part1");
    ArrayList<String> partVals2 = Lists.newArrayList("part2");
    String tabDir1 = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR)
        + File.separator + newPath1;

    policyFile.addRolesToGroup(USERGROUP1, uri_role)
        .addRolesToGroup(USERGROUP2, uri_role)
        .addPermissionsToRole(uri_role, "server=server1->URI=" + tabDir1);
    writePolicyFile(policyFile);

    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    Table tbl1 = createMetastoreTableWithPartition(client, dbName,
        tabName1, Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    addPartition(client, dbName, tabName1, partVals1, tbl1);
    tbl1 = client.getTable(dbName, tabName1);
    addPartition(client, dbName, tabName1, partVals2, tbl1);
    client.close();

    // user with DB and URI privileges should be able to alter partition set location
    client = context.getMetaStoreClient(USER1_1);
    Partition newPartition = client.getPartition(dbName, tabName1, partVals1);
    newPartition.getSd().setLocation(tabDir1);
    client.alter_partition(dbName, tabName1, newPartition);
    client.close();

    // user with Table and URI privileges should be able to alter partition set location
    client = context.getMetaStoreClient(USER2_1);
    newPartition = client.getPartition(dbName, tabName1, partVals2);
    newPartition.getSd().setLocation(tabDir1);
    client.alter_partition(dbName, tabName1, newPartition);
    client.close();

    policyFile.addRolesToGroup(USERGROUP3, db_all_role);
    writePolicyFile(policyFile);
    // user without URI privileges should not be able to alter partition set location
    client = context.getMetaStoreClient(USER3_1);
    newPartition = client.getPartition(dbName, tabName1, partVals2);
    newPartition.getSd().setLocation(tabDir1);
    try {
      client.alter_partition(dbName, tabName1, newPartition);
      fail("alter partition with location should have failed");
    } catch (MetaException e) {
      Context.verifyMetastoreAuthException(e);
    }
    client.close();

  }

  /**
   * Verify data load into new partition using INSERT .. PARTITION statement
   */
  @Test
  public void testPartionInsert() throws Exception {
    String partVal1 = "part1", partVal2 = "part2", partVal3 = "part3";

    policyFile.addRolesToGroup(USERGROUP1, uri_role).addPermissionsToRole(
        uri_role, "server=server1->uri=file://" + dataFile.getPath());
    writePolicyFile(policyFile);

    execHiveSQL("CREATE TABLE " + dbName + "." + tabName1
        + " (id int) PARTITIONED BY (part_col string)", USER1_1);
    execHiveSQL("CREATE TABLE " + dbName + "." + tabName2 + " (id int)",
        USER1_1);
    execHiveSQL("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
        + "' INTO TABLE " + dbName + "." + tabName2, USER1_1);

    // verify that user with DB all can add partition using INSERT .. PARTITION
    execHiveSQL("INSERT OVERWRITE TABLE " + dbName + "." + tabName1
        + " PARTITION (part_col='" + partVal1 + "') SELECT * FROM " + dbName
        + "." + tabName2, USER1_1);
    verifyPartitionExists(dbName, tabName1, partVal1);

    // verify that user with Table all can add partition using INSERT
    execHiveSQL("INSERT OVERWRITE TABLE " + dbName + "." + tabName1
        + " PARTITION (part_col='" + partVal2 + "') SELECT * FROM " + dbName
        + "." + tabName2, USER2_1);
    verifyPartitionExists(dbName, tabName1, partVal2);
  }

  private void verifyPartitionExists(String dbName, String tabName,
      String partVal) throws Exception {
    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    Partition newPartition = client.getPartition(dbName, tabName,
        Lists.newArrayList(partVal));
    Assert.assertNotNull(newPartition);
    client.close();
  }
}
