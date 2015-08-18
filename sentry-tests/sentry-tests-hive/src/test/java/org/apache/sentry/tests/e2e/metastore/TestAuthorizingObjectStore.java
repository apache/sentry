/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.metastore;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestAuthorizingObjectStore extends
		AbstractMetastoreTestWithStaticConfiguration {
  private PolicyFile policyFile;
  private static final String dbName1 = "db_1";
  private static final String dbName2 = "db_2";
  private static final String tabName1 = "tab1";
  private static final String tabName2 = "tab2";
  private static final String tabName3 = "tab3";
  private static final String tabName4 = "tab4";
  private static final String all_role = "all_role";
  private static final String db1_t1_role = "db1_t1_role";
  private static final String partitionVal = "part1";
  private static final String colName1 = "col1";
  // this user is configured for sentry.metastore.service.users,
  // for this test, the value is set when creating the HiveServer.
  private static final String userWithoutAccess = "accessAllMetaUser";

  @BeforeClass
  public static void setupTestStaticConfiguration () throws Exception {
    AbstractMetastoreTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    policyFile = setAdminOnServer1(ADMINGROUP);
    policyFile.setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
    super.setup();

    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    client.dropDatabase(dbName1, true, true, true);
    client.dropDatabase(dbName2, true, true, true);
    createMetastoreDB(client, dbName1);
    createMetastoreDB(client, dbName2);

    Table tbl1 = createMetastoreTableWithPartition(client, dbName1, tabName1,
        Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    addPartition(client, dbName1, tabName1, Lists.newArrayList(partitionVal), tbl1);

    Table tbl2 = createMetastoreTableWithPartition(client, dbName1, tabName2,
        Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    addPartition(client, dbName1, tabName2, Lists.newArrayList(partitionVal), tbl2);

    Table tbl3 = createMetastoreTableWithPartition(client, dbName2, tabName3,
        Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    addPartition(client, dbName2, tabName3, Lists.newArrayList(partitionVal), tbl3);

    Table tbl4 = createMetastoreTableWithPartition(client, dbName2, tabName4,
        Lists.newArrayList(new FieldSchema("col1", "int", "")),
        Lists.newArrayList(new FieldSchema("part_col1", "string", "")));
    addPartition(client, dbName2, tabName4, Lists.newArrayList(partitionVal), tbl4);

    client.close();

    policyFile
            .addRolesToGroup(USERGROUP1, all_role)
            .addRolesToGroup(USERGROUP2, db1_t1_role)
            .addPermissionsToRole(all_role, "server=server1->db=" + dbName1)
            .addPermissionsToRole(all_role, "server=server1->db=" + dbName2)
            .addPermissionsToRole(
                    all_role,
                    "server=server1->db=" + dbName1 + "->table=" + tabName1
                            + "->action=SELECT")
            .addPermissionsToRole(
                    all_role,
                    "server=server1->db=" + dbName1 + "->table=" + tabName2
                            + "->action=SELECT")
            .addPermissionsToRole(
                    all_role,
                    "server=server1->db=" + dbName2 + "->table=" + tabName3
                            + "->action=SELECT")
            .addPermissionsToRole(
                    all_role,
                    "server=server1->db=" + dbName2 + "->table=" + tabName4
                            + "->action=SELECT")
            .addPermissionsToRole(
                    db1_t1_role,
                    "server=server1->db=" + dbName1 + "->table=" + tabName1
                            + "->action=SELECT")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
  }

  /**
   * The configuration "sentry.metastore.service.users" is for user who has all
   * access to metadata, and the value should be case-sensitive.
   * 
   * @throws Exception
   */
  @Test
  public void testPrivilegesForUserNameCaseSensitive() throws Exception {
    // The value of "sentry.metastore.service.users" is "accessAllMetaUser", and
    // the value is case-sensitive.
    // The input name is "ACCESSALLMEATAUSER", and the client should has no
    // access to metadata.
    HiveMetaStoreClient client = context.getMetaStoreClient(userWithoutAccess.toUpperCase());
    try {
      client.getDatabase(dbName1);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
  }

  /**
   * User accessAllMetaUser is configured as the value of
   * "sentry.metastore.service.users" and has all access to databases and
   * tables.
   * 
   * @throws Exception
   */
  @Test
  public void testPrivilegesForUserWithoutAccess() throws Exception {
    HiveMetaStoreClient client = context.getMetaStoreClient(userWithoutAccess);
    assertThat(client.getDatabase(dbName1), notNullValue());
    assertThat(client.getDatabase(dbName2), notNullValue());
    // including the "default" db
    assertThat(client.getAllDatabases().size(), equalTo(3));
    // including the "default" db
    assertThat(client.getDatabases("*").size(), equalTo(3));
    assertThat(client.getTable(dbName1, tabName1), notNullValue());
    assertThat(client.getTable(dbName1, tabName2), notNullValue());
    assertThat(client.getTable(dbName2, tabName3), notNullValue());
    assertThat(client.getTable(dbName2, tabName4), notNullValue());
    assertThat(client.listPartitions(dbName1, tabName1, (short) 1).size(), equalTo(1));
    assertThat(client.listPartitions(dbName1, tabName2, (short) 1).size(), equalTo(1));
    assertThat(client.listPartitions(dbName2, tabName3, (short) 1).size(), equalTo(1));
    assertThat(client.listPartitions(dbName2, tabName4, (short) 1).size(), equalTo(1));

    assertThat(
        client.listPartitions(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));
    assertThat(
        client.listPartitions(dbName1, tabName2,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));
    assertThat(
        client.listPartitions(dbName2, tabName3,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));
    assertThat(
        client.listPartitions(dbName2, tabName4,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));

    assertThat(
        client.getPartition(dbName1, tabName1, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(
        client.getPartition(dbName1, tabName2, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(
        client.getPartition(dbName2, tabName3, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(
        client.getPartition(dbName2, tabName4, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(client.getTables(dbName1, "tab*").size(), equalTo(2));
    assertThat(client.getTables(dbName2, "tab*").size(), equalTo(2));
    assertThat(
        client.getTableObjectsByName(dbName1,
            new ArrayList<String>(Arrays.asList(tabName1, tabName2))).size(), equalTo(2));
    assertThat(
        client.getTableObjectsByName(dbName2,
            new ArrayList<String>(Arrays.asList(tabName3, tabName4))).size(), equalTo(2));
    assertThat(client.getAllTables(dbName1).size(), equalTo(2));
    assertThat(client.getAllTables(dbName2).size(), equalTo(2));
    assertThat(client.listPartitionNames(dbName1, tabName1, (short) 2).size(), equalTo(1));
    assertThat(client.listPartitionNames(dbName1, tabName2, (short) 2).size(), equalTo(1));
    assertThat(client.listPartitionNames(dbName2, tabName3, (short) 2).size(), equalTo(1));
    assertThat(client.listPartitionNames(dbName2, tabName4, (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName1, tabName2,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName2, tabName3,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName2, tabName4,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.getPartitionsByNames(dbName1, tabName1, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    assertThat(
        client.getPartitionsByNames(dbName1, tabName2, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    assertThat(
        client.getPartitionsByNames(dbName2, tabName3, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    assertThat(
        client.getPartitionsByNames(dbName2, tabName4, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    try {
      client.getIndex(dbName1, tabName1, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }
    try {
      client.getIndex(dbName1, tabName2, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }

    assertThat(client.listIndexes(dbName1, tabName1, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexes(dbName1, tabName2, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexes(dbName2, tabName3, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexes(dbName2, tabName4, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName1, tabName1, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName1, tabName2, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName2, tabName3, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName2, tabName4, (short) 1).size(), equalTo(0));
    assertThat(client.getPartitionWithAuthInfo(dbName1, tabName1,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(client.getPartitionWithAuthInfo(dbName1, tabName2,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(client.getPartitionWithAuthInfo(dbName2, tabName3,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(client.getPartitionWithAuthInfo(dbName2, tabName4,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(
        client.getTableColumnStatistics(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    assertThat(
        client.getTableColumnStatistics(dbName1, tabName2,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    assertThat(
        client.getTableColumnStatistics(dbName2, tabName3,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    assertThat(
        client.getTableColumnStatistics(dbName2, tabName4,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    client.close();
  }

  /**
   * The group of USER1_1 is USERGROUP1, and has all access to databases and
   * tables.
   * 
   * @throws Exception
   */
  @Test
  public void testPrivilegesForFullAccess() throws Exception {
    HiveMetaStoreClient client = context.getMetaStoreClient(USER1_1);
    assertThat(client.getDatabase(dbName1), notNullValue());
    assertThat(client.getDatabase(dbName2), notNullValue());
    // including the "default" db
    assertThat(client.getAllDatabases().size(), equalTo(3));
    // including the "default" db
    assertThat(client.getDatabases("*").size(), equalTo(3));
    assertThat(client.getTable(dbName1, tabName1), notNullValue());
    assertThat(client.getTable(dbName1, tabName2), notNullValue());
    assertThat(client.getTable(dbName2, tabName3), notNullValue());
    assertThat(client.getTable(dbName2, tabName4), notNullValue());
    assertThat(client.listPartitions(dbName1, tabName1, (short) 1).size(), equalTo(1));
    assertThat(client.listPartitions(dbName1, tabName2, (short) 1).size(), equalTo(1));
    assertThat(client.listPartitions(dbName2, tabName3, (short) 1).size(), equalTo(1));
    assertThat(client.listPartitions(dbName2, tabName4, (short) 1).size(), equalTo(1));

    assertThat(
        client.listPartitions(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));
    assertThat(
        client.listPartitions(dbName1, tabName2,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));
    assertThat(
        client.listPartitions(dbName2, tabName3,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));
    assertThat(
        client.listPartitions(dbName2, tabName4,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));

    assertThat(
        client.getPartition(dbName1, tabName1, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(
        client.getPartition(dbName1, tabName2, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(
        client.getPartition(dbName2, tabName3, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(
        client.getPartition(dbName2, tabName4, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    assertThat(client.getTables(dbName1, "tab*").size(), equalTo(2));
    assertThat(client.getTables(dbName2, "tab*").size(), equalTo(2));
    assertThat(
        client.getTableObjectsByName(dbName1,
            new ArrayList<String>(Arrays.asList(tabName1, tabName2))).size(), equalTo(2));
    assertThat(
        client.getTableObjectsByName(dbName2,
            new ArrayList<String>(Arrays.asList(tabName3, tabName4))).size(), equalTo(2));
    assertThat(client.getAllTables(dbName1).size(), equalTo(2));
    assertThat(client.getAllTables(dbName2).size(), equalTo(2));
    assertThat(client.listPartitionNames(dbName1, tabName1, (short) 2).size(), equalTo(1));
    assertThat(client.listPartitionNames(dbName1, tabName2, (short) 2).size(), equalTo(1));
    assertThat(client.listPartitionNames(dbName2, tabName3, (short) 2).size(), equalTo(1));
    assertThat(client.listPartitionNames(dbName2, tabName4, (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName1, tabName2,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName2, tabName3,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.listPartitionNames(dbName2, tabName4,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    assertThat(
        client.getPartitionsByNames(dbName1, tabName1, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    assertThat(
        client.getPartitionsByNames(dbName1, tabName2, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    assertThat(
        client.getPartitionsByNames(dbName2, tabName3, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    assertThat(
        client.getPartitionsByNames(dbName2, tabName4, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    try {
      client.getIndex(dbName1, tabName1, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }
    try {
      client.getIndex(dbName1, tabName2, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }

    assertThat(client.listIndexes(dbName1, tabName1, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexes(dbName1, tabName2, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexes(dbName2, tabName3, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexes(dbName2, tabName4, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName1, tabName1, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName1, tabName2, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName2, tabName3, (short) 1).size(), equalTo(0));
    assertThat(client.listIndexNames(dbName2, tabName4, (short) 1).size(), equalTo(0));
    assertThat(client.getPartitionWithAuthInfo(dbName1, tabName1,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(client.getPartitionWithAuthInfo(dbName1, tabName2,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(client.getPartitionWithAuthInfo(dbName2, tabName3,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(client.getPartitionWithAuthInfo(dbName2, tabName4,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    assertThat(
        client.getTableColumnStatistics(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    assertThat(
        client.getTableColumnStatistics(dbName1, tabName2,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    assertThat(
        client.getTableColumnStatistics(dbName2, tabName3,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    assertThat(
        client.getTableColumnStatistics(dbName2, tabName4,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    client.close();
  }
  
  /**
   * The group of USER2_1 is USERGROUP2, and has the access to db1 and t1.
   * 
   * @throws Exception
   */
  @Test
  public void testPrivilegesForPartialAccess() throws Exception {
    HiveMetaStoreClient client = context.getMetaStoreClient(USER2_1);
    assertThat(client.getDatabase(dbName1), notNullValue());
    try {
      client.getDatabase(dbName2);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    // including the "default" db
    assertThat(client.getAllDatabases().size(), equalTo(2));
    // including the "default" db
    assertThat(client.getDatabases("*").size(), equalTo(2));
    assertThat(client.getTable(dbName1, tabName1), notNullValue());
    try {
      client.getTable(dbName1, tabName2);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTable(dbName2, tabName3);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTable(dbName2, tabName4);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(client.listPartitions(dbName1, tabName1, (short) 1).size(), equalTo(1));
    try {
      client.listPartitions(dbName1, tabName2, (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName3, (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName4, (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(
        client.listPartitions(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 1).size(), equalTo(1));
    try {
      client.listPartitions(dbName1, tabName2, new ArrayList<String>(Arrays.asList(partitionVal)),
          (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName3, new ArrayList<String>(Arrays.asList(partitionVal)),
          (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName4, new ArrayList<String>(Arrays.asList(partitionVal)),
          (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(
        client.getPartition(dbName1, tabName1, new ArrayList<String>(Arrays.asList(partitionVal))),
        notNullValue());
    try {
      client.getPartition(dbName1, tabName2, new ArrayList<String>(Arrays.asList(partitionVal)));
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartition(dbName2, tabName3, new ArrayList<String>(Arrays.asList(partitionVal)));
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartition(dbName2, tabName4, new ArrayList<String>(Arrays.asList(partitionVal)));
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(client.getTables(dbName1, "tab*").size(), equalTo(1));
    assertThat(client.getTables(dbName2, "tab*").size(), equalTo(0));
    assertThat(
        client.getTableObjectsByName(dbName1,
            new ArrayList<String>(Arrays.asList(tabName1, tabName2))).size(), equalTo(1));
    assertThat(
        client.getTableObjectsByName(dbName2,
            new ArrayList<String>(Arrays.asList(tabName3, tabName4))).size(), equalTo(0));
    assertThat(client.getAllTables(dbName1).size(), equalTo(1));
    assertThat(client.getAllTables(dbName2).size(), equalTo(0));

    assertThat(client.listPartitionNames(dbName1, tabName1, (short) 2).size(), equalTo(1));
    try {
      client.listPartitionNames(dbName1, tabName2, (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException te) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName3, (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException te) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName4, (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException te) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(
        client.listPartitionNames(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2).size(), equalTo(1));
    try {
      client.listPartitionNames(dbName1, tabName2,
          new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName3,
          new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName4,
          new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(
        client.getPartitionsByNames(dbName1, tabName1, new ArrayList<String>(Arrays.asList("")))
            .size(), equalTo(0));
    try {
      client.getPartitionsByNames(dbName1, tabName2, new ArrayList<String>(Arrays.asList("")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionsByNames(dbName2, tabName3, new ArrayList<String>(Arrays.asList("")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionsByNames(dbName2, tabName4, new ArrayList<String>(Arrays.asList("")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.getIndex(dbName1, tabName1, "empty");
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is successful.
    }
    try {
      client.getIndex(dbName1, tabName2, "empty");
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(client.listIndexes(dbName1, tabName1, (short) 1).size(), equalTo(0));
    try {
      client.listIndexes(dbName1, tabName2, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexes(dbName2, tabName3, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexes(dbName2, tabName4, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(client.listIndexNames(dbName1, tabName1, (short) 1).size(), equalTo(0));
    try {
      client.listIndexNames(dbName1, tabName2, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexNames(dbName2, tabName3, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexNames(dbName2, tabName4, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(client.getPartitionWithAuthInfo(dbName1, tabName1,
        new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
            Arrays.asList("tempgroup"))), notNullValue());
    try {
      client.getPartitionWithAuthInfo(dbName1, tabName2,
          new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
              Arrays.asList("tempgroup")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionWithAuthInfo(dbName2, tabName3,
          new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
              Arrays.asList("tempgroup")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionWithAuthInfo(dbName2, tabName4,
          new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
              Arrays.asList("tempgroup")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(
        client.getTableColumnStatistics(dbName1, tabName1,
            new ArrayList<String>(Arrays.asList(colName1))).size(), equalTo(0));
    try {
      client.getTableColumnStatistics(dbName1, tabName2,
          new ArrayList<String>(Arrays.asList(colName1)));
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTableColumnStatistics(dbName2, tabName3,
          new ArrayList<String>(Arrays.asList(colName1)));
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTableColumnStatistics(dbName2, tabName4,
          new ArrayList<String>(Arrays.asList(colName1)));
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    client.close();
  }

  /**
   * The group of USER3_1 is USERGROUP3, and has no access to database and
   * table.
   * 
   * @throws Exception
   */
  @Test
  public void testPrivilegesForNoAccess() throws Exception {
    HiveMetaStoreClient client = context.getMetaStoreClient(USER3_1);
    try {
      client.getDatabase(dbName1);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getDatabase(dbName2);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    // including the "default" db
    assertThat(client.getAllDatabases().size(), equalTo(1));
    // including the "default" db
    assertThat(client.getDatabases("*").size(), equalTo(1));
    try {
      client.getTable(dbName1, tabName1);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTable(dbName1, tabName2);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTable(dbName2, tabName3);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTable(dbName2, tabName4);
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.listPartitions(dbName1, tabName1, (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName1, tabName2, (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName3, (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName4, (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.listPartitions(dbName1, tabName1, new ArrayList<String>(Arrays.asList(partitionVal)),
          (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName1, tabName2, new ArrayList<String>(Arrays.asList(partitionVal)),
          (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName3, new ArrayList<String>(Arrays.asList(partitionVal)),
          (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitions(dbName2, tabName4, new ArrayList<String>(Arrays.asList(partitionVal)),
          (short) 1);
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.getPartition(dbName1, tabName1, new ArrayList<String>(Arrays.asList(partitionVal)));
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartition(dbName1, tabName2, new ArrayList<String>(Arrays.asList(partitionVal)));
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartition(dbName2, tabName3, new ArrayList<String>(Arrays.asList(partitionVal)));
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartition(dbName2, tabName4, new ArrayList<String>(Arrays.asList(partitionVal)));
      fail("NoSuchObjectException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    assertThat(client.getTables(dbName1, "tab*").size(), equalTo(0));
    assertThat(client.getTables(dbName2, "tab*").size(), equalTo(0));
    assertThat(
        client.getTableObjectsByName(dbName1,
            new ArrayList<String>(Arrays.asList(tabName1, tabName2))).size(), equalTo(0));
    assertThat(
        client.getTableObjectsByName(dbName2,
            new ArrayList<String>(Arrays.asList(tabName3, tabName4))).size(), equalTo(0));
    assertThat(client.getAllTables(dbName1).size(), equalTo(0));
    assertThat(client.getAllTables(dbName2).size(), equalTo(0));

    try {
      client.listPartitionNames(dbName1, tabName1, (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName1, tabName2, (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName3, (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName4, (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.listPartitionNames(dbName1, tabName1,
          new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName1, tabName2,
          new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName3,
          new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listPartitionNames(dbName2, tabName4,
          new ArrayList<String>(Arrays.asList(partitionVal)), (short) 2);
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.getPartitionsByNames(dbName1, tabName1, new ArrayList<String>(Arrays.asList("")));
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionsByNames(dbName1, tabName2, new ArrayList<String>(Arrays.asList("")));
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionsByNames(dbName2, tabName3, new ArrayList<String>(Arrays.asList("")));
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionsByNames(dbName2, tabName4, new ArrayList<String>(Arrays.asList("")));
      fail("MetaException should have been thrown");
    } catch (TException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.getIndex(dbName1, tabName1, "empty");
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getIndex(dbName1, tabName2, "empty");
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getIndex(dbName2, tabName3, "empty");
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.listIndexes(dbName1, tabName1, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexes(dbName1, tabName2, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexes(dbName2, tabName3, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexes(dbName2, tabName4, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.listIndexNames(dbName1, tabName1, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexNames(dbName1, tabName2, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexNames(dbName2, tabName3, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.listIndexNames(dbName2, tabName4, (short) 1);
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.getPartitionWithAuthInfo(dbName1, tabName1,
          new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
              Arrays.asList("tempgroup")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionWithAuthInfo(dbName1, tabName2,
          new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
              Arrays.asList("tempgroup")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionWithAuthInfo(dbName2, tabName3,
          new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
              Arrays.asList("tempgroup")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getPartitionWithAuthInfo(dbName2, tabName4,
          new ArrayList<String>(Arrays.asList(partitionVal)), "tempuser", new ArrayList<String>(
              Arrays.asList("tempgroup")));
      fail("MetaException should have been thrown");
    } catch (NoSuchObjectException noe) {
      // ignore, just make sure the authorization is failed.
    }

    try {
      client.getTableColumnStatistics(dbName1, tabName1,
          new ArrayList<String>(Arrays.asList(colName1)));
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTableColumnStatistics(dbName1, tabName2,
          new ArrayList<String>(Arrays.asList(colName1)));
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTableColumnStatistics(dbName2, tabName3,
          new ArrayList<String>(Arrays.asList(colName1)));
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    try {
      client.getTableColumnStatistics(dbName2, tabName4,
          new ArrayList<String>(Arrays.asList(colName1)));
      fail("MetaException should have been thrown");
    } catch (MetaException noe) {
      // ignore, just make sure the authorization is failed.
    }
    client.close();
  }
}
