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

import java.io.File;
import java.io.FileOutputStream;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hive.hcatalog.pig.HCatStorer;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestMetaStoreWithPigHCat extends
    AbstractMetastoreTestWithStaticConfiguration {
  private PolicyFile policyFile;
  private File dataFile;
  private static final String dbName = "db_1";
  private static final String tabName1 = "tab1";
  private static final String tabName2 = "tab2";
  private static final String db_all_role = "all_db1";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("hadoopversion", "23");
  }

  @Before
  public void setup() throws Exception {
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    policyFile = setAdminOnServer1(ADMINGROUP);
    policyFile
        .addRolesToGroup(USERGROUP1, db_all_role)
        .addRolesToGroup(USERGROUP2, "read_db_role")
        .addPermissionsToRole(db_all_role, "server=server1->db=" + dbName)
        .addPermissionsToRole("read_db_role",
            "server=server1->db=" + dbName + "->table=" + tabName2 + "->action=SELECT")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    client.dropDatabase(dbName, true, true, true);
    createMetastoreDB(client, dbName);
    client.close();
  }

  /**
   * Verify add partition via Pig+HCatStore
   *
   * *** Disabled due to HCat inputformat compatibility issue in Hive 1.1.0
   */
  @Ignore
  @Test
  public void testPartionLoad() throws Exception {
    execHiveSQL("CREATE TABLE " + dbName + "." + tabName1
        + " (id int) PARTITIONED BY (part_col STRING)", ADMIN1);
    execHiveSQL("CREATE TABLE " + dbName + "." + tabName2
        + " (id int) PARTITIONED BY (part_col STRING)", ADMIN1);

    // user with ALL on DB should be able to add partion using Pig/HCatStore
    PigServer pigServer = context.getPigServer(USER1_1, ExecType.LOCAL);
    execPigLatin(USER1_1, pigServer, "A = load '" + dataFile.getPath()
        + "' as (id:int);");
    execPigLatin(USER1_1, pigServer, "store A into '" + dbName + "." + tabName1
        + "' using " + HCatStorer.class.getName() + " ('part_col=part1');");
    HiveMetaStoreClient client = context.getMetaStoreClient(ADMIN1);
    assertEquals(1, client.listPartitionNames(dbName, tabName1, (short) 10)
        .size());

    // user without select on DB should NOT be able to add partition with Pig/HCatStore
    pigServer = context.getPigServer(USER2_1, ExecType.LOCAL);
    execPigLatin(USER2_1, pigServer, "A = load '" + dataFile.getPath()
        + "' as (id:int);");
    // This action won't be successful because of no permission, but there is no exception will
    // be thrown in this thread. The detail exception can be found in
    // sentry-tests/sentry-tests-hive/target/surefire-reports/org.apache.sentry.tests.e2e.metastore.TestMetaStoreWithPigHCat-output.txt.
    execPigLatin(USER2_1, pigServer, "store A into '" + dbName + "." + tabName2 + "' using "
        + HCatStorer.class.getName() + " ('part_col=part2');");
    // The previous action is failed, and there will be no data.
    assertEquals(0, client.listPartitionNames(dbName, tabName2, (short) 10).size());
    client.close();
  }

}
