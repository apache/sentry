/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.dbprovider;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.sentry.tests.e2e.hdfs.TestHDFSIntegrationBase;
import org.junit.Before;
import org.junit.Test;

/**
 * This class covers basic scenario of snapshot creation and makes sure that
 * HMSFollower takes a full snapshot when sentry server comes up and not
 * subsequently.
 */
public class TestSnapshotCreation extends TestHDFSIntegrationBase {

  private final static String tableName1 = "tb_1";
  private final static String tableName2 = "tb_2";
  private final static String tableName3 = "tb_3";
  private final static String tableName4 = "tb_4";

  protected static final String DB1 = "db_1",
          DB2 = "db_2";

  private Connection connection;
  private Statement statement;

  @Before
  public void initialize() throws Exception {
    super.setUpTempDir();
    admin = "hive";
    connection = hiveServer2.createConnection(admin, admin);
    statement = connection.createStatement();
    statement.execute("create role admin_role");
    statement.execute("grant role admin_role to group hive");
    statement.execute("grant all on server server1 to role admin_role");
  }

  @Test
  public void BasicSanity() throws Exception {
    long latestSnapshotId = 0;
    //Sleep for a sec allowing HMSFollower to create a snapshot
    Thread.sleep(1000);
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "all_db1", "all_tbl1", "all_tbl2"};
    do {
      //Sleep for a sec allowing HMSFollower to create a snapshot
      Thread.sleep(1000);
      latestSnapshotId = sentryServer.get(0).getCurrentAuthzPathsSnapshotID();
    } while (latestSnapshotId == 0);

    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("create table " + DB1 + "." + tableName1
            + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + DB1 + "." + tableName2
            + " (under_col int comment 'the under column', value string)");

    Thread.sleep(5000);

    statement.execute("create table " + DB2 + "." + tableName3
            + " (under_col int comment 'the under column', value string)");
    statement.execute("create table " + DB2 + "." + tableName4
            + " (under_col int comment 'the under column', value string)");

    assertEquals("Another snapshot is created, Snapshot ID: ", latestSnapshotId, sentryServer.get(0).getCurrentAuthzPathsSnapshotID());
  }
}
