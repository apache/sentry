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
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.Statement;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.sentry.tests.e2e.hdfs.TestHDFSIntegrationBase;
import org.apache.sentry.tests.e2e.hive.SlowE2ETest;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class covers scenario where HMSFollower takes a snapshot initially when sentry server comes up
 * and also subsequently when HMS event information is cleaned up and HMSFollower detects that it is out of sync and can
 * not recover by fetching delta notifications.
 */
@Ignore
@SlowE2ETest
public class TestSnapshotWithLongerHMSFollowerLongerInterval extends TestHDFSIntegrationBase {

  private static final Logger LOGGER = LoggerFactory
          .getLogger(TestSnapshotWithLongerHMSFollowerLongerInterval.class);

  protected static final String DB1 = "db_1",
          DB2 = "db_2";

  private Connection connection;
  private Statement statement;

  @BeforeClass
  public static void setup() throws Exception {
    // Reduces the TTL of the HMS event data so that it is cleaned up faster.
    shorterMetaStoreEventDbTtl = true;
    // Increases the interval between the fetches the HMSFollower does.
    longerHMSFollowerInterval = true;
    TestHDFSIntegrationBase.setup();
  }

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
    Thread th = null;
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "all_db1", "all_tbl1", "all_tbl2"};
    do {
      //Sleep for a sec allowing HMSFollower to create a snapshot
      Thread.sleep(1000);
      latestSnapshotId = sentryServer.get(0).getCurrentAuthzPathsSnapshotID();
    } while (latestSnapshotId == 0);

    final AtomicBoolean keepRunning = new AtomicBoolean(true);
    th = new Thread() {
      @Override
      public void run() {
        try {
          int counter = 1;
          while (keepRunning.get()) {
            statement.execute("CREATE DATABASE " + "db"+counter);
            Thread.sleep(1000L);
            counter++;
          }
        } catch (Exception e) {
          LOGGER.info("Could not start Hive Server");
        }
      }
    };
    th.start();

    Thread.sleep(130000);
    assertEquals("Another snapshot is created, Snapshot ID: ", latestSnapshotId, sentryServer.get(0).getCurrentAuthzPathsSnapshotID());

    Thread.sleep(130000);
    long newSnapShot = sentryServer.get(0).getCurrentAuthzPathsSnapshotID();
    assertFalse("Another snapshot should have been created",
            (latestSnapshotId == newSnapShot));
  }
}
