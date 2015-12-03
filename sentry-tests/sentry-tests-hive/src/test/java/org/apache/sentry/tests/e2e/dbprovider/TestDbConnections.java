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
package org.apache.sentry.tests.e2e.dbprovider;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDbConnections extends AbstractTestWithStaticConfiguration {
  private PolicyFile policyFile;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  /**
   * Currently the hive binding opens a new server connection for each
   * statement. This test verifies that the client connection is closed properly
   * at the end. Test Queries, DDLs, Auth DDLs and metadata filtering (eg show
   * tables/databases)
   * @throws Exception
   */
  @Test
  public void testClientConnections() throws Exception {
    String roleName = "connectionTest";
    long preConnectionClientId;
    // Connect through user admin1.
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    assertEquals(0, getSentrySrv().getNumActiveClients());

    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify that client connection is closed after DDLs.
    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.execute("CREATE TABLE t1 (c1 string)");
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify that client connection is closed after queries.
    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.execute("SELECT * FROM t1");
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify client invocation via metastore filter.
    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.executeQuery("show tables");
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify that client connection is closed after drop table.
    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.execute("DROP TABLE t1");
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify that client connection is closed after auth DDL.
    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.execute("CREATE ROLE " + roleName);
    assertEquals(0, getSentrySrv().getNumActiveClients());
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());

    context.assertSentryException(statement, "CREATE ROLE " + roleName,
        SentryAlreadyExistsException.class.getSimpleName());
    assertEquals(0, getSentrySrv().getNumActiveClients());
    statement.execute("DROP ROLE " + roleName);
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify client invocation via metastore filter
    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.executeQuery("show tables");
    // There are no tables, so auth check does not happen
    // sentry will create connection to get privileges for cache
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    statement.close();
    connection.close();
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Connect through user user1_1.
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify that client connection is closed after statement auth error.
    preConnectionClientId = getSentrySrv().getTotalClients();
    context.assertAuthzException(statement, "USE DB_1");
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify that client connection is closed after auth DDL error.
    preConnectionClientId = getSentrySrv().getTotalClients();
    context.assertSentryException(statement, "CREATE ROLE " + roleName,
        SentryAccessDeniedException.class.getSimpleName());
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    // Verify that client invocation via metastore filter.
    preConnectionClientId = getSentrySrv().getTotalClients();
    statement.executeQuery("show databases");
    assertTrue(preConnectionClientId < getSentrySrv().getTotalClients());
    assertEquals(0, getSentrySrv().getNumActiveClients());

    statement.close();
    connection.close();
    assertEquals(0, getSentrySrv().getNumActiveClients());
  }

}
