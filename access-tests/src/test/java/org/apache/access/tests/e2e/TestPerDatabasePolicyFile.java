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

package org.apache.access.tests.e2e;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestPerDatabasePolicyFile {

  private EndToEndTestContext testContext;
  private File globalPolicyFile;
  private File dataDir;
  private File dataFile;
  private static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  @Before
  public void setup() throws Exception {
    testContext = new EndToEndTestContext();
    globalPolicyFile = testContext.getPolicyFile();
    dataDir = testContext.getDataDir();
    assertTrue("Could not delete " + globalPolicyFile, testContext.deletePolicyFile());
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
  }

  @After
  public void teardown() throws Exception {
    if (testContext != null) {
      try {
        Connection connection = testContext.createConnection("hive", "hive");
        Statement statement = testContext.createStatement(connection);
        statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
        statement.execute("DROP DATABASE IF EXISTS db2 CASCADE");
      } finally {
        testContext.close();
      }
    }
  }

  @AfterClass
  public static void shutDown() throws IOException {
    EndToEndTestContext.shutdown();
  }

  private void append(String from, File to) throws IOException {
    Files.append(from + "\n", to, Charsets.UTF_8);
  }
  private void createSampleDbTable(Statement statement, String db, String table)
      throws Exception {
    statement.execute("CREATE DATABASE " + db);
    statement.execute("USE " + db);
    statement.execute("CREATE TABLE " + table + "(a STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE " + table);

  }

  /**
   * Ensure that db specific file cannot grant to other db
   */
  @Test
  public void testDbSpecificFileGrantsToOtherDb() throws Exception {
    doTestDbSpecificFileGrants("server=server1->db=db1");
  }
  /**
   * Ensure that db specific file cannot grant to all db
   */
  @Test
  public void testDbSpecificFileGrantsToAllDb() throws Exception {
    doTestDbSpecificFileGrants("server=server1");
  }
  /**
   * Ensure that db specific file cannot grant to all servers
   */
  @Test
  public void testDbSpecificFileGrantsToAllServers() throws Exception {
    doTestDbSpecificFileGrants("server=*");
  }
  /**
   * Ensure that db specific file cannot grant to all
   */
  @Test
  public void testDbSpecificFileGrantsToAll() throws Exception {
    doTestDbSpecificFileGrants("*");
  }

  public void doTestDbSpecificFileGrants(String grant) throws Exception {
    File db2SpecificPolicyFile = new File(testContext.getBaseDir(), "db2-policy.ini");
    append("[databases]", globalPolicyFile);
    append("db2 = " + db2SpecificPolicyFile.getPath(), globalPolicyFile);
    append("[users]", globalPolicyFile);
    append("user1 = group1", globalPolicyFile);
    append("hive = admin1", globalPolicyFile);
    append("[groups]", globalPolicyFile);
    append("admin1 = server1_all", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("server1_all = server=server1", globalPolicyFile);

    append("[groups]", db2SpecificPolicyFile);
    append("group1 = db1_all", db2SpecificPolicyFile);
    append("[roles]", db2SpecificPolicyFile);
    append("db1_all = " + grant, db2SpecificPolicyFile);
    // setup db objects needed by the test
    Connection connection = testContext.createConnection("hive", "hive");
    Statement statement = testContext.createStatement(connection);
    createSampleDbTable(statement, "db1", "tbl1");
    createSampleDbTable(statement, "db2", "tbl1");
    statement.close();
    connection.close();

    // test execution
    connection = testContext.createConnection("user1", "password");
    statement = testContext.createStatement(connection);
    statement.execute("USE db1");
    // test user can query table
    try {
      statement.executeQuery("SELECT COUNT(a) FROM tbl1");
      Assert.fail();
    } catch (SQLException ex) {
      testContext.verifyAuthzException(ex);
    }
  }
}