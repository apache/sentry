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

package org.apache.sentry.tests.e2e.hive;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestPerDatabasePolicyFile extends AbstractTestWithStaticLocalFS {
  private static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private Context context;
  private PolicyFile policyFile;
  private File globalPolicyFile;
  private File dataDir;
  private File dataFile;

  @Before
  public void setup() throws Exception {
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
    context = createContext();
    globalPolicyFile = context.getPolicyFile();
    dataDir = context.getDataDir();
    assertTrue("Could not delete " + globalPolicyFile, context.deletePolicyFile());
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
  }

  @After
  public void teardown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  private void createSampleDbTable(Statement statement, String db, String table)
      throws Exception {
    statement.execute("DROP DATABASE IF EXISTS " + db + " CASCADE");
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

    policyFile
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1, "password");
    Statement statement = context.createStatement(connection);
    createSampleDbTable(statement, "db1", "tbl1");
    createSampleDbTable(statement, "db2", "tbl1");
    statement.close();
    connection.close();

    File specificPolicyFileFile = new File(context.getBaseDir(), "db2-policy.ini");

    PolicyFile specificPolicyFile = new PolicyFile()
    .addPermissionsToRole("db1_role", grant)
    .addRolesToGroup("group1", "db1_role");
    specificPolicyFile.write(specificPolicyFileFile);

    policyFile.addDatabase("db2", specificPolicyFileFile.getPath());
    policyFile.write(context.getPolicyFile());



    // test execution
    connection = context.createConnection(USER1_1, "password");
    statement = context.createStatement(connection);
    // test user can query table
    context.assertAuthzException(statement, "USE db1");
    context.assertAuthzException(statement, "SELECT COUNT(a) FROM db1.tbl1");
  }
}