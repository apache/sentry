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

package org.apache.sentry.tests.e2e;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.sentry.provider.file.SimplePolicyEngine;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Test privileges per database policy files
 */
public class TestPerDBConfiguration extends AbstractTestWithStaticLocalFS {
  private static final String MULTI_TYPE_DATA_FILE_NAME = "emp.dat";
  private static final String DB2_POLICY_FILE = "db2-policy-file.ini";

  private Context context;

  @After
  public void teardown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void testPerDB() throws Exception {
    context = createContext();
    File policyFile = context.getPolicyFile();
    File db2PolicyFile = new File(policyFile.getParent(), DB2_POLICY_FILE);
    File dataDir = context.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, context.deletePolicyFile());
    assertTrue("Could not delete " + db2PolicyFile,!db2PolicyFile.exists() || db2PolicyFile.delete());

    String[] policyFileContents = {
        // groups : role -> group
        "[groups]",
        "admin = all_server",
        "user_group1 = select_tbl1",
        "user_group2 = select_tbl2",
        // roles: privileges -> role
        "[roles]",
        "all_server = server=server1",
        "select_tbl1 = server=server1->db=db1->table=tbl1->action=select",
        // users: users -> groups
        "[users]",
        "hive = admin",
        "user1 = user_group1",
        "user2 = user_group2",
        "[databases]",
        "db2 = " + db2PolicyFile.getPath(),
    };
    context.makeNewPolicy(policyFileContents);

    String[] db2PolicyFileContents = {
        "[groups]",
        "user_group2 = select_tbl2",
        "[roles]",
        "select_tbl2 = server=server1->db=db2->table=tbl2->action=select"
    };
    Files.write(Joiner.on("\n").join(db2PolicyFileContents), db2PolicyFile, Charsets.UTF_8);

    // setup db objects needed by the test
    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS db2 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");
    statement.execute("CREATE TABLE tbl1(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl1");
    statement.execute("DROP DATABASE IF EXISTS db2 CASCADE");
    statement.execute("CREATE DATABASE db2");
    statement.execute("USE db2");
    statement.execute("CREATE TABLE tbl2(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl2");
    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection("user1", "password");
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl1");

    // user1 cannot query db2.tbl2
    context.assertAuthzException(statement, "USE db2");
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM db2.tbl2");
    statement.close();
    connection.close();

    // test per-db file for db2

    connection = context.createConnection("user2", "password");
    statement = context.createStatement(connection);
    statement.execute("USE db2");
    // test user2 can execute query on tbl2
    verifyCount(statement, "SELECT COUNT(*) FROM tbl2");

    // user2 cannot query db1.tbl1
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM db1.tbl1");
    context.assertAuthzException(statement, "USE db1");

    statement.close();
    connection.close();

    //test cleanup
    connection = context.createConnection("hive", "hive");
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE db1 CASCADE");
    statement.execute("DROP DATABASE db2 CASCADE");
    statement.close();
    connection.close();
  }

  /**
   * Multiple DB files with some containing badly formatted rules
   * The privileges should work for good files
   * No access for bad formatted ones
   * @throws Exception
   */
  @Test
  public void testMultiPerDBwithErrors() throws Exception {
    String DB3_POLICY_FILE = "db3-policy-file.ini";
    String DB4_POLICY_FILE = "db4-policy-file.ini";

    context = createContext();
    File policyFile = context.getPolicyFile();
    File db2PolicyFile = new File(policyFile.getParent(), DB2_POLICY_FILE);
    File db3PolicyFile = new File(policyFile.getParent(), DB3_POLICY_FILE);
    File db4PolicyFile = new File(policyFile.getParent(), DB4_POLICY_FILE);
    File dataDir = context.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, context.deletePolicyFile());
    assertTrue("Could not delete " + db2PolicyFile,!db2PolicyFile.exists() || db2PolicyFile.delete());

    String[] policyFileContents = {
        // groups : role -> group
        "[groups]",
        "admin = all_server",
        "user_group1 = select_tbl1",
        "user_group2 = select_tbl2",
        // roles: privileges -> role
        "[roles]",
        "all_server = server=server1",
        "select_tbl1 = server=server1->db=db1->table=tbl1->action=select",
        // users: users -> groups
        "[users]",
        "hive = admin",
        "user1 = user_group1",
        "user2 = user_group2",
        "user3 = user_group3",
        "user4 = user_group4",
        "[databases]",
        "db2 = " + db2PolicyFile.getPath(),
        "db3 = " + db3PolicyFile.getPath(),
        "db4 = " + db4PolicyFile.getPath(),
    };
    context.makeNewPolicy(policyFileContents);

    String[] db2PolicyFileContents = {
        "[groups]",
        "user_group2 = select_tbl2",
        "[roles]",
        "select_tbl2 = server=server1->db=db2->table=tbl2->action=select"
    };
    String[] db3PolicyFileContents = {
        "[groups]",
        "user_group3 = select_tbl3_BAD",
        "[roles]",
        "select_tbl3_BAD = server=server1->db=db3------>table->action=select"
    };
    String[] db4PolicyFileContents = {
        "[groups]",
        "user_group4 = select_tbl4",
        "[roles]",
        "select_tbl4 = server=server1->db=db4->table=tbl4->action=select"
    };

    Files.write(Joiner.on("\n").join(db2PolicyFileContents), db2PolicyFile, Charsets.UTF_8);
    Files.write(Joiner.on("\n").join(db3PolicyFileContents), db3PolicyFile, Charsets.UTF_8);
    Files.write(Joiner.on("\n").join(db4PolicyFileContents), db4PolicyFile, Charsets.UTF_8);

    // setup db objects needed by the test
    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");
    statement.execute("CREATE TABLE tbl1(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl1");

    statement.execute("DROP DATABASE IF EXISTS db2 CASCADE");
    statement.execute("CREATE DATABASE db2");
    statement.execute("USE db2");
    statement.execute("CREATE TABLE tbl2(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl2");

    statement.execute("DROP DATABASE IF EXISTS db3 CASCADE");
    statement.execute("CREATE DATABASE db3");
    statement.execute("USE db3");
    statement.execute("CREATE TABLE tbl3(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl3");

    statement.execute("DROP DATABASE IF EXISTS db4 CASCADE");
    statement.execute("CREATE DATABASE db4");
    statement.execute("USE db4");
    statement.execute("CREATE TABLE tbl4(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl4");

    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection("user1", "password");
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl1");
    connection.close();

    connection = context.createConnection("user2", "password");
    statement = context.createStatement(connection);
    statement.execute("USE db2");
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl2");
    connection.close();

    // verify no access to db3 due to badly formatted rule in db3 policy file
    connection = context.createConnection("user3", "password");
    statement = context.createStatement(connection);
    context.assertAuthzException(statement, "USE db3");
    // test user1 can execute query on tbl1
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM db3.tbl3");
    connection.close();

    connection = context.createConnection("user4", "password");
    statement = context.createStatement(connection);
    statement.execute("USE db4");
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl4");
    connection.close();

    //test cleanup
    connection = context.createConnection("hive", "hive");
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE db1 CASCADE");
    statement.execute("DROP DATABASE db2 CASCADE");
    statement.execute("DROP DATABASE db3 CASCADE");
    statement.execute("DROP DATABASE db4 CASCADE");
    statement.close();
    connection.close();
  }

  @Test
  public void testPerDBPolicyFileWithURI() throws Exception {
    context = createContext();
    File policyFile = context.getPolicyFile();
    File db2PolicyFile = new File(policyFile.getParent(), DB2_POLICY_FILE);
    File dataDir = context.getDataDir();
    //copy data file to test dir
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();
    //delete existing policy file; create new policy file
    assertTrue("Could not delete " + policyFile, context.deletePolicyFile());
    assertTrue("Could not delete " + db2PolicyFile,!db2PolicyFile.exists() || db2PolicyFile.delete());

    String[] policyFileContents = {
        // groups : role -> group
        "[groups]",
        "admin = all_server",
        "user_group1 = select_tbl1",
        "user_group2 = select_tbl2",
        // roles: privileges -> role
        "[roles]",
        "all_server = server=server1",
        "select_tbl1 = server=server1->db=db1->table=tbl1->action=select",
        // users: users -> groups
        "[users]",
        "hive = admin",
        "user1 = user_group1",
        "user2 = user_group2",
        "[databases]",
        "db2 = " + db2PolicyFile.getPath(),
    };
    context.makeNewPolicy(policyFileContents);

    String[] db2PolicyFileContents = {
        "[groups]",
        "user_group2 = select_tbl2, data_read, insert_tbl2",
        "[roles]",
        "select_tbl2 = server=server1->db=db2->table=tbl2->action=select",
        "insert_tbl2 = server=server1->db=db2->table=tbl2->action=insert",
        "data_read = server=server1->URI=file://" + dataFile
    };
    Files.write(Joiner.on("\n").join(db2PolicyFileContents), db2PolicyFile, Charsets.UTF_8);
    // ugly hack: needs to go away once this becomes a config property. Note that this property
    // will not be set with external HS and this test will fail. Hope is this fix will go away
    // by then.
    System.setProperty(SimplePolicyEngine.ACCESS_ALLOW_URI_PER_DB_POLICYFILE, "true");
    // setup db objects needed by the test
    Connection connection = context.createConnection("hive", "hive");
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS db1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS db2 CASCADE");
    statement.execute("CREATE DATABASE db1");
    statement.execute("USE db1");
    statement.execute("CREATE TABLE tbl1(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl1");
    statement.execute("DROP DATABASE IF EXISTS db2 CASCADE");
    statement.execute("CREATE DATABASE db2");
    statement.execute("USE db2");
    statement.execute("CREATE TABLE tbl2(B INT, A STRING) " +
                      " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl2");
    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection("user1", "password");
    statement = context.createStatement(connection);
    statement.execute("USE db1");
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl1");

    // user1 cannot query db2.tbl2
    context.assertAuthzException(statement, "USE db2");
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM db2.tbl2");
    statement.close();
    connection.close();

    // test per-db file for db2
    connection = context.createConnection("user2", "password");
    statement = context.createStatement(connection);
    statement.execute("USE db2");
    // test user2 can execute query on tbl2
    verifyCount(statement, "SELECT COUNT(*) FROM tbl2");

    // verify user2 can execute LOAD
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl2");

    // user2 cannot query db1.tbl1
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM db1.tbl1");
    context.assertAuthzException(statement, "USE db1");

    statement.close();
    connection.close();

    //test cleanup
    connection = context.createConnection("hive", "hive");
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE db1 CASCADE");
    statement.execute("DROP DATABASE db2 CASCADE");
    statement.close();
    connection.close();
    System.setProperty(SimplePolicyEngine.ACCESS_ALLOW_URI_PER_DB_POLICYFILE, "false");
  }

  private void verifyCount(Statement statement, String query) throws SQLException {
    ResultSet resultSet = statement.executeQuery(query);
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 12);
  }
}
