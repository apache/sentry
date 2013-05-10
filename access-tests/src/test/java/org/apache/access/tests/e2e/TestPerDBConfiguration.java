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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

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
        "user_1 = user_group1",
        "user_2 = user_group2",
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
    connection = context.createConnection("user_1", "password");
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

    connection = context.createConnection("user_2", "password");
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