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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Assert;

import org.apache.sentry.tests.e2e.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUriPermissions extends AbstractTestWithStaticLocalFS {
  private Context context;
  private static final String dataFile = "/kv1.dat";
  private String dataFilePath = this.getClass().getResource(dataFile).getFile();

  @Before
  public void setup() throws Exception {
    context = createContext();
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  // test load data into table
  @Test
  public void testLoadPrivileges() throws Exception {
    String dbName = "db1";
    String tabName = "tab1";
    Connection userConn = null;
    Statement userStmt = null;

    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = db1_read, db1_write, data_read",
        "user_group2  = db1_write",
        "[roles]",
        "db1_write = server=server1->db=" + dbName + "->table=" + tabName + "->action=INSERT",
        "db1_read = server=server1->db=" + dbName + "->table=" + tabName + "->action=SELECT",
        // role below has duplicate privilege for ACCESS-178
        "data_read = server=server1->URI=file://" + dataFilePath + ", server=server1->URI=file://" + dataFilePath,
        "admin_role = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        "admin = admin_group"
        };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);
    adminStmt.execute("CREATE TABLE " + tabName + "(id int)");
    context.close();

    // positive test, user1 has access to file being loaded
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("load data local inpath '" + dataFilePath +
        "' into table " + tabName);
    userStmt.execute("select * from " + tabName + " limit 1");
    ResultSet res = userStmt.getResultSet();
    Assert.assertTrue("Table should have data after load", res.next());
    res.close();
    context.close();

    // Negative test, user2 doesn't have access to the file being loaded
    userConn = context.createConnection("user2", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    context.assertAuthzException(userStmt, "load data local inpath '" + dataFilePath +
        "' into table " + tabName);
    userStmt.close();
    userConn.close();
  }

  // Test alter partition location
  @Test
  public void testAlterPartitionLocationPrivileges() throws Exception {
    String dbName = "db1";
    String tabName = "tab1";
    String newPartitionDir = "foo";
    String tabDir = "file://" + hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR) +
      "/" + tabName + "/" + newPartitionDir;
    Connection userConn = null;
    Statement userStmt = null;

    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = db1_all, data_read",
        "user_group2  = db1_all",
        "user_group3  = db1_tab1_all, data_read",
        "[roles]",
        "db1_all = server=server1->db=" + dbName,
        "db1_tab1_all = server=server1->db=" + dbName + "->table=" + tabName,
        "data_read = server=server1->URI=" + tabDir,
        "admin_role = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        "user3 = user_group3",
        "admin = admin_group"
        };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);
    adminStmt.execute("CREATE TABLE " + tabName + " (id int) PARTITIONED BY (dt string)");
    adminCon.close();

    // positive test: user1 has privilege to alter table add partition but not set location
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("ALTER TABLE " + tabName + " ADD PARTITION (dt = '21-Dec-2012') " +
            " LOCATION '" + tabDir + "'");
    // negative test user1 cannot alter partition location
    context.assertAuthzException(userStmt,
        "ALTER TABLE " + tabName + " PARTITION (dt = '21-Dec-2012') " + " SET LOCATION '" + tabDir + "'");
    userConn.close();

    // negative test: user2 doesn't have privilege to alter table add partition
    userConn = context.createConnection("user2", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    context.assertAuthzException(userStmt,
        "ALTER TABLE " + tabName + " ADD PARTITION (dt = '22-Dec-2012') " +
          " LOCATION '" + tabDir + "/foo'");
    // positive test, user2 can alter managed partitions
    userStmt.execute("ALTER TABLE " + tabName + " ADD PARTITION (dt = '22-Dec-2012')");
    userStmt.execute("ALTER TABLE " + tabName + " DROP PARTITION (dt = '22-Dec-2012')");
    userConn.close();

    // negative test: user3 doesn't have privilege to add/drop partitions
    userConn = context.createConnection("user3", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    context.assertAuthzException(userStmt,
        "ALTER TABLE " + tabName + " ADD PARTITION (dt = '22-Dec-2012') " +
          " LOCATION '" + tabDir + "/foo'");
    context.assertAuthzException(userStmt,
        "ALTER TABLE " + tabName + " DROP PARTITION (dt = '21-Dec-2012')");
    userConn.close();

    // positive test: user1 has privilege to alter drop partition
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("ALTER TABLE " + tabName + " DROP PARTITION (dt = '21-Dec-2012')");
    userStmt.close();
    userConn.close();
  }

  // test alter table set location
  @Test
  public void testAlterTableLocationPrivileges() throws Exception {
    String dbName = "db1";
    String tabName = "tab1";
    String tabDir = "file://" + hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR) + "/" + tabName;
    Connection userConn = null;
    Statement userStmt = null;

    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = server1_all",
        "user_group2  = db1_all, data_read",
        "[roles]",
        "db1_all = server=server1->db=" + dbName,
        "data_read = server=server1->URI=" + tabDir,
        "admin_role = server=server1",
        "server1_all = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        "admin = admin_group"
        };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.execute("use " + dbName);
    adminStmt.execute("CREATE TABLE " + tabName + " (id int)  PARTITIONED BY (dt string)");
    adminCon.close();

    // negative test: user2 doesn't have privilege to alter table set partition
    userConn = context.createConnection("user2", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    context.assertAuthzException(userStmt,
        "ALTER TABLE " + tabName + " SET LOCATION '" + tabDir +  "'");
    userConn.close();

    // positive test: user1 has privilege to alter table set partition
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("ALTER TABLE " + tabName + " SET LOCATION '" + tabDir + "'");
    userConn.close();
  }

  // Test external table
  @Test
  public void testExternalTablePrivileges() throws Exception {
    String dbName = "db1";
    Connection userConn = null;
    Statement userStmt = null;
    String tableDir = "file://" + context.getDataDir();
    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = db1_all, data_read",
        "user_group2  = db1_all",
        "[roles]",
        "db1_all = server=server1->db=" + dbName,
        "data_read = server=server1->URI=" + tableDir,
        "admin_role = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        "admin = admin_group"
        };
    context.makeNewPolicy(testPolicies);

    // create dbs
    Connection adminCon = context.createConnection("admin", "foo");
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + dbName);
    adminStmt.close();
    adminCon.close();

    // negative test: user2 doesn't have privilege to create external table in given path
    userConn = context.createConnection("user2", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    context.assertAuthzException(userStmt,
        "CREATE EXTERNAL TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    context.assertAuthzException(userStmt, "CREATE TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    userStmt.close();
    userConn.close();

    // positive test: user1 has privilege to create external table in given path
    userConn = context.createConnection("user1", "foo");
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + dbName);
    userStmt.execute("CREATE EXTERNAL TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    userStmt.execute("DROP TABLE extab1");
    userStmt.execute("CREATE TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    userStmt.close();
    userConn.close();
  }

}
