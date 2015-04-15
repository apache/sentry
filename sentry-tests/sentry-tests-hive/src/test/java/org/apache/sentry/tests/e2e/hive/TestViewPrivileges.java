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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import junit.framework.Assert;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.io.Resources;

public class TestViewPrivileges extends AbstractTestWithHiveServer {
  protected static final String SERVER_HOST = "localhost";

  private static Context context;
  private static Map<String, String> properties;
  private PolicyFile policyFile;

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  @BeforeClass
  public static void setUp() throws Exception {
    properties = Maps.newHashMap();
    context = createContext(properties);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(context != null) {
      context.close();
    }
  }
  
  @Before
  public void setupPolicyFile() throws Exception {
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  @Test
  public void testPartitioned() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    String viewName = "view1";
    String db = "db1";
    String tabName = "tab1";
    policyFile
        .addPermissionsToRole("view", "server=server1->db=" + db + "->table=" + viewName)
        .addRolesToGroup(USERGROUP1, "view")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(context.getPolicyFile());

    //admin creates a view
    Connection conn = context.createConnection(ADMIN1);
    Statement stmt = context.createStatement(conn);
    stmt.execute("DROP DATABASE IF EXISTS " + db + " CASCADE");
    stmt.execute("CREATE DATABASE " + db);

    stmt.execute("use " + db);
    stmt.execute("create table " + tabName + " (id int) partitioned by (part string)");
    stmt.execute("load data local inpath '" + dataFile + "' into table " + tabName + " PARTITION (part=\"a\")");
    stmt.execute("load data local inpath '" + dataFile + "' into table " + tabName + " PARTITION (part=\"b\")");
    ResultSet res = stmt.executeQuery("select count(*) from " + tabName);
    org.junit.Assert.assertThat(res, notNullValue());
    while(res.next()) {
      Assume.assumeTrue(res.getInt(1) == new Integer(1000));
    }
    stmt.execute("create view " + viewName + " as select * from " + tabName + " where id<100");
    res = stmt.executeQuery("select count(*) from " + viewName);
    org.junit.Assert.assertThat(res, notNullValue());
    int rowsInView = 0;
    while(res.next()) {
      rowsInView = res.getInt(1);
    }
    stmt.close();
    conn.close();

    Connection userConn = context.createConnection(USER1_1);
    Statement userStmt = context.createStatement(userConn);
    userStmt.execute("use " + db);
    res = userStmt.executeQuery("select count(*) from " + viewName);
    org.junit.Assert.assertThat(res, notNullValue());
    while(res.next()) {
      org.junit.Assert.assertThat(res.getInt(1), is(rowsInView));
    }
    userStmt.close();
    userConn.close();

    // user2 hasn't the privilege for the view
    userConn = context.createConnection(USER2_1);
    userStmt = context.createStatement(userConn);
    try {
      userStmt.executeQuery("select count(*) from " + viewName);
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      // ignore the exception
    }
    userStmt.close();
    userConn.close();
  }
}
