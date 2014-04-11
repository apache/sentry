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
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestViewPrivileges extends AbstractTestWithHiveServer {
  protected static final String SERVER_HOST = "localhost";

  private Context context;
  private Map<String, String> properties;
  private PolicyFile policyFile;

  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  @Before
  public void setUp() throws Exception {
    properties = Maps.newHashMap();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
    context = createContext(properties);
  }

  @After
  public void tearDown() throws Exception {
    if(context != null) {
      context.close();
    }
  }

  @Test
  public void testPartitionedViewOnJoin() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    String viewName = "view1";
    String db = "db1";
    String tabNames[] = { "tab1", "tab2" } ;
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
    for (String tabName : tabNames) {
      stmt.execute("create table " + tabName + " (id int) partitioned by (part string)");
      stmt.execute("load data local inpath '" + dataFile + "' into table " + tabName + " PARTITION (part=\"a\")");
      stmt.execute("load data local inpath '" + dataFile + "' into table " + tabName + " PARTITION (part=\"b\")");
      ResultSet res = stmt.executeQuery("select count(*) from " + tabName);
      org.junit.Assert.assertThat(res, notNullValue());
      while(res.next()) {
        Assume.assumeTrue(res.getInt(1) == new Integer(1000));
      }
    }
    stmt.execute("create view " + viewName + " as select t1.id from " +
        tabNames[0] + " t1 JOIN " + tabNames[1] + " t2 on (t1.id = t2.id) where t1.id<100");
    ResultSet res = stmt.executeQuery("select count(*) from " + viewName);
    org.junit.Assert.assertThat(res, notNullValue());
    int rowsInView = 0;
    while(res.next()) {
      System.out.println("Admin: Rows in view: " + res.getInt(1));
      rowsInView = res.getInt(1);
    }

    Connection userConn = context.createConnection(USER1_1);
    Statement userStmt = context.createStatement(userConn);
    userStmt.execute("use " + db);
    res = userStmt.executeQuery("select count(*) from " + viewName);
    org.junit.Assert.assertThat(res, notNullValue());
    while(res.next()) {
      System.out.println("User1_1: Rows in view: " + res.getInt(1));
      org.junit.Assert.assertThat(res.getInt(1), is(rowsInView));
    }
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
      System.out.println("Admin: Rows in view: " + res.getInt(1));
      rowsInView = res.getInt(1);
    }
    Connection userConn = context.createConnection(USER1_1);
    Statement userStmt = context.createStatement(userConn);
    userStmt.execute("use " + db);
    res = userStmt.executeQuery("select count(*) from " + viewName);
    org.junit.Assert.assertThat(res, notNullValue());
    while(res.next()) {
      System.out.println("User1_1: Rows in view: " + res.getInt(1));
      org.junit.Assert.assertThat(res.getInt(1), is(rowsInView));
    }
  }
}
