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

import com.google.common.collect.Maps;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.utils.PolicyFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.CodeSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class TestCustomSerdePrivileges extends AbstractTestWithHiveServer {
  private static Context context;
  private static Map<String, String> properties;
  private PolicyFile policyFile;

  @BeforeClass
  public static void setUp() throws Exception {
    properties = Maps.newHashMap();

    // Start the Hive Server without buildin Serde, such as
    // "org.apache.hadoop.hive.serde2.OpenCSVSerde". Instead,
    // used a bogus class name for testing.
    properties.put(HiveAuthzConf.HIVE_SENTRY_SERDE_WHITELIST, "org.example.com");
    properties.put(HiveAuthzConf.HIVE_SENTRY_SERDE_URI_PRIVILIEGES_ENABLED, "true");
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

  /**
   * User with db level access and Uri privileges on the Serde Jar should be able
   * to create tables with Serde.
   * User with db level access but without Uri privileges on the Serde Jar will fail
   * on creating tables with Serde.
   */
  @Test
  public void testSerdePrivilegesWithoutBuildinJar() throws Exception {
    String db = "db1";
    String tableName1 = "tab1";

    String serdeClassName = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    CodeSource serdeSrc = Class.forName(serdeClassName).getProtectionDomain().getCodeSource();
    String serdeLocation = serdeSrc.getLocation().getPath();

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all")
        .addRolesToGroup(USERGROUP2, "db1_all", "SERDE_JAR")
        .addPermissionsToRole("db1_all", "server=server1->db=" + db)
        .addPermissionsToRole("db1_tab1", "server=server1->db=" + db + "->table=" + tableName1)
        .addPermissionsToRole("SERDE_JAR", "server=server1->uri=file://" + serdeLocation)
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(context.getPolicyFile());

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + db + " CASCADE");
    statement.execute("CREATE DATABASE " + db);
    context.close();

    // User1 does not have the URI privileges to use the Serde Jar.
    // The table creation will fail.
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + db);
    try {
      statement.execute("create table " + db + "." + tableName1 + " (a string, b string) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " + " STORED AS TEXTFILE");
      Assert.fail("Expect create table with Serde to fail");
    } catch (SQLException e) {
        context.verifyAuthzException(e);
    }
    context.close();

    // User2 has the URI privileges to use the Serde Jar.
    // The table creation will succeed.
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + db);
    statement.execute("create table " + db + "." + tableName1 + " (a string, b string) ROW FORMAT" +
        " SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " + " STORED AS TEXTFILE");
    context.close();
  }
}
