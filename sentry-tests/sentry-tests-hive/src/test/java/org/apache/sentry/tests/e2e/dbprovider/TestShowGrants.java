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

package org.apache.sentry.tests.e2e.dbprovider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;

import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.DummySentryOnFailureHook;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestShowGrants extends AbstractTestWithStaticConfiguration {

  private static int SHOW_GRANT_DB_POSITION = 1;
  private static int SHOW_GRANT_TABLE_POSITION = 2;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    String hiveServer2Type = System
        .getProperty(HiveServerFactory.HIVESERVER2_TYPE);
    if ((hiveServer2Type == null)
        || HiveServerFactory.isInternalServer(HiveServerFactory.HiveServer2Type
        .valueOf(hiveServer2Type.trim()))) {
      System.setProperty(
          HiveAuthzConf.AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(),
          DummySentryOnFailureHook.class.getName());
    }
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    DummySentryOnFailureHook.invoked = false;
    super.setupAdmin();
    super.setup();
    setupTestCase();
  }

  private void setupTestCase() throws Exception {
    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS db_1 CASCADE");
    statement.execute("CREATE DATABASE db_1");
    statement.execute("CREATE DATABASE db_2");
    statement.execute("USE db_1");
    statement.execute("CREATE TABLE foo (id int)");
    statement.execute("USE db_2");
    statement.execute("CREATE TABLE bar (id int)");

    statement.execute("CREATE ROLE role1");
    statement.execute("CREATE ROLE role2");
    statement.execute("CREATE ROLE role3");
    statement.execute("CREATE ROLE role4");

    statement.execute("GRANT ROLE role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE role1 TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE role2 TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE role3 TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE role4 TO GROUP " + USERGROUP3);
    statement.execute("GRANT ROLE role4 TO GROUP " + USERGROUP4);

    //Grant on server to role 1
    statement.execute("GRANT ALL ON SERVER server1 TO ROLE role1");

    // Grant select on database db_1 to role2
    // Grant insert on database db_1 to role3
    // Grant all on database db_2 to role3
    statement.execute("GRANT SELECT ON DATABASE db_1 TO ROLE role2");
    statement.execute("GRANT INSERT ON DATABASE db_1 TO ROLE role3");
    statement.execute("GRANT ALL ON DATABASE db_2 TO ROLE role4");

    // Grant select on table db_2.bar to role role2
    // Grant insert on table db_2.bar to role role3
    // Grant all on table db_1.foo to role role4
    statement.execute("GRANT ALL ON TABLE db_1.foo TO ROLE role4");
    statement.execute("GRANT ALL ON TABLE db_1.foo TO ROLE role3");
    statement.execute("GRANT SELECT ON TABLE db_2.bar TO ROLE role2");
    statement.execute("GRANT INSERT ON TABLE db_2.bar TO ROLE role3");

    //Grant all on uri '/a/b/c' to role1
    //Grant all on uri '/x/y/z' to role2, role3, role4
    statement.execute("GRANT ALL ON URI \"file:///a/b/c\" TO ROLE role1");
    statement.execute("GRANT ALL ON URI \"file:///x/y/z\" TO ROLE role3");
    statement.execute("GRANT ALL ON URI \"file:///x/y/z\" TO ROLE role4");

    statement.close();
    connection.close();
  }

  /**
   * Test show grants on objects for admin users
   * @throws Exception
   */
  @Test
  public void testShowGrantsOnObjectsForAdmin() throws Exception {
    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    ResultSet resultSet = null;
    resultSet = statement.executeQuery("SHOW GRANT ON SERVER server1");
    assertResultSize(resultSet, 2);//Role1 + Admin
    Assert.assertEquals("*", resultSet.getString(SHOW_GRANT_DB_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON DATABASE db_1");
    assertResultSize(resultSet, 2);
    Assert.assertEquals("db_1", resultSet.getString(SHOW_GRANT_DB_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON DATABASE db_2");
    assertResultSize(resultSet, 1);
    Assert.assertEquals("db_2", resultSet.getString(SHOW_GRANT_DB_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON TABLE db_1.foo");
    assertResultSize(resultSet, 2);
    Assert.assertEquals("foo", resultSet.getString(SHOW_GRANT_TABLE_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON TABLE db_2.bar");
    assertResultSize(resultSet, 2);
    Assert.assertEquals("bar", resultSet.getString(SHOW_GRANT_TABLE_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON URI \"file:///a/b/c\"");
    assertResultSize(resultSet, 1);
    Assert.assertEquals("file:///a/b/c", resultSet.getString(SHOW_GRANT_DB_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON URI \"file:///x/y/z\"");
    assertResultSize(resultSet, 2);
    Assert.assertEquals("file:///x/y/z", resultSet.getString(SHOW_GRANT_DB_POSITION));

    statement.close();
    connection.close();
  }

  /**
   * Test show grants on objects for non-admin users
   * @throws Exception
   */
  @Test
  public void testShowGrantsOnObjectsForNonAdmins() throws Exception {
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);

    ResultSet resultSet = null;

    resultSet = statement.executeQuery("SHOW GRANT ON DATABASE db_1");
    assertResultSize(resultSet, 0);

    resultSet = statement.executeQuery("SHOW GRANT ON DATABASE db_2");
    assertResultSize(resultSet, 0);

    statement.close();
    connection.close();

    // setup db objects needed by the test
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);

    resultSet = statement.executeQuery("SHOW GRANT ON DATABASE db_1");
    assertResultSize(resultSet, 2);
    Assert.assertEquals("db_1", resultSet.getString(SHOW_GRANT_DB_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON DATABASE db_2");
    assertResultSize(resultSet, 0);

    resultSet = statement.executeQuery("SHOW GRANT ON TABLE db_1.foo");
    assertResultSize(resultSet, 2);

    statement.close();
    connection.close();

    // setup db objects needed by the test
    connection = context.createConnection(USER4_1);
    statement = context.createStatement(connection);

    resultSet = statement.executeQuery("SHOW GRANT ON TABLE db_1.foo");
    assertResultSize(resultSet, 2);
    Assert.assertEquals("foo", resultSet.getString(SHOW_GRANT_TABLE_POSITION));

    resultSet = statement.executeQuery("SHOW GRANT ON TABLE db_2.bar");
    assertResultSize(resultSet, 0);

    resultSet = statement.executeQuery("SHOW GRANT ON URI \"file:///a/b/c\"");
    assertResultSize(resultSet, 0);

    resultSet = statement.executeQuery("SHOW GRANT ON URI \"file:///x/y/z\"");
    assertResultSize(resultSet, 2);
    Assert.assertEquals("file:///x/y/z", resultSet.getString(SHOW_GRANT_DB_POSITION));

    statement.close();
    connection.close();
  }

  private void assertResultSize(ResultSet resultSet, int expected) throws SQLException{
    int count = 0;
    while(resultSet.next()) {
      count++;
    }
    Assert.assertEquals(count, expected);
  }

}
