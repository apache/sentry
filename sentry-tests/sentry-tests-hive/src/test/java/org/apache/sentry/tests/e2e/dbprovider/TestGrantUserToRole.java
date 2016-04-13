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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class TestGrantUserToRole extends AbstractTestWithStaticConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestGrantUserToRole.class);

  private static String ROLENAME1 = "testGrantUserToRole_r1";
  private static String ROLENAME2 = "testGrantUserToRole_r2";
  private static String ROLENAME3 = "testGrantUserToRole_r3";

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
    prepareTestData();
  }

  private void prepareTestData() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE " + ROLENAME1);
    statement.execute("CREATE ROLE " + ROLENAME2);
    statement.execute("CREATE ROLE " + ROLENAME3);
    // grant role to groups and users as the following:
    statement.execute("GRANT ROLE " + ROLENAME1 + " TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE " + ROLENAME2 + " TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE " + ROLENAME3 + " TO USER " + USER2_1);
    statement.execute("GRANT ROLE " + ROLENAME2 + " TO USER " + USER3_1);
    statement.execute("GRANT ROLE " + ROLENAME2 + " TO USER " + USER4_1);
    statement.execute("GRANT ROLE " + ROLENAME3 + " TO USER " + USER4_1);
    statement.close();
    connection.close();
  }

  @Test
  public void testAddDeleteRolesForUser() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    Set<String> emptyRoleSet = Sets.newHashSet();
    // admin can get all roles for users
    // user1 get the role1 for group1
    ResultSet resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER1_1);
    verifyResultRoles(resultSet, emptyRoleSet);

    // user2 get the role1 for group1 and role2 for user2
    resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER2_1);
    verifyResultRoles(resultSet, Sets.newHashSet(ROLENAME3.toLowerCase()));

    // user3 get the role1 for group1 and role2 for group2
    resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER3_1);
    verifyResultRoles(resultSet, Sets.newHashSet(ROLENAME2.toLowerCase()));

    // user4 get the role2 for group2 and group3, role3 for user4
    resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER4_1);
    verifyResultRoles(resultSet, Sets.newHashSet(ROLENAME2.toLowerCase(), ROLENAME3.toLowerCase()));
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // user1 can show his own roles
    resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER1_1);
    verifyResultRoles(resultSet, emptyRoleSet);
    // test the command : show current roles
    resultSet = statement.executeQuery("SHOW CURRENT ROLES");
    verifyResultRoles(resultSet, Sets.newHashSet(ROLENAME1.toLowerCase()));

    try {
      // user1 can't show other's roles if he isn't an admin
      resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER2_1);
      fail("Can't show other's role if the user is not an admin.");
    } catch (Exception e) {
      // excepted exception
    }
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    // user2 can show his own roles
    resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER2_1);
    verifyResultRoles(resultSet, Sets.newHashSet(ROLENAME3.toLowerCase()));
    // test the command : show current roles
    resultSet = statement.executeQuery("SHOW CURRENT ROLES");
    verifyResultRoles(resultSet, Sets.newHashSet(ROLENAME2.toLowerCase(), ROLENAME3.toLowerCase()));
    statement.close();
    connection.close();

    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    // revoke the role from user
    statement.execute("REVOKE ROLE " + ROLENAME3 + " FROM USER " + USER2_1);
    statement.execute("REVOKE ROLE " + ROLENAME3 + " FROM USER " + USER4_1);

    resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER2_1);
    verifyResultRoles(resultSet, emptyRoleSet);

    resultSet = statement.executeQuery("SHOW ROLE GRANT USER " + USER4_1);
    verifyResultRoles(resultSet, Sets.newHashSet(ROLENAME2.toLowerCase()));
    statement.close();
    connection.close();
  }

  @Test
  public void testAuthorizationForUsersWithRoles() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("CREATE TABLE t2 (c1 string)");
    statement.execute("CREATE TABLE t3 (c1 string)");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE " + ROLENAME1);
    statement.execute("GRANT SELECT ON TABLE t2 TO ROLE " + ROLENAME2);
    statement.execute("GRANT SELECT ON TABLE t3 TO ROLE " + ROLENAME3);
    statement.close();
    connection.close();

    // user1 can access the t1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("select c1 from t1");
    try {
      statement.execute("select c1 from t2");
      fail("Can't access the table t2");
    } catch (Exception e) {
      // excepted exception
    }
    try {
      statement.execute("select c1 from t3");
      fail("Can't access the table t3");
    } catch (Exception e) {
      // excepted exception
    }
    statement.close();
    connection.close();

    // user2 can access the t2, t3
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("select c1 from t1");
      fail("Can't access the table t1");
    } catch (Exception e) {
      // excepted exception
    }
    statement.execute("select c1 from t2");
    statement.execute("select c1 from t3");
    statement.close();
    connection.close();

    // user3 can access the t2
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("select c1 from t1");
      fail("Can't access the table t1");
    } catch (Exception e) {
      // excepted exception
    }
    statement.execute("select c1 from t2");
    try {
      statement.execute("select c1 from t3");
      fail("Can't access the table t3");
    } catch (Exception e) {
      // excepted exception
    }
    statement.close();
    connection.close();

    // user4 can access the t2,t3
    connection = context.createConnection(USER4_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("select c1 from t1");
      fail("Can't access the table t1");
    } catch (Exception e) {
      // excepted exception
    }
    statement.execute("select c1 from t2");
    statement.execute("select c1 from t3");
    statement.close();
    connection.close();
  }

  private void verifyResultRoles(ResultSet resultSet, Set<String> exceptedRoles) throws Exception {
    int size = 0;
    while (resultSet.next()) {
      String tempRole = resultSet.getString(1);
      LOGGER.debug("tempRole:" + tempRole);
      assertTrue(exceptedRoles.contains(tempRole));
      size++;
    }
    assertEquals(exceptedRoles.size(), size);
    resultSet.close();
  }
}
