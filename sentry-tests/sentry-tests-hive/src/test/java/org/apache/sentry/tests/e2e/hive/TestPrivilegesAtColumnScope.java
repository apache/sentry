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

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at column scope within a single database.
 */

public class TestPrivilegesAtColumnScope extends AbstractTestWithStaticConfiguration {

  private static PolicyFile policyFile;
  private final static String MULTI_TYPE_DATA_FILE_NAME = "emp.dat";

  @Before
  public void setup() throws Exception {
    policyFile = super.setupPolicy();
    super.setup();
    prepareDBDataForTest();
  }

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  private static void prepareDBDataForTest() throws Exception {
    // copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE TAB_1(A STRING, B STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE VIEW VIEW_1(A,B) AS SELECT A,B FROM TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING, B STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");
    statement.execute("CREATE VIEW VIEW_2(A,B) AS SELECT A,B FROM TAB_2");
    //create table with partitions
    statement.execute("CREATE TABLE TAB_3 (A STRING, B STRING) partitioned by (C STRING)");
    statement.execute("ALTER TABLE TAB_3 ADD PARTITION (C=1)");
    statement.execute("ALTER TABLE TAB_3 ADD PARTITION (C=2)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_3 PARTITION (C=1)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_3 PARTITION (C=2)");
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2.
   * Admin grants SELECT on just one column of TAB_1, TAB_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   */
  @Test
  public void testSelectColumnOnTable() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1_A", "select_tab2_A")
        .addRolesToGroup(USERGROUP2, "select_tab1_A", "select_tab1_B", "select_tab2_A", "select_tab2_B")
        .addPermissionsToRole("select_tab1_A", "server=server1->db=DB_1->table=TAB_1->column=A->action=select")
        .addPermissionsToRole("select_tab1_B", "server=server1->db=DB_1->table=TAB_1->column=B->action=select")
        .addPermissionsToRole("select_tab2_A", "server=server1->db=DB_1->table=TAB_2->column=A->action=select")
        .addPermissionsToRole("select_tab2_B", "server=server1->db=DB_1->table=TAB_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution on user1
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can execute query count on column A on tab_1
    statement.executeQuery("SELECT COUNT(A) FROM TAB_1");

    // test user can execute query column A on tab_1
    statement.executeQuery("SELECT A FROM TAB_1");

    // negative test: test user can't execute query count of column B on tab_1
    try {
      statement.execute("SELECT COUNT(B) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query column B on tab_1
    try {
      statement.execute("SELECT B FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query view
    try {
      statement.execute("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();

    // test execution on user2
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query count of column A on tab_1
    statement.executeQuery("SELECT COUNT(A) FROM TAB_1");

    // test user can execute query count of column B on tab_1
    statement.executeQuery("SELECT COUNT(B) FROM TAB_1");

    // test user can't execute query count using * on tab_1
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can execute SELECT * on tab_1
    statement.executeQuery("SELECT * FROM TAB_1");

    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin view on TAB_1 and TAB_2
   * Admin grants SELECT on just one column of VIEW_1, VIEW_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   * Note: We don't support column level privilege on VIEW
   */
  @Test
  public void testSelectColumnOnView() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_view1_A", "select_view2_A")
        .addRolesToGroup(USERGROUP2, "select_view1_A", "select_view1_B", "select_view2_A", "select_view2_B")
        .addPermissionsToRole("select_view1_A", "server=server1->db=DB_1->table=VIEW_1->column=A->action=select")
        .addPermissionsToRole("select_view1_B", "server=server1->db=DB_1->table=VIEW_1->column=B->action=select")
        .addPermissionsToRole("select_view2_A", "server=server1->db=DB_1->table=VIEW_2->column=A->action=select")
        .addPermissionsToRole("select_view2_B", "server=server1->db=DB_1->table=VIEW_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution on user1
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    // negative test: test user can't execute query count of column B on tab_1
    try {
      statement.execute("SELECT COUNT(B) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // negative test: test user can't execute query count of column A on tab_1
    try {
      statement.execute("SELECT COUNT(A) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query column of view
    try {
      statement.execute("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // negative test: test user can't query column of view
    try {
      statement.execute("SELECT COUNT(B) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();

    // test execution on user2
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE DB_1");
    // test user can execute query count of column A on tab_1
    try {
      statement.execute("SELECT COUNT(A) FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // test user can execute query count of column B on tab_1
    try {
      statement.execute("SELECT COUNT(B) FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    // test user can't execute query count using * on tab_1
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("SELECT * FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't query view
    try {
      statement.execute("SELECT COUNT(A) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    try {
      statement.execute("SELECT COUNT(B) FROM VIEW_1");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't create a new view
    try {
      statement.execute("CREATE VIEW VIEW_2(A) AS SELECT A FROM TAB_1");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, VIEW_1 on TAB_1
   * loads data into TAB_1, TAB_2. Admin grants SELECT on TAB_1,TAB_2 to
   * USER_GROUPS. All test cases in this method will do the authorization on the condition of join
   * or where clause
   */
  @Test
  public void testSelectColumnOnTableJoin() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1_A", "select_tab1_B", "select_tab2_B")
        .addPermissionsToRole("select_tab1_A", "server=server1->db=DB_1->table=TAB_1->column=A->action=select")
        .addPermissionsToRole("select_tab1_B", "server=server1->db=DB_1->table=TAB_1->column=B->action=select")
        .addPermissionsToRole("select_tab2_B", "server=server1->db=DB_1->table=TAB_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution user1
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can execute query TAB_1 JOIN TAB_2, do the column authorization on the condition of
    // join clause
    statement
        .executeQuery("SELECT COUNT(T1.B) FROM TAB_1 T1 JOIN TAB_2 T2 ON T1.B = T2.B AND T1.A = '21' ");

    // negative test: test user can't execute query if do the column authorization on the condition
    // of join clause failed
    try {
      statement
          .execute("SELECT COUNT(T1.B) FROM TAB_1 T1 JOIN TAB_2 T2 ON T1.B = T2.B AND T1.A = '21' AND T2.A = '21'");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can execute query TAB_1 JOIN TAB_2, do the column authorization on the condition of
    // where clause
    statement
        .executeQuery("SELECT T1.* FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B AND T1.A = '21'");

    // negative test: test user can't execute query if do the column authorization on the condition
    // of where clause failed
    try {
      statement
          .execute("SELECT T1.* FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B AND T1.A = '21' AND T2.A = '21'");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    try {
      statement
          .execute("SELECT T1.* FROM TAB_1 T1, TAB_2 T2 WHERE T1.B = T2.B AND T1.A = '21' AND T2.A = '21'");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // negative test: test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.executeQuery("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin view on TAB_1 and TAB_2
   * Admin grants SELECT on just one column of VIEW_1, VIEW_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   * Note: We don't support column level privilege on VIEW
   */
  @Test
  public void testSelectColumnOnViewJoin() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_view1_A", "select_view1_B", "select_view2_B")
        .addRolesToGroup(USERGROUP2, "select_view1_B", "select_view2_B")
        .addRolesToGroup(USERGROUP3, "select_view1_B", "select_view2_A")
        .addPermissionsToRole("select_view1_A", "server=server1->db=DB_1->table=VIEW_1->column=A->action=select")
        .addPermissionsToRole("select_view1_B", "server=server1->db=DB_1->table=VIEW_1->column=B->action=select")
        .addPermissionsToRole("select_view2_A", "server=server1->db=DB_1->table=VIEW_2->column=A->action=select")
        .addPermissionsToRole("select_view2_B", "server=server1->db=DB_1->table=VIEW_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can't execute query VIEW_1 JOIN VIEW_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN VIEW_2 V2 ON (V1.B = V2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  /*
   * Admin creates database DB_1, table TAB_1, TAB_2 in DB_1, loads data into
   * TAB_1, TAB_2. Admin view on TAB_1 and TAB_2
   * Admin grants SELECT on just one column of VIEW_1, VIEW_2 to USER_GROUP1 of which
   * user1 is a member.
   * Admin grants SELECT on all column of TAB_1, TAB_2 to USER_GROUP2 of which
   * user2 is a member.
   * Note: We don't support column level privilege on VIEW
   */
  @Test
  public void testSelectColumnOnTableViewJoin() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1_A", "select_tab1_B", "select_view2_B")
        .addPermissionsToRole("select_tab1_A", "server=server1->db=DB_1->table=VIEW_1->column=A->action=select")
        .addPermissionsToRole("select_tab1_B", "server=server1->db=DB_1->table=VIEW_1->column=B->action=select")
        .addPermissionsToRole("select_view2_B", "server=server1->db=DB_1->table=VIEW_2->column=B->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE DB_1");

    // test user can't execute query VIEW_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN TAB_2 T2 ON (V1.B = T2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query VIEW_1 JOIN VIEW_2
    try {
      statement.execute("SELECT COUNT(*) FROM VIEW_1 V1 JOIN VIEW_2 V2 ON (V1.B = V2.B)");
      Assert.fail("Expected SQL Exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    // test user can't execute query TAB_1 JOIN TAB_2
    try {
      statement.execute("SELECT COUNT(*) FROM TAB_1 T1 JOIN TAB_2 T2 ON (T1.B = T2.B)");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  @Test
  public void testPartition() throws Exception{
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab3_A", "select_tab3_C")
        .addRolesToGroup(USERGROUP2, "select_tab3_A")
        .addRolesToGroup(USERGROUP3, "select_tab3_C")
        .addPermissionsToRole("select_tab3_A", "server=server1->db=DB_1->table=TAB_3->column=A->action=select")
        .addPermissionsToRole("select_tab3_C", "server=server1->db=DB_1->table=TAB_3->column=C->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // Users with privileges on partition column can access it
    String [] positiveUsers = {USER1_1, USER3_1};
    for(String user:positiveUsers) {
      Connection connection = context.createConnection(user);
      Statement statement = context.createStatement(connection);
      statement.execute("USE DB_1");
      statement.execute("SELECT C FROM TAB_3");
      statement.close();
      connection.close();
    }

    // Users with out privileges on partition column can not access it
    String [] negativeUsers = {USER2_1};
    for(String user:negativeUsers) {
      Connection connection = context.createConnection(USER1_1);
      Statement statement = context.createStatement(connection);
      statement.execute("USE DB_1");
      try {
        statement.execute("SELECT C FROM TAB_3");
      } catch (SQLException e) {
        context.verifyAuthzException(e);
      }
      statement.close();
      connection.close();
    }
  }
}
