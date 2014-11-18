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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestColumnEndToEnd extends AbstractTestWithStaticConfiguration {
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataFile;
  private PolicyFile policyFile;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  @Test
  public void testBasic() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE database " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("CREATE ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    context.assertSentryException(statement, "CREATE ROLE r2",
        SentryAccessDeniedException.class.getSimpleName());

    statement.execute("SELECT * FROM " + DB1 + ".t1");
    statement.close();
    connection.close();
  }

  @Test
  public void testNegative() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE t1 (c1 string, c2 string, c3 string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("CREATE ROLE user_role2");
    statement.execute("GRANT SELECT (c1) ON TABLE t1 TO ROLE user_role1");
    statement.execute("GRANT SELECT (c1,c2) ON TABLE t1 TO ROLE user_role2");
    try {
      statement.execute("GRANT INSERT (c2) ON TABLE t1 TO ROLE user_role2");
      assertTrue("Sentry should not support privilege: Insert on Column", false);
    } catch (Exception e) {
      assertTrue("The error should be 'Sentry does not support privilege: Insert on Column'",
          e.getMessage().contains("Sentry does not support privilege: Insert on Column"));
    }
    try {
      statement.execute("GRANT ALL (c2) ON TABLE t1 TO ROLE user_role2");
      assertTrue("Sentry should not support privilege: ALL on Column", false);
    } catch (Exception e) {
      assertTrue("The error should be 'Sentry does not support privilege: All on Column'",
          e.getMessage().contains("Sentry does not support privilege: All on Column"));
    }
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.close();
    connection.close();

    // 1.1 user_role1 select c1,c2 from t1, will throw exception
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("SELECT c1,c2 FROM t1");
      assertTrue("only SELECT allowed on t1.c1!!", false);
    } catch (Exception e) {
      // Ignore
    }

    // 1.2 user_role1 select * from t1, will throw exception
    try {
      statement.execute("SELECT * FROM t1");
      assertTrue("only SELECT allowed on t1.c1!!", false);
    } catch (Exception e) {
      // Ignore
    }

    // 2.1 user_role2 select c1,c2,c3 from t1, will throw exception
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("SELECT c1,c2,c3 FROM t1");
      assertTrue("no permission on table t1!!", false);
    } catch (Exception e) {
      // Ignore
    }

    // 2.2 user_role2 select * from t1, will throw exception
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("SELECT * FROM t1");
      assertTrue("no permission on table t1!!", false);
    } catch (Exception e) {
      // Ignore
    }

    statement.close();
    connection.close();
  }

  @Test
  public void testPostive() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE database " + DB1);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string, c2 string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("CREATE ROLE user_role2");
    statement.execute("CREATE ROLE user_role3");
    statement.execute("GRANT SELECT (c1) ON TABLE t1 TO ROLE user_role1");
    statement.execute("GRANT SELECT (c1, c2) ON TABLE t1 TO ROLE user_role2");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role3");
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE user_role3 TO GROUP " + USERGROUP3);
    statement.close();
    connection.close();

    // 1 user_role1 select c1 on t1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("SELECT c1 FROM t1");

    // 2.1 user_role2 select c1,c2 on t1
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("SELECT c1,c2 FROM t1");
    // 2.2 user_role2 select * on t1
    statement.execute("SELECT * FROM t1");

    // 3.1 user_role3 select * on t1
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("SELECT * FROM t1");
    // 3.2 user_role3 select c1,c2 on t1
    statement.execute("SELECT c1,c2 FROM t1");

    statement.close();
    connection.close();
  }

  @Test
  public void testCreateTableAsSelect() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE database " + DB1);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE t1 (c1 string, c2 string)");
    statement.execute("CREATE ROLE user_role1");
    statement.execute("CREATE ROLE user_role2");
    statement.execute("CREATE ROLE user_role3");
    statement.execute("GRANT SELECT (c1) ON TABLE t1 TO ROLE user_role1");
    statement.execute("GRANT SELECT (c1, c2) ON TABLE t1 TO ROLE user_role2");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role3");
    statement.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE user_role1");
    statement.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE user_role2");
    statement.execute("GRANT CREATE ON DATABASE " + DB1 + " TO ROLE user_role3");
    statement.execute("GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
    statement.execute("GRANT ROLE user_role3 TO GROUP " + USERGROUP3);
    statement.close();
    connection.close();

    // 1 user_role1 create table as select
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE t1_1 AS SELECT c1 FROM t1");
    try {
      statement.execute("CREATE TABLE t1_2 AS SELECT * FROM t1");
      assertTrue("no permission on table t1!!", false);
    } catch (Exception e) {
      // Ignore
    }

    // 2 user_role2 create table as select
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE t2_1 AS SELECT c1 FROM t1");
    statement.execute("CREATE TABLE t2_2 AS SELECT * FROM t1");

    // 3 user_role3 create table as select
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE t3_1 AS SELECT c1 FROM t1");
    statement.execute("CREATE TABLE t3_2 AS SELECT * FROM t1");

    statement.close();
    connection.close();
  }
}
