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

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.sentry.tests.e2e.hive.Context;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestDbEndToEnd extends AbstractTestWithDbProvider {
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataFile;

  @Before
  public void setup() throws Exception {
    createContext();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    setupAdmin(context);
  }

  @Test
  public void testBasic() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("CREATE ROLE user_role");
    statement.execute("GRANT SELECT ON TABLE t1 TO ROLE user_role");
    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.close();
    connection.close();
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    context.assertSentryServiceAccessDenied(statement, "CREATE ROLE r2");
    // test default of ALL
    statement.execute("SELECT * FROM t1");
    // test a specific role
    statement.execute("SET ROLE user_role");
    statement.execute("SELECT * FROM t1");
    // test NONE
    statement.execute("SET ROLE NONE");
    context.assertAuthzException(statement, "SELECT * FROM t1");
    // test ALL
    statement.execute("SET ROLE ALL");
    statement.execute("SELECT * FROM t1");
    statement.close();
    connection.close();
  }

  @Test
  public void testUPrivileges() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE t1 (c1 string)");
    statement.execute("CREATE ROLE user_role");
    statement.execute("CREATE ROLE uri_role");
    statement.execute("GRANT SELECT ON URI 'file://" + dataDir.getPath()
        + "' TO ROLE uri_role");
    statement.execute("GRANT INSERT ON TABLE t1 TO ROLE user_role");

    statement.execute("GRANT ROLE user_role TO GROUP " + USERGROUP1);
    statement.execute("GRANT ROLE uri_role TO GROUP " + USERGROUP1);

    ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE uri_role");
    assertTrue(resultSet.next());
    assertEquals("file://" + dataDir.getPath(), resultSet.getString(1));
    resultSet.close();

    resultSet = statement.executeQuery("SHOW GRANT ROLE user_role");
    assertTrue(resultSet.next());
    assertEquals("default", resultSet.getString(1));
    assertEquals("t1", resultSet.getString(2));
    resultSet.close();

    statement.close();
    connection.close();

  }

  /**
   * Steps:
   * 1. admin create a new experimental database
   * 2. admin create a new production database, create table, load data
   * 3. admin grant privilege all@'experimental database' to usergroup1
   * 4. user create table, load data in experimental DB
   * 5. user create view based on table in experimental DB
   * 6. admin create table (same name) in production DB
   * 7. admin grant read@productionDB.table to group
   *    admin grant select@productionDB.table to group
   * 8. user load data from experimental table to production table
   */
  @Test
  public void testEndToEnd1() throws Exception {

    String dbName1 = "db_1";
    String dbName2 = "productionDB";
    String tableName1 = "tb_1";
    String tableName2 = "tb_2";
    String viewName1 = "view_1";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    // 1
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    // 2
    statement.execute("DROP DATABASE IF EXISTS " + dbName2 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName2);
    statement.execute("USE " + dbName2);
    statement.execute("DROP TABLE IF EXISTS " + dbName2 + "." + tableName2);
    statement.execute("create table " + dbName2 + "." + tableName2
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName2);

    // 3
    statement.execute("CREATE ROLE all_db1");
    statement.execute("GRANT ALL ON DATABASE " + dbName1 + " TO ROLE all_db1");

    statement.execute("CREATE ROLE select_tb1");
    statement.execute("CREATE ROLE insert_tb1");
    statement.execute("CREATE ROLE insert_tb2");
    statement.execute("CREATE ROLE data_uri");

    statement.execute("USE " + dbName2);
    statement.execute("GRANT INSERT ON TABLE " + tableName1
        + " TO ROLE insert_tb1");
    statement.execute("GRANT INSERT ON TABLE " + tableName2
        + " TO ROLE insert_tb2");
    statement.execute("GRANT ALL ON URI 'file://" + dataDir.getPath()
        + "' TO ROLE data_uri");

    statement.execute("USE " + dbName1);
    statement.execute("GRANT SELECT ON TABLE " + tableName1
        + " TO ROLE select_tb1");

    statement
        .execute("GRANT ROLE all_db1, select_tb1, insert_tb1, insert_tb2, data_uri TO GROUP "
            + USERGROUP1);

    statement.close();
    connection.close();

    // 4
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + tableName1);

    // 5
    statement.execute("CREATE VIEW " + viewName1 + " (value) AS SELECT value from " + tableName1 + " LIMIT 10");
    statement.close();
    connection.close();

    // 7
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName2);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.close();
    connection.close();

    // 8
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName2);
    statement.execute("INSERT OVERWRITE TABLE " +
        dbName2 + "." + tableName2 + " SELECT * FROM " + dbName1
        + "." + tableName1);
    statement.close();
    connection.close();
  }
}
