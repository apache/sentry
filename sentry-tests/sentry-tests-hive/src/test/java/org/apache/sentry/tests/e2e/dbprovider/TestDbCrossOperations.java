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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.SlowE2ETest;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SlowE2ETest
public class TestDbCrossOperations extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory.
      getLogger(TestDbCrossOperations.class);

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    LOGGER.info("TestDbCrossOperations setupTestStaticConfiguration");
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
    super.setupPolicy();
  }

  private void createTestData() throws Exception {
    try (Connection connection = context.createConnection(ADMIN1)) {
      try (Statement statement = context.createStatement(connection)) {
        exec(statement, "CREATE database " + DB1);
        exec(statement, "CREATE database " + DB2);
        exec(statement, "use " + DB1);
        exec(statement, "CREATE ROLE user_role1");
        exec(statement, "CREATE ROLE user_role2");
        exec(statement, "CREATE TABLE tb1 (id int , name String)");
        exec(statement, "GRANT ALL ON DATABASE db_1 TO ROLE user_role1");
        exec(statement, "GRANT ALL ON DATABASE db_1 TO ROLE user_role2");
        exec(statement, "GRANT SELECT (id) ON TABLE tb1 TO ROLE user_role1");
        exec(statement, "GRANT SELECT  ON TABLE tb1 TO ROLE user_role2");
        exec(statement, "GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
        exec(statement, "GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
        exec(statement, "INSERT INTO TABLE db_1.tb1 VALUES (1, 'tb1')");
        exec(statement, "use " + DB2);
        exec(statement, "CREATE TABLE tb2 (id int, num String)");
        exec(statement, "CREATE TABLE tb3 (id int, val String)");
        exec(statement, "GRANT SELECT (num) ON TABLE tb2 TO ROLE user_role1");
        exec(statement, "GRANT SELECT (val) ON TABLE tb3 TO ROLE user_role1");
        exec(statement, "GRANT SELECT  ON TABLE tb2 TO ROLE user_role2");
        exec(statement, "GRANT SELECT  ON TABLE tb3 TO ROLE user_role2");
        exec(statement, "GRANT ROLE user_role1 TO GROUP " + USERGROUP1);
        exec(statement, "GRANT ROLE user_role2 TO GROUP " + USERGROUP2);
        exec(statement, "INSERT INTO TABLE db_2.tb2 VALUES (2, 'tb2')");
        exec(statement, "INSERT INTO TABLE db_2.tb3 VALUES (3, 'tb3')" );
      }
    }
  }

  @Test
  public void testCrossDbTableOperations() throws Exception {
    //The privilege of user_role1 is used to test create table as select.
    createTestData();

    //Test create table as select from cross db table
    try (Connection connection =context.createConnection(USER1_1)) {
      try (Statement statement = context.createStatement(connection)) {
        exec(statement, "use " + DB1);
        exec(statement, "CREATE table db_1.t1 as select tb1.id, tb3.val, tb2.num from db_1.tb1,db_2.tb3,db_2.tb2");
        exec(statement, "SELECT * FROM db_1.t1 LIMIT 1");
      }
    }
  }

  @Test
  public void testsCrossDbView() throws Exception {
    //The privilege of user_role2 is used to test create view as select.
    createTestData();

    //Test create view as select from cross db tables
    try (Connection connection =context.createConnection(USER2_1)) {
      try (Statement statement = context.createStatement(connection)) {
        //The db_1.tb1 and db_2.tb3 is same with db_2.tb2.
        ResultSet res = execQuery(statement, "select * from db_2.tb2 limit 1");
        List<String> expectedResult = new ArrayList<String>();
        List<String> returnedResult = new ArrayList<String>();
        expectedResult.add("2");
        while(res.next()){
          returnedResult.add(res.getString(1).trim());
        }
        validateReturnedResult(expectedResult, returnedResult);
        expectedResult.clear();
        returnedResult.clear();
        exec(statement, "use " + DB1);
        exec(statement, "CREATE VIEW db_1.v1 as select tb1.id, tb3.val, tb2.num from db_1.tb1,db_2.tb3,db_2.tb2");
      }
    }
  }
}
