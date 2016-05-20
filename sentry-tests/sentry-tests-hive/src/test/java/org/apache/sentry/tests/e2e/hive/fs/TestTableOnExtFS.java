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
package org.apache.sentry.tests.e2e.hive.fs;

import org.apache.hadoop.fs.Path;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test create an external db or table with its loc on external FS:
 * For example, create external tbl location 's3a://sentry-s3/db/tbl'
 * To run this test, ensure pass the below parameters:
 * -Dsentry.e2etest.DFSType=S3DFS
 * -Dsentry.e2etest.storage.uri=s3a://sentry-s3
 * -Dfs.s3a.access.key=a
 * -Dfs.s3a.secret.key=s
 * export HIVE_CONF_DIR=/etc/hive/conf/hite-site.xml
 */
public class TestTableOnExtFS extends TestFSBase {

  private static final String TEST_DB = "test_dfs_db";
  private static final String TEST_TBL = "test_dfs_ext_tbl";
  private static final String TEST_ROLE = "test_dfs_role";
  private static final String StrTestUriWithoutSchema = StrWarehouseDirFromConfFile + "/exttbl";
  // TEST_PATH could be:
  // hdfs://nameservice1/tblpath,
  // s3a://bucketname/tblpath
  private static final Path TEST_PATH =
      getFullPathWithSchemeAndAuthority(new Path(StrTestUriWithoutSchema));

  @Before
  public void setTestPath() throws Exception {
    createPath(TEST_PATH);
    try (Connection connection = context.createConnection(ADMIN1)) {
      try (Statement statement = connection.createStatement()) {
        exec(statement, "DROP DATABASE IF EXISTS " + TEST_DB + " CASCADE");
        exec(statement, "CREATE DATABASE " + TEST_DB);
        dropRecreateRole(statement, TEST_ROLE);
        exec(statement, "GRANT ALL ON DATABASE " + TEST_DB + " TO ROLE " + TEST_ROLE);
        exec(statement, "GRANT ROLE " + TEST_ROLE + " TO GROUP " + USERGROUP1);
      }
    }
  }

  private void testTableWithUriHelper(String strUri) throws Exception {
    try (Connection connection = context.createConnection((ADMIN1))) {
      try (Statement statement = connection.createStatement()) {
        exec(statement, "GRANT ALL ON URI '" + strUri + "' TO ROLE " + TEST_ROLE);
      }
    }
    try (Connection connection = context.createConnection(USER1_1)) {
      try (Statement statement = connection.createStatement()) {
        exec(statement, "USE " + TEST_DB);
        exec(statement, "CREATE EXTERNAL TABLE " + TEST_TBL
            + " (value STRING, number INT) PARTITIONED BY (par INT) LOCATION '"
            + strUri + "'");
        exec(statement, "INSERT INTO TABLE " + TEST_TBL + " PARTITION (par=1) VALUES ('test1', 1)");
        try (ResultSet rs = execQuery(statement, "SELECT number FROM " + TEST_TBL + " LIMIT 1")) {
          assertTrue("No number returned", rs.next());
          int number = rs.getInt("number");
          assertEquals("expected number 1, actual is " + number, number, 1);
        }
        exec(statement, "ALTER TABLE " + TEST_TBL + " DROP PARTITION (par=1) PURGE");
      }
    }
  }

  /**
   * Test full URI case: with Scheme and Authority
   * @throws Exception
   */
  @Test
  public void TestCreateExtTableWithFullUri() throws Exception {
    testTableWithUriHelper(TEST_PATH.toString());
  }

  /**
   * Test a URI without Scheme and Authority
   * @throws Exception
   */
  @Test
  public void TestCreateExtTableWithoutScheme() throws Exception {
    testTableWithUriHelper(StrTestUriWithoutSchema);
  }
}
