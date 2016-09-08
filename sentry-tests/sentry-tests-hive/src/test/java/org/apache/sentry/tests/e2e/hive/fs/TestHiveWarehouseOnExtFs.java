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

import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Configure Hive warehouse dir to an external storage FS system:
 * for example,
 * <property>
 * <name>hive.metastore.warehouse.dir</name>
 * <value>s3a://sentry-s3/user/hive/warehouse</value>
 * </property>
 * Test basic db and tbl permissions
 * Ensure export HIVE_CONF_DIR=/etc/hive/conf, in the dir, can find hive-site.xml
 */
public class TestHiveWarehouseOnExtFs extends TestFSBase {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestHiveWarehouseOnExtFs.class);
  private static final String TEST_DB = "test_warehouse_db";
  private static final String TEST_TBL = "test_warehouse_ext_tbl";
  private static final String TEST_ROLE = "test_warehouse_role";

  @Before
  public void createDbAndRole() throws Exception {

    LOGGER.info("StrWarehouseDirFromConfFile = " + StrWarehouseDirFromConfFile);
    // Ensure Hive warehouse is configured to s3a or other file system than hdfs
    // warehouse dir is extracted from ${HIVE_CONF_DIR}/hive-site.xml
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

  @Test
  public void TestCreateTable() throws Exception {
    try (Connection connection = context.createConnection(USER1_1)) {
      try (Statement statement = connection.createStatement()) {
        exec(statement, "USE " + TEST_DB);
        exec(statement, "CREATE TABLE " + TEST_TBL
            + " (value STRING, number INT) PARTITIONED BY (par INT)");
        exec(statement, "INSERT INTO TABLE " + TEST_TBL + " PARTITION (par=1) VALUES ('test1', 1)");
        try (ResultSet rs = execQuery(statement, "SELECT number FROM " + TEST_TBL + " LIMIT 1")) {
          assertTrue("No number returned", rs.next());
          int number = rs.getInt("number");
          assertEquals("expected number 1, actual is " + number, number, 1);
        }
        String tblDir = StrWarehouseDirFromConfFile + "/" + TEST_DB + ".db/" + TEST_TBL;
        String msg = "tblDir = " + tblDir + " does not exist";
        if (new Path(StrWarehouseDirFromConfFile).isAbsoluteAndSchemeAuthorityNull()) {
          // Warehouse locates on default fileSystem, for example ClusterDFS
          assertTrue(msg, fileSystem.exists(new Path(tblDir)));
        } else {
          // Warehosue locates on other storage file system, for example s3a
          assertTrue(msg, storageFileSystem.exists(new Path(tblDir)));
        }
      }
    }
  }

}
