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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.sentry.tests.e2e.hive.fs.DFSFactory;
import org.apache.sentry.tests.e2e.hive.fs.DFS;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServer;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

public abstract class AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AbstractTestWithStaticConfiguration.class);
  protected static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  protected static final String ALL_DB1 = "server=server1->db=db_1",
      ALL_DB2 = "server=server1->db=db_2",
      SELECT_DB1_TBL1 = "server=server1->db=db_1->table=tb_1->action=select",
      SELECT_DB1_TBL2 = "server=server1->db=db_1->table=tb_2->action=select",
      SELECT_DB1_NONTABLE = "server=server1->db=db_1->table=this table does not exist->action=select",
      INSERT_DB1_TBL1 = "server=server1->db=db_1->table=tb_1->action=insert",
      INSERT_DB1_TBL2 = "server=server1->db=db_1->table=tb_2->action=insert",
      SELECT_DB2_TBL2 = "server=server1->db=db_2->table=tb_2->action=select",
      INSERT_DB2_TBL1 = "server=server1->db=db_2->table=tb_1->action=insert",
      SELECT_DB1_VIEW1 = "server=server1->db=db_1->table=view_1->action=select",
      ADMIN1 = StaticUserGroup.ADMIN1,
      ADMINGROUP = StaticUserGroup.ADMINGROUP,
      USER1_1 = StaticUserGroup.USER1_1,
      USER1_2 = StaticUserGroup.USER1_2,
      USER2_1 = StaticUserGroup.USER2_1,
      USER3_1 = StaticUserGroup.USER3_1,
      USER4_1 = StaticUserGroup.USER4_1,
      USERGROUP1 = StaticUserGroup.USERGROUP1,
      USERGROUP2 = StaticUserGroup.USERGROUP2,
      USERGROUP3 = StaticUserGroup.USERGROUP3,
      USERGROUP4 = StaticUserGroup.USERGROUP4,
      GROUP1_ROLE = "group1_role",
      DB1 = "db_1",
      DB2 = "db_2",
      DB3 = "db_3",
      TBL1 = "tb_1",
      TBL2 = "tb_2",
      TBL3 = "tb_3",
      VIEW1 = "view_1",
      VIEW2 = "view_2",
      VIEW3 = "view_3",
      INDEX1 = "index_1",
      INDEX2 = "index_2";


  protected static File baseDir;
  protected static File logDir;
  protected static File confDir;
  protected static File dataDir;
  protected static File policyFileLocation;
  protected static HiveServer hiveServer;
  protected static FileSystem fileSystem;
  protected static DFS dfs;
  protected static Map<String, String> properties;
  protected Context context;

  public Context createContext() throws Exception {
    return new Context(hiveServer, fileSystem,
        baseDir, confDir, dataDir, policyFileLocation);
  }
  protected void dropDb(String user, String...dbs) throws Exception {
    Connection connection = context.createConnection(user, "password");
    Statement statement = connection.createStatement();
    for(String db : dbs) {
      statement.execute("DROP DATABASE IF EXISTS " + db + " CASCADE");
    }
    statement.close();
    connection.close();
  }
  protected void createDb(String user, String...dbs) throws Exception {
    Connection connection = context.createConnection(user, "password");
    Statement statement = connection.createStatement();
    for(String db : dbs) {
      statement.execute("CREATE DATABASE " + db);
    }
    statement.close();
    connection.close();
  }
  protected void createTable(String user, String db, File dataFile, String...tables)
      throws Exception {
    Connection connection = context.createConnection(user, "password");
    Statement statement = connection.createStatement();
    statement.execute("USE " + db);
    for(String table : tables) {
      statement.execute("DROP TABLE IF EXISTS " + table);
      statement.execute("create table " + table
          + " (under_col int comment 'the under column', value string)");
      statement.execute("load data local inpath '" + dataFile.getPath()
          + "' into table " + table);
      ResultSet res = statement.executeQuery("select * from " + table);
      Assert.assertTrue("Table should have data after load", res.next());
      res.close();
    }
    statement.close();
    connection.close();
  }

  protected static File assertCreateDir(File dir) {
    if(!dir.isDirectory()) {
      Assert.assertTrue("Failed creating " + dir, dir.mkdirs());
    }
    return dir;
  }

  @BeforeClass
  public static void setupTestStaticConfiguration()
      throws Exception {
    properties = Maps.newHashMap();
    baseDir = Files.createTempDir();
    LOGGER.info("BaseDir = " + baseDir);
    logDir = assertCreateDir(new File(baseDir, "log"));
    confDir = assertCreateDir(new File(baseDir, "etc"));
    dataDir = assertCreateDir(new File(baseDir, "data"));
    policyFileLocation = new File(confDir, HiveServerFactory.AUTHZ_PROVIDER_FILENAME);

    String dfsType = System.getProperty(DFSFactory.FS_TYPE);
    dfs = DFSFactory.create(dfsType, baseDir);

    fileSystem = dfs.getFileSystem();
    hiveServer = HiveServerFactory.create(properties, baseDir, confDir, logDir, policyFileLocation, fileSystem);
    hiveServer.start();
  }

  @Before
  public void setup() throws Exception{
    dfs.createBaseDir();
  }

  @AfterClass
  public static void tearDownTestStaticConfiguration() throws Exception {
    if(hiveServer != null) {
      hiveServer.shutdown();
      hiveServer = null;
    }
    if(baseDir != null) {
      if(System.getProperty(HiveServerFactory.KEEP_BASEDIR) == null) {
        FileUtils.deleteQuietly(baseDir);
      }
      baseDir = null;
    }
    if(dfs!=null) {
      dfs.tearDown();
    }
  }
}
