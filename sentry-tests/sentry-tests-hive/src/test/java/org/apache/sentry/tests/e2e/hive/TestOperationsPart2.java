/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.sentry.provider.file.PolicyFile;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test all operations that require index on table alone (part 2)
 1. Create index : HiveOperation.CREATEINDEX
 2. Drop index : HiveOperation.DROPINDEX
 3. HiveOperation.ALTERINDEX_REBUILD
 4. TODO: HiveOperation.ALTERINDEX_PROPS
 */
public class TestOperationsPart2 extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestOperationsPart2.class);

  private PolicyFile policyFile;
  final String tableName = "tb1";

  static Map<String, String> privileges = new HashMap<String, String>();
  static {
    privileges.put("all_server", "server=server1->action=all");
    privileges.put("create_server", "server=server1->action=create");
    privileges.put("all_db1", "server=server1->db=" + DB1 + "->action=all");
    privileges.put("select_db1", "server=server1->db=" + DB1 + "->action=select");
    privileges.put("insert_db1", "server=server1->db=" + DB1 + "->action=insert");
    privileges.put("create_db1", "server=server1->db=" + DB1 + "->action=create");
    privileges.put("drop_db1", "server=server1->db=" + DB1 + "->action=drop");
    privileges.put("alter_db1", "server=server1->db=" + DB1 + "->action=alter");
    privileges.put("create_db2", "server=server1->db=" + DB2 + "->action=create");

    privileges.put("all_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=all");
    privileges.put("select_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=select");
    privileges.put("insert_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=insert");
    privileges.put("alter_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=alter");
    privileges.put("alter_db1_ptab", "server=server1->db=" + DB1 + "->table=ptab->action=alter");
    privileges.put("index_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=index");
    privileges.put("lock_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=lock");
    privileges.put("drop_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=drop");
    privileges.put("insert_db2_tb2", "server=server1->db=" + DB2 + "->table=tb2->action=insert");
    privileges.put("select_db1_view1", "server=server1->db=" + DB1 + "->table=view1->action=select");

  }

  @Before
  public void setup() throws Exception{
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP)
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);
  }

  private void adminCreate(String db, String table) throws Exception{
    adminCreate(db, table, false);
  }

  private void adminCreate(String db, String table, boolean partitioned) throws Exception{
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    exec(statement, "DROP DATABASE IF EXISTS " + db + " CASCADE");
    exec(statement, "CREATE DATABASE " + db);
    if(table !=null) {
      if (partitioned) {
        exec(statement, "CREATE table  " + db + "." + table + " (a string) PARTITIONED BY (b string)");
      } else{
        exec(statement, "CREATE table  " + db + "." + table + " (a string)");
      }

    }
    statement.close();
    connection.close();
  }

  private void assertSemanticException(Statement stmt, String command) throws SQLException{
    context.assertSentrySemanticException(stmt, command, semanticException);
  }

  @Test
  public void testIndexTable() throws Exception {
    adminCreate(DB1, tableName, true);
    String indexLocation = dfs.getBaseDir() + "/" + Math.random();
    policyFile
        .addPermissionsToRole("index_db1_tb1", privileges.get("index_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "index_db1_tb1")
        .addRolesToGroup(USERGROUP3, "index_db1_tb1")
        .addPermissionsToRole("uri_role", "server=server1->uri=" + indexLocation)
        .addRolesToGroup(USERGROUP3, "uri_role")
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection;
    Statement statement;

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "CREATE INDEX table01_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    exec(statement, "ALTER INDEX table01_index ON tb1 REBUILD");
    statement.close();
    connection.close();

    //Negative case
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    assertSemanticException(statement, "CREATE INDEX table02_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    assertSemanticException(statement, "ALTER INDEX table01_index ON tb1 REBUILD");
    assertSemanticException(statement, "DROP INDEX table01_index ON tb1");
    statement.close();
    connection.close();

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "DROP INDEX table01_index ON tb1");
    statement.close();
    connection.close();

    //Positive case for location
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "CREATE INDEX table01_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD LOCATION '"
        + indexLocation + "'");
    exec(statement, "ALTER INDEX table01_index ON tb1 REBUILD");
    exec(statement, "DROP INDEX table01_index ON tb1");
    statement.close();
    connection.close();

    //Negative case
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    assertSemanticException(statement, "CREATE INDEX table01_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD " +
            "LOCATION '" + indexLocation + "'");
    statement.close();
    connection.close();
  }

  /* Test all operations that require drop on table alone
  1. Create index : HiveOperation.DROPTABLE
  */
  @Test
  public void testDropTable() throws Exception {
    adminCreate(DB1, tableName, true);
    policyFile
        .addPermissionsToRole("drop_db1_tb1", privileges.get("drop_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "drop_db1_tb1")
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection;
    Statement statement;

    //Negative case
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    assertSemanticException(statement, "drop table " + tableName);

    statement.close();
    connection.close();

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "drop table " + tableName);

    statement.close();
    connection.close();
  }

  @Ignore
  @Test
  public void testLockTable() throws Exception {
    //TODO
  }

  /* Operations that require alter + drop on table
    1. HiveOperation.ALTERTABLE_DROPPARTS
  */
  @Test
  public void dropPartition() throws Exception {
    adminCreate(DB1, tableName, true);
    policyFile
        .addPermissionsToRole("alter_db1_tb1", privileges.get("alter_db1_tb1"))
        .addPermissionsToRole("drop_db1_tb1", privileges.get("drop_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "alter_db1_tb1", "drop_db1_tb1")
        .addRolesToGroup(USERGROUP2, "alter_db1_tb1");

    writePolicyFile(policyFile);

    Connection connection;
    Statement statement;
    //Setup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '10') ");

    //Negative case
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    exec(statement, "USE " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 DROP PARTITION (b = 10)");

    //Positive case
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "ALTER TABLE tb1 DROP PARTITION (b = 10)");
    statement.close();
    connection.close();
  }

  /*
   1. HiveOperation.ALTERTABLE_RENAME
   */
  @Test
  public void renameTable() throws Exception {
    adminCreate(DB1, "TAB_1");
    adminCreate(DB2, "TAB_3");
    adminCreate(DB3, null);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    exec(statement, "CREATE table  " + DB1 + ".TAB_2 (a string)");
    statement.close();
    connection.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "all_db1")
        .addRolesToGroup(USERGROUP1, "drop_db2")
        .addRolesToGroup(USERGROUP1, "create_db3")
        .addPermissionsToRole("all_db1", "server=server1->db=" + DB1)
        .addPermissionsToRole("drop_db2", "server=server1->db=" + DB2 + "->action=drop")
        .addPermissionsToRole("create_db3", "server=server1->db=" + DB3 + "->action=create")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // user1 haven't create permission with db_2, can't move table to db_2
    exec(statement, "use " + DB1);
    try {
      exec(statement, "alter table TAB_1 rename to " + DB2 + ".TAB_1");
      fail("the exception should be thrown");
    } catch (Exception e) {
      // ignore the exception
    }
    try {
      // test with the format of table name: db.table
      exec(statement, "alter table " + DB1 + ".TAB_1 rename to " + DB2 + ".TAB_1");
      fail("the exception should be thrown");
    } catch (Exception e) {
      // ignore the exception
    }

    // user1 haven't create permission with db_2, can't move table from db_2
    exec(statement, "use " + DB2);
    try {
      exec(statement, "alter table TAB_3 rename to " + DB2 + ".TAB_1");
      fail("the exception should be thrown");
    } catch (Exception e) {
      // ignore the exception
    }
    try {
      // test with the format of table name: db.table
      exec(statement, "alter table " + DB2 + ".TAB_3 rename to " + DB2 + ".TAB_1");
      fail("the exception should be thrown");
    } catch (Exception e) {
      // ignore the exception
    }

    // user1 have all permission with db_1 and create permission with db_3, alter_table_rename pass
    exec(statement, "use " + DB1);
    exec(statement, "alter table TAB_1 rename to " + DB3 + ".TAB_1");
    exec(statement, "alter table " + DB1 + ".TAB_2 rename to " + DB3 + ".TAB_2");

    // user1 have drop permission with db_2 and create permission with db_3, alter_table_rename pass
    exec(statement, "use " + DB2);
    exec(statement, "alter table TAB_3 rename to " + DB3 + ".TAB_3");

    // user1 haven't drop permission with db_3, can't move table to db_3
    exec(statement, "use " + DB3);
    try {
      exec(statement, "alter table TAB_3 rename to TAB_4");
      fail("the exception should be thrown");
    } catch (Exception e) {
      // ignore the exception
    }
  }

  /* Test all operations which require alter on table (+ all on URI)
   1. HiveOperation.ALTERTABLE_LOCATION
   2. HiveOperation.ALTERTABLE_ADDPARTS
   3. TODO: HiveOperation.ALTERPARTITION_LOCATION
   4. TODO: HiveOperation.ALTERTBLPART_SKEWED_LOCATION
   */
  @Test
  public void testAlterOnTableAndURI() throws Exception {
    adminCreate(DB1, tableName, true);
    String tabLocation = dfs.getBaseDir() + "/" + Math.random();
    policyFile
        .addPermissionsToRole("alter_db1_tb1", privileges.get("alter_db1_tb1"))
        .addPermissionsToRole("all_uri", "server=server1->uri=" + tabLocation)
        .addRolesToGroup(USERGROUP1, "alter_db1_tb1", "all_uri")
        .addRolesToGroup(USERGROUP2, "alter_db1_tb1");

    writePolicyFile(policyFile);

    //Case with out uri
    Connection connection = context.createConnection(USER2_1);
    Statement statement = context.createStatement(connection);
    exec(statement, "USE " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'");
    assertSemanticException(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '" + tabLocation + "/part'");
    exec(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '1') ");

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'");
    exec(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '" + tabLocation + "/part'");
    exec(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '10') ");
    statement.close();
    connection.close();

    //Negative case: User2_1 has privileges on table but on on uri
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    context.assertSentrySemanticException(statement, "ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'",
        semanticException);
    context.assertSentrySemanticException(statement,
        "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '" + tabLocation + "/part'",
        semanticException);
    statement.close();
    connection.close();

    //Negative case: User3_1 has only insert privileges on table
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "insert_db1_tb1", "all_uri");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '2') ");
    assertSemanticException(statement, "ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'");

    assertSemanticException(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '"
        + tabLocation + "/part'");
    statement.close();
    connection.close();


  }

  /* Create on Database and select on table
  1. Create view :  HiveOperation.CREATEVIEW
   */
  @Test
  public void testCreateView() throws Exception {
    adminCreate(DB1, tableName);
    adminCreate(DB2, null);
    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("create_db2", privileges.get("create_db2"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "create_db2");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    exec(statement, "use " + DB2);
    exec(statement, "create view view1 as select a from " + DB1 + ".tb1");
    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "insert_db1_tb1", "create_db2");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB2);
    context.assertSentrySemanticException(statement, "create view view1 as select a from " + DB1 + ".tb1",
        semanticException);
    statement.close();
    connection.close();


  }

  /*
   1. HiveOperation.IMPORT : Create on db + all on URI
   2. HiveOperation.EXPORT : SELECT on table + all on uri
   */

  @Test
  public void testExportImport() throws Exception {
    File dataFile;
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, tableName);
    String location = dfs.getBaseDir() + "/" + Math.random();
    policyFile
        .addPermissionsToRole("create_db1", privileges.get("create_db1"))
        .addPermissionsToRole("all_uri", "server=server1->uri="+ location)
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("insert_db1", privileges.get("insert_db1"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "all_uri")
        .addRolesToGroup(USERGROUP2, "create_db1", "all_uri")
        .addRolesToGroup(USERGROUP3, "insert_db1", "all_uri");
    writePolicyFile(policyFile);
    Connection connection;
    Statement statement;

    //Negative case
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    // Import/Export works with s3 storage system only when this is turned on.
    exec(statement, "set hive.exim.uri.scheme.whitelist=hdfs,pfile,s3a;");
    context.assertSentrySemanticException(statement, "export table tb1 to '" + location + "'",
        semanticException);
    statement.close();
    connection.close();

    //Positive
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "set hive.exim.uri.scheme.whitelist=hdfs,pfile,s3a;");
    exec(statement, "export table tb1 to '" + location + "'" );
    statement.close();
    connection.close();

    //Negative
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "set hive.exim.uri.scheme.whitelist=hdfs,pfile,s3a;");
    context.assertSentrySemanticException(statement, "import table tb2 from '" + location + "'",
        semanticException);
    statement.close();
    connection.close();

    //Positive
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "set hive.exim.uri.scheme.whitelist=hdfs,pfile,s3a;");
    exec(statement, "import table tb2 from '" + location + "'");
    statement.close();
    connection.close();

  }

  /*
  1. HiveOperation.LOAD: INSERT on table + all on uri
   */
  @Test
  public void testLoad() throws Exception {
    File dataFile;
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    adminCreate(DB1, tableName);

    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addPermissionsToRole("all_uri", "server=server1->uri=file://" + dataDir)
        .addRolesToGroup(USERGROUP1, "insert_db1_tb1", "all_uri");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "load data local inpath '" + dataFile.getPath() + "' into table tb1" );
    statement.close();
    connection.close();
  }

  /*
  1. HiveOperation.CREATETABLE_AS_SELECT : Create on db + select on table
   */
  @Test
  public void testCTAS() throws Exception {
    adminCreate(DB1, tableName);
    adminCreate(DB2, null);

    String location = dfs.getBaseDir() + "/" + Math.random();

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    exec(statement, "Use " + DB1);
    exec(statement, "create view view1 as select a from " + DB1 + ".tb1");
    statement.close();
    connection.close();

    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("select_db1_view1", privileges.get("select_db1_view1"))
        .addPermissionsToRole("create_db2", privileges.get("create_db2"))
        .addPermissionsToRole("all_uri", "server=server1->uri=" + location)
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "create_db2")
        .addRolesToGroup(USERGROUP2, "select_db1_view1", "create_db2")
        .addRolesToGroup(USERGROUP3, "select_db1_tb1", "create_db2,all_uri");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB2);
    exec(statement, "create table tb2 as select a from " + DB1 + ".tb1");
    //Ensure CTAS fails without URI
    context.assertSentrySemanticException(statement, "create table tb3 location '" + location +
            "' as select a from " + DB1 + ".tb1",
        semanticException);
    context.assertSentrySemanticException(statement, "create table tb3 as select a from " + DB1 + ".view1",
        semanticException);


    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    exec(statement, "Use " + DB2);
    exec(statement, "create table tb3 as select a from " + DB1 + ".view1" );
    context.assertSentrySemanticException(statement, "create table tb4 as select a from " + DB1 + ".tb1",
        semanticException);

    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    //CTAS is valid with URI
    exec(statement, "Use " + DB2);
    exec(statement, "create table tb4 location '" + location +
        "' as select a from " + DB1 + ".tb1");

    statement.close();
    connection.close();

  }


  /*
  1. INSERT : IP: select on table, OP: insert on table + all on uri(optional)
   */
  @Test
  public void testInsert() throws Exception {
    File dataFile;
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    dropDb(ADMIN1, DB1);
    dropDb(ADMIN1, DB2);
    createDb(ADMIN1, DB1);
    createDb(ADMIN1, DB2);
    createTable(ADMIN1, DB1, dataFile, tableName);
    createTable(ADMIN1, DB2, null, "tb2");
    String location = dfs.getBaseDir() + "/" + Math.random();

    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("insert_db2_tb2", privileges.get("insert_db2_tb2"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "insert_db2_tb2")
        .addPermissionsToRole("all_uri", "server=server1->uri=" + location)
        .addRolesToGroup(USERGROUP2, "select_db1_tb1", "all_uri");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    assertSemanticException(statement, "insert overwrite directory '" + location + "' select * from " + DB1 + ".tb1");
    exec(statement, "insert overwrite table " + DB2 + ".tb2 select * from " + DB1 + ".tb1");
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    exec(statement, "insert overwrite directory '" + location + "' select * from " + DB1 + ".tb1" );
    assertSemanticException(statement, "insert overwrite table " + DB2 + ".tb2 select * from " + DB1 + ".tb1");
    statement.close();
    connection.close();
  }

  @Test
  public void testFullyQualifiedTableName() throws Exception{
    Connection connection;
    Statement statement;
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    exec(statement, "create database " + DB1);
    exec(statement, "create table " + DB1 + ".tb1(a int)");
    exec(statement, "DROP table " + DB1 + ".tb1");
    exec(statement, "create table " + DB1 + ".tb1(a int)");
    exec(statement, "use " + DB1);
    exec(statement, "drop table tb1");
  }

  @Test
  public void testExternalTables() throws Exception{
    createDb(ADMIN1, DB1);
    File externalTblDir = new File(dataDir, "exttab");
    assertTrue("Unable to create directory for external table test" , externalTblDir.mkdir());

    policyFile
        .addPermissionsToRole("create_db1", privileges.get("create_db1"))
        .addPermissionsToRole("all_uri", "server=server1->uri=file://" + dataDir.getPath())
        .addRolesToGroup(USERGROUP1, "create_db1", "all_uri")
        .addRolesToGroup(USERGROUP2, "create_db1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER2_1);
    Statement statement = context.createStatement(connection);
    assertSemanticException(statement, "create external table " + DB1 + ".tb1(a int) stored as " +
        "textfile location 'file:" + externalTblDir.getAbsolutePath() + "'");
    //Create external table on HDFS
    assertSemanticException(statement, "create external table " + DB1 + ".tb2(a int) location '/user/hive/warehouse/blah'");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    exec(statement, "create external table " + DB1 + ".tb1(a int) stored as " +
        "textfile location 'file:" + externalTblDir.getAbsolutePath() + "'");
    statement.close();
    connection.close();


  }

  @Test
  public void testCaseSensitivity() throws Exception {
    Statement statement = null;
    Connection connection = null;
    try {
      createDb(ADMIN1, DB1);
      String scratchLikeDir = context.getProperty(HiveConf.ConfVars.SCRATCHDIR.varname);
      LOGGER.info("scratch like dir = " + scratchLikeDir);
      String extParentDir = scratchLikeDir + "/ABC/hhh";
      String extTableDir = scratchLikeDir + "/abc/hhh";
      LOGGER.info("Creating extParentDir = " + extParentDir + ", extTableDir = " + extTableDir);
      dfs.assertCreateDir(extParentDir);
      dfs.assertCreateDir(extTableDir);

      if (! (extParentDir.toLowerCase().startsWith("hdfs://")
          || extParentDir.toLowerCase().startsWith("s3://")
          || extParentDir.contains("://"))) {
        String scheme = fileSystem.getUri().toString();
        LOGGER.info("scheme = " + scheme);
        extParentDir = scheme + extParentDir;
        extTableDir = scheme + extTableDir;
        LOGGER.info("Add scheme in extParentDir = " + extParentDir + ", extTableDir = " + extTableDir);
      }

      policyFile
          .addPermissionsToRole("all_db1", privileges.get("all_db1"))
          .addPermissionsToRole("all_uri", "server=server1->uri=" + extParentDir)
          .addRolesToGroup(USERGROUP1, "all_db1", "all_uri");
      writePolicyFile(policyFile);
      connection = context.createConnection(USER1_1);
      statement = context.createStatement(connection);
      assertSemanticException(statement,
          "create external table " + DB1 + ".tb1(a int) location '" + extTableDir + "'");
    } finally {
      if (statement != null) {
        statement.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }
}
