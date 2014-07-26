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

import com.google.common.io.Resources;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class TestOperations extends AbstractTestWithStaticConfiguration {
  private PolicyFile policyFile;
  final String dbName = "db1";
  final String tableName = "tb1";
  final String semanticException = "SemanticException No valid privileges";

  static Map<String, String> privileges = new HashMap<String, String>();
  static {
    privileges.put("all_server", "server=server1->action=all");
    privileges.put("all_db1", "server=server1->db=db1->action=all");
    privileges.put("select_db1", "server=server1->db=db1->action=select");
    privileges.put("insert_db1", "server=server1->db=db1->action=insert");
    privileges.put("all_db2", "server=server1->db=db2->action=all");
    privileges.put("all_db1_tb1", "server=server1->db=db1->table=tb1->action=all");
    privileges.put("select_db1_tb1", "server=server1->db=db1->table=tb1->action=select");
    privileges.put("insert_db1_tb1", "server=server1->db=db1->table=tb1->action=insert");
    privileges.put("insert_db2_tb2", "server=server1->db=db2->table=tb2->action=insert");
    privileges.put("select_db1_view1", "server=server1->db=db1->table=view1->action=select");

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
    statement.execute("DROP DATABASE IF EXISTS " + db + " CASCADE");
    statement.execute("CREATE DATABASE " + db);
    if(table !=null) {
      if (partitioned) {
        statement.execute("CREATE table  " + db + "." + table + " (a string) PARTITIONED BY (b string)");
      } else{
        statement.execute("CREATE table  " + db + "." + table + " (a string)");
      }

    }
    statement.close();
    connection.close();
  }

  private void adminCreatePartition() throws Exception{
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE db1");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '1') ");
    statement.close();
    connection.close();
  }

  /* Test all operations that require all on Database alone
  1. Create table : HiveOperation.CREATETABLE
  2. Alter database : HiveOperation.ALTERDATABASE
  3. Drop database : HiveOperation.DROPDATABASE
   */
  @Test
  public void testAllOnDatabase() throws Exception{
    adminCreate(dbName, null);
    policyFile
        .addPermissionsToRole("all_db1", privileges.get("all_db1"))
        .addRolesToGroup(USERGROUP1, "all_db1");

    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE db1.tb1(a int)");
    statement.execute("ALTER DATABASE db1 SET DBPROPERTIES ('comment'='comment')");
    statement.execute("DROP database db1 cascade");
    statement.close();
    connection.close();

    //Negative case
    adminCreate(dbName, null);
    policyFile
        .addPermissionsToRole("select_db1", privileges.get("select_db1"))
        .addRolesToGroup(USERGROUP2, "select_db1");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "CREATE TABLE db1.tb1(a int)", semanticException);
    context.assertSentrySemanticException(statement, "ALTER DATABASE db1 SET DBPROPERTIES ('comment'='comment')", semanticException);
    context.assertSentrySemanticException(statement, "DROP database db1 cascade", semanticException);
    statement.close();
    connection.close();

  }
  /* SELECT/INSERT on DATABASE
   1. HiveOperation.DESCDATABASE
   */
  @Test
  public void testDescDB() throws Exception {
    adminCreate(dbName, null);
    policyFile
        .addPermissionsToRole("select_db1", privileges.get("select_db1"))
        .addPermissionsToRole("insert_db1", privileges.get("insert_db1"))
        .addRolesToGroup(USERGROUP1, "select_db1")
        .addRolesToGroup(USERGROUP2, "insert_db1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("describe database db1");
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("describe database db1");
    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("all_db1_tb1", privileges.get("all_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "all_db1_tb1");
    writePolicyFile(policyFile);
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "describe database db1", semanticException);
    statement.close();
    connection.close();

  }

  private void assertSemanticException(Statement stmt, String command) throws SQLException{
    context.assertSentrySemanticException(stmt,command, semanticException);
  }
  /* Test all operations that require all on table alone
  1. Create index : HiveOperation.CREATEINDEX
  2. Drop index : HiveOperation.DROPINDEX
  3. Alter table add partition : HiveOperation.ALTERTABLE_ADDPARTS
  4. HiveOperation.ALTERTABLE_PROPERTIES
  5. HiveOperation.ALTERTABLE_SERDEPROPERTIES
  6. HiveOperation.ALTERTABLE_CLUSTER_SORT
  7. HiveOperation.ALTERTABLE_TOUCH
  8. HiveOperation.ALTERTABLE_PROTECTMODE
  9. HiveOperation.ALTERTABLE_FILEFORMAT
  10. HiveOperation.ALTERTABLE_RENAMEPART
  11. HiveOperation.ALTERPARTITION_SERDEPROPERTIES
  12. TODO: archive partition
  13. TODO: unarchive partition
  14. HiveOperation.ALTERPARTITION_FILEFORMAT
  15. TODO: partition touch (is it same as  HiveOperation.ALTERTABLE_TOUCH?)
  16. HiveOperation.ALTERPARTITION_PROTECTMODE
  17. HiveOperation.ALTERTABLE_DROPPARTS
  18. HiveOperation.ALTERTABLE_RENAMECOL
  19. HiveOperation.ALTERTABLE_ADDCOLS
  20. HiveOperation.ALTERTABLE_REPLACECOLS
  21. TODO: HiveOperation.ALTERVIEW_PROPERTIES
  22. HiveOperation.CREATEINDEX
  23. TODO: HiveOperation.ALTERINDEX_REBUILD
  21. HiveOperation.ALTERTABLE_RENAME
  22. HiveOperation.DROPTABLE
  23. TODO: HiveOperation.ALTERTABLE_SERIALIZER
  24. TODO: HiveOperation.ALTERPARTITION_SERIALIZER
  25. TODO: HiveOperation.ALTERINDEX_PROPS
  */
  @Test
  public void testAllOnTable() throws Exception{
    adminCreate(dbName, tableName, true);
    policyFile
        .addPermissionsToRole("all_db1_tb1", privileges.get("all_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "all_db1_tb1")
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection;
    Statement statement;
    //Negative test cases
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    assertSemanticException(statement, "CREATE INDEX table01_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    assertSemanticException(statement, "DROP INDEX table01_index ON tb1");
    assertSemanticException(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '1') ");
    assertSemanticException(statement, "ALTER TABLE tb1 SET TBLPROPERTIES ('comment' = 'new_comment')");
    assertSemanticException(statement, "ALTER TABLE tb1 SET SERDEPROPERTIES ('field.delim' = ',')");
    assertSemanticException(statement, "ALTER TABLE tb1 CLUSTERED BY (a) SORTED BY (a) INTO 1 BUCKETS");
    assertSemanticException(statement, "ALTER TABLE tb1 TOUCH");
    assertSemanticException(statement, "ALTER TABLE tb1 ENABLE NO_DROP");
    assertSemanticException(statement, "ALTER TABLE tb1 DISABLE OFFLINE");
    assertSemanticException(statement, "ALTER TABLE tb1 SET FILEFORMAT RCFILE");

    //Setup
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '10') ");

    //Negative test cases
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) RENAME TO PARTITION (b = 2)");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) SET SERDEPROPERTIES ('field.delim' = ',')");
    //assertSemanticException(statement, "ALTER TABLE tb1 ARCHIVE PARTITION (b = 2)");
    //assertSemanticException(statement, "ALTER TABLE tb1 UNARCHIVE PARTITION (b = 2)");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) SET FILEFORMAT RCFILE");
    assertSemanticException(statement, "ALTER TABLE tb1 TOUCH PARTITION (b = 10)");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) DISABLE NO_DROP");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) DISABLE OFFLINE");
    assertSemanticException(statement, "ALTER TABLE tb1 DROP PARTITION (b = 10)");

    assertSemanticException(statement, "ALTER TABLE tb1 CHANGE COLUMN a c int");
    assertSemanticException(statement, "ALTER TABLE tb1 ADD COLUMNS (a int)");
    assertSemanticException(statement, "ALTER TABLE tb1 REPLACE COLUMNS (a int, c int)");

    //assertSemanticException(statement, "ALTER VIEW view1 SET TBLPROPERTIES ('comment' = 'new_comment')");

    assertSemanticException(statement, "CREATE INDEX tb1_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    //assertSemanticException(statement, "ALTER INDEX tb1_index ON tb1 REBUILD");
    assertSemanticException(statement, "ALTER TABLE tb1 RENAME TO tb2");

    assertSemanticException(statement, "DROP TABLE db1.tb1");

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("CREATE INDEX table01_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    statement.execute("DROP INDEX table01_index ON tb1");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '1') ");
    statement.execute("ALTER TABLE tb1 SET TBLPROPERTIES ('comment' = 'new_comment')");
    statement.execute("ALTER TABLE tb1 SET SERDEPROPERTIES ('field.delim' = ',')");
    statement.execute("ALTER TABLE tb1 CLUSTERED BY (a) SORTED BY (a) INTO 1 BUCKETS");
    statement.execute("ALTER TABLE tb1 TOUCH");
    statement.execute("ALTER TABLE tb1 ENABLE NO_DROP");
    statement.execute("ALTER TABLE tb1 DISABLE NO_DROP");
    statement.execute("ALTER TABLE tb1 DISABLE OFFLINE");
    statement.execute("ALTER TABLE tb1 SET FILEFORMAT RCFILE");

    statement.execute("ALTER TABLE tb1 PARTITION (b = 1) RENAME TO PARTITION (b = 2)");
    statement.execute("ALTER TABLE tb1 PARTITION (b = 2) SET SERDEPROPERTIES ('field.delim' = ',')");
    //statement.execute("ALTER TABLE tb1 ARCHIVE PARTITION (b = 2)");
    //statement.execute("ALTER TABLE tb1 UNARCHIVE PARTITION (b = 2)");
    statement.execute("ALTER TABLE tb1 PARTITION (b = 2) SET FILEFORMAT RCFILE");
    statement.execute("ALTER TABLE tb1 TOUCH PARTITION (b = 2)");
    statement.execute("ALTER TABLE tb1 PARTITION (b = 2) DISABLE NO_DROP");
    statement.execute("ALTER TABLE tb1 PARTITION (b = 2) DISABLE OFFLINE");
    statement.execute("ALTER TABLE tb1 DROP PARTITION (b = 2)");

    statement.execute("ALTER TABLE tb1 CHANGE COLUMN a c int");
    statement.execute("ALTER TABLE tb1 ADD COLUMNS (a int)");
    statement.execute("ALTER TABLE tb1 REPLACE COLUMNS (a int, c int)");

    //statement.execute("ALTER VIEW view1 SET TBLPROPERTIES ('comment' = 'new_comment')");

    statement.execute("CREATE INDEX tb1_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    //statement.execute("ALTER INDEX tb1_index ON tb1 REBUILD");
    statement.execute("ALTER TABLE tb1 RENAME TO tb2");

    //Drop of the new tablename works only when Hive meta store syncs the alters with the sentry privileges.
    //This is currently not set for pseudo cluster runs
    if( hiveServer2Type.equals(HiveServerFactory.HiveServer2Type.UnmanagedHiveServer2)) {
      statement.execute("DROP TABLE db1.tb2");
    } else {
      statement.execute("DROP TABLE db1.tb1");
    }

    statement.close();
    connection.close();

  }

  /*
  1. Analyze table (HiveOperation.QUERY) : select + insert on table
   */
  @Test
  public void testSelectAndInsertOnTable() throws Exception {
    adminCreate(dbName, tableName, true);
    adminCreatePartition();
    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("ANALYZE TABLE tb1 PARTITION (b='1' ) COMPUTE STATISTICS");
    statement.close();
    connection.close();
  }

  /* Operations which require select on table alone
  1. HiveOperation.QUERY
  2. HiveOperation.SHOW_TBLPROPERTIES
  3. HiveOperation.SHOW_CREATETABLE
  4. HiveOperation.SHOWINDEXES
  5. HiveOperation.SHOWCOLUMNS
  6. Describe tb1 : HiveOperation.DESCTABLE5.
  7. HiveOperation.SHOWPARTITIONS
  8. TODO: show functions?
   */
  @Test
  public void testSelectOnTable() throws Exception {
    adminCreate(dbName, tableName, true);
    adminCreatePartition();
    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("select * from tb1");

    statement.executeQuery("SHOW Partitions tb1");
    statement.executeQuery("SHOW TBLPROPERTIES tb1");
    statement.executeQuery("SHOW CREATE TABLE tb1");
    statement.executeQuery("SHOW indexes on tb1");
    statement.executeQuery("SHOW COLUMNS from tb1");
    statement.executeQuery("SHOW functions '.*'");

    statement.executeQuery("DESCRIBE tb1");
    statement.executeQuery("DESCRIBE tb1 PARTITION (b=1)");

    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "insert_db1_tb1");
    writePolicyFile(policyFile);
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    context.assertSentrySemanticException(statement, "select * from tb1", semanticException);

    statement.close();
    connection.close();


  }

  /* Operations which require insert on table alone
  1. HiveOperation.SHOW_TBLPROPERTIES
  2. HiveOperation.SHOW_CREATETABLE
  3. HiveOperation.SHOWINDEXES
  4. HiveOperation.SHOWCOLUMNS
  5. HiveOperation.DESCTABLE
  6. HiveOperation.SHOWPARTITIONS
  7. TODO: show functions?
  8. TODO: lock, unlock, Show locks
   */
  @Test
  public void testInsertOnTable() throws Exception {
    adminCreate(dbName, tableName, true);
    adminCreatePartition();
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use db1");
    /*statement.execute("LOCK TABLE tb1 EXCLUSIVE");
    statement.execute("UNLOCK TABLE tb1");
    */
    statement.executeQuery("SHOW TBLPROPERTIES tb1");
    statement.executeQuery("SHOW CREATE TABLE tb1");
    statement.executeQuery("SHOW indexes on tb1");
    statement.executeQuery("SHOW COLUMNS from tb1");
    statement.executeQuery("SHOW functions '.*'");
    //statement.executeQuery("SHOW LOCKS tb1");

    //NoViableAltException
    //statement.executeQuery("SHOW transactions");
    //statement.executeQuery("SHOW compactions");
    statement.executeQuery("DESCRIBE tb1");
    statement.executeQuery("DESCRIBE tb1 PARTITION (b=1)");
    statement.executeQuery("SHOW Partitions tb1");


    statement.close();
    connection.close();
  }

  /* Test all operations which require all on table + all on URI
   1. HiveOperation.ALTERTABLE_LOCATION
   2. HiveOperation.ALTERTABLE_ADDPARTS
   3. TODO: HiveOperation.ALTERPARTITION_LOCATION
   4. TODO: HiveOperation.ALTERTBLPART_SKEWED_LOCATION
   */
  @Test
  public void testAlterAllOnTableAndURI() throws Exception {
    adminCreate(dbName, tableName, true);
    String tabLocation = dfs.getBaseDir() + "/" + Math.random();
    policyFile
        .addPermissionsToRole("all_db1_tb1", privileges.get("all_db1_tb1"))
        .addPermissionsToRole("all_uri", "server=server1->uri=" + tabLocation)
        .addRolesToGroup(USERGROUP1, "all_db1_tb1", "all_uri")
        .addRolesToGroup(USERGROUP2, "all_db1_tb1");

    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '" + tabLocation + "/part'");
    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "insert_db1_tb1", "all_uri");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    context.assertSentrySemanticException(statement, "ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'",
        semanticException);
    context.assertSentrySemanticException(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '"
        + tabLocation + "/part'", semanticException);
    statement.close();
    connection.close();


  }

  /* All on Database and select on table
  1. Create view :  HiveOperation.CREATEVIEW
   */
  @Test
  public void testCreateView() throws Exception {
    adminCreate(dbName, tableName);
    adminCreate("db2", null);
    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("all_db2", privileges.get("all_db2"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "all_db2");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("use db2");
    statement.execute("create view view1 as select a from db1.tb1");
    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "insert_db1_tb1", "all_db2");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use db2");
    context.assertSentrySemanticException(statement, "create view view1 as select a from db1.tb1",
        semanticException);
    statement.close();
    connection.close();


  }

  /*
   1. HiveOperation.IMPORT : All on db + all on URI
   2. HiveOperation.EXPORT : SELECT on table + all on uri
   */

  @Test
  public void testExportImport() throws Exception {
    File dataFile;
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    dropDb(ADMIN1, dbName);
    createDb(ADMIN1, dbName);
    createTable(ADMIN1, dbName, dataFile, tableName);
    String location = dfs.getBaseDir() + "/" + Math.random();
    policyFile
        .addPermissionsToRole("all_db1", privileges.get("all_db1"))
        .addPermissionsToRole("all_uri", "server=server1->uri="+ location)
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("insert_db1", privileges.get("insert_db1"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "all_uri")
        .addRolesToGroup(USERGROUP2, "all_db1", "all_uri")
        .addRolesToGroup(USERGROUP3, "insert_db1", "all_uri");
    writePolicyFile(policyFile);
    Connection connection;
    Statement statement;

    //Negative case
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    context.assertSentrySemanticException(statement, "export table tb1 to '" + location + "'",
        semanticException);
    statement.close();
    connection.close();

    //Positive
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("export table tb1 to '" + location + "'" );
    statement.close();
    connection.close();

    //Negative
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    context.assertSentrySemanticException(statement, "import table tb2 from '" + location + "'",
        semanticException);
    statement.close();
    connection.close();

    //Positive
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("import table tb2 from '" + location + "'");
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

    adminCreate(dbName, tableName);

    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addPermissionsToRole("all_uri", "server=server1->uri=file://" + dataDir)
        .addRolesToGroup(USERGROUP1, "insert_db1_tb1", "all_uri");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("load data local inpath '" + dataFile.getPath() + "' into table tb1" );
    statement.close();
    connection.close();
  }

  /*
  1. HiveOperation.CREATETABLE_AS_SELECT : All on db + select on table
   */
  @Test
  public void testCTAS() throws Exception {
    adminCreate(dbName, tableName);
    adminCreate("db2", null);

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use db1");
    statement.execute("create view view1 as select a from db1.tb1");
    statement.close();
    connection.close();

    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("select_db1_view1", privileges.get("select_db1_view1"))
        .addPermissionsToRole("all_db2", privileges.get("all_db2"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "all_db2")
        .addRolesToGroup(USERGROUP2, "select_db1_view1", "all_db2");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use db2");
    statement.execute("create table tb2 as select a from db1.tb1" );
    context.assertSentrySemanticException(statement, "create table tb3 as select a from db1.view1",
        semanticException);
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use db2");
    statement.execute("create table tb3 as select a from db1.view1" );
    context.assertSentrySemanticException(statement, "create table tb4 as select a from db1.tb1",
        semanticException);

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

    dropDb(ADMIN1, dbName);
    dropDb(ADMIN1, "db2");
    createDb(ADMIN1, dbName);
    createDb(ADMIN1, "db2");
    createTable(ADMIN1, dbName, dataFile, tableName);
    createTable(ADMIN1, "db2", null, "tb2");
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
    assertSemanticException(statement, "insert overwrite directory '" + location + "' select * from db1.tb1" );
    statement.execute("insert overwrite table db2.tb2 select * from db1.tb1");
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("insert overwrite directory '" + location + "' select * from db1.tb1" );
    assertSemanticException(statement,"insert overwrite table db2.tb2 select * from db1.tb1");
    statement.close();
    connection.close();
  }

  @Test
  public void testFullyQualifiedTableName() throws Exception{
    Connection connection;
    Statement statement;
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("create database db1");
    statement.execute("create table db1.tb1(a int)");
    statement.execute("DROP table db1.tb1");
    statement.execute("create table db1.tb1(a int)");
    statement.execute("use db1");
    statement.execute("drop table tb1");
  }

}
