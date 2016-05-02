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

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Test all operations that require index on table alone (part 1)
 1. Create index : HiveOperation.CREATEINDEX
 2. Drop index : HiveOperation.DROPINDEX
 3. HiveOperation.ALTERINDEX_REBUILD
 4. TODO: HiveOperation.ALTERINDEX_PROPS
 */
public class TestOperationsPart1 extends AbstractTestWithStaticConfiguration {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestOperationsPart1.class);

  private PolicyFile policyFile;
  final String tableName = "tb1";

  static Map<String, String> privileges = new HashMap<String, String>();
  static {
    privileges.put("all_server", "server=server1->action=all");
    privileges.put("all_db1", "server=server1->db=" + DB1 + "->action=all");
    privileges.put("select_db1", "server=server1->db=" + DB1 + "->action=select");
    privileges.put("insert_db1", "server=server1->db=" + DB1 + "->action=insert");
    privileges.put("all_db2", "server=server1->db=" + DB2 + "->action=all");
    privileges.put("all_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=all");
    privileges.put("select_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=select");
    privileges.put("insert_db1_tb1", "server=server1->db=" + DB1 + "->table=tb1->action=insert");
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
    statement.execute("USE " + DB1);
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
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("all_db1", privileges.get("all_db1"))
        .addRolesToGroup(USERGROUP1, "all_db1");

    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE " + DB1 + ".tb1(a int)");
    statement.execute("USE " +DB1);
    statement.execute("ALTER TABLE tb1 RENAME TO tb2");
    statement.execute("ALTER DATABASE " + DB1 + " SET DBPROPERTIES ('comment'='comment')");

    statement.execute("DROP database " + DB1 + " cascade");
    statement.close();
    connection.close();

    //Negative case
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("select_db1", privileges.get("select_db1"))
        .addRolesToGroup(USERGROUP2, "select_db1");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "CREATE TABLE " + DB1 + ".tb1(a int)", semanticException);
    context.assertSentrySemanticException(statement, "ALTER DATABASE " + DB1 + " SET DBPROPERTIES ('comment'='comment')", semanticException);
    context.assertSentrySemanticException(statement, "DROP database " + DB1 + " cascade", semanticException);
    statement.close();
    connection.close();

  }
  /* SELECT/INSERT on DATABASE
   1. HiveOperation.DESCDATABASE
   */
  @Test
  public void testDescDB() throws Exception {
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("select_db1", privileges.get("select_db1"))
        .addPermissionsToRole("insert_db1", privileges.get("insert_db1"))
        .addRolesToGroup(USERGROUP1, "select_db1")
        .addRolesToGroup(USERGROUP2, "insert_db1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("describe database " + DB1);
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("describe database " + DB1);
    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("all_db1_tb1", privileges.get("all_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "all_db1_tb1");
    writePolicyFile(policyFile);
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "describe database " + DB1, semanticException);
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
    adminCreate(DB1, tableName, true);
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
    statement.execute("Use " + DB1);
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
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '10') ");

    //Negative test cases
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
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
    // TODO: fix alter table replace column testcase for Hive 0.13
    // assertSemanticException(statement,
    // "ALTER TABLE tb1 REPLACE COLUMNS (a int, c int)");

    //assertSemanticException(statement, "ALTER VIEW view1 SET TBLPROPERTIES ('comment' = 'new_comment')");

    assertSemanticException(statement, "CREATE INDEX tb1_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    //assertSemanticException(statement, "ALTER INDEX tb1_index ON tb1 REBUILD");

    assertSemanticException(statement, "DROP TABLE " + DB1 + ".tb1");

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
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
    // TODO: fix alter table replace column testcase for Hive 0.13
    // statement.execute("ALTER TABLE tb1 REPLACE COLUMNS (a int, c int)");

    //statement.execute("ALTER VIEW view1 SET TBLPROPERTIES ('comment' = 'new_comment')");

    statement.execute("CREATE INDEX tb1_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    //statement.execute("ALTER INDEX tb1_index ON tb1 REBUILD");

    assertSemanticException(statement, "ALTER TABLE tb1 RENAME TO tb2");


    statement.close();
    connection.close();

  }

  /*
  1. Analyze table (HiveOperation.QUERY) : select + insert on table
   */
  @Test
  public void testSelectAndInsertOnTable() throws Exception {
    adminCreate(DB1, tableName, true);
    adminCreatePartition();
    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1", "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
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
  9. HiveOperation.SHOW_TABLESTATUS
   */
  @Test
  public void testSelectOnTable() throws Exception {
    adminCreate(DB1, tableName, true);
    adminCreatePartition();
    policyFile
        .addPermissionsToRole("select_db1_tb1", privileges.get("select_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "select_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("select * from tb1");

    statement.executeQuery("SHOW Partitions tb1");
    statement.executeQuery("SHOW TBLPROPERTIES tb1");
    statement.executeQuery("SHOW CREATE TABLE tb1");
    statement.executeQuery("SHOW indexes on tb1");
    statement.executeQuery("SHOW COLUMNS from tb1");
    statement.executeQuery("SHOW functions '.*'");
    statement.executeQuery("SHOW TABLE EXTENDED IN " + DB1 + " LIKE 'tb*'");

    statement.executeQuery("DESCRIBE tb1");
    statement.executeQuery("DESCRIBE tb1 PARTITION (b=1)");

    statement.close();
    connection.close();

    //Negative case
    adminCreate(DB2, tableName);
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "insert_db1_tb1");
    writePolicyFile(policyFile);
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    context.assertSentrySemanticException(statement, "select * from tb1", semanticException);
    context.assertSentrySemanticException(statement,
        "SHOW TABLE EXTENDED IN " + DB2 + " LIKE 'tb*'", semanticException);

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
  9. HiveOperation.SHOW_TABLESTATUS
   */
  @Test
  public void testInsertOnTable() throws Exception {
    adminCreate(DB1, tableName, true);
    adminCreatePartition();
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    /*statement.execute("LOCK TABLE tb1 EXCLUSIVE");
    statement.execute("UNLOCK TABLE tb1");
    */
    statement.executeQuery("SHOW TBLPROPERTIES tb1");
    statement.executeQuery("SHOW CREATE TABLE tb1");
    statement.executeQuery("SHOW indexes on tb1");
    statement.executeQuery("SHOW COLUMNS from tb1");
    statement.executeQuery("SHOW functions '.*'");
    //statement.executeQuery("SHOW LOCKS tb1");
    statement.executeQuery("SHOW TABLE EXTENDED IN " + DB1 + " LIKE 'tb*'");

    //NoViableAltException
    //statement.executeQuery("SHOW transactions");
    //statement.executeQuery("SHOW compactions");
    statement.executeQuery("DESCRIBE tb1");
    statement.executeQuery("DESCRIBE tb1 PARTITION (b=1)");
    statement.executeQuery("SHOW Partitions tb1");


    statement.close();
    connection.close();
  }

  /**
   * Test all operations which require all on table + all on URI
   1. HiveOperation.ALTERTABLE_LOCATION
   2. HiveOperation.ALTERTABLE_ADDPARTS
   3. TODO: HiveOperation.ALTERPARTITION_LOCATION
   4. TODO: HiveOperation.ALTERTBLPART_SKEWED_LOCATION
   */
  @Test
  public void testAlterAllOnTableAndURI() throws Exception {
    adminCreate(DB1, tableName, true);
    String tabLocation = dfs.getBaseDir() + "/" + Math.random();
    policyFile
        .addPermissionsToRole("all_db1_tb1", privileges.get("all_db1_tb1"))
        .addPermissionsToRole("all_uri", "server=server1->uri=" + tabLocation)
        .addRolesToGroup(USERGROUP1, "all_db1_tb1", "all_uri")
        .addRolesToGroup(USERGROUP2, "all_db1_tb1");

    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '" + tabLocation + "/part'");
    statement.execute("MSCK REPAIR TABLE tb1");
    statement.close();
    connection.close();

    //Negative case: User2_1 has privileges on table but on on uri
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
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
    statement.execute("Use " + DB1);
    context.assertSentrySemanticException(statement, "ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'",
        semanticException);
    context.assertSentrySemanticException(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '"
        + tabLocation + "/part'", semanticException);
    context.assertSentrySemanticException(statement, "MSCK REPAIR TABLE tb1", semanticException);
    statement.close();
    connection.close();


  }

  @Test
  public void testDbPrefix() throws Exception {
    Connection connection;
    Statement statement;
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    //Create db1.table1
    statement.execute("create database " + DB1);
    statement.execute("create table " + DB1 + "." + tableName + "(a int)");
    //Create db2.table1
    statement.execute("create database " + DB2);
    statement.execute("create table " + DB2 + "." + tableName + "(a int)");
    //grant on db1.table1
    policyFile
      .addPermissionsToRole("all_db1_tb1", privileges.get("all_db1_tb1"))
      .addRolesToGroup(USERGROUP1, "all_db1_tb1");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    //Use db2
    statement.execute("use " + DB1);
    //MSCK db1.table1
    assertSemanticException(statement, "MSCK REPAIR TABLE " + DB2 + "." + tableName);
    statement.execute("MSCK REPAIR TABLE " + DB1 + "." + tableName);
  }
}
