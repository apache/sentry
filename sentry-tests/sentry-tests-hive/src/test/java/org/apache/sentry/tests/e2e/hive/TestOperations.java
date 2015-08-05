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
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestOperations extends AbstractTestWithStaticConfiguration {
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

  /* Test all operations that require create on Server
  1. Create database : HiveOperation.CREATEDATABASE
   */
  @Test
  public void testCreateOnServer() throws Exception{
    policyFile
        .addPermissionsToRole("create_server", privileges.get("create_server"))
        .addRolesToGroup(USERGROUP1, "create_server");

    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Create database " + DB2);
    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("create_db1", privileges.get("create_db1"))
        .addRolesToGroup(USERGROUP2, "create_db1");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "CREATE database " + DB1, semanticException);
    statement.close();
    connection.close();

  }

  /* Test all operations that require create on Database alone
  1. Create table : HiveOperation.CREATETABLE
  */
  @Test
  public void testCreateOnDatabase() throws Exception{
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("create_db1", privileges.get("create_db1"))
        .addPermissionsToRole("all_db1", privileges.get("all_db1"))
        .addRolesToGroup(USERGROUP1, "create_db1")
        .addRolesToGroup(USERGROUP2, "all_db1");

    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE TABLE " + DB1 + ".tb2(a int)");
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("CREATE TABLE " + DB1 + ".tb3(a int)");

    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("all_db1_tb1", privileges.get("select_db1"))
        .addRolesToGroup(USERGROUP3, "all_db1_tb1");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "CREATE TABLE " + DB1 + ".tb1(a int)", semanticException);
    statement.close();
    connection.close();
  }

  /* Test all operations that require drop on Database alone
  1. Drop database : HiveOperation.DROPDATABASE
  */
  @Test
  public void testDropOnDatabase() throws Exception{
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("drop_db1", privileges.get("drop_db1"))
        .addRolesToGroup(USERGROUP1, "drop_db1");

    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE " + DB1);
    statement.close();
    connection.close();

    policyFile
        .addPermissionsToRole("all_db1", privileges.get("all_db1"))
        .addRolesToGroup(USERGROUP2, "all_db1");
    writePolicyFile(policyFile);

    adminCreate(DB1, null);

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE " + DB1);

    statement.close();
    connection.close();

    //Negative case
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("select_db1", privileges.get("select_db1"))
        .addRolesToGroup(USERGROUP3, "select_db1");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "drop database " + DB1, semanticException);
    statement.close();
    connection.close();
  }

  /* Test all operations that require alter on Database alone
  1. Alter database : HiveOperation.ALTERDATABASE
   */
  @Test
  public void testAlterOnDatabase() throws Exception{
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("alter_db1", privileges.get("alter_db1"))
        .addPermissionsToRole("all_db1", privileges.get("all_db1"))
        .addRolesToGroup(USERGROUP2, "all_db1")
        .addRolesToGroup(USERGROUP1, "alter_db1");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("ALTER DATABASE " + DB1 + " SET DBPROPERTIES ('comment'='comment')");

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("ALTER DATABASE " + DB1 + " SET DBPROPERTIES ('comment'='comment')");
    statement.close();
    connection.close();

    //Negative case
    adminCreate(DB1, null);
    policyFile
        .addPermissionsToRole("select_db1", privileges.get("select_db1"))
        .addRolesToGroup(USERGROUP3, "select_db1");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    context.assertSentrySemanticException(statement, "ALTER DATABASE " + DB1 + " SET DBPROPERTIES ('comment'='comment')", semanticException);
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

  /* Test all operations that require alter on table
  1. HiveOperation.ALTERTABLE_PROPERTIES
  2. HiveOperation.ALTERTABLE_SERDEPROPERTIES
  3. HiveOperation.ALTERTABLE_CLUSTER_SORT
  4. HiveOperation.ALTERTABLE_TOUCH
  5. HiveOperation.ALTERTABLE_PROTECTMODE
  6. HiveOperation.ALTERTABLE_FILEFORMAT
  7. HiveOperation.ALTERTABLE_RENAMEPART
  8. HiveOperation.ALTERPARTITION_SERDEPROPERTIES
  9. TODO: archive partition
  10. TODO: unarchive partition
  11. HiveOperation.ALTERPARTITION_FILEFORMAT
  12. TODO: partition touch (is it same as  HiveOperation.ALTERTABLE_TOUCH?)
  13. HiveOperation.ALTERPARTITION_PROTECTMODE
  14. HiveOperation.ALTERTABLE_RENAMECOL
  15. HiveOperation.ALTERTABLE_ADDCOLS
  16. HiveOperation.ALTERTABLE_REPLACECOLS
  17. TODO: HiveOperation.ALTERVIEW_PROPERTIES
  18. TODO: HiveOperation.ALTERTABLE_SERIALIZER
  19. TODO: HiveOperation.ALTERPARTITION_SERIALIZER
   */
  @Test
  public void testAlterTable() throws Exception {
    adminCreate(DB1, tableName, true);
    policyFile
        .addPermissionsToRole("alter_db1_tb1", privileges.get("alter_db1_tb1"))
        .addPermissionsToRole("alter_db1_ptab", privileges.get("alter_db1_ptab"))
        .addRolesToGroup(USERGROUP1, "alter_db1_tb1", "alter_db1_ptab")
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection;
    Statement statement;
    //Setup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '10') ");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '1') ");
    statement.execute("CREATE TABLE ptab (a int) STORED AS PARQUET");
    //Negative test cases
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 SET TBLPROPERTIES ('comment' = 'new_comment')");
    assertSemanticException(statement, "ALTER TABLE tb1 SET SERDEPROPERTIES ('field.delim' = ',')");
    assertSemanticException(statement, "ALTER TABLE tb1 CLUSTERED BY (a) SORTED BY (a) INTO 1 BUCKETS");
    assertSemanticException(statement, "ALTER TABLE tb1 TOUCH");
    assertSemanticException(statement, "ALTER TABLE tb1 ENABLE NO_DROP");
    assertSemanticException(statement, "ALTER TABLE tb1 DISABLE OFFLINE");
    assertSemanticException(statement, "ALTER TABLE tb1 SET FILEFORMAT RCFILE");

    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) RENAME TO PARTITION (b = 2)");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) SET SERDEPROPERTIES ('field.delim' = ',')");
    //assertSemanticException(statement, "ALTER TABLE tb1 ARCHIVE PARTITION (b = 2)");
    //assertSemanticException(statement, "ALTER TABLE tb1 UNARCHIVE PARTITION (b = 2)");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) SET FILEFORMAT RCFILE");
    assertSemanticException(statement, "ALTER TABLE tb1 TOUCH PARTITION (b = 10)");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) DISABLE NO_DROP");
    assertSemanticException(statement, "ALTER TABLE tb1 PARTITION (b = 10) DISABLE OFFLINE");

    assertSemanticException(statement, "ALTER TABLE tb1 CHANGE COLUMN a c int");
    assertSemanticException(statement, "ALTER TABLE tb1 ADD COLUMNS (a int)");
    assertSemanticException(statement, "ALTER TABLE ptab REPLACE COLUMNS (a int, c int)");
    assertSemanticException(statement, "MSCK REPAIR TABLE tb1");

    //assertSemanticException(statement, "ALTER VIEW view1 SET TBLPROPERTIES ('comment' = 'new_comment')");


    statement.close();
    connection.close();

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 SET TBLPROPERTIES ('comment' = 'new_comment')");
    statement.execute("ALTER TABLE tb1 SET SERDEPROPERTIES ('field.delim' = ',')");
    statement.execute("ALTER TABLE tb1 CLUSTERED BY (a) SORTED BY (a) INTO 1 BUCKETS");
    statement.execute("ALTER TABLE tb1 TOUCH");
    statement.execute("ALTER TABLE tb1 ENABLE NO_DROP");
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

    statement.execute("ALTER TABLE tb1 CHANGE COLUMN a c int");
    statement.execute("ALTER TABLE tb1 ADD COLUMNS (a int)");
    statement.execute("ALTER TABLE ptab REPLACE COLUMNS (a int, c int)");
    statement.execute("MSCK REPAIR TABLE tb1");

    //statement.execute("ALTER VIEW view1 SET TBLPROPERTIES ('comment' = 'new_comment')");

    statement.close();
    connection.close();
  }

  /* Test all operations that require index on table alone
  1. Create index : HiveOperation.CREATEINDEX
  2. Drop index : HiveOperation.DROPINDEX
  3. HiveOperation.ALTERINDEX_REBUILD
  4. TODO: HiveOperation.ALTERINDEX_PROPS
  */
  @Test
  public void testIndexTable() throws Exception {
    adminCreate(DB1, tableName, true);
    policyFile
        .addPermissionsToRole("index_db1_tb1", privileges.get("index_db1_tb1"))
        .addRolesToGroup(USERGROUP1, "index_db1_tb1")
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP2, "insert_db1_tb1");
    writePolicyFile(policyFile);

    Connection connection;
    Statement statement;

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("CREATE INDEX table01_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    statement.execute("ALTER INDEX table01_index ON tb1 REBUILD");
    statement.close();
    connection.close();

    //Negative case
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    assertSemanticException(statement, "CREATE INDEX table02_index ON TABLE tb1 (a) AS 'COMPACT' WITH DEFERRED REBUILD");
    assertSemanticException(statement, "ALTER INDEX table01_index ON tb1 REBUILD");
    assertSemanticException(statement, "DROP INDEX table01_index ON tb1");
    statement.close();
    connection.close();

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("DROP INDEX table01_index ON tb1");
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
    statement.execute("Use " + DB1);
    assertSemanticException(statement, "drop table " + tableName);

    statement.close();
    connection.close();

    //Positive cases
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("drop table " + tableName);

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
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '10') ");

    //Negative case
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 DROP PARTITION (b = 10)");

    //Positive case
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 DROP PARTITION (b = 10)");
    statement.close();
    connection.close();
  }

  /*
   1. HiveOperation.ALTERTABLE_RENAME
   */
  @Test
  public void renameTable() throws Exception {
    adminCreate(DB1, tableName);
    policyFile
        .addPermissionsToRole("alter_db1_tb1", privileges.get("alter_db1_tb1"))
        .addPermissionsToRole("create_db1", privileges.get("create_db1"))
        .addRolesToGroup(USERGROUP1, "alter_db1_tb1", "create_db1")
        .addRolesToGroup(USERGROUP2, "create_db1")
        .addRolesToGroup(USERGROUP3, "alter_db1_tb1");

    writePolicyFile(policyFile);

    Connection connection;
    Statement statement;

    //Negative cases
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 RENAME TO tb2");
    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 RENAME TO tb2");
    statement.close();
    connection.close();

    //Positive case
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 RENAME TO tb2");
    statement.close();
    connection.close();
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
    statement.execute("USE " + DB1);
    assertSemanticException(statement, "ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'");
    assertSemanticException(statement, "ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '" + tabLocation + "/part'");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '1') ");

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("ALTER TABLE tb1 SET LOCATION '" + tabLocation + "'");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '3') LOCATION '" + tabLocation + "/part'");
    statement.execute("ALTER TABLE tb1 ADD IF NOT EXISTS PARTITION (b = '10') ");
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
    statement.execute("use " + DB2);
    statement.execute("create view view1 as select a from " + DB1 + ".tb1");
    statement.close();
    connection.close();

    //Negative case
    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addRolesToGroup(USERGROUP3, "insert_db1_tb1", "create_db2");
    writePolicyFile(policyFile);

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB2);
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
    statement.execute("Use " + DB1);
    context.assertSentrySemanticException(statement, "export table tb1 to '" + location + "'",
        semanticException);
    statement.close();
    connection.close();

    //Positive
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("export table tb1 to '" + location + "'" );
    statement.close();
    connection.close();

    //Negative
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    context.assertSentrySemanticException(statement, "import table tb2 from '" + location + "'",
        semanticException);
    statement.close();
    connection.close();

    //Positive
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
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

    adminCreate(DB1, tableName);

    policyFile
        .addPermissionsToRole("insert_db1_tb1", privileges.get("insert_db1_tb1"))
        .addPermissionsToRole("all_uri", "server=server1->uri=file://" + dataDir)
        .addRolesToGroup(USERGROUP1, "insert_db1_tb1", "all_uri");
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("Use " + DB1);
    statement.execute("load data local inpath '" + dataFile.getPath() + "' into table tb1" );
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
    statement.execute("Use " + DB1);
    statement.execute("create view view1 as select a from " + DB1 + ".tb1");
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
    statement.execute("Use " + DB2);
    statement.execute("create table tb2 as select a from " + DB1 + ".tb1");
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
    statement.execute("Use " + DB2);
    statement.execute("create table tb3 as select a from " + DB1 + ".view1" );
    context.assertSentrySemanticException(statement, "create table tb4 as select a from " + DB1 + ".tb1",
      semanticException);

    statement.close();
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    //CTAS is valid with URI
    statement.execute("Use " + DB2);
    statement.execute("create table tb4 location '" + location +
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
    assertSemanticException(statement, "insert overwrite directory '" + location + "' select * from " + DB1 + ".tb1" );
    statement.execute("insert overwrite table " + DB2 + ".tb2 select * from " + DB1 + ".tb1");
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("insert overwrite directory '" + location + "' select * from " + DB1 + ".tb1" );
    assertSemanticException(statement,"insert overwrite table " + DB2 + ".tb2 select * from " + DB1 + ".tb1");
    statement.close();
    connection.close();
  }

  @Test
  public void testFullyQualifiedTableName() throws Exception{
    Connection connection;
    Statement statement;
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("create database " + DB1);
    statement.execute("create table " + DB1 + ".tb1(a int)");
    statement.execute("DROP table " + DB1 + ".tb1");
    statement.execute("create table " + DB1 + ".tb1(a int)");
    statement.execute("use " + DB1);
    statement.execute("drop table tb1");
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
    statement.execute("create external table " + DB1 + ".tb1(a int) stored as " +
        "textfile location 'file:" + externalTblDir.getAbsolutePath() + "'");
    statement.close();
    connection.close();


  }
}
