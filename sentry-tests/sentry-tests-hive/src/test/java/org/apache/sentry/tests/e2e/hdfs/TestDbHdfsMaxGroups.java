package org.apache.sentry.tests.e2e.hdfs;

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

import com.google.common.base.Strings;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;

import static org.junit.Assume.assumeThat;
import static org.hamcrest.Matchers.not;

import org.apache.sentry.tests.e2e.hive.PrivilegeResultSet;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sentry-583:
 * Add boundary condition test coverage to HDFS synchronization test suite around max #of groups;
 * Normally, HDFS ACLs has a limit of 32 entries per object (HDFS-5617), but this limit should
 * not be enforced when using Sentry HDFS synchronization.
 */
public class TestDbHdfsMaxGroups extends TestDbHdfsBase {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestDbHdfsMaxGroups.class);
  private static final int MAX_NUM_OF_GROUPS = 33;
  protected static final String TEST_TBL = "tbl";
  protected static final String TEST_ROLE1 = "test_hdfs_max_group_role1";
  protected static final String TEST_ROLE2 = "test_hdfs_max_group_role2";
  protected static final String TEST_ROLE3 = "test_hdfs_max_group_role3";

  /**
   * Test Db and tbl level acls are synced up to db, tbl and par paths
   * @throws Exception
   */
  @Test
  public void testIntDbTblMaxAclsWithGroups() throws Exception {
    final String TEST_DB = "test_hdfs_max_group_int_db";
    String extDbDir = Path.getPathWithoutSchemeAndAuthority(new Path(metastoreDir)) + "/" + TEST_DB + ".db";
    LOGGER.info("extDbDir = " + extDbDir);
    dropRecreateDbTblRl(TEST_DB, TEST_TBL);
    testMaxGroupsDbTblHelper(extDbDir, TEST_DB);
  }

  /**
   * Test col level acls should not sync up to db, tbl and par paths
   * @throws Exception
   */
  @Test
  public void testIntColMaxAclsWithGroups() throws Exception {
    final String TEST_DB = "test_hdfs_max_group_int_col_db";
    String extDbDir = Path.getPathWithoutSchemeAndAuthority(new Path(metastoreDir)) + "/" + TEST_DB + ".db";
    LOGGER.info("extDbDir = " + extDbDir);
    dropRecreateDbTblRl(TEST_DB, TEST_TBL);
    testMaxGroupsColHelper(extDbDir, TEST_DB);
  }

  /**
   * Test Db and tbl level acls are synced up to db, tbl and par paths
   * The path is pre-configured in "sentry.hdfs.integration.path.prefixes"
   * @throws Exception
   */
  @Test
  public void testExtMaxAclsWithGroups() throws Exception {
    final String TEST_DB = "test_hdfs_max_group_ext_db";
    assumeThat(Strings.isNullOrEmpty(testExtPathDir), not(true));
    String extDbDir = Path.getPathWithoutSchemeAndAuthority(new Path(testExtPathDir)) + "/" + TEST_DB;
    LOGGER.info("extDbDir = " + extDbDir);
    Path extDbPath = new Path(extDbDir);
    kinitFromKeytabFile(dfsAdmin, getKeyTabFileFullPath(dfsAdmin));
    if (fileSystem.exists(extDbPath)) {
      LOGGER.info("Deleting " + extDbDir);
      fileSystem.delete(extDbPath, true);
    }
    dropRecreateDbTblRl(extDbDir, TEST_DB, TEST_TBL);
    testMaxGroupsDbTblHelper(extDbDir, TEST_DB);
  }

  /**
   * A negative test case where path is not in prefix list.
   * In this case, acls should not be applied to db, tbl and par paths
   * @throws Exception
   */
  @Test
  public void testPathNotInPrefix() throws Exception {
    final String TEST_DB = "test_hdfs_max_group_bad_db";
    String extDbDir = Path.getPathWithoutSchemeAndAuthority(new Path(scratchLikeDir)) + "/" + TEST_DB;
    LOGGER.info("extDbDir = " + extDbDir);
    Path extDbPath = new Path(extDbDir);
    kinitFromKeytabFile(dfsAdmin, getKeyTabFileFullPath(dfsAdmin));
    if (fileSystem.exists(extDbPath)) {
      fileSystem.delete(extDbPath, true);
    }
    dropRecreateDbTblRl(extDbDir, TEST_DB, TEST_TBL);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    exec(statement, "USE " + TEST_DB);
    dropRecreateRole(statement, TEST_ROLE1);
    String dbgrp = "dbgrp";
    exec(statement, "GRANT ALL ON DATABASE " + TEST_DB + " TO ROLE " + TEST_ROLE1);
    exec(statement, "GRANT ROLE " + TEST_ROLE1 + " TO GROUP " + dbgrp);

    context.close();

    List<AclEntry> acls = new ArrayList<>();
    acls.add(AclEntry.parseAclEntry("group:" + dbgrp + ":rwx", true));
    verifyNoAclRecursive(acls, extDbDir, true);
  }

  protected void testMaxGroupsDbTblHelper(String extDbDir, String db) throws Exception {
    String tblPathLoc = extDbDir + "/" + TEST_TBL;
    String colPathLoc = tblPathLoc + "/par=1";
    LOGGER.info("tblPathLoc = " + tblPathLoc);
    LOGGER.info("colPathLoc = " + colPathLoc);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    exec(statement, "USE " + db);
    dropRecreateRole(statement, TEST_ROLE1);
    dropRecreateRole(statement, TEST_ROLE2);
    exec(statement, "GRANT ALL ON DATABASE " + db + " TO ROLE " + TEST_ROLE1);
    exec(statement, "GRANT INSERT ON TABLE " + TEST_TBL + " TO ROLE " + TEST_ROLE2);

    List<AclEntry> dbacls = new ArrayList<>();
    List<AclEntry> tblacls = new ArrayList<>();
    for (int i = 0; i < MAX_NUM_OF_GROUPS; i ++) {
      String dbgrp = "dbgrp" + String.valueOf(i);
      String tblgrp = "tblgrp" + String.valueOf(i);
      dbacls.add(AclEntry.parseAclEntry("group:" + dbgrp + ":rwx", true));
      tblacls.add(AclEntry.parseAclEntry("group:" + tblgrp + ":-wx", true));
      exec(statement, "GRANT ROLE " + TEST_ROLE1 + " TO GROUP " + dbgrp);
      exec(statement, "GRANT ROLE " + TEST_ROLE2 + " TO GROUP " + tblgrp);
    }
    context.close();

    // db level privileges should sync up acls to db, tbl and par paths
    verifyAclsRecursive(dbacls, extDbDir, true);
    // tbl level privileges should sync up acls to tbl and par paths
    verifyAclsRecursive(tblacls, tblPathLoc, true);
    // tbl level privileges should not sync up acls to db path
    verifyNoAclRecursive(tblacls, extDbDir, false);
  }

  protected void testMaxGroupsColHelper(String extDbDir, String db) throws Exception {
    String tblPathLoc = extDbDir + "/" + TEST_TBL;
    String colPathLoc = tblPathLoc + "/par=1";
    LOGGER.info("tblPathLoc = " + tblPathLoc);
    LOGGER.info("colPathLoc = " + colPathLoc);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    exec(statement, "USE " + db);
    dropRecreateRole(statement, TEST_ROLE3);
    exec(statement, "GRANT SELECT(value) ON TABLE " + TEST_TBL + " TO ROLE " + TEST_ROLE3);

    List<AclEntry> colacls = new ArrayList<>();
    for (int i = 0; i < MAX_NUM_OF_GROUPS; i ++) {
      String colgrp = "colgrp" + String.valueOf(i);
      colacls.add(AclEntry.parseAclEntry("group:" + colgrp + ":r-x", true));
      exec(statement, "GRANT ROLE " + TEST_ROLE3 + " TO GROUP " + colgrp);
    }

    PrivilegeResultSet pRset = new PrivilegeResultSet(statement, "SHOW GRANT ROLE " + TEST_ROLE3);
    LOGGER.info(TEST_ROLE3 + " privileges = " + pRset.toString());
    assertTrue(pRset.verifyResultSetColumn("database", db));
    assertTrue(pRset.verifyResultSetColumn("table", TEST_TBL));
    assertTrue(pRset.verifyResultSetColumn("column", "value"));
    assertTrue(pRset.verifyResultSetColumn("privilege", "select"));
    assertTrue(pRset.verifyResultSetColumn("principal_name", TEST_ROLE3));

    context.close();

    // column level perm should not syncup acls to any db, tbl and par paths
    verifyNoAclRecursive(colacls, extDbDir, true);
  }

  /**
   * Test Db and tbl level acls are synced up to db, tbl (no partitions)
   * @throws Exception
   */
  @Test
  public void testIntDbTblMaxAclsWithGroupsNoPar() throws Exception {
    final String TEST_DB = "test_hdfs_max_group_int_nopar_db";
    String extDbDir = Path.getPathWithoutSchemeAndAuthority(new Path(metastoreDir))
        + "/" + TEST_DB + ".db";
    LOGGER.info("extDbDir = " + extDbDir);
    dropRecreateDbTblNoPar(TEST_DB, TEST_TBL);

    String tblPathLoc = extDbDir + "/" + TEST_TBL;
    LOGGER.info("tblPathLoc = " + tblPathLoc);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    exec(statement, "USE " + TEST_DB);
    dropRecreateRole(statement, TEST_ROLE1);
    exec(statement, "GRANT SELECT ON TABLE " + TEST_TBL + " TO ROLE " + TEST_ROLE1);

    List<AclEntry> dbacls = new ArrayList<>();
    List<AclEntry> tblacls = new ArrayList<>();
    for (int i = 0; i < MAX_NUM_OF_GROUPS; i ++) {
      String tblgrp = "tblgrp" + String.valueOf(i);
      tblacls.add(AclEntry.parseAclEntry("group:" + tblgrp + ":r-x", true));
      exec(statement, "GRANT ROLE " + TEST_ROLE1 + " TO GROUP " + tblgrp);
    }
    context.close();

    // tbl level privileges should sync up acls to tbl and par paths
    verifyAclsRecursive(tblacls, tblPathLoc, true);
    // tbl level privileges should not sync up acls to db path
    verifyNoAclRecursive(tblacls, extDbDir, false);
  }
}
