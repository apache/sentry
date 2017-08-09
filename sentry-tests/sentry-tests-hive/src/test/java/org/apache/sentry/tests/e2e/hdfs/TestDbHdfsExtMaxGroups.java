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

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sentry-583 part 2:
 * Add boundary condition test coverage to HDFS synchronization.
 * Testing paths are in the pre-defined external path (instead of internal HiveWareDir)
 * test suite around max #of groups; Normally, HDFS ACLs has a limit of 32 entries per
 * object (HDFS-5617), but this limit should not be enforced when using Sentry HDFS
 * synchronization.
 */
public class TestDbHdfsExtMaxGroups extends TestDbHdfsMaxGroups {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestDbHdfsExtMaxGroups.class);

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
}
