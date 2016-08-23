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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.sentry.tests.e2e.hive.fs.DFSFactory.DFSType;
import static org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory.HiveServer2Type;

import org.apache.sentry.tests.e2e.hive.fs.TestFSContants;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.lessThan;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assert.fail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;

/**
 * A base class for HDFS SynUp tests:
 * The way to run one test could be like the below:
 * mvn test
 -P cluster-hadoop-provider-db \
 -f pom.xml \
 -Dsentry.e2etest.admin.user=hive \
 -Dsentry.e2etest.admin.group=hive \
 -Dhive.server2.thrift.port=10000 \
 -Dhive.server2.authentication.kerberos.keytab=.. \
 -Dhive.server2.authentication.kerberos.principal=.. \
 -Dhive.server2.thrift.bind.host=${HS2_HOST} \
 -Dhive.server2.authentication=kerberos \
 -Dsentry.e2e.hive.keytabs.location=.. \
 -Dsentry.host=${SENTRY_HOST} \
 -Dsentry.service.security.mode=kerberos \
 -Dtest.hdfs.e2e.ext.path=/data
 */

public abstract class TestDbHdfsBase extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestDbHdfsBase.class);

  protected static String metastoreDir;
  protected static String scratchLikeDir;
  protected static String authenticationType;
  protected static UserGroupInformation adminUgi;
  protected static UserGroupInformation hiveUgi;
  protected static int NUM_RETRIES_FOR_ACLS = 12;
  protected static int WAIT_SECS_FOR_ACLS = Integer.parseInt(
      System.getProperty(TestFSContants.SENTRY_E2E_TEST_HDFS_ACLS_SYNCUP_SECS, "1000")); // seconds
  protected static String testExtPathDir =
      System.getProperty(TestFSContants.SENTRY_E2E_TEST_HDFS_EXT_PATH);
  protected static final String KEYTAB_LOCATION =
      System.getProperty(TestFSContants.SENTRY_E2E_TEST_HIVE_KEYTAB_LOC, "/cdep/keytabs");
  protected static String DFS_TYPE =
      System.getProperty(TestFSContants.SENTRY_E2E_TEST_DFS_TYPE, DFSType.MiniDFS.name());

  protected final static String dfsAdmin = System.getProperty(TestFSContants.SENTRY_E2E_TEST_DFS_ADMIN, "hdfs");
  protected final static String storageUriStr = System.getProperty(TestFSContants.SENTRY_E2E_TEST_STORAGE_URI);

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    if (!Strings.isNullOrEmpty(storageUriStr)) {
      LOGGER.warn("Skip HDFS tests if HDFS fileSystem is not configured on hdfs");
      Assume.assumeTrue(storageUriStr.toLowerCase().startsWith("hdfs"));
    }
    useSentryService = true;
    enableHDFSAcls = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
    AbstractTestWithStaticConfiguration.setupAdmin();
    scratchLikeDir = context.getProperty(HiveConf.ConfVars.SCRATCHDIR.varname);
    metastoreDir = context.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    authenticationType = System.getProperty(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname);
    assumeNotNull(metastoreDir, scratchLikeDir);
    if (dfsType.equals(DFSType.ClusterDFS.name())) {
      LOGGER.info("Start to run hdfs e2e tests on a real cluster.");
      assumeNotNull(KEYTAB_LOCATION, authenticationType);
      assumeThat(authenticationType, equalToIgnoringCase("kerberos"));
    } else if (dfsType.equals(DFSType.MiniDFS.name())) {
      LOGGER.info("Start to run hdfs e2e tests on a mini cluster.");
      setupMiniCluster();
    } else {
      LOGGER.error("Unknown DFS cluster type: either MiniCluster or ClusterDFS");
      return;
    }
    // Since they are real e2e tests,for now they
    // work on a real cluster managed outside of the tests
    assumeThat(hiveServer2Type, equalTo(HiveServer2Type.UnmanagedHiveServer2));
    assumeThat(dfsType, equalTo(DFSType.ClusterDFS.name()));
  }

  private static void setupMiniCluster() throws Exception {
    createGgis();
  }

  @After
  public void clearAfterPerTest() throws Exception {
    super.clearAfterPerTest();
    // Clean up any extra data created during testing in external path
    LOGGER.info("TestDbHdfsBase clearAfterPerTest");
    kinitFromKeytabFile(dfsAdmin, getKeyTabFileFullPath(dfsAdmin));
    if (!Strings.isNullOrEmpty(testExtPathDir)) {
      Path path = new Path(testExtPathDir);
      FileStatus[] children = fileSystem.listStatus(path);
      for (FileStatus fs : children) {
        LOGGER.info("Deleting " + fs.toString());
        fileSystem.delete(fs.getPath(), true);
      }
    }
  }

  private FileSystem getFS(UserGroupInformation ugi) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        Configuration conf = new Configuration();
        return FileSystem.get(conf);
      }
    });
  }

  private static void createGgis() throws Exception {
    if (dfsType.equals(DFSType.MiniDFS.name())) {
      adminUgi = UserGroupInformation.createUserForTesting(
          System.getProperty("user.name"), new String[]{"supergroup"});
      hiveUgi = UserGroupInformation.createUserForTesting(
          "hive", new String[]{"hive"});
    } else if (dfsType.equals(DFSType.ClusterDFS.name())) {
      adminUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hdfs", KEYTAB_LOCATION + "/hdfs.keytab");
      hiveUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hive", KEYTAB_LOCATION + "/hive.keytab");
    }
  }

  protected void verifyAclsRecursive(final List<AclEntry> expectedAcls, final String pathLoc,
                                     final boolean recursive) throws Exception {
    if (DFS_TYPE.equals(DFSType.MiniDFS.name())) {
      fileSystem = getFS(adminUgi);
      adminUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          verifyAclsHelper(expectedAcls, pathLoc, recursive);
          return null;
        }
      });
    } else if (DFS_TYPE.equals(DFSType.ClusterDFS.name())) {
      kinitFromKeytabFile(dfsAdmin, getKeyTabFileFullPath(dfsAdmin));
      verifyAclsHelper(expectedAcls, pathLoc, recursive);
    } else {
      fail("Unknown DFS cluster type: " + DFS_TYPE);
    }
  }

  protected void verifyNoAclRecursive(final List<AclEntry> noAcls, final String pathLoc,
                                      final boolean recursive) throws Exception {
    if (DFS_TYPE.equals(DFSType.MiniDFS.name())) {
      fileSystem = getFS(adminUgi);
      adminUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          verifyNoAclHelper(noAcls, pathLoc, recursive);
          return null;
        }
      });
    } else if (DFS_TYPE.equals(DFSType.ClusterDFS.name())) {
      kinitFromKeytabFile(dfsAdmin, getKeyTabFileFullPath(dfsAdmin));
      verifyNoAclHelper(noAcls, pathLoc, recursive);
    } else {
      fail("Unknown DFS cluster type: " + DFS_TYPE);
    }
  }

  /**
   * Verify extended acl entries are correctly synced up
   * @param expectedAcls
   * @param pathLoc
   * @param recursive
   * @throws Exception
   */
  private void verifyAclsHelper(List<AclEntry> expectedAcls, String pathLoc,
                                boolean recursive) throws Exception {
    int retry = 0;
    Path path = new Path(pathLoc);
    LOGGER.info("expectedAcls of [" + pathLoc + "] = " + expectedAcls.toString());
    // Syncing up acls takes some time so make validation in a loop
    while (retry < NUM_RETRIES_FOR_ACLS) {
      AclStatus aclStatus = fileSystem.getAclStatus(path);
      List<AclEntry> actualAcls = new ArrayList<>(aclStatus.getEntries());
      LOGGER.info("[" + retry + "] actualAcls of [" + pathLoc + "] = " + actualAcls.toString());
      retry += 1;
      if (!actualAcls.isEmpty() && !actualAcls.contains(expectedAcls.get(expectedAcls.size()-1))) {
        Thread.sleep(WAIT_SECS_FOR_ACLS);
        continue;
      }
      for (AclEntry expected : expectedAcls) {
        assertTrue("Fail to find aclEntry: " + expected.toString(),
            actualAcls.contains(expected));
      }
      break;
    }
    assertThat(retry, lessThan(NUM_RETRIES_FOR_ACLS));
    if (recursive && fileSystem.getFileStatus(path).isDirectory()) {
      FileStatus[] children = fileSystem.listStatus(path);
      for (FileStatus fs : children) {
        verifyAclsRecursive(expectedAcls, fs.getPath().toString(), recursive);
      }
    }
  }

  /**
   * Verify there is no specified acls gotten synced up in the path status
   * @param noAcls
   * @param pathLoc
   * @param recursive
   * @throws Exception
   */
  private void verifyNoAclHelper(List<AclEntry> noAcls, String pathLoc,
                                 boolean recursive) throws Exception {
    int retry = 0;
    // Retry a couple of times in case the incorrect acls take time to be synced up
    while (retry < NUM_RETRIES_FOR_ACLS) {
      Path path = new Path(pathLoc);
      AclStatus aclStatus = fileSystem.getAclStatus(path);
      List<AclEntry> actualAcls = new ArrayList<>(aclStatus.getEntries());
      LOGGER.info("[" + retry + "] actualAcls of [" + pathLoc + "] = " + actualAcls.toString());
      Thread.sleep(1000); // wait for syncup
      retry += 1;
      for (AclEntry acl : actualAcls) {
        if (noAcls.contains(acl)) {
          fail("Path [ " + pathLoc + " ] should not contain " + acl.toString());
        }
      }
    }
    Path path = new Path(pathLoc);
    if (recursive && fileSystem.getFileStatus(path).isDirectory()) {
      FileStatus[] children = fileSystem.listStatus(path);
      for (FileStatus fs : children) {
        verifyNoAclRecursive(noAcls, fs.getPath().toString(), recursive);
      }
    }
  }

  /**
   * Drop and create role, in case the previous
   * tests leave same roles uncleaned up
   * @param statement
   * @param roleName
   * @throws Exception
   */
  protected void dropRecreateRole(Statement statement, String roleName) throws Exception {
    try {
      exec(statement, "DROP ROLE " + roleName);
    } catch (Exception ex) {
      //noop
      LOGGER.info("Role " + roleName  + " does not exist. But it's ok.");
    } finally {
      exec(statement, "CREATE ROLE " + roleName);
    }
  }

  /**
   * Create an internal test database and table
   * @param db
   * @param tbl
   * @throws Exception
   */
  protected void dropRecreateDbTblRl(String db, String tbl) throws Exception {
    dropRecreateDbTblRl(null, db, tbl);
  }

  /**
   * Create test database and table with location pointing to testPathLoc
   * @param testPathLoc
   * @param db
   * @param tbl
   * @throws Exception
   */
  protected void dropRecreateDbTblRl(String testPathLoc, String db, String tbl) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    exec(statement, "DROP DATABASE IF EXISTS " + db  + " CASCADE");
    if (testPathLoc != null ) {
      exec(statement, "CREATE DATABASE " + db + " LOCATION \'" + testPathLoc + "\'");
    } else {
      exec(statement, "CREATE DATABASE " + db);
    }
    exec(statement, "USE " + db);
    exec(statement, "CREATE TABLE " + tbl + "(number INT, value STRING) PARTITIONED BY (par INT)");
    exec(statement, "INSERT INTO TABLE " + tbl + " PARTITION(par=1) VALUES (1, 'test1')");
    exec(statement, "SELECT * FROM " + tbl);
    if (statement != null) {
      statement.close();
    }
    if (connection != null ) {
      connection.close();
    }
  }

  /**
   * Create test database and table with location pointing
   * to testPathLoc without partitions
   * @param db
   * @param tbl
   * @throws Exception
   */
  protected void dropRecreateDbTblNoPar(String db, String tbl) throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = connection.createStatement();
    exec(statement, "DROP DATABASE IF EXISTS " + db  + " CASCADE");
    exec(statement, "CREATE DATABASE " + db);
    exec(statement, "USE " + db);
    exec(statement, "CREATE TABLE " + tbl + "(number INT, value STRING)");
    exec(statement, "INSERT INTO TABLE " + tbl + " VALUES (1, 'test1')");
    exec(statement, "SELECT * FROM " + tbl);
    if (statement != null) {
      statement.close();
    }
    if (connection != null ) {
      connection.close();
    }
  }

  protected static void kinitFromKeytabFile (String user, String keyTabFile) throws IOException {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", authenticationType);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keyTabFile);
  }

  protected static String getKeyTabFileFullPath(String user) {
    return KEYTAB_LOCATION + "/" + user + ".keytab";
  }
}

