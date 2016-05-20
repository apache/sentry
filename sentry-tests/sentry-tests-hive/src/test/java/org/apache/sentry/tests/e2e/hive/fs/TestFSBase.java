package org.apache.sentry.tests.e2e.hive.fs;
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
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.apache.sentry.tests.e2e.hive.fs.DFSFactory.DFSType;

/**
 * This is an abstract base test class for any file system tests:
 * 1. tests will be run on a real cluster, which is not managed by this framework;
 * 2. the real cluster can be configured to have HDFS, S3 or MS Azure as an
 * external storage file system, tests can be run on any of these file systems;
 * 3. defaultFS might or might not be the same as storage file system: for example,
 * defaultFS=hdfs, while S3 is configured as an external storage system;
 * 3. The condition to trigger tests: a. hdfs cluster; b. S3 or MS Azure cluster with
 * explicitly specified sentry.e2etest.storage.uri;
 * 4. To run test, could run the below mvn command:
 * mvn test -P cluster-hadoop-provider-db -f pom.xml \
 * -Dsentry.e2etest.DFSType=S3Cluster \ (or if not specify DFSType will be derived from storage.uri)
 * -Dsentry.e2e.hive.keytabs.location=/root/keytabs \
 * -Dsentry.e2etest.dfs.admin=hdfs \
 * -Dsentry.e2etest.storage.uri=s3a://bucketname (use lowercase here)
 */
public class TestFSBase extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestFSBase.class);

  protected static String authenticationType;
  protected static final String dfsAdmin =
      System.getProperty(TestFSContants.SENTRY_E2E_TEST_DFS_ADMIN, "hdfs");
  protected static final String KEYTAB_LOCATION = System.getProperty(
      TestFSContants.SENTRY_E2E_TEST_HIVE_KEYTAB_LOC);
  protected static final DFSType DFS_TYPE =
      DFSType.valueOf(System.getProperty(TestFSContants.SENTRY_E2E_TEST_DFS_TYPE, "ClusterDFS"));
  protected static DFSType storageDFSType = DFS_TYPE;
  protected static URI defaultStorageUri;
  protected static FileSystem storageFileSystem;
  protected static String StrWarehouseDirFromConfFile;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
    AbstractTestWithStaticConfiguration.setupAdmin();
    authenticationType = System.getProperty(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname);
    LOGGER.info("authenticationType = " + authenticationType);
    Assume.assumeThat(hiveServer2Type, equalTo(HiveServerFactory.HiveServer2Type.UnmanagedHiveServer2));

    if (DFSType.ClusterDFS.equals(DFS_TYPE) ||
        DFSType.S3DFS.equals(DFS_TYPE)) {
      LOGGER.info("DFSType = " + DFS_TYPE);
    } else {
      LOGGER.warn("Incorrect DFSType " + DFS_TYPE);
      Assume.assumeTrue(false);
    }

    String storageUriStr = System.getProperty(TestFSContants.SENTRY_E2E_TEST_STORAGE_URI);
    if (!Strings.isNullOrEmpty(storageUriStr)) {
      storageUriStr = storageUriStr.toLowerCase();
      if (storageUriStr.startsWith("hdfs") || storageUriStr.startsWith("file")) {
        storageDFSType = DFSType.ClusterDFS;
      } else if (storageUriStr.startsWith("s3a")) {
        storageDFSType = DFSType.S3DFS;
      }
     }

    storageFileSystem = fileSystem;
    if (storageDFSType.equals(DFSType.ClusterDFS)) {
      // hdfs cluster
      defaultStorageUri = FileSystem.getDefaultUri(fileSystem.getConf());
    } else {
      // non-hdfs file sytem must specify defaultStorageUri
      if (!Strings.isNullOrEmpty(storageUriStr)) {
        defaultStorageUri = URI.create(storageUriStr);
      } else {
        LOGGER.warn("Skipping test: Unknown sentry.e2etest.storage.uri, " +
            "for example, s3a://bucketname");
        Assume.assumeTrue(false);
      }
      LOGGER.info("defaultStorageUri = " + defaultStorageUri.toString());

      if (storageDFSType.equals(DFSType.S3DFS)) {
        // currently defaultFS = s3a doesn't work for NN
        // needs to explicitly specify s3a's defaultUri
        String accessKey = System.getProperty(TestFSContants.S3A_ACCESS_KEY,
            hiveServer.getProperty(TestFSContants.S3A_ACCESS_KEY));
        String secretKey = System.getProperty(TestFSContants.S3A_SECRET_KEY,
            hiveServer.getProperty(TestFSContants.S3A_SECRET_KEY));
        LOGGER.info("accessKey = " + accessKey);
        LOGGER.info("secretKey = " + secretKey);
        Assume.assumeTrue(Strings.isNullOrEmpty(accessKey) == false);
        Assume.assumeTrue(Strings.isNullOrEmpty(secretKey) == false);

        Configuration conf = new Configuration();
        conf.set(TestFSContants.S3A_ACCESS_KEY, accessKey);
        conf.set(TestFSContants.S3A_SECRET_KEY, secretKey);
        storageFileSystem = new S3AFileSystem();
        Assume.assumeNotNull(storageFileSystem);
        LOGGER.info("Configuring S3DFS defaultStorageUri = " + defaultStorageUri.toString());
        storageFileSystem.initialize(defaultStorageUri, conf);
      }
      /*
      else if (DFS_TYPE.equals(DFSType.MSAZUREDFS)) {
      }
      */
    }
    // Get warehouse dir from hite-site.xml conf file
    StrWarehouseDirFromConfFile = hiveServer.getOrgWarehouseDir();
  }

  @Override
  @Before
  public void setup() throws Exception {
    LOGGER.info("TestFSBase setup");
    //no-op
  }

  /**
   * Return a full path starting with scheme and authority
   * hdfs:/nameserver/relativePath; s3a://bucketname/relativePath
   * @param relativePath
   * @return full path
   */
  protected static Path getFullPathWithSchemeAndAuthority(Path relativePath) {
    return relativePath.makeQualified(defaultStorageUri, relativePath);
  }

  protected void createPath(Path relativePath) throws Exception {
    Path fullPath = getFullPathWithSchemeAndAuthority(relativePath);
    FileSystem adminFS = storageFileSystem;
    LOGGER.info("Creating path " + fullPath);
    if (storageDFSType.equals(DFSType.ClusterDFS)) {
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          dfsAdmin, KEYTAB_LOCATION + "/" + dfsAdmin + ".keytab");
      adminFS = getFS(ugi);
    }
    if (adminFS.exists(fullPath)) {
      adminFS.delete(fullPath, true);
    }
    adminFS.mkdirs(fullPath);
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

  protected static void kinitFromKeytabFile (String user, String keyTabFile) throws IOException {
    Configuration conf = new Configuration();
    conf.set(TestFSContants.SENTRY_E2E_TEST_SECURITY_AUTH, authenticationType);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keyTabFile);
  }

  protected static String getKeyTabFileFullPath(String user) {
    return KEYTAB_LOCATION + "/" + user + ".keytab";
  }

  protected FileSystem getFS(UserGroupInformation ugi) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        Configuration conf = new Configuration();
        conf.set(TestFSContants.SENTRY_E2E_TEST_SECURITY_AUTH, authenticationType);
        return FileSystem.get(conf);
      }
    });
  }
}
