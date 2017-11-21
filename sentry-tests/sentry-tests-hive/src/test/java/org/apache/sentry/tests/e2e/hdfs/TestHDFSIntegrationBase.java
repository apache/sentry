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
package org.apache.sentry.tests.e2e.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.binding.hive.authz.SentryHiveAuthorizerFactory;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.hdfs.SentryHDFSServiceClientFactory;
import org.apache.sentry.hdfs.SentryINodeAttributesProvider;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.fs.MiniDFS;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.apache.sentry.tests.e2e.hive.hiveserver.InternalHiveServer;
import org.apache.sentry.tests.e2e.hive.hiveserver.InternalMetastoreServer;
import org.apache.sentry.tests.e2e.minisentry.SentrySrv;
import org.apache.sentry.tests.e2e.minisentry.SentrySrvFactory;
import org.fest.reflect.core.Reflection;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;


import org.junit.*;
import org.junit.rules.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;

import static org.apache.sentry.hdfs.ServiceConstants.ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES;

/**
 * Base abstract class for HDFS Sync integration
 * (both Non-HA and HA modes)
 */
public abstract class TestHDFSIntegrationBase {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestHDFSIntegrationBase.class);

  @ClassRule
  public static Timeout classTimeout = new Timeout(1200000); //millis, each class runs less than 600s (10m)
  @Rule
  public Timeout timeout = new Timeout(360000); //millis, each test runs less than 180s (3m)

  public static class WordCountMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, String, Long> {

    public void map(LongWritable key, Text value,
                    OutputCollector<String, Long> output, Reporter reporter)
        throws IOException {
      StringTokenizer st = new StringTokenizer(value.toString());
      while (st.hasMoreTokens()) {
        output.collect(st.nextToken(), 1L);
      }
    }

  }

  public static class SumReducer extends MapReduceBase implements
      Reducer<Text, Long, Text, Long> {

    public void reduce(Text key, Iterator<Long> values,
                       OutputCollector<Text, Long> output, Reporter reporter)
        throws IOException {

      long sum = 0;
      while (values.hasNext()) {
        sum += values.next();
      }
      output.collect(key, sum);
    }

  }

  private static final String MANAGED_PREFIXES = "/user/hive/warehouse,/tmp/external";

  protected static final int NUM_RETRIES = 10;
  protected static final int RETRY_WAIT = 1000; //ms
  protected static final String EXTERNAL_SENTRY_SERVICE = "sentry.e2etest.external.sentry";

  protected static MiniDFSCluster miniDFS;
  protected static InternalHiveServer hiveServer2;
  protected static InternalMetastoreServer metastore;
  protected static HiveMetaStoreClient hmsClient;

  protected static int sentryPort = -1;
  protected static SentrySrv sentryServer;
  protected static boolean testSentryHA = false;
  protected static final long STALE_THRESHOLD = 5000;

  // It is the interval in milliseconds that hdfs uses to get acl from sentry. Default is 500, but
  // we want it to be low in our tests so that changes reflect soon
  protected static final long CACHE_REFRESH = 100;

  // It is Used to wait before verifying result in test.
  // We want to make sure the cache is updated in our tests so that changes reflect soon. The unit is milliseconds
  // It takes at most (ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS_DEFAULT + ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT)
  // for sentry to get the path configured in Hive, adding ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT to be safe.
  // And then it takes at most CACHE_REFRESH for HDFS to get this from sentry, adding CACHE_REFRESH to be sure
  protected static final long WAIT_BEFORE_TESTVERIFY =
          ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS_DEFAULT +
              ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT * 2 + CACHE_REFRESH * 2;

  // Time to wait before running next tests. The unit is milliseconds.
  // Deleting HDFS may finish, but HDFS may not be ready for creating the same file again.
  // We need to to make sure that creating the same file in the next test will succeed
  // If we don't wait, next test may get exception similar to
  // "org.apache.hadoop.security.AccessControlException Permission denied: user=hive, access=EXECUTE,
  // inode="/tmp/external/p1":hdfs:hdfs:drwxrwx---"
  protected static final long WAIT_BEFORE_NEXTTEST = 50;

  protected static String fsURI;
  protected static int hmsPort;
  protected static File baseDir;
  protected static File policyFileLocation;
  protected static UserGroupInformation adminUgi;
  protected static UserGroupInformation hiveUgi;

  // Variables which are used for cleanup after test
  // Please set these values in each test
  protected Path tmpHDFSDir;
  protected String tmpHDFSDirStr;
  protected String tmpHDFSPartitionStr;
  protected Path partitionDir;
  protected String[] dbNames;
  protected String[] roles;
  protected String admin;
  protected static Configuration hadoopConf;

  protected static File assertCreateDir(File dir) {
    if(!dir.isDirectory()) {
      Assert.assertTrue("Failed creating " + dir, dir.mkdirs());
    }
    return dir;
  }

  private static int findPort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  protected void verifyOnAllSubDirs(String path, FsAction fsAction, String group, boolean groupShouldExist) throws Throwable {
    verifyOnAllSubDirs(path, fsAction, group, groupShouldExist, true);
  }

  protected void verifyOnPath(String path, FsAction fsAction, String group, boolean groupShouldExist) throws Throwable {
    long elapsed_Time = 0, start_time = System.nanoTime();
    final long TOTAL_SYNC_TIME = NUM_RETRIES * RETRY_WAIT; //ms
    while (elapsed_Time <= TOTAL_SYNC_TIME) {
      try {
        verifyOnAllSubDirs(path, fsAction, group, groupShouldExist, false);
        break;
      } catch (Exception ex) {
        LOGGER.warn("verifyOnAllSubDirs fails: elapsed time = " + elapsed_Time + " ms.");
      }
      elapsed_Time = (System.nanoTime() - start_time) / 1000000L; //ms
    }
    Assert.assertTrue(elapsed_Time <= TOTAL_SYNC_TIME);
  }

  protected void verifyOnAllSubDirs(String path, FsAction fsAction, String group, boolean groupShouldExist, boolean recurse) throws Throwable {
    verifyOnAllSubDirs(new Path(path), fsAction, group, groupShouldExist, recurse, NUM_RETRIES);
  }

  protected void verifyOnAllSubDirs(Path p, FsAction fsAction, String group, boolean groupShouldExist, boolean recurse, int retry) throws Throwable {
    verifyOnAllSubDirsHelper(p, fsAction, group, groupShouldExist, recurse, retry);
  }

  /* SENTRY-1471 - fixing the validation logic.
   * a) When the number of retries exceeds the limit, propagate the Assert exception to the caller.
   * b) Throw an exception instead of returning false, to pass valuable debugging info up the stack
   *    - expected vs. found permissions.
   */
  private void verifyOnAllSubDirsHelper(Path p, FsAction fsAction, String group,
                                           boolean groupShouldExist, boolean recurse, int retry) throws Throwable {
    FileStatus fStatus = null;
    // validate parent dir's acls
    try {
      fStatus = miniDFS.getFileSystem().getFileStatus(p);
      if (groupShouldExist) {
        Assert.assertEquals("Error at verifying Path action : " + p + " ;", fsAction, getAcls(p).get(group));
      } else {
        Assert.assertFalse("Error at verifying Path : " + p + " ," +
            " group : " + group + " ;", getAcls(p).containsKey(group));
      }
      LOGGER.info("Successfully found acls for path = " + p.getName());
    } catch (Throwable th) {
      if (retry > 0) {
        LOGGER.info("Retry: " + retry);
        Thread.sleep(RETRY_WAIT);
        verifyOnAllSubDirsHelper(p, fsAction, group, groupShouldExist, recurse, retry - 1);
      } else {
        throw th;
      }
    }
    // validate children dirs
    if (recurse && fStatus.isDirectory()) {
      FileStatus[] children = miniDFS.getFileSystem().listStatus(p);
      for (FileStatus fs : children) {
        verifyOnAllSubDirsHelper(fs.getPath(), fsAction, group, groupShouldExist, recurse, NUM_RETRIES);
      }
    }
  }

  protected Map<String, FsAction> getAcls(Path path) throws Exception {
    AclStatus aclStatus = miniDFS.getFileSystem().getAclStatus(path);
    Map<String, FsAction> acls = new HashMap<String, FsAction>();
    for (AclEntry ent : aclStatus.getEntries()) {
      if (ent.getType().equals(AclEntryType.GROUP)) {

        // In case of duplicate acl exist, exception should be thrown.
        if (acls.containsKey(ent.getName())) {
          throw new SentryAlreadyExistsException("The acl " + ent.getName() + " already exists.\n");
        } else {
          acls.put(ent.getName(), ent.getPermission());
        }
      }
    }
    return acls;
  }

  protected void loadData(Statement stmt) throws IOException, SQLException {
    FSDataOutputStream f1 = miniDFS.getFileSystem().create(new Path("/tmp/f1.txt"));
    f1.writeChars("m1d1_t1\n");
    f1.writeChars("m1d1_t2\n");
    f1.writeChars("m1d1_t3\n");
    f1.flush();
    f1.close();
    stmt.execute("load data inpath \'/tmp/f1.txt\' overwrite into table p1 partition (month=1, day=1)");
    FSDataOutputStream f2 = miniDFS.getFileSystem().create(new Path("/tmp/f2.txt"));
    f2.writeChars("m2d2_t4\n");
    f2.writeChars("m2d2_t5\n");
    f2.writeChars("m2d2_t6\n");
    f2.flush();
    f2.close();
    stmt.execute("load data inpath \'/tmp/f2.txt\' overwrite into table p1 partition (month=2, day=2)");
    ResultSet rs = stmt.executeQuery("select * from p1");
    List<String> vals = new ArrayList<String>();
    while (rs.next()) {
      vals.add(rs.getString(1));
    }
    Assert.assertEquals(6, vals.size());
    rs.close();
  }

  protected void verifyQuery(Statement stmt, String table, int n) throws Throwable {
    verifyQuery(stmt, table, n, NUM_RETRIES);
  }

  /* SENTRY-1471 - fixing the validation logic.
   * a) When the number of retries exceeds the limit, propagate the Assert exception to the caller.
   * b) Throw an exception immediately, instead of using boolean variable, to pass valuable debugging
   *    info up the stack - expected vs. found number of rows.
   */
  protected void verifyQuery(Statement stmt, String table, int n, int retry) throws Throwable {
    ResultSet rs = null;
    try {
      rs = stmt.executeQuery("select * from " + table);
      int numRows = 0;
      while (rs.next()) { numRows++; }
      Assert.assertEquals(n, numRows);
    } catch (Throwable th) {
      if (retry > 0) {
        LOGGER.info("Retry: " + retry);
        Thread.sleep(RETRY_WAIT);
        verifyQuery(stmt, table, n, retry - 1);
      } else {
        throw th;
      }
    }
  }

  protected void verifyAccessToPath(String user, String group, String path, boolean hasPermission) throws Exception{
    Path p = new Path(path);
    FileSystem fs = miniDFS.getFileSystem();
    try {
      fs.listFiles(p, true);
      if(!hasPermission) {
        Assert.assertFalse("Expected listing files to fail", false);
      }
    } catch (Exception e) {
      if(hasPermission) {
        throw e;
      }
    }
  }

  protected void writeToPath(String path, int numRows, String user, String group) throws IOException {
    Path p = new Path(path);
    miniDFS.getFileSystem().mkdirs(p);
    miniDFS.getFileSystem().setOwner(p, user, group);
    FSDataOutputStream f1 = miniDFS.getFileSystem().create(new Path(path + "/stuff.txt"));
    for (int i = 0; i < numRows; i++) {
      f1.writeChars("random" + i + "\n");
    }
    f1.flush();
    f1.close();
    miniDFS.getFileSystem().setOwner(new Path(path + "/stuff.txt"), "asuresh", "supergroup");
    miniDFS.getFileSystem().setPermission(new Path(path + "/stuff.txt"),
    FsPermission.valueOf("-rwxrwx--x"));
  }

  protected void verifyHDFSandMR(Statement stmt) throws Throwable {
    // hbase user should not be allowed to read...
    UserGroupInformation hbaseUgi = UserGroupInformation.createUserForTesting("hbase", new String[] {"hbase"});
    hbaseUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          miniDFS.getFileSystem().open(new Path("/user/hive/warehouse/p1/month=1/day=1/f1.txt"));
          Assert.fail("Should not be allowed !!");
        } catch (Exception e) {
          Assert.assertEquals("Wrong Error : " + e.getMessage(), true, e.getMessage().contains("Permission denied: user=hbase"));
        }
        return null;
      }
    });

    // WordCount should fail..
    // runWordCount(new JobConf(miniMR.getConfig()), "/user/hive/warehouse/p1/month=1/day=1", "/tmp/wc_out");

    stmt.execute("grant select on table p1 to role p1_admin");

    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.READ_EXECUTE, "hbase", true);
    // hbase user should now be allowed to read...
    hbaseUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Path p = new Path("/user/hive/warehouse/p1/month=2/day=2/f2.txt");
        BufferedReader in = new BufferedReader(new InputStreamReader(miniDFS.getFileSystem().open(p)));
        String line = null;
        List<String> lines = new ArrayList<String>();
        do {
          line = in.readLine();
          if (line != null) {
            lines.add(line);
          }
        } while (line != null);
        Assert.assertEquals(3, lines.size());
        in.close();
        return null;
      }
    });

  }

  protected void loadDataTwoCols(Statement stmt) throws IOException, SQLException {
    FSDataOutputStream f1 = miniDFS.getFileSystem().create(new Path("/tmp/f2.txt"));
    f1.writeChars("m1d1_t1, m1d1_t2\n");
    f1.writeChars("m1d1_t2, m1d1_t2\n");
    f1.writeChars("m1d1_t3, m1d1_t2\n");
    f1.flush();
    f1.close();
    stmt.execute("load data inpath \'/tmp/f2.txt\' overwrite into table p1 partition (month=1, day=1)");
    ResultSet rs = stmt.executeQuery("select * from p1");
    List<String> vals = new ArrayList<String>();
    while (rs.next()) {
      vals.add(rs.getString(1));
    }
    Assert.assertEquals(3, vals.size());
    rs.close();
  }

  @BeforeClass
  public static void setup() throws Exception {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    baseDir = Files.createTempDir();
    policyFileLocation = new File(baseDir, HiveServerFactory.AUTHZ_PROVIDER_FILENAME);
    PolicyFile policyFile = PolicyFile.setAdminOnServer1("hive")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(policyFileLocation);

    adminUgi = UserGroupInformation.createUserForTesting(
        System.getProperty("user.name"), new String[] { "supergroup" });

    hiveUgi = UserGroupInformation.createUserForTesting(
        "hive", new String[] { "hive" });

    // Create SentryService and its internal objects.
    // Set Sentry port
    createSentry();

    // Create hive-site.xml that contains the metastore uri
    // it is used by HMSFollower
    configureHiveAndMetastoreForSentry();

    // Start SentryService after Hive configuration hive-site.xml is available
    // So HMSFollower can contact metastore using its URI
    startSentry();

    // Start HDFS and MR with Sentry Port. Set fsURI
    startDFSandYARN();

    // Configure Hive and Metastore with Sentry Port and fsURI
    // Read src/test/resources/sentry-site.xml.
    // Create hive-site.xml and sentry-site.xml used by Hive.
    HiveConf hiveConf = configureHiveAndMetastore();

    // Start Hive and Metastore after SentryService is started
    startHiveAndMetastore(hiveConf);
  }

  @Before
  public void setUpTempDir() throws IOException {
    LOGGER.debug("setUpTempDir starts");
    tmpHDFSDirStr = "/tmp/external";
    tmpHDFSPartitionStr = tmpHDFSDirStr + "/p1";
    tmpHDFSDir = new Path(tmpHDFSDirStr);
    if (miniDFS.getFileSystem().exists(tmpHDFSDir)) {
      miniDFS.getFileSystem().delete(tmpHDFSDir, true);
    }
    Assert.assertTrue(miniDFS.getFileSystem().mkdirs(tmpHDFSDir));
    miniDFS.getFileSystem().setOwner(tmpHDFSDir, "hive", "hive");
    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx--x"));
    partitionDir  = new Path(tmpHDFSPartitionStr);
    if (miniDFS.getFileSystem().exists(partitionDir)) {
      miniDFS.getFileSystem().delete(partitionDir, true);
    }
    Assert.assertTrue(miniDFS.getFileSystem().mkdirs(partitionDir));
    LOGGER.debug("setUpTempDir ends");
  }

  private static HiveConf configureHiveAndMetastoreForSentry() throws IOException, InterruptedException {
    final HiveConf hiveConfiguration = new HiveConf();
    hiveUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        HiveConf hiveConf = hiveConfiguration;
        hmsPort = findPort();
        LOGGER.info("\n\n HMS port : " + hmsPort + "\n\n");

        // Sets hive.metastore.authorization.storage.checks to true, so that
        // disallow the operations such as drop-partition if the user in question
        // doesn't have permissions to delete the corresponding directory
        // on the storage.
        hiveConf.set("hive.metastore.authorization.storage.checks", "true");
        hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hmsPort);
        hiveConf.set("sentry.metastore.service.users", "hive");// queries made by hive user (beeline) skip meta store check

        File confDir = assertCreateDir(new File(baseDir, "etc"));
        File hiveSite = new File(confDir, "hive-site.xml");
        hiveConf.set("hive.server2.enable.doAs", "false");
        OutputStream out = new FileOutputStream(hiveSite);
        hiveConf.writeXml(out);
        out.close();

        Reflection.staticField("hiveSiteURL")
                .ofType(URL.class)
                .in(HiveConf.class)
                .set(hiveSite.toURI().toURL());
        return null;
      }
    });

    return hiveConfiguration;
  }

  private static HiveConf configureHiveAndMetastore() throws IOException, InterruptedException {
    final HiveConf hiveConfiguration = new HiveConf();
    hiveUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        HiveConf hiveConf = hiveConfiguration;
        hiveConf.set("sentry.metastore.plugins", "org.apache.sentry.hdfs.MetastorePlugin");
        hiveConf.set("sentry.service.client.server.rpc-addresses", "localhost");
        hiveConf.set("sentry.hdfs.service.client.server.rpc-addresses", "localhost");
        hiveConf.set("sentry.hdfs.service.client.server.rpc-port", String.valueOf(sentryPort));
        hiveConf.set("sentry.service.client.server.rpc-port", String.valueOf(sentryPort));
//        hiveConf.set("sentry.service.server.compact.transport", "true");
//        hiveConf.set("sentry.service.client.compact.transport", "true");
        hiveConf.set("sentry.service.security.mode", "none");
        hiveConf.set("sentry.hdfs.service.security.mode", "none");
        hiveConf.set("sentry.hdfs.init.update.retry.delay.ms", "500");
        hiveConf.set("sentry.hive.provider.backend", "org.apache.sentry.provider.db.SimpleDBProviderBackend");
        hiveConf.set("sentry.provider", LocalGroupResourceAuthorizationProvider.class.getName());
        hiveConf.set("sentry.hive.provider", LocalGroupResourceAuthorizationProvider.class.getName());
        hiveConf.set("sentry.hive.provider.resource", policyFileLocation.getPath());
        hiveConf.set("sentry.hive.testing.mode", "true");
        hiveConf.set("sentry.hive.server", "server1");

        hiveConf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
        hiveConf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFileLocation.getPath());
        hiveConf.set("fs.defaultFS", fsURI);
        hiveConf.set("fs.default.name", fsURI);
        hiveConf.set("hive.metastore.execute.setugi", "true");
        hiveConf.set("hive.metastore.warehouse.dir", "hdfs:///user/hive/warehouse");
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + baseDir.getAbsolutePath() + "/metastore_db;create=true");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        hiveConf.set("javax.jdo.option.ConnectionUserName", "hive");
        hiveConf.set("javax.jdo.option.ConnectionPassword", "hive");
        hiveConf.set("datanucleus.autoCreateSchema", "true");
        hiveConf.set("datanucleus.fixedDatastore", "false");
        hiveConf.set("datanucleus.autoStartMechanism", "SchemaTable");
        hiveConf.set("datanucleus.schema.autoCreateTables", "true");

        hiveConf.set(ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, "true");
        hiveConf.set(ConfVars.HIVE_AUTHORIZATION_MANAGER.varname, SentryHiveAuthorizerFactory.class.getName());
        hiveConf.set(ConfVars.HIVE_CBO_ENABLED.varname, "false");
        hiveConf.set(ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname, "false");
        hiveConf.set(ConfVars.HIVE_IN_TEST.varname, "true");

        // Sets the hadoop temporary directory specified by the java.io.tmpdir (already set to the
        // maven build directory to avoid writing to the /tmp directly
        String hadoopTempDir = System.getProperty("java.io.tmpdir") + File.separator + "hadoop-tmp";
        hiveConf.set("hadoop.tmp.dir", hadoopTempDir);

        // This configuration will avoid that the HMS fails if the metastore schema has not version
        // information. For some reason, HMS does not set a version initially on our tests.
        hiveConf.set(ConfVars.METASTORE_SCHEMA_VERIFICATION.varname, "false");

        // Sets hive.metastore.authorization.storage.checks to true, so that
        // disallow the operations such as drop-partition if the user in question
        // doesn't have permissions to delete the corresponding directory
        // on the storage.
        hiveConf.set("hive.metastore.authorization.storage.checks", "true");
        hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hmsPort);
        hiveConf.set("hive.metastore.pre.event.listeners", "org.apache.sentry.binding.metastore.MetastoreAuthzBinding");
        hiveConf.set("hive.metastore.transactional.event.listeners", "org.apache.sentry.binding.metastore.SentryMetastorePostEventListenerNotificationLog");
        hiveConf.set("hcatalog.message.factory.impl.json", "org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory");
        hiveConf.set("hive.security.authorization.task.factory", "org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl");
        hiveConf.set("hive.server2.session.hook", "org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook");
        hiveConf.set("sentry.metastore.service.users", "hive");// queries made by hive user (beeline) skip meta store check

        HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
        authzConf.addResource(hiveConf);
        File confDir = assertCreateDir(new File(baseDir, "etc"));
        File accessSite = new File(confDir, HiveAuthzConf.AUTHZ_SITE_FILE);
        OutputStream out = new FileOutputStream(accessSite);
        authzConf.set("fs.defaultFS", fsURI);
        authzConf.writeXml(out);
        out.close();

        hiveConf.set("hive.sentry.conf.url", accessSite.getPath());
        LOGGER.info("Sentry client file : " + accessSite.getPath());

        File hiveSite = new File(confDir, "hive-site.xml");
        hiveConf.set("hive.server2.enable.doAs", "false");
        hiveConf.set(HiveAuthzConf.HIVE_SENTRY_CONF_URL, accessSite.toURI().toURL()
            .toExternalForm());
        out = new FileOutputStream(hiveSite);
        hiveConf.writeXml(out);
        out.close();

        Reflection.staticField("hiveSiteURL")
            .ofType(URL.class)
            .in(HiveConf.class)
            .set(hiveSite.toURI().toURL());
        return null;
      }
    });

    return hiveConfiguration;
  }

  private static void startHiveAndMetastore(HiveConf hiveConfig) throws IOException, InterruptedException {
    startHiveAndMetastore(hiveConfig, NUM_RETRIES);
  }

  private static void startHiveAndMetastore(final HiveConf hiveConf, final int retries) throws IOException, InterruptedException {
    hiveUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final CountDownLatch hmsStartedSignal = new CountDownLatch(1);

        metastore = new InternalMetastoreServer(hiveConf);
        new Thread() {
          @Override
          public void run() {
            try {
              metastore.start();
              hmsStartedSignal.countDown();
              while (true) {
                Thread.sleep(1000L);
              }
            } catch (Exception e) {
              LOGGER.info("Could not start Hive Server");
            }
          }
        }.start();

        hmsStartedSignal.await(30, TimeUnit.SECONDS);
        hmsClient = new HiveMetaStoreClient(hiveConf);
        startHiveServer2(retries, hiveConf);
        return null;
      }
    });
  }

  private static void startHiveServer2(final int retries, HiveConf hiveConf)
      throws IOException, InterruptedException, SQLException {
    Connection conn = null;
    Thread th = null;
    final AtomicBoolean keepRunning = new AtomicBoolean(true);
    try {
      hiveServer2 = new InternalHiveServer(hiveConf);
      th = new Thread() {
        @Override
        public void run() {
          try {
            hiveServer2.start();
            while (keepRunning.get()) {
              Thread.sleep(1000L);
            }
          } catch (Exception e) {
            LOGGER.info("Could not start Hive Server");
          }
        }
      };
      th.start();
      Thread.sleep(RETRY_WAIT * 5);
      conn = hiveServer2.createConnection("hive", "hive");
    } catch (Exception ex) {
      if (retries > 0) {
        try {
          keepRunning.set(false);
          hiveServer2.shutdown();
        } catch (Exception e) {
          // Ignore
        }
        LOGGER.info("Re-starting Hive Server2 !!");
        startHiveServer2(retries - 1, hiveConf);
      }
    }
    if (conn != null) {
      conn.close();
    }
  }

  private static void startDFSandYARN() throws IOException,
      InterruptedException {
    adminUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "target/test/data");
        hadoopConf = new HdfsConfiguration();
        hadoopConf.set(DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
            SentryINodeAttributesProvider.class.getName());
        hadoopConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
        hadoopConf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
        File dfsDir = assertCreateDir(new File(baseDir, "dfs"));
        hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
        hadoopConf.set("hadoop.security.group.mapping",
            MiniDFS.PseudoGroupMappingService.class.getName());
        Configuration.addDefaultResource("test.xml");

        hadoopConf.set("sentry.authorization-provider.hdfs-path-prefixes", MANAGED_PREFIXES);
        hadoopConf.set("sentry.authorization-provider.cache-refresh-retry-wait.ms", "5000");
        hadoopConf.set("sentry.authorization-provider.cache-refresh-interval.ms", String.valueOf(CACHE_REFRESH));

        hadoopConf.set("sentry.authorization-provider.cache-stale-threshold.ms", String.valueOf(STALE_THRESHOLD));

        hadoopConf.set("sentry.hdfs.service.security.mode", "none");
        hadoopConf.set("sentry.hdfs.service.client.server.rpc-addresses", "localhost");
        hadoopConf.set("sentry.hdfs.service.client.server.rpc-port", String.valueOf(sentryPort));
        EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
        miniDFS = new MiniDFSCluster.Builder(hadoopConf).build();
        Path tmpPath = new Path("/tmp");
        Path hivePath = new Path("/user/hive");
        Path warehousePath = new Path(hivePath, "warehouse");
        miniDFS.getFileSystem().mkdirs(warehousePath);
        boolean directory = miniDFS.getFileSystem().isDirectory(warehousePath);
        LOGGER.info("\n\n Is dir :" + directory + "\n\n");
        LOGGER.info("\n\n DefaultFS :" + miniDFS.getFileSystem().getUri() + "\n\n");
        fsURI = miniDFS.getFileSystem().getUri().toString();
        hadoopConf.set("fs.defaultFS", fsURI);

        // Create Yarn cluster
        // miniMR = MiniMRClientClusterFactory.create(this.getClass(), 1, conf);

        miniDFS.getFileSystem().mkdirs(tmpPath);
        miniDFS.getFileSystem().setPermission(tmpPath, FsPermission.valueOf("drwxrwxrwx"));
        miniDFS.getFileSystem().setOwner(hivePath, "hive", "hive");
        miniDFS.getFileSystem().setOwner(warehousePath, "hive", "hive");
        LOGGER.info("\n\n Owner :"
            + miniDFS.getFileSystem().getFileStatus(warehousePath).getOwner()
            + ", "
            + miniDFS.getFileSystem().getFileStatus(warehousePath).getGroup()
            + "\n\n");
        LOGGER.info("\n\n Owner tmp :"
            + miniDFS.getFileSystem().getFileStatus(tmpPath).getOwner() + ", "
            + miniDFS.getFileSystem().getFileStatus(tmpPath).getGroup() + ", "
            + miniDFS.getFileSystem().getFileStatus(tmpPath).getPermission() + ", "
            + "\n\n");

        int dfsSafeCheckRetry = 30;
        boolean hasStarted = false;
        for (int i = dfsSafeCheckRetry; i > 0; i--) {
          if (!miniDFS.getFileSystem().isInSafeMode()) {
            hasStarted = true;
            LOGGER.info("HDFS safemode check num times : " + (31 - i));
            break;
          }
        }
        if (!hasStarted) {
          throw new RuntimeException("HDFS hasnt exited safe mode yet..");
        }

        return null;
      }
    });
  }

  private static void startSentry() throws Exception {
    SentryServiceClientFactory factory = SentryServiceClientFactory.factoryReset(null);
    if (factory != null) {
      factory.close();
    }
    SentryHDFSServiceClientFactory.factoryReset();
    try {
      hiveUgi.doAs(new PrivilegedExceptionAction() {
        @Override
        public Void run() throws Exception {
          sentryServer.startAll();
          LOGGER.info("\n\n Sentry service started \n\n");
          return null;
        }
      });
    } catch (Exception ex) {
      //An exception happening in above block will result in a wrapped UndeclaredThrowableException.
      throw new Exception(ex.getCause());
    }
  }

  private static void createSentry() throws Exception {
    try {

      hiveUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Configuration sentryConf = new Configuration(false);
          sentryConf.set(SENTRY_HDFS_INTEGRATION_PATH_PREFIXES, MANAGED_PREFIXES);
          Map<String, String> properties = Maps.newHashMap();
          properties.put(HiveServerFactory.AUTHZ_PROVIDER_BACKEND,
              SimpleDBProviderBackend.class.getName());
          properties.put(ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
              SentryHiveAuthorizationTaskFactoryImpl.class.getName());
          properties
              .put(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS.varname, "2");
          properties.put("hive.exec.local.scratchdir", Files.createTempDir().getAbsolutePath());
          properties.put(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
//        properties.put("sentry.service.server.compact.transport", "true");
          properties.put("sentry.hive.testing.mode", "true");
          properties.put("sentry.service.reporting", "JMX");
          properties.put(ServerConfig.ADMIN_GROUPS, "hive,admin");
          properties.put(ServerConfig.RPC_ADDRESS, "localhost");
          properties.put(ServerConfig.RPC_PORT, String.valueOf(sentryPort > 0 ? sentryPort : 0));
          properties.put(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
          properties.put("sentry.hive.server", "server1");

          properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING, ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
          properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFileLocation.getPath());
          properties.put(ServerConfig.SENTRY_STORE_JDBC_URL,
              "jdbc:derby:;databaseName=" + baseDir.getPath()
                  + "/sentrystore_db;create=true");
          properties.put(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
          properties.put("sentry.service.processor.factories",
              "org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessorFactory,org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
          properties.put("sentry.policy.store.plugins", "org.apache.sentry.hdfs.SentryPlugin");
          properties.put(ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS, "10000");
          properties.put(ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS, "50");
          properties.put(ServerConfig.RPC_MIN_THREADS, "3");
          for (Map.Entry<String, String> entry : properties.entrySet()) {
            sentryConf.set(entry.getKey(), entry.getValue());
          }
          sentryServer = SentrySrvFactory.create(SentrySrvFactory.SentrySrvType.INTERNAL_SERVER,
              sentryConf, testSentryHA ? 2 : 1);
          sentryPort = sentryServer.get(0).getAddress().getPort();
          LOGGER.info("Sentry service is created on port {}", sentryPort);
          return null;
        }
      });
    } catch (Exception e) {
      //An exception happening in above block will result in a wrapped UndeclaredThrowableException.
      throw new Exception(e.getCause());
    }
  }

  @After
  public void cleanAfterTest() throws Exception {
    //Clean up database
    Connection conn;
    Statement stmt;
    Preconditions.checkArgument(admin != null && dbNames !=null && roles != null && tmpHDFSDir != null,
        "Test case did not set some of these values required for clean up: admin, dbNames, roles, tmpHDFSDir");

    conn = hiveServer2.createConnection(admin, admin);
    stmt = conn.createStatement();
    for( String dbName: dbNames) {
      stmt.execute("drop database if exists " + dbName + " cascade");
    }
    stmt.close();
    conn.close();

    //Clean up roles
    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    LOGGER.info("About to clear all roles");
    for( String role:roles) {
      stmt.execute("drop role " + role);
    }
    stmt.close();
    conn.close();

    //Clean up hdfs directories
    miniDFS.getFileSystem().delete(tmpHDFSDir, true);

    tmpHDFSDir = null;
    dbNames = null;
    roles = null;
    admin = null;
    Thread.sleep(WAIT_BEFORE_NEXTTEST); // make sure the clean up is done before next test starts. otherwise, the next test may fail
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    try {
      if (miniDFS != null) {
        miniDFS.shutdown();
      }
    } finally {
      try {
        if (hiveServer2 != null) {
          hiveServer2.shutdown();
        }
      } finally {
        try {
          if (metastore != null) {
            metastore.shutdown();
          }
        } finally {
          sentryServer.close();
        }
      }
    }
  }
}
