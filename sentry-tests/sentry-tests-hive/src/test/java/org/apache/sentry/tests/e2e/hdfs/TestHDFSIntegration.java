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
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

import junit.framework.Assert;

import org.apache.sentry.core.common.utils.PathUtils;

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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.SentryAuthorizationProvider;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.fs.MiniDFS;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.apache.sentry.tests.e2e.hive.hiveserver.InternalHiveServer;
import org.apache.sentry.tests.e2e.hive.hiveserver.InternalMetastoreServer;
import org.apache.sentry.tests.e2e.minisentry.SentrySrv;
import org.apache.sentry.tests.e2e.minisentry.SentrySrvFactory;
import org.fest.reflect.core.Reflection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.hadoop.hive.metastore.api.Table;

public class TestHDFSIntegration {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestHDFSIntegration.class);


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

  private static final int NUM_RETRIES = 10;
  private static final int RETRY_WAIT = 1000;

  private static final String EXTERNAL_SENTRY_SERVICE = "sentry.e2etest.external.sentry";

  private static MiniDFSCluster miniDFS;
  private MiniMRClientCluster miniMR;
  private static InternalHiveServer hiveServer2;
  private static InternalMetastoreServer metastore;
  private static HiveMetaStoreClient hmsClient;

  private static int sentryPort = -1;
  protected static SentrySrv sentryServer;
  protected static boolean testSentryHA = false;
  protected boolean ignoreCleanUp = false;
  private static final long STALE_THRESHOLD = 5000;
  private static final long CACHE_REFRESH = 100; //Default is 500, but we want it to be low
                                                // in our tests so that changes reflect soon

  private static String fsURI;
  private static int hmsPort;
  private static File baseDir;
  private static File policyFileLocation;
  private static UserGroupInformation adminUgi;
  private static UserGroupInformation hiveUgi;

  // Variables which are used for cleanup after test
  // Please set these values in each test
  private Path tmpHDFSDir;
  private String[] dbNames;
  private String[] roles;
  private String admin;

  private static Configuration hadoopConf;

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

    // Start Sentry
    startSentry();

    // Start HDFS and MR
    startDFSandYARN();

    // Start HiveServer2 and Metastore
    startHiveAndMetastore();

  }

  private static void startHiveAndMetastore() throws IOException, InterruptedException {
    startHiveAndMetastore(NUM_RETRIES);
  }

  private static void startHiveAndMetastore(final int retries) throws IOException, InterruptedException {
    hiveUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("sentry.metastore.plugins", "org.apache.sentry.hdfs.MetastorePlugin");
        hiveConf.set("sentry.service.client.server.rpc-address", "localhost");
        hiveConf.set("sentry.hdfs.service.client.server.rpc-address", "localhost");
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
        //Workaround for CDH-23735
        //hiveConf.set("hive.metastore.warehouse.dir", "hdfs:///user/hive/warehouse");
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + baseDir.getAbsolutePath() + "/metastore_db;create=true");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        hiveConf.set("javax.jdo.option.ConnectionUserName", "hive");
        hiveConf.set("javax.jdo.option.ConnectionPassword", "hive");
        hiveConf.set("datanucleus.autoCreateSchema", "true");
        hiveConf.set("datanucleus.fixedDatastore", "false");
        hiveConf.set("datanucleus.autoStartMechanism", "SchemaTable");
        hmsPort = findPort();
        LOGGER.info("\n\n HMS port : " + hmsPort + "\n\n");

        // Sets hive.metastore.authorization.storage.checks to true, so that
        // disallow the operations such as drop-partition if the user in question
        // doesn't have permissions to delete the corresponding directory
        // on the storage.
        hiveConf.set("hive.metastore.authorization.storage.checks", "true");
        hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hmsPort);
        hiveConf.set("hive.metastore.pre.event.listeners", "org.apache.sentry.binding.metastore.MetastoreAuthzBinding");
        hiveConf.set("hive.metastore.event.listeners", "org.apache.sentry.binding.metastore.SentryMetastorePostEventListener");
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

        metastore = new InternalMetastoreServer(hiveConf);
        new Thread() {
          @Override
          public void run() {
            try {
              metastore.start();
              while (true) {
              }
            } catch (Exception e) {
              LOGGER.info("Could not start Hive Server");
            }
          }
        }.start();

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
            while(keepRunning.get()){}
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

  private static String getSentryPort() throws Exception{
    if(sentryServer!=null) {
      return String.valueOf(sentryServer.get(0).getAddress().getPort());
    } else {
      throw new Exception("Sentry server not initialized");
    }
  }
  private static void startDFSandYARN() throws IOException,
      InterruptedException {
    adminUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "target/test/data");
        hadoopConf = new HdfsConfiguration();
        hadoopConf.set(DFSConfigKeys.DFS_NAMENODE_AUTHORIZATION_PROVIDER_KEY,
            SentryAuthorizationProvider.class.getName());
        hadoopConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
        hadoopConf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
        File dfsDir = assertCreateDir(new File(baseDir, "dfs"));
        hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
        hadoopConf.set("hadoop.security.group.mapping",
            MiniDFS.PseudoGroupMappingService.class.getName());
        Configuration.addDefaultResource("test.xml");

        hadoopConf.set("sentry.authorization-provider.hdfs-path-prefixes", "/user/hive/warehouse,/tmp/external");
        hadoopConf.set("sentry.authorization-provider.cache-refresh-retry-wait.ms", "5000");
        hadoopConf.set("sentry.authorization-provider.cache-refresh-interval.ms", String.valueOf(CACHE_REFRESH));

        hadoopConf.set("sentry.authorization-provider.cache-stale-threshold.ms", String.valueOf(STALE_THRESHOLD));

        hadoopConf.set("sentry.hdfs.service.security.mode", "none");
        hadoopConf.set("sentry.hdfs.service.client.server.rpc-address", "localhost");
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
    try {

      hiveUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Configuration sentryConf = new Configuration(false);
          Map<String, String> properties = Maps.newHashMap();
          properties.put(HiveServerFactory.AUTHZ_PROVIDER_BACKEND,
              SimpleDBProviderBackend.class.getName());
          properties.put(ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
              SentryHiveAuthorizationTaskFactoryImpl.class.getName());
          properties
              .put(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS.varname, "2");
          properties.put("hive.metastore.uris", "thrift://localhost:" + hmsPort);
          properties.put("hive.exec.local.scratchdir", Files.createTempDir().getAbsolutePath());
          properties.put(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
//        properties.put("sentry.service.server.compact.transport", "true");
          properties.put("sentry.hive.testing.mode", "true");
          properties.put("sentry.service.reporting", "JMX");
          properties.put(ServerConfig.ADMIN_GROUPS, "hive,admin");
          properties.put(ServerConfig.RPC_ADDRESS, "localhost");
          properties.put(ServerConfig.RPC_PORT, String.valueOf(sentryPort > 0 ? sentryPort : 0));
          properties.put(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");

          properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING, ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
          properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFileLocation.getPath());
          properties.put(ServerConfig.SENTRY_STORE_JDBC_URL,
              "jdbc:derby:;databaseName=" + baseDir.getPath()
                  + "/sentrystore_db;create=true");
          properties.put(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
          properties.put("sentry.service.processor.factories",
              "org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessorFactory,org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
          properties.put("sentry.policy.store.plugins", "org.apache.sentry.hdfs.SentryPlugin");
          properties.put(ServerConfig.RPC_MIN_THREADS, "3");
          for (Map.Entry<String, String> entry : properties.entrySet()) {
            sentryConf.set(entry.getKey(), entry.getValue());
          }
          sentryServer = SentrySrvFactory.create(SentrySrvFactory.SentrySrvType.INTERNAL_SERVER,
              sentryConf, testSentryHA ? 2 : 1);
          sentryPort = sentryServer.get(0).getAddress().getPort();
          sentryServer.startAll();
          LOGGER.info("\n\n Sentry service started \n\n");
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

    if (ignoreCleanUp) {
      LOGGER.info("\n\n Do not clean up test cases since ignoreCleanUp is set to true \n\n");
      return;
    }

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

  @Test
  public void testEnd2End() throws Throwable {
    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{"db1"};
    roles = new String[]{"admin_role", "db_role", "tab_role", "p1_admin"};
    admin = "hive";

    Connection conn;
    Statement stmt;
    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant role admin_role to group hive");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("create table p1 (s string) partitioned by (month int, day int)");
    stmt.execute("alter table p1 add partition (month=1, day=1)");
    stmt.execute("alter table p1 add partition (month=1, day=2)");
    stmt.execute("alter table p1 add partition (month=2, day=1)");
    stmt.execute("alter table p1 add partition (month=2, day=2)");

    // db privileges
    stmt.execute("create database db5");
    stmt.execute("create role db_role");
    stmt.execute("create role tab_role");
    stmt.execute("grant role db_role to group hbase");
    stmt.execute("grant role tab_role to group flume");
    stmt.execute("create table db5.p2(id int)");

    stmt.execute("create role p1_admin");
    stmt.execute("grant role p1_admin to group hbase");

    // Verify default db is inaccessible initially
    verifyOnAllSubDirs("/user/hive/warehouse", null, "hbase", false);

    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    stmt.execute("grant all on database db5 to role db_role");
    stmt.execute("use db5");
    stmt.execute("grant all on table p2 to role tab_role");
    stmt.execute("use default");
    verifyOnAllSubDirs("/user/hive/warehouse/db5.db", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db5.db/p2", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db5.db/p2", FsAction.ALL, "flume", true);
    verifyOnPath("/user/hive/warehouse/db5.db", FsAction.ALL, "flume", false);

    loadData(stmt);

    verifyHDFSandMR(stmt);

    // Verify default db is STILL inaccessible after grants but tables are fine
    verifyOnPath("/user/hive/warehouse", null, "hbase", false);
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.READ_EXECUTE, "hbase", true);

    adminUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        // Simulate hdfs dfs -setfacl -m <aclantry> <path>
        AclStatus existing =
            miniDFS.getFileSystem()
            .getAclStatus(new Path("/user/hive/warehouse/p1"));
        ArrayList<AclEntry> newEntries =
            new ArrayList<AclEntry>(existing.getEntries());
        newEntries.add(AclEntry.parseAclEntry("user::---", true));
        newEntries.add(AclEntry.parseAclEntry("group:bla:rwx", true));
        newEntries.add(AclEntry.parseAclEntry("other::---", true));
        miniDFS.getFileSystem().setAcl(new Path("/user/hive/warehouse/p1"),
            newEntries);
        return null;
      }
    });

    stmt.execute("revoke select on table p1 from role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    // Verify default db grants work
    stmt.execute("grant select on database default to role p1_admin");
    verifyOnPath("/user/hive/warehouse", FsAction.READ_EXECUTE, "hbase", true);

    // Verify default db grants are propagated to the tables
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.READ_EXECUTE, "hbase", true);

    // Verify default db revokes work
    stmt.execute("revoke select on database default from role p1_admin");
    verifyOnPath("/user/hive/warehouse", null, "hbase", false);
    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    stmt.execute("grant all on table p1 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.ALL, "hbase", true);

    stmt.execute("revoke select on table p1 from role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.WRITE_EXECUTE, "hbase", true);


    // Verify table rename works when locations are also changed
    stmt.execute("alter table p1 rename to p3");
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);
    //This is true as parent hive object's (p3) ACLS are used.
    verifyOnAllSubDirs("/user/hive/warehouse/p3/month=1/day=1", FsAction.WRITE_EXECUTE, "hbase", true);

    // Verify when oldName == newName and oldPath != newPath
    stmt.execute("alter table p3 partition (month=1, day=1) rename to partition (month=1, day=3)");
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/p3/month=1/day=3", FsAction.WRITE_EXECUTE, "hbase", true);

    // Test DB case insensitivity
    stmt.execute("create database extdb");
    stmt.execute("grant all on database ExtDb to role p1_admin");
    writeToPath("/tmp/external/ext100", 5, "foo", "bar");
    writeToPath("/tmp/external/ext101", 5, "foo", "bar");
    stmt.execute("use extdb");
    stmt.execute(
            "create table ext100 (s string) location \'/tmp/external/ext100\'");
    verifyQuery(stmt, "ext100", 5);
    verifyOnAllSubDirs("/tmp/external/ext100", FsAction.ALL, "hbase", true);
    stmt.execute("use default");

    stmt.execute("use EXTDB");
    stmt.execute(
            "create table ext101 (s string) location \'/tmp/external/ext101\'");
    verifyQuery(stmt, "ext101", 5);
    verifyOnAllSubDirs("/tmp/external/ext101", FsAction.ALL, "hbase", true);

    // Test table case insensitivity
    stmt.execute("grant all on table exT100 to role tab_role");
    verifyOnAllSubDirs("/tmp/external/ext100", FsAction.ALL, "flume", true);

    stmt.execute("use default");

    //TODO: SENTRY-795: HDFS permissions do not sync when Sentry restarts in HA mode.
    if(!testSentryHA) {
      long beforeStop = System.currentTimeMillis();
      sentryServer.stopAll();
      long timeTakenForStopMs = System.currentTimeMillis() - beforeStop;
      LOGGER.info("Time taken for Sentry server stop: " + timeTakenForStopMs);

      // Verify that Sentry permission are still enforced for the "stale" period only if stop did not take too long
      if(timeTakenForStopMs < STALE_THRESHOLD) {
        verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);
        Thread.sleep((STALE_THRESHOLD - timeTakenForStopMs));
      } else {
        LOGGER.warn("Sentry server stop took too long");
      }

      // Verify that Sentry permission are NOT enforced AFTER "stale" period
      verifyOnAllSubDirs("/user/hive/warehouse/p3", null, "hbase", false);

      sentryServer.startAll();
    }

    // Verify that After Sentry restart permissions are re-enforced
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);

    // Create new table and verify everything is fine after restart...
    stmt.execute("create table p2 (s string) partitioned by (month int, day int)");
    stmt.execute("alter table p2 add partition (month=1, day=1)");
    stmt.execute("alter table p2 add partition (month=1, day=2)");
    stmt.execute("alter table p2 add partition (month=2, day=1)");
    stmt.execute("alter table p2 add partition (month=2, day=2)");

    verifyOnAllSubDirs("/user/hive/warehouse/p2", null, "hbase", false);

    stmt.execute("grant select on table p2 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.execute("grant select on table p2 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    // Create external table
    writeToPath("/tmp/external/ext1", 5, "foo", "bar");

    stmt.execute("create table ext1 (s string) location \'/tmp/external/ext1\'");
    verifyQuery(stmt, "ext1", 5);

    // Ensure existing group permissions are never returned..
    verifyOnAllSubDirs("/tmp/external/ext1", null, "bar", false);
    verifyOnAllSubDirs("/tmp/external/ext1", null, "hbase", false);

    stmt.execute("grant all on table ext1 to role p1_admin");
    verifyOnAllSubDirs("/tmp/external/ext1", FsAction.ALL, "hbase", true);

    stmt.execute("revoke select on table ext1 from role p1_admin");
    verifyOnAllSubDirs("/tmp/external/ext1", FsAction.WRITE_EXECUTE, "hbase", true);

    // Verify database operations works correctly
    stmt.execute("create database db1");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db", null, "hbase", false);

    stmt.execute("create table db1.tbl1 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", null, "hbase", false);
    stmt.execute("create table db1.tbl2 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", null, "hbase", false);

    // Verify default db grants do not affect other dbs
    stmt.execute("grant all on database default to role p1_admin");
    verifyOnPath("/user/hive/warehouse", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db", null, "hbase", false);

    // Verify table rename works
    stmt.execute("create table q1 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/q1", FsAction.ALL, "hbase", true);
    stmt.execute("alter table q1 rename to q2");
    verifyOnAllSubDirs("/user/hive/warehouse/q2", FsAction.ALL, "hbase", true);

    // Verify table GRANTS do not trump db GRANTS
    stmt.execute("grant select on table q2 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/q2", FsAction.ALL, "hbase", true);

    stmt.execute("create table q3 (s string)");
    verifyOnAllSubDirs("/user/hive/warehouse/q3", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/q2", FsAction.ALL, "hbase", true);

    // Verify db privileges are propagated to tables
    stmt.execute("grant select on database db1 to role p1_admin");
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.READ_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    // Verify default db revokes do not affect other dbs
    stmt.execute("revoke all on database default from role p1_admin");
    verifyOnPath("/user/hive/warehouse", null, "hbase", false);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.READ_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.execute("use db1");
    stmt.execute("grant all on table tbl1 to role p1_admin");

    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    // Verify recursive revoke
    stmt.execute("revoke select on database db1 from role p1_admin");

    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.WRITE_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", null, "hbase", false);

    // Verify cleanup..
    stmt.execute("drop table tbl1");
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db/tbl1")));

    stmt.execute("drop table tbl2");
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db/tbl2")));

    stmt.execute("use default");
    stmt.execute("drop database db1");
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db")));

    // START : Verify external table set location..
    writeToPath("/tmp/external/tables/ext2_before/i=1", 5, "foo", "bar");
    writeToPath("/tmp/external/tables/ext2_before/i=2", 5, "foo", "bar");

    stmt.execute("create external table ext2 (s string) partitioned by (i int) location \'/tmp/external/tables/ext2_before\'");
    stmt.execute("alter table ext2 add partition (i=1)");
    stmt.execute("alter table ext2 add partition (i=2)");
    verifyQuery(stmt, "ext2", 10);
    verifyOnAllSubDirs("/tmp/external/tables/ext2_before", null, "hbase", false);
    stmt.execute("grant all on table ext2 to role p1_admin");
    verifyOnPath("/tmp/external/tables/ext2_before", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2/stuff.txt", FsAction.ALL, "hbase", true);

    writeToPath("/tmp/external/tables/ext2_after/i=1", 6, "foo", "bar");
    writeToPath("/tmp/external/tables/ext2_after/i=2", 6, "foo", "bar");

    stmt.execute("alter table ext2 set location \'hdfs:///tmp/external/tables/ext2_after\'");
    // Even though table location is altered, partition location is still old (still 10 rows)
    verifyQuery(stmt, "ext2", 10);
    // You have to explicitly alter partition location..
    verifyOnPath("/tmp/external/tables/ext2_before", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2/stuff.txt", FsAction.ALL, "hbase", true);

    stmt.execute("alter table ext2 partition (i=1) set location \'hdfs:///tmp/external/tables/ext2_after/i=1\'");
    stmt.execute("alter table ext2 partition (i=2) set location \'hdfs:///tmp/external/tables/ext2_after/i=2\'");
    // Now that partition location is altered, it picks up new data (12 rows instead of 10)
    verifyQuery(stmt, "ext2", 12);

    verifyOnPath("/tmp/external/tables/ext2_before", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=1/stuff.txt", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_before/i=2/stuff.txt", null, "hbase", false);
    verifyOnPath("/tmp/external/tables/ext2_after", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2/stuff.txt", FsAction.ALL, "hbase", true);
    // END : Verify external table set location..

    //Create a new table partition on the existing partition
    stmt.execute("create table tmp (s string) partitioned by (i int)");
    stmt.execute("alter table tmp add partition (i=1)");
    stmt.execute("alter table tmp partition (i=1) set location \'hdfs:///tmp/external/tables/ext2_after/i=1\'");
    stmt.execute("grant all on table tmp to role tab_role");
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "flume", true);

    //Alter table rename of external table => oldName != newName, oldPath == newPath
    stmt.execute("alter table ext2 rename to ext3");
    //Verify all original paths still have the privileges
    verifyOnPath("/tmp/external/tables/ext2_after", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1", FsAction.ALL, "flume", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=1/stuff.txt", FsAction.ALL, "hbase", true);
    verifyOnPath("/tmp/external/tables/ext2_after/i=2/stuff.txt", FsAction.ALL, "hbase", true);


    // Restart HDFS to verify if things are fine after re-start..

    // TODO : this is currently commented out since miniDFS.restartNameNode() does
    //        not work corectly on the version of hadoop sentry depends on
    //        This has been verified to work on a real cluster.
    //        Once miniDFS is fixed, this should be uncommented..
    // miniDFS.shutdown();
    // miniDFS.restartNameNode(true);
    // miniDFS.waitActive();
    // verifyOnPath("/tmp/external/tables/ext2_after", FsAction.ALL, "hbase", true);
    // verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.close();
    conn.close();
  }

  /**
   * Make sure non HDFS paths are not added to the object - location map.
   * @throws Throwable
   */
  @Test
  public void testNonHDFSLocations() throws Throwable {
    String dbName = "db2";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "user_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant all on uri 'file:///tmp/external' to role admin_role");
    stmt.execute("grant all on uri 'hdfs:///tmp/external' to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(admin, admin);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role user_role");
    stmt.execute("grant all on database " + dbName + " to role user_role");
    stmt.execute("grant role user_role to group " + StaticUserGroup.USERGROUP1);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(admin, admin);
    stmt = conn.createStatement();

    miniDFS.getFileSystem().mkdirs(tmpHDFSDir);
    miniDFS.getFileSystem().setOwner(tmpHDFSDir, "hive", "hive");

    //External table on local file system
    miniDFS.getFileSystem().mkdirs(new Path("/tmp/external/tab1_loc"));
    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1(a int) location 'file:///tmp/external/tab1_loc'");
    verifyOnAllSubDirs("/tmp/external/tab1_loc", null, StaticUserGroup.USERGROUP1, false);

    //External partitioned table on local file system
    miniDFS.getFileSystem().mkdirs(new Path("/tmp/external/tab2_loc/i=1"));
    stmt.execute("create external table tab2 (s string) partitioned by (i int) location 'file:///tmp/external/tab2_loc'");
    verifyOnAllSubDirs("/tmp/external/tab2_loc", null, StaticUserGroup.USERGROUP1, false);
    //Partition on local file system
    stmt.execute("alter table tab2 add partition (i=1)");
    stmt.execute("alter table tab2 partition (i=1) set location 'file:///tmp/external/tab2_loc/i=1'");

    verifyOnAllSubDirs("/tmp/external/tab2_loc/i=1", null, StaticUserGroup.USERGROUP1, false);

    //HDFS to local file system, also make sure does not specifying scheme still works
    stmt.execute("create external table tab3(a int) location '/tmp/external/tab3_loc'");
    // SENTRY-546
    // verifyOnAllSubDirs("/tmp/external/tab3_loc", FsAction.ALL, StaticUserGroup.USERGROUP1, true);
    verifyOnAllSubDirs("/tmp/external/tab3_loc", null, StaticUserGroup.USERGROUP1, true);
    stmt.execute("alter table tab3 set location 'file:///tmp/external/tab3_loc'");
    verifyOnAllSubDirs("/tmp/external/tab3_loc", null, StaticUserGroup.USERGROUP1, false);

    //Local file system to HDFS
    stmt.execute("create table tab4(a int) location 'file:///tmp/external/tab4_loc'");
    stmt.execute("alter table tab4 set location 'hdfs:///tmp/external/tab4_loc'");
    miniDFS.getFileSystem().mkdirs(new Path("/tmp/external/tab4_loc"));
    // SENTRY-546
    // verifyOnAllSubDirs("/tmp/external/tab4_loc", FsAction.ALL, StaticUserGroup.USERGROUP1, true);
    verifyOnAllSubDirs("/tmp/external/tab4_loc", null, StaticUserGroup.USERGROUP1, true);
    stmt.close();
    conn.close();
  }

  @Ignore("SENTRY-546")
  @Test
  public void testExternalTable() throws Throwable {
    String dbName = "db2";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant all on uri 'hdfs:///tmp/external' to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("create external table tab1(a int) location '/tmp/external/tab1_loc'");
    verifyOnAllSubDirs("/tmp/external/tab1_loc", FsAction.ALL, StaticUserGroup.ADMINGROUP, true);

    stmt.close();
    conn.close();

  }

  /**
   * Make sure when events such as table creation fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testTableCreationFailure() throws Throwable {
    String dbName = "db1";

    tmpHDFSDir = new Path("/tmp/external");
    if (!miniDFS.getFileSystem().exists(tmpHDFSDir)) {
      miniDFS.getFileSystem().mkdirs(tmpHDFSDir);
    }

    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));
    Path partitionDir = new Path("/tmp/external/p1");
    if (!miniDFS.getFileSystem().exists(partitionDir)) {
      miniDFS.getFileSystem().mkdirs(partitionDir);
    }

    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant all on uri 'hdfs:///tmp/external' to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.execute("grant role admin_role to group " + StaticUserGroup.HIVE);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);

    // Expect table creation to fail because hive:hive does not have
    // permission to write at parent directory.
    try {
      stmt.execute("create external table tab1(a int) location 'hdfs:///tmp/external/p1'");
      Assert.fail("Expect table creation to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when creating table: " + ex.getMessage());
    }

    // When the table creation failed, the path will not be managed by sentry. And the
    // permission of the path will not be hive:hive.
    verifyOnAllSubDirs("/tmp/external/p1", null, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  /**
   * Make sure when events such as add partition fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testAddPartitionFailure() throws Throwable {
    String dbName = "db1";

    tmpHDFSDir = new Path("/tmp/external");
    if (!miniDFS.getFileSystem().exists(tmpHDFSDir)) {
      miniDFS.getFileSystem().mkdirs(tmpHDFSDir);
    }

    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));
    Path partitionDir = new Path("/tmp/external/p1");
    if (!miniDFS.getFileSystem().exists(partitionDir)) {
      miniDFS.getFileSystem().mkdirs(partitionDir);
    }

    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("create external table tab2 (s string) partitioned by (month int)");

    // Expect adding partition to fail because hive:hive does not have
    // permission to write at parent directory.
    try {
      stmt.execute("alter table tab2 add partition (month = 1) location '/tmp/external/p1'");
      Assert.fail("Expect adding partition to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when adding partition: " + ex.getMessage());
    }

    // When the table creation failed, the path will not be managed by sentry. And the
    // permission of the path will not be hive:hive.
    verifyOnAllSubDirs("/tmp/external/p1", null, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  /**
   * Make sure when events such as drop table fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testDropTableFailure() throws Throwable {
    String dbName = "db1";
    tmpHDFSDir = new Path("/tmp/external");
    if (!miniDFS.getFileSystem().exists(tmpHDFSDir)) {
      miniDFS.getFileSystem().mkdirs(tmpHDFSDir);
    }

    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwxrwx"));
    Path partitionDir = new Path("/tmp/external/p1");
    if (!miniDFS.getFileSystem().exists(partitionDir)) {
      miniDFS.getFileSystem().mkdirs(partitionDir);
    }
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("create external table tab1(a int) location 'hdfs:///tmp/external/p1'");

    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));

    // Expect dropping table to fail because hive:hive does not have
    // permission to write at parent directory when
    // hive.metastore.authorization.storage.checks property is true.
    try {
      stmt.execute("drop table tab1");
      Assert.fail("Expect dropping table to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when creating table: " + ex.getMessage());
    }

    // When the table dropping failed, the path will still be managed by sentry. And the
    // permission of the path still should be hive:hive.
    verifyOnAllSubDirs("/tmp/external/p1", FsAction.ALL, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  /**
   * Make sure when events such as drop table fail, the path should not be sync to NameNode plugin.
   */
  @Test
  public void testDropPartitionFailure() throws Throwable {
    String dbName = "db1";

    tmpHDFSDir = new Path("/tmp/external");
    if (!miniDFS.getFileSystem().exists(tmpHDFSDir)) {
      miniDFS.getFileSystem().mkdirs(tmpHDFSDir);
    }

    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwxrwx"));
    Path partitionDir = new Path("/tmp/external/p1");
    if (!miniDFS.getFileSystem().exists(partitionDir)) {
      miniDFS.getFileSystem().mkdirs(partitionDir);
    }

    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
    stmt.close();
    conn.close();

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("create table tab3 (s string) partitioned by (month int)");
    stmt.execute("alter table tab3 add partition (month = 1) location '/tmp/external/p1'");

    miniDFS.getFileSystem().setPermission(tmpHDFSDir, FsPermission.valueOf("drwxrwx---"));


    // Expect dropping partition to fail because because hive:hive does not have
    // permission to write at parent directory.
    try {
      stmt.execute("ALTER TABLE tab3 DROP PARTITION (month = 1)");
      Assert.fail("Expect dropping partition to fail");
    } catch  (Exception ex) {
      LOGGER.error("Exception when dropping partition: " + ex.getMessage());
    }

    // When the partition dropping failed, the path for the partition will still
    // be managed by sentry. And the permission of the path still should be hive:hive.
    verifyOnAllSubDirs("/tmp/external/p1", FsAction.ALL, StaticUserGroup.HIVE, true);

    stmt.close();
    conn.close();
  }

  @Test
  public void testColumnPrivileges() throws Throwable {
    String dbName = "db2";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab_role", "db_role", "col_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role with grant option");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use "+ dbName);
    stmt.execute("create table p1 (s string) partitioned by (month int, day int)");
    stmt.execute("alter table p1 add partition (month=1, day=1)");
    stmt.execute("alter table p1 add partition (month=1, day=2)");
    stmt.execute("alter table p1 add partition (month=2, day=1)");
    stmt.execute("alter table p1 add partition (month=2, day=2)");
    loadData(stmt);

    stmt.execute("create role db_role");
    stmt.execute("grant select on database " + dbName + " to role db_role");
    stmt.execute("create role tab_role");
    stmt.execute("grant select on p1 to role tab_role");
    stmt.execute("create role col_role");
    stmt.execute("grant select(s) on p1 to role col_role");

    stmt.execute("grant role col_role to group "+ StaticUserGroup.USERGROUP1);

    stmt.execute("grant role tab_role to group "+ StaticUserGroup.USERGROUP2);
    stmt.execute("grant role col_role to group "+ StaticUserGroup.USERGROUP2);

    stmt.execute("grant role db_role to group "+ StaticUserGroup.USERGROUP3);
    stmt.execute("grant role col_role to group "+ StaticUserGroup.USERGROUP3);

    stmt.execute("grant role col_role to group " + StaticUserGroup.ADMINGROUP);

    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode

    //User with just column level privileges cannot read HDFS
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", null, StaticUserGroup.USERGROUP1, false);

    //User with permissions on table and column can read HDFS file
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);

    //User with permissions on db and column can read HDFS file
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP3, true);

    //User with permissions on server and column cannot read HDFS file
    //TODO:SENTRY-751
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", null, StaticUserGroup.ADMINGROUP, false);

    stmt.close();
    conn.close();

  }

  /*
  TODO:SENTRY-819
  */
  @Test
  public void testAllColumn() throws Throwable {
    String dbName = "db2";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "col_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role with grant option");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create table p1 (c1 string, c2 string) partitioned by (month int, day int)");
    stmt.execute("alter table p1 add partition (month=1, day=1)");
    loadDataTwoCols(stmt);

    stmt.execute("create role col_role");
    stmt.execute("grant select(c1,c2) on p1 to role col_role");
    stmt.execute("grant role col_role to group "+ StaticUserGroup.USERGROUP1);
    Thread.sleep(100);

    //User with privileges on all columns of the data cannot still read the HDFS files
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/p1", null, StaticUserGroup.USERGROUP1, false);

    stmt.close();
    conn.close();

  }
  //SENTRY-780
  @Test
  public void testViews() throws Throwable {
    String dbName= "db1";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    try {
      stmt.execute("create database " + dbName);
      stmt.execute("create table test(a string)");
      stmt.execute("create view testView as select * from test");
      stmt.execute("create or replace view testView as select * from test");
      stmt.execute("drop view testView");
    } catch(Exception s) {
      throw s;
    }

    stmt.close();
    conn.close();
  }

  @Test
  public void testURIsWithoutSchemeandAuthority() throws Throwable {
    // In the local test environment, EXTERNAL_SENTRY_SERVICE is false,
    // set the default URI scheme to be hdfs.
    boolean testConfOff = new Boolean(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));
    if (!testConfOff) {
      PathUtils.getConfiguration().set("fs.defaultFS", fsURI);
    }

    String dbName= "db1";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "db_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();

    stmt.execute("create database " + dbName);
    stmt.execute("create role db_role");
    stmt.execute("grant all on database " + dbName +" to role db_role");
    stmt.execute("grant all on URI '/tmp/external' to role db_role");
    stmt.execute("grant role db_role to group " + StaticUserGroup.USERGROUP1);

    conn = hiveServer2.createConnection(StaticUserGroup.USER1_1, StaticUserGroup.USER1_1);
    stmt = conn.createStatement();

    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) location '/tmp/external'");

    stmt.close();
    conn.close();
  }

  //SENTRY-884
  @Test
  public void testAccessToTableDirectory() throws Throwable {
    String dbName= "db1";

    tmpHDFSDir = new Path("/tmp/external");
    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "table_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create table tb1(a string)");

    stmt.execute("create role table_role");
    stmt.execute("grant all on table tb1 to role table_role");
    stmt.execute("grant role table_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode
    //Verify user1 is able to access table directory
    verifyAccessToPath(StaticUserGroup.USER1_1, StaticUserGroup.USERGROUP1, "/user/hive/warehouse/db1.db/tb1", true);

    stmt.close();
    conn.close();
  }

  /* SENTRY-953 */
  @Test
  public void testAuthzObjOnPartitionMultipleTables() throws Throwable {
    String dbName = "db1";

    tmpHDFSDir = new Path("/tmp/external");
    miniDFS.getFileSystem().mkdirs(tmpHDFSDir);
    Path partitionDir = new Path("/tmp/external/p1");
    miniDFS.getFileSystem().mkdirs(partitionDir);

    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab1_role", "tab2_role", "tab3_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    // Create external table tab1 on location '/tmp/external/p1'.
    // Create tab1_role, and grant it with insert permission on table tab1 to user_group1.
    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) partitioned by (month int) location '/tmp/external/p1'");
    stmt.execute("create role tab1_role");
    stmt.execute("grant insert on table tab1 to role tab1_role");
    stmt.execute("grant role tab1_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode

    // Verify that user_group1 has insert(write_execute) permission on '/tmp/external/p1'.
    verifyOnAllSubDirs("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    // Create external table tab2 and partition on location '/tmp/external'.
    // Create tab2_role, and grant it with select permission on table tab2 to user_group2.
    stmt.execute("create external table tab2 (s string) partitioned by (month int)");
    stmt.execute("alter table tab2 add partition (month = 1) location '/tmp/external'");
    stmt.execute("create role tab2_role");
    stmt.execute("grant select on table tab2 to role tab2_role");
    stmt.execute("grant role tab2_role to group " + StaticUserGroup.USERGROUP2);
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode

    // Verify that user_group2 have select(read_execute) permission on both paths.
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab2", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);
    verifyOnPath("/tmp/external", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);

    // Create table tab3 and partition on the same location '/tmp/external' as tab2.
    // Create tab3_role, and grant it with insert permission on table tab3 to user_group3.
    stmt.execute("create table tab3 (s string) partitioned by (month int)");
    stmt.execute("alter table tab3 add partition (month = 1) location '/tmp/external'");
    stmt.execute("create role tab3_role");
    stmt.execute("grant insert on table tab3 to role tab3_role");
    stmt.execute("grant role tab3_role to group " + StaticUserGroup.USERGROUP3);
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode

    // When two partitions of different tables pointing to the same location with different grants,
    // ACLs should have union (no duplicates) of both rules.
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab3", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);
    verifyOnPath("/tmp/external", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);
    verifyOnPath("/tmp/external", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    // When alter the table name (tab2 to be tabx), ACLs should remain the same.
    stmt.execute("alter table tab2 rename to tabx");
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode
    verifyOnPath("/tmp/external", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP2, true);
    verifyOnPath("/tmp/external", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    // When drop a partition that shares the same location with other partition belonging to
    // other table, should still have the other table permissions.
    stmt.execute("ALTER TABLE tabx DROP PARTITION (month = 1)");
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab3", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);
    verifyOnPath("/tmp/external", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    // When drop a table that has a partition shares the same location with other partition
    // belonging to other table, should still have the other table permissions.
    stmt.execute("DROP TABLE IF EXISTS tabx");
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode
    verifyOnAllSubDirs("/user/hive/warehouse/" + dbName + ".db/tab3", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);
    verifyOnPath("/tmp/external", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP3, true);

    stmt.close();
    conn.close();

    miniDFS.getFileSystem().delete(partitionDir, true);
  }

  /* SENTRY-953 */
  @Test
  public void testAuthzObjOnPartitionSameTable() throws Throwable {
    String dbName = "db1";

    tmpHDFSDir = new Path("/tmp/external/p1");
    miniDFS.getFileSystem().mkdirs(tmpHDFSDir);

    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab1_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    // Create table tab1 and partition on the same location '/tmp/external/p1'.
    // Create tab1_role, and grant it with insert permission on table tab1 to user_group1.
    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create table tab1 (s string) partitioned by (month int)");
    stmt.execute("alter table tab1 add partition (month = 1) location '/tmp/external/p1'");
    stmt.execute("create role tab1_role");
    stmt.execute("grant insert on table tab1 to role tab1_role");
    stmt.execute("grant role tab1_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode

    // Verify that user_group1 has insert(write_execute) permission on '/tmp/external/p1'.
    verifyOnAllSubDirs("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    // When two partitions of the same table pointing to the same location,
    // ACLS should not be repeated. Exception will be thrown if there are duplicates.
    stmt.execute("alter table tab1 add partition (month = 2) location '/tmp/external/p1'");
    verifyOnPath("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    stmt.close();
    conn.close();
  }

  /* SENTRY-953 */
  @Test
  public void testAuthzObjOnMultipleTables() throws Throwable {
    String dbName = "db1";

    tmpHDFSDir = new Path("/tmp/external/p1");
    miniDFS.getFileSystem().mkdirs(tmpHDFSDir);

    dbNames = new String[]{dbName};
    roles = new String[]{"admin_role", "tab1_role", "tab2_role"};
    admin = StaticUserGroup.ADMIN1;

    Connection conn;
    Statement stmt;

    conn = hiveServer2.createConnection("hive", "hive");
    stmt = conn.createStatement();

    stmt.execute("create role admin_role");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);

    // Create external table tab1 on location '/tmp/external/p1'.
    // Create tab1_role, and grant it with insert permission on table tab1 to user_group1.
    conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
    stmt = conn.createStatement();
    stmt.execute("create database " + dbName);
    stmt.execute("use " + dbName);
    stmt.execute("create external table tab1 (s string) partitioned by (month int) location '/tmp/external/p1'");
    stmt.execute("create role tab1_role");
    stmt.execute("grant insert on table tab1 to role tab1_role");
    stmt.execute("grant role tab1_role to group " + StaticUserGroup.USERGROUP1);
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode

    // Verify that user_group1 has insert(write_execute) permission on '/tmp/external/p1'.
    verifyOnAllSubDirs("/tmp/external/p1", FsAction.WRITE_EXECUTE, StaticUserGroup.USERGROUP1, true);

    // Create table tab2 on the same location '/tmp/external/p1' as table tab1.
    // Create tab2_role, and grant it with select permission on table tab2 to user_group1.
    stmt.execute("create table tab2 (s string) partitioned by (month int) location '/tmp/external/p1'");
    stmt.execute("create role tab2_role");
    stmt.execute("grant select on table tab2 to role tab2_role");
    stmt.execute("grant role tab2_role to group " + StaticUserGroup.USERGROUP1);

    // When two tables pointing to the same location, ACLS should have union (no duplicates)
    // of both rules.
    verifyOnPath("/tmp/external/p1", FsAction.ALL, StaticUserGroup.USERGROUP1, true);

    // When drop table tab1, ACLs of tab2 still remain.
    stmt.execute("DROP TABLE IF EXISTS tab1");
    Thread.sleep(CACHE_REFRESH);//Wait till sentry cache is updated in Namenode
    verifyOnPath("/tmp/external/p1", FsAction.READ_EXECUTE, StaticUserGroup.USERGROUP1, true);

    stmt.close();
    conn.close();
  }

  private void verifyAccessToPath(String user, String group, String path, boolean hasPermission) throws Exception{
    Path p = new Path(path);
    UserGroupInformation hadoopUser =
        UserGroupInformation.createUserForTesting(user, new String[] {group});
    FileSystem fs = DFSTestUtil.getFileSystemAs(hadoopUser, hadoopConf);
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

  private void verifyQuery(Statement stmt, String table, int n) throws Throwable {
    verifyQuery(stmt, table, n, NUM_RETRIES);
  }

  private void verifyQuery(Statement stmt, String table, int n, int retry) throws Throwable {
    ResultSet rs = null;
    try {
      rs = stmt.executeQuery("select * from " + table);
      int numRows = 0;
      while (rs.next()) { numRows++; }
      Assert.assertEquals(n, numRows);
    } catch (Throwable th) {
      if (retry > 0) {
        Thread.sleep(RETRY_WAIT);
        verifyQuery(stmt, table, n, retry - 1);
      } else {
        throw th;
      }
    }
  }

  /**
   * SENTRY-1002:
   * Ensure the paths with no scheme will not cause NPE during paths update.
   */
   @Test
   public void testMissingScheme() throws Throwable {

     // In the local test environment, EXTERNAL_SENTRY_SERVICE is false,
     // set the default URI scheme to be hdfs.
     boolean testConfOff = new Boolean(System.getProperty(EXTERNAL_SENTRY_SERVICE, "false"));
     if (!testConfOff) {
       PathsUpdate.getConfiguration().set("fs.defaultFS", "hdfs:///");
     }

     tmpHDFSDir = new Path("/tmp/external");
     if (!miniDFS.getFileSystem().exists(tmpHDFSDir)) {
       miniDFS.getFileSystem().mkdirs(tmpHDFSDir);
     }

     Path partitionDir = new Path("/tmp/external/p1");
     if (!miniDFS.getFileSystem().exists(partitionDir)) {
       miniDFS.getFileSystem().mkdirs(partitionDir);
     }

     String dbName = "db1";
     String tblName = "tab1";
     dbNames = new String[]{dbName};
     roles = new String[]{"admin_role"};
     admin = StaticUserGroup.ADMIN1;

     Connection conn;
     Statement stmt;

     conn = hiveServer2.createConnection("hive", "hive");
     stmt = conn.createStatement();
     stmt.execute("create role admin_role");
     stmt.execute("grant all on server server1 to role admin_role");
     stmt.execute("grant role admin_role to group " + StaticUserGroup.ADMINGROUP);
     stmt.close();
     conn.close();

     conn = hiveServer2.createConnection(StaticUserGroup.ADMIN1, StaticUserGroup.ADMIN1);
     stmt = conn.createStatement();
     stmt.execute("create database " + dbName);
     stmt.execute("create external table " + dbName + "." + tblName + "(s string) location '/tmp/external/p1'");

     // Deep copy of table tab1
     Table tbCopy = hmsClient.getTable(dbName, tblName);

     // Change the location of the table to strip the scheme.
     StorageDescriptor sd = hmsClient.getTable(dbName, tblName).getSd();
     sd.setLocation("/tmp/external");
     tbCopy.setSd(sd);

     // Alter table tab1 to be tbCopy which is at scheme-less location.
     // And the corresponding path will be updated to sentry server.
     hmsClient.alter_table(dbName, "tab1", tbCopy);

     // Remove the checking for the location of the table. Because of CDH-37318, alter_table will
     // no longer have the scheme-less uri location. However, if any NPE being triggered in future
     // because of any changes, the test case will cover it and capture it.
     // Assert.assertEquals("/tmp/external", hmsClient.getTable(dbName, tblName).getSd().getLocation());

     verifyOnPath("/tmp/external", FsAction.ALL, StaticUserGroup.HIVE, true);

     stmt.close();
     conn.close();
   }

  private void loadData(Statement stmt) throws IOException, SQLException {
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

  private void loadDataTwoCols(Statement stmt) throws IOException, SQLException {
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

  private void writeToPath(String path, int numRows, String user, String group) throws IOException {
    Path p = new Path(path);
    miniDFS.getFileSystem().mkdirs(p);
    miniDFS.getFileSystem().setOwner(p, user, group);
//    miniDFS.getFileSystem().setPermission(p, FsPermission.valueOf("-rwxrwx---"));
    FSDataOutputStream f1 = miniDFS.getFileSystem().create(new Path(path + "/stuff.txt"));
    for (int i = 0; i < numRows; i++) {
      f1.writeChars("random" + i + "\n");
    }
    f1.flush();
    f1.close();
    miniDFS.getFileSystem().setOwner(new Path(path + "/stuff.txt"), "asuresh", "supergroup");
    miniDFS.getFileSystem().setPermission(new Path(path + "/stuff.txt"), FsPermission.valueOf("-rwxrwx---"));
  }

  private void verifyHDFSandMR(Statement stmt) throws Throwable {
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
          if (line != null) lines.add(line);
        } while (line != null);
        Assert.assertEquals(3, lines.size());
        in.close();
        return null;
      }
    });

  }

  private void verifyOnAllSubDirs(String path, FsAction fsAction, String group, boolean groupShouldExist) throws Throwable {
    verifyOnAllSubDirs(path, fsAction, group, groupShouldExist, true);
  }

  private void verifyOnPath(String path, FsAction fsAction, String group, boolean groupShouldExist) throws Throwable {
    verifyOnAllSubDirs(path, fsAction, group, groupShouldExist, false);
  }

  private void verifyOnAllSubDirs(String path, FsAction fsAction, String group, boolean groupShouldExist, boolean recurse) throws Throwable {
    verifyOnAllSubDirs(new Path(path), fsAction, group, groupShouldExist, recurse, NUM_RETRIES);
  }

  private void verifyOnAllSubDirs(Path p, FsAction fsAction, String group, boolean groupShouldExist, boolean recurse, int retry) throws Throwable {
    FileStatus fStatus = null;
    try {
      fStatus = miniDFS.getFileSystem().getFileStatus(p);
      if (groupShouldExist) {
        Assert.assertEquals("Error at verifying Path action : " + p + " ;", fsAction, getAcls(p).get(group));
      } else {
        Assert.assertFalse("Error at verifying Path : " + p + " ," +
                " group : " + group + " ;", getAcls(p).containsKey(group));
      }
    } catch (Throwable th) {
      if (retry > 0) {
        Thread.sleep(RETRY_WAIT);
        verifyOnAllSubDirs(p, fsAction, group, groupShouldExist, recurse, retry - 1);
      } else {
        throw th;
      }
    }
    if (recurse) {
      if (fStatus.isDirectory()) {
        FileStatus[] children = miniDFS.getFileSystem().listStatus(p);
        for (FileStatus fs : children) {
          verifyOnAllSubDirs(fs.getPath(), fsAction, group, groupShouldExist, recurse, NUM_RETRIES);
        }
      }
    }
  }

  private Map<String, FsAction> getAcls(Path path) throws Exception {
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

  private void runWordCount(JobConf job, String inPath, String outPath) throws Exception {
    Path in = new Path(inPath);
    Path out = new Path(outPath);
    miniDFS.getFileSystem().delete(out, true);
    job.setJobName("TestWC");
    JobClient jobClient = new JobClient(job);
    RunningJob submittedJob = null;
    FileInputFormat.setInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, out);
    job.set("mapreduce.output.textoutputformat.separator", " ");
    job.setInputFormat(TextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setNumReduceTasks(1);
    job.setInt("mapreduce.map.maxattempts", 1);
    job.setInt("mapreduce.reduce.maxattempts", 1);

    submittedJob = jobClient.submitJob(job);
    if (!jobClient.monitorAndPrintJob(job, submittedJob)) {
      throw new IOException("job Failed !!");
    }

  }

}
