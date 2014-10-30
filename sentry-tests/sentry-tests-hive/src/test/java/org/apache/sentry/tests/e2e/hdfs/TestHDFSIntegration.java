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
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.hdfs.SentryAuthorizationProvider;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.sentry.tests.e2e.hive.fs.MiniDFS;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.apache.sentry.tests.e2e.hive.hiveserver.InternalHiveServer;
import org.apache.sentry.tests.e2e.hive.hiveserver.InternalMetastoreServer;
import org.fest.reflect.core.Reflection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestHDFSIntegration {

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

  private MiniDFSCluster miniDFS;
  private MiniMRClientCluster miniMR;
  private InternalHiveServer hiveServer2;
  private InternalMetastoreServer metastore;
  private SentryService sentryService;
  private String fsURI;
  private int hmsPort;
  private int sentryPort = -1;
  private File baseDir;
  private File policyFileLocation;
  private UserGroupInformation adminUgi;
  private UserGroupInformation hiveUgi;

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

  private void waitOnSentryService() throws Exception {
    sentryService.start();
    final long start = System.currentTimeMillis();
    while (!sentryService.isRunning()) {
      Thread.sleep(1000);
      if (System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  @Before
  public void setup() throws Exception {
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

  private void startHiveAndMetastore() throws IOException, InterruptedException {
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
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + baseDir.getAbsolutePath() + "/metastore_db;create=true");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        hiveConf.set("javax.jdo.option.ConnectionUserName", "hive");
        hiveConf.set("javax.jdo.option.ConnectionPassword", "hive");
        hiveConf.set("datanucleus.autoCreateSchema", "true");
        hiveConf.set("datanucleus.fixedDatastore", "false");
        hiveConf.set("datanucleus.autoStartMechanism", "SchemaTable");
        hmsPort = findPort();
        System.out.println("\n\n HMS port : " + hmsPort + "\n\n");
        hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hmsPort);
        hiveConf.set("hive.metastore.pre.event.listeners", "org.apache.sentry.binding.metastore.MetastoreAuthzBinding");
        hiveConf.set("hive.metastore.event.listeners", "org.apache.sentry.binding.metastore.SentryMetastorePostEventListener");
        hiveConf.set("hive.security.authorization.task.factory", "org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl");
        hiveConf.set("hive.server2.session.hook", "org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook");

        HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
        authzConf.addResource(hiveConf);
        File confDir = assertCreateDir(new File(baseDir, "etc"));
        File accessSite = new File(confDir, HiveAuthzConf.AUTHZ_SITE_FILE);
        OutputStream out = new FileOutputStream(accessSite);
        authzConf.set("fs.defaultFS", fsURI);
        authzConf.writeXml(out);
        out.close();

        hiveConf.set("hive.sentry.conf.url", accessSite.getPath());
        System.out.println("Sentry client file : " + accessSite.getPath());

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
              while(true){}
            } catch (Exception e) {
              System.out.println("Could not start Hive Server");
            }
          }
        }.start();

        hiveServer2 = new InternalHiveServer(hiveConf);
        new Thread() {
          @Override
          public void run() {
            try {
              hiveServer2.start();
              while(true){}
            } catch (Exception e) {
              System.out.println("Could not start Hive Server");
            }
          }
        }.start();

        Thread.sleep(10000);
        return null;
      }
    });
  }

  private void startDFSandYARN() throws IOException,
      InterruptedException {
    adminUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "target/test/data");
        Configuration conf = new HdfsConfiguration();
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUTHORIZATION_PROVIDER_KEY,
            SentryAuthorizationProvider.class.getName());
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
        conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
        File dfsDir = assertCreateDir(new File(baseDir, "dfs"));
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
        conf.set("hadoop.security.group.mapping",
            MiniDFS.PseudoGroupMappingService.class.getName());
        Configuration.addDefaultResource("test.xml");

        conf.set("sentry.authorization-provider.hdfs-path-prefixes", "/user/hive/warehouse,/tmp/external");
        conf.set("sentry.authorization-provider.cache-refresh-retry-wait.ms", "5000");
        conf.set("sentry.authorization-provider.cache-stale-threshold.ms", "3000");

        conf.set("sentry.hdfs.service.security.mode", "none");
        conf.set("sentry.hdfs.service.client.server.rpc-address", "localhost");
        conf.set("sentry.hdfs.service.client.server.rpc-port", String.valueOf(sentryPort));
        EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
        miniDFS = new MiniDFSCluster.Builder(conf).build();
        Path tmpPath = new Path("/tmp");
        Path hivePath = new Path("/user/hive");
        Path warehousePath = new Path(hivePath, "warehouse");
        miniDFS.getFileSystem().mkdirs(warehousePath);
        boolean directory = miniDFS.getFileSystem().isDirectory(warehousePath);
        System.out.println("\n\n Is dir :" + directory + "\n\n");
        System.out.println("\n\n DefaultFS :" + miniDFS.getFileSystem().getUri() + "\n\n");
        fsURI = miniDFS.getFileSystem().getUri().toString();
        conf.set("fs.defaultFS", fsURI);

        // Create Yarn cluster
        // miniMR = MiniMRClientClusterFactory.create(this.getClass(), 1, conf);

        miniDFS.getFileSystem().mkdirs(tmpPath);
        miniDFS.getFileSystem().setPermission(tmpPath, FsPermission.valueOf("drwxrwxrwx"));
        miniDFS.getFileSystem().setOwner(hivePath, "hive", "hive");
        miniDFS.getFileSystem().setOwner(warehousePath, "hive", "hive");
        System.out.println("\n\n Owner :"
            + miniDFS.getFileSystem().getFileStatus(warehousePath).getOwner()
            + ", "
            + miniDFS.getFileSystem().getFileStatus(warehousePath).getGroup()
            + "\n\n");
        System.out.println("\n\n Owner tmp :"
            + miniDFS.getFileSystem().getFileStatus(tmpPath).getOwner() + ", "
            + miniDFS.getFileSystem().getFileStatus(tmpPath).getGroup() + ", "
            + miniDFS.getFileSystem().getFileStatus(tmpPath).getPermission() + ", "
            + "\n\n");

        int dfsSafeCheckRetry = 30;
        boolean hasStarted = false;
        for (int i = dfsSafeCheckRetry; i > 0; i--) {
          if (!miniDFS.getFileSystem().isInSafeMode()) {
            hasStarted = true;
            System.out.println("HDFS safemode check num times : " + (31 - i));
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

  private void startSentry() throws IOException,
      InterruptedException {
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
        properties.put(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
//        properties.put("sentry.service.server.compact.transport", "true");
        properties.put("sentry.hive.testing.mode", "true");
        properties.put("sentry.service.reporting", "CONSOLE");
        properties.put(ServerConfig.ADMIN_GROUPS, "hive,admin");
        properties.put(ServerConfig.RPC_ADDRESS, "localhost");
        properties.put(ServerConfig.RPC_PORT, String.valueOf(sentryPort < 0 ? 0 : sentryPort));
        properties.put(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");

        properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING, ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
        properties.put(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFileLocation.getPath());
        properties.put(ServerConfig.SENTRY_STORE_JDBC_URL,
            "jdbc:derby:;databaseName=" + baseDir.getPath()
                + "/sentrystore_db;create=true");
        properties.put("sentry.service.processor.factories",
            "org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessorFactory,org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
        properties.put("sentry.policy.store.plugins", "org.apache.sentry.hdfs.SentryPlugin");
        properties.put(ServerConfig.RPC_MIN_THREADS, "3");
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          sentryConf.set(entry.getKey(), entry.getValue());
        }
        sentryService = new SentryServiceFactory().create(sentryConf);
        properties.put(ClientConfig.SERVER_RPC_ADDRESS, sentryService.getAddress()
            .getHostName());
        sentryConf.set(ClientConfig.SERVER_RPC_ADDRESS, sentryService.getAddress()
            .getHostName());
        properties.put(ClientConfig.SERVER_RPC_PORT,
            String.valueOf(sentryService.getAddress().getPort()));
        sentryConf.set(ClientConfig.SERVER_RPC_PORT,
            String.valueOf(sentryService.getAddress().getPort()));
        waitOnSentryService();
        sentryPort = sentryService.getAddress().getPort();
        System.out.println("\n\n Sentry port : " + sentryPort + "\n\n");
        return null;
      }
    });
  }

  @After
  public void cleanUp() throws Exception {
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
        if (metastore != null) {
          metastore.shutdown();
        }
      }
    }
  }

  @Test
  public void testEnd2End() throws Exception {
    Thread.sleep(1000);

    Connection conn = hiveServer2.createConnection("hive", "hive");
    Statement stmt = conn.createStatement();
    stmt.execute("create role admin_role");
    stmt.execute("grant role admin_role to group hive");
    stmt.execute("grant all on server server1 to role admin_role");
    stmt.execute("create table p1 (s string) partitioned by (month int, day int)");
    stmt.execute("alter table p1 add partition (month=1, day=1)");
    stmt.execute("alter table p1 add partition (month=1, day=2)");
    stmt.execute("alter table p1 add partition (month=2, day=1)");
    stmt.execute("alter table p1 add partition (month=2, day=2)");

    stmt.execute("create role p1_admin");
    stmt.execute("grant role p1_admin to group hbase");

    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    loadData(stmt);

    verifyHDFSandMR(stmt);

    stmt.execute("revoke select on table p1 from role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p1", null, "hbase", false);

    stmt.execute("grant all on table p1 to role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.ALL, "hbase", true);

    stmt.execute("revoke select on table p1 from role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p1", FsAction.WRITE_EXECUTE, "hbase", true);

    // Verify table rename works
    stmt.execute("alter table p1 rename to p3");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);

    stmt.execute("alter table p3 partition (month=1, day=1) rename to partition (month=1, day=3)");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/p3/month=1/day=3", FsAction.WRITE_EXECUTE, "hbase", true);

    sentryService.stop();
    // Verify that Sentry permission are still enforced for the "stale" period
    Thread.sleep(500);
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);

    // Verify that Sentry permission are NOT enforced AFTER "stale" period
    Thread.sleep(3000);
    verifyOnAllSubDirs("/user/hive/warehouse/p3", null, "hbase", false);

    startSentry();
    // Verify that After Sentry restart permissions are re-enforced
    Thread.sleep(5000);
    verifyOnAllSubDirs("/user/hive/warehouse/p3", FsAction.WRITE_EXECUTE, "hbase", true);

    // Create new table and verify everything is fine after restart...
    stmt.execute("create table p2 (s string) partitioned by (month int, day int)");
    stmt.execute("alter table p2 add partition (month=1, day=1)");
    stmt.execute("alter table p2 add partition (month=1, day=2)");
    stmt.execute("alter table p2 add partition (month=2, day=1)");
    stmt.execute("alter table p2 add partition (month=2, day=2)");

    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p2", null, "hbase", false);

    stmt.execute("grant select on table p2 to role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.execute("grant select on table p2 to role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/p2", FsAction.READ_EXECUTE, "hbase", true);

    // Create external table
    writeToPath("/tmp/external/ext1", 5, "foo", "bar");

    stmt.execute("create table ext1 (s string) location \'/tmp/external/ext1\'");
    ResultSet rs = stmt.executeQuery("select * from ext1");
    int numRows = 0;
    while (rs.next()) { numRows++; }
    Assert.assertEquals(5, numRows);

    // Ensure existing group permissions are never returned..
    verifyOnAllSubDirs("/tmp/external/ext1", null, "bar", false);
    verifyOnAllSubDirs("/tmp/external/ext1", null, "hbase", false);

    stmt.execute("grant all on table ext1 to role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/tmp/external/ext1", FsAction.ALL, "hbase", true);

    stmt.execute("revoke select on table ext1 from role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/tmp/external/ext1", FsAction.WRITE_EXECUTE, "hbase", true);

    // Verify database operations works correctly
    stmt.execute("create database db1");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db", null, "hbase", false);

    stmt.execute("create table db1.tbl1 (s string)");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", null, "hbase", false);
    stmt.execute("create table db1.tbl2 (s string)");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", null, "hbase", false);

    // Verify db privileges are propagated to tables
    stmt.execute("grant select on database db1 to role p1_admin");
    Thread.sleep(1000);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.READ_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    stmt.execute("use db1");
    stmt.execute("grant all on table tbl1 to role p1_admin");
    Thread.sleep(1000);

    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.ALL, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", FsAction.READ_EXECUTE, "hbase", true);

    // Verify recursive revoke
    stmt.execute("revoke select on database db1 from role p1_admin");
    Thread.sleep(1000);

    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl1", FsAction.WRITE_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db1.db/tbl2", null, "hbase", false);

    // Verify cleanup..
    stmt.execute("drop table tbl1");
    Thread.sleep(1000);
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db/tbl1")));

    stmt.execute("drop table tbl2");
    Thread.sleep(1000);
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db/tbl2")));

    stmt.execute("use default");
    stmt.execute("drop database db1");
    Thread.sleep(1000);
    Assert.assertFalse(miniDFS.getFileSystem().exists(new Path("/user/hive/warehouse/db1.db")));

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
  }

  private void verifyHDFSandMR(Statement stmt) throws IOException,
      InterruptedException, SQLException, Exception {
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
    Thread.sleep(1000);

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

  private void verifyOnAllSubDirs(String path, FsAction fsAction, String group, boolean groupShouldExist) throws Exception {
    verifyOnAllSubDirs(new Path(path), fsAction, group, groupShouldExist);
  }

  private void verifyOnAllSubDirs(Path p, FsAction fsAction, String group, boolean groupShouldExist) throws Exception {
    FileStatus fStatus = miniDFS.getFileSystem().getFileStatus(p);
    if (groupShouldExist) {
      Assert.assertEquals(fsAction, getAcls(p).get(group));
    } else {
      Assert.assertFalse(getAcls(p).containsKey(group));
    }
    if (fStatus.isDirectory()) {
      FileStatus[] children = miniDFS.getFileSystem().listStatus(p);
      for (FileStatus fs : children) {
        verifyOnAllSubDirs(fs.getPath(), fsAction, group, groupShouldExist);
      }
    }
  }

  private Map<String, FsAction> getAcls(Path path) throws Exception {
    AclStatus aclStatus = miniDFS.getFileSystem().getAclStatus(path);
    Map<String, FsAction> acls = new HashMap<String, FsAction>();
    for (AclEntry ent : aclStatus.getEntries()) {
      if (ent.getType().equals(AclEntryType.GROUP)) {
        acls.put(ent.getName(), ent.getPermission());
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
