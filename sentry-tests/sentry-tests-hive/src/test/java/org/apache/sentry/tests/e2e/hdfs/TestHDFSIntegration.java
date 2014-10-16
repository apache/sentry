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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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

  // mock user group mapping that maps user to same group
  public static class PseudoGroupMappingService implements
      GroupMappingServiceProvider {

    @Override
    public List<String> getGroups(String user) {
      return Lists.newArrayList(user, System.getProperty("user.name"));
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      // no-op
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      // no-op
    }
  }

  private MiniDFSCluster miniDFS;
  private InternalHiveServer hiveServer2;
  private InternalMetastoreServer metastore;
  private String fsURI;
  private int hmsPort;
  private int sentryPort;
  private File baseDir;
  private UserGroupInformation admin;

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

  private static void startSentryService(SentryService sentryServer) throws Exception {
    sentryServer.start();
    final long start = System.currentTimeMillis();
    while (!sentryServer.isRunning()) {
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
    final File policyFileLocation = new File(baseDir, HiveServerFactory.AUTHZ_PROVIDER_FILENAME);
    PolicyFile policyFile = PolicyFile.setAdminOnServer1("hive")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    policyFile.write(policyFileLocation);

    admin = UserGroupInformation.createUserForTesting(
        System.getProperty("user.name"), new String[] { "supergroup" });

    UserGroupInformation hiveUgi = UserGroupInformation.createUserForTesting(
        "hive", new String[] { "hive" });

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
        properties.put("sentry.hive.testing.mode", "true");
        properties.put(ServerConfig.ADMIN_GROUPS, "hive,admin");
        properties.put(ServerConfig.RPC_ADDRESS, "localhost");
        properties.put(ServerConfig.RPC_PORT, String.valueOf(0));
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
        SentryService sentryServer = new SentryServiceFactory().create(sentryConf);
        properties.put(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.getAddress()
            .getHostName());
        sentryConf.set(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.getAddress()
            .getHostName());
        properties.put(ClientConfig.SERVER_RPC_PORT,
            String.valueOf(sentryServer.getAddress().getPort()));
        sentryConf.set(ClientConfig.SERVER_RPC_PORT,
            String.valueOf(sentryServer.getAddress().getPort()));
        startSentryService(sentryServer);
        sentryPort = sentryServer.getAddress().getPort();
        System.out.println("\n\n Sentry port : " + sentryPort + "\n\n");
        return null;
      }
    });

    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "target/test/data");
        Configuration conf = new HdfsConfiguration();
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUTHORIZATION_PROVIDER_KEY,
            SentryAuthorizationProvider.class.getName());
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
        
        File dfsDir = assertCreateDir(new File(baseDir, "dfs"));
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
        conf.set("hadoop.security.group.mapping",
            MiniDFS.PseudoGroupMappingService.class.getName());
        Configuration.addDefaultResource("test.xml");

        conf.set("sentry.authorization-provider.hdfs-path-prefixes", "/user/hive/warehouse");
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
        return null;
      }
    });


    hiveUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("sentry.metastore.plugins", "org.apache.sentry.hdfs.MetastorePlugin");
        hiveConf.set("sentry.service.client.server.rpc-address", "localhost");
        hiveConf.set("sentry.hdfs.service.client.server.rpc-address", "localhost");
        hiveConf.set("sentry.hdfs.service.client.server.rpc-port", String.valueOf(sentryPort));
        hiveConf.set("sentry.service.client.server.rpc-port", String.valueOf(sentryPort));
        hiveConf.set("sentry.service.security.mode", "none");
        hiveConf.set("sentry.hdfs.service.security.mode", "none");
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

//        hiveConf.set("hive.sentry.conf.url", "file://" + accessSite.getCanonicalPath());
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
        metastore.start();

        hiveServer2 = new InternalHiveServer(hiveConf);
        hiveServer2.start();

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

//  public Connection createConnection(String username) throws Exception {
//    String password = username;
//    Connection connection =  hiveServer2.createConnection(username, password);
//    assertNotNull("Connection is null", connection);
//    assertFalse("Connection should not be closed", connection.isClosed());
//    Statement statement  = connection.createStatement();
//    statement.close();
//    return connection;
//  }
//
//  public Statement createStatement(Connection connection)
//  throws Exception {
//    Statement statement  = connection.createStatement();
//    assertNotNull("Statement is null", statement);
//    return statement;
//  }

  @Test
  public void testSimple() throws Exception {
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
    AclStatus aclStatus = miniDFS.getFileSystem().getAclStatus(new Path("/user/hive/warehouse/p1"));
    Set<String> groups = new HashSet<String>(); 
    for (AclEntry ent : aclStatus.getEntries()) {
      if (ent.getType().equals(AclEntryType.GROUP)) {
        groups.add(ent.getName());
      }
    }
    System.out.println("Final acls [" + aclStatus + "]");
    Assert.assertEquals(false, groups.contains("hbase"));

    stmt.execute("create role p1_admin");
    stmt.execute("grant role p1_admin to group hbase");
    stmt.execute("grant select on table p1 to role p1_admin");
    Thread.sleep(1000);
    aclStatus = miniDFS.getFileSystem().getAclStatus(new Path("/user/hive/warehouse/p1"));
    groups = new HashSet<String>();
    for (AclEntry ent : aclStatus.getEntries()) {
      if (ent.getType().equals(AclEntryType.GROUP)) {
        groups.add(ent.getName());
      }
    }
    Assert.assertEquals(true, groups.contains("hbase"));

    stmt.execute("revoke select on table p1 from role p1_admin");
    Thread.sleep(1000);
    aclStatus = miniDFS.getFileSystem().getAclStatus(new Path("/user/hive/warehouse/p1"));
    groups = new HashSet<String>();
    for (AclEntry ent : aclStatus.getEntries()) {
      if (ent.getType().equals(AclEntryType.GROUP)) {
        groups.add(ent.getName());
      }
    }
    Assert.assertEquals(false, groups.contains("hbase"));
  }
}
