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
package org.apache.sentry.binding.hive;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.authz.SentryHiveAuthorizerFactory;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for hive authz bindingshook
 * It uses the access.provider.file.ResourceAuthorizationProvider with the
 * resource test-authz-provider.ini
 */
public class TestHiveAuthzBindingHookBase {
  private static final String RESOURCE_PATH = "test-authz-provider.ini";
  // Servers, Database, Table
  private static final String SERVER1 = "server1";

  //Columns
  private static final String COLUMN1 = "col1";
  private static final String COLUMN2 = "col2";
  private static final String COLUMN3 = "col3";
  private static final List<String>COLUMN_LIST = Arrays.asList(COLUMN1, COLUMN2, COLUMN3);

  // Entities
  private Set<List<DBModelAuthorizable>> inputTabHierarcyList = new HashSet<List<DBModelAuthorizable>>();
  private Set<List<DBModelAuthorizable>> outputTabHierarcyList = new HashSet<List<DBModelAuthorizable>>();
  private Set<ReadEntity>inputs = new HashSet<ReadEntity>();
  Map<String, List<String>> tableToColumnAccessMap = new HashMap<String, List<String>>();
  private HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-deprecated-site.xml"));

  // auth bindings handler
  private HiveAuthzBindingHook testAuth = null;
  private File baseDir;
  protected File policyFileLocation;


  @Before
  public void setUp() throws Exception {
    inputTabHierarcyList.clear();
    outputTabHierarcyList.clear();
    inputs.clear();
    tableToColumnAccessMap.clear();
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);

    // create auth configuration
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
        new File(baseDir, RESOURCE_PATH).getPath());
    authzConf.set(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), SERVER1);
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");

    policyFileLocation = new File(baseDir, RESOURCE_PATH);
    HiveConf hiveConf = configureHiveAndMetastore();

    SessionState.start(hiveConf);
    testAuth = new HiveAuthzBindingHook();
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  /**
   * Test getInputHierarchyFromInputs with duplicate ReadEntity inputs
   */
  @Test
  public void testGetInputHierarchyFromInputsWithRepeatedInputs() throws Exception {

    inputTabHierarcyList = new HashSet<List<DBModelAuthorizable>>();
    ReadEntity entityObj = buildInputListWithColumnInputs(COLUMN_LIST);
    inputs.add(entityObj);
    testAuth.getInputHierarchyFromInputs(inputTabHierarcyList, inputs);

    Assert.assertEquals(1, inputTabHierarcyList.size());
    //Add the same inputs again
    testAuth.getInputHierarchyFromInputs(inputTabHierarcyList, inputs);
    Assert.assertEquals(1, inputTabHierarcyList.size());
  }

  private ReadEntity buildInputListWithColumnInputs(List<String> columns) {

    ReadEntity entity = Mockito.mock(ReadEntity.class);
    Mockito.when(entity.getAccessedColumns()).thenReturn(columns);
    Mockito.when(entity.getType()).thenReturn(Type.DATABASE);

    return entity;
  }

  private HiveConf configureHiveAndMetastore() throws IOException, InterruptedException {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set("sentry.metastore.plugins", "org.apache.sentry.hdfs.MetastorePlugin");
    hiveConf.set("sentry.service.client.server.rpc-addresses", "localhost");
    hiveConf.set("sentry.hdfs.service.client.server.rpc-addresses", "localhost");
    hiveConf.set("sentry.hdfs.service.client.server.rpc-port", String.valueOf(findPort()));
    hiveConf.set("sentry.service.client.server.rpc-port", String.valueOf(findPort()));
    hiveConf.set("sentry.service.security.mode", "none");
    hiveConf.set("sentry.hdfs.service.security.mode", "none");
    hiveConf.set("sentry.hdfs.init.update.retry.delay.ms", "500");
    hiveConf.set("sentry.hive.provider.backend",
        "org.apache.sentry.provider.db.SimpleDBProviderBackend");
    hiveConf.set("sentry.provider", LocalGroupResourceAuthorizationProvider.class.getName());
    hiveConf
        .set("sentry.hive.provider", LocalGroupResourceAuthorizationProvider.class.getName());
    hiveConf.set("sentry.hive.provider.resource", policyFileLocation.getPath());
    hiveConf.set("sentry.hive.testing.mode", "true");
    hiveConf.set("sentry.hive.server", SERVER1);

    hiveConf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    hiveConf
        .set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFileLocation.getPath());
    hiveConf.set("hive.metastore.execute.setugi", "true");
    hiveConf.set("hive.metastore.warehouse.dir", "hdfs:///user/hive/warehouse");
    hiveConf.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=" + baseDir.getAbsolutePath() + "/metastore_db;create=true");
    hiveConf
        .set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
    hiveConf.set("javax.jdo.option.ConnectionUserName", "hive");
    hiveConf.set("javax.jdo.option.ConnectionPassword", "hive");
    hiveConf.set("datanucleus.schema.autoCreateAll", "true");
    hiveConf.set("datanucleus.autoStartMechanism", "SchemaTable");
    hiveConf.set("datanucleus.schema.autoCreateTables", "true");

    hiveConf.set(ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, "true");
    hiveConf.set(ConfVars.HIVE_AUTHORIZATION_MANAGER.varname,
        SentryHiveAuthorizerFactory.class.getName());
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
    hiveConf.set("hive.metastore.uris", "thrift://localhost:" + findPort());
    hiveConf.set("hive.metastore.pre.event.listeners",
        "org.apache.sentry.binding.metastore.MetastoreAuthzBinding");
    hiveConf.set("hive.metastore.transactional.event.listeners",
        "org.apache.hive.hcatalog.listener.DbNotificationListener");
    hiveConf.set("hive.metastore.event.listeners",
        "org.apache.sentry.binding.metastore.SentrySyncHMSNotificationsPostEventListener");
    hiveConf.set("hive.metastore.event.message.factory",
        "org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory");
    hiveConf.set("hive.security.authorization.task.factory",
        "org.apache.sentry.binding.hive.SentryHiveAuthorizationTaskFactoryImpl");
    hiveConf.set("hive.server2.session.hook",
        "org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook");
    hiveConf.set("sentry.metastore.service.users",
        "hive");// queries made by hive user (beeline) skip meta store check
    // make sure metastore calls sentry post event listener
    hiveConf.set("hive.metastore.event.listeners",
        "org.apache.sentry.binding.metastore.SentrySyncHMSNotificationsPostEventListener");

    HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
    authzConf.addResource(hiveConf);
    File confDir = assertCreateDir(new File(baseDir, "etc"));
    File accessSite = new File(confDir, HiveAuthzConf.AUTHZ_SITE_FILE);
    OutputStream out = new FileOutputStream(accessSite);
    authzConf.writeXml(out);
    out.close();

    hiveConf.set("hive.sentry.conf.url", accessSite.getPath());

    File hiveSite = new File(confDir, "hive-site.xml");
    hiveConf.set("hive.server2.enable.doAs", "false");
    hiveConf.set(HiveAuthzConf.HIVE_SENTRY_CONF_URL, accessSite.toURI().toURL()
        .toExternalForm());
    out = new FileOutputStream(hiveSite);
    hiveConf.writeXml(out);
    out.close();

    return hiveConf;
  }

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
}
