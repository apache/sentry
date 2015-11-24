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
package org.apache.sentry.tests.e2e.hive.hiveserver;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.fest.reflect.core.Reflection;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class HiveServerFactory {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(HiveServerFactory.class);
  private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String DERBY_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
  public static final String HIVESERVER2_TYPE = "sentry.e2etest.hiveServer2Type";
  public static final String KEEP_BASEDIR = "sentry.e2etest.keepBaseDir";
  public static final String METASTORE_CONNECTION_URL = HiveConf.ConfVars.METASTORECONNECTURLKEY.varname;
  public static final String WAREHOUSE_DIR = HiveConf.ConfVars.METASTOREWAREHOUSE.varname;
  public static final String AUTHZ_PROVIDER = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER.getVar();
  public static final String AUTHZ_PROVIDER_RESOURCE = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar();
  public static final String AUTHZ_PROVIDER_BACKEND = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar();
  public static final String AUTHZ_PROVIDER_FILENAME = "sentry-provider.ini";
  public static final String AUTHZ_SERVER_NAME = HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME.getVar();
  public static final String ACCESS_TESTING_MODE = HiveAuthzConf.AuthzConfVars.SENTRY_TESTING_MODE.getVar();
  public static final String HS2_PORT = ConfVars.HIVE_SERVER2_THRIFT_PORT.toString();
  public static final String SUPPORT_CONCURRENCY = HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname;
  public static final String HADOOPBIN = ConfVars.HADOOPBIN.toString();
  public static final String DEFAULT_AUTHZ_SERVER_NAME = "server1";
  public static final String HIVESERVER2_IMPERSONATION = "hive.server2.enable.doAs";
  public static final String METASTORE_URI = HiveConf.ConfVars.METASTOREURIS.varname;
  public static final String METASTORE_HOOK = HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname;
  public static final String METASTORE_SETUGI = HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname;
  public static final String METASTORE_BYPASS = AuthzConfVars.AUTHZ_METASTORE_SERVICE_USERS.getVar();
  public static final String METASTORE_CLIENT_TIMEOUT = HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname;
  public static final String METASTORE_RAW_STORE_IMPL = HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname;

  static {
    try {
      Assert.assertNotNull(DERBY_DRIVER_NAME + " is null", Class.forName(DERBY_DRIVER_NAME));
      Assert.assertNotNull(HIVE_DRIVER_NAME + " is null", Class.forName(HIVE_DRIVER_NAME));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static HiveServer create(Map<String, String> properties,
      File baseDir, File confDir, File logDir, String policyFile,
      FileSystem fileSystem) throws Exception {
    String type = properties.get(HIVESERVER2_TYPE);
    if(type == null) {
      type = System.getProperty(HIVESERVER2_TYPE);
    }
    if(type == null) {
      type = HiveServer2Type.InternalHiveServer2.name();
    }
    return create(HiveServer2Type.valueOf(type.trim()), properties,
        baseDir, confDir, logDir, policyFile, fileSystem);
  }
  public static HiveServer create(HiveServer2Type type,
      Map<String, String> properties, File baseDir, File confDir,
      File logDir, String policyFile, FileSystem fileSystem) throws Exception {

    if(type.equals(HiveServer2Type.UnmanagedHiveServer2)){
      LOGGER.info("Creating UnmanagedHiveServer");
      return new UnmanagedHiveServer();
    }
    if(!properties.containsKey(WAREHOUSE_DIR)) {
      LOGGER.info("fileSystem " + fileSystem.getClass().getSimpleName());
      if (fileSystem instanceof DistributedFileSystem) {
        @SuppressWarnings("static-access")
        String dfsUri = fileSystem.getDefaultUri(fileSystem.getConf()).toString();
        LOGGER.info("dfsUri " + dfsUri);
        properties.put(WAREHOUSE_DIR, dfsUri + "/data");
        fileSystem.mkdirs(new Path("/data/"), new FsPermission((short) 0777));
      } else {
        properties.put(WAREHOUSE_DIR, new File(baseDir, "warehouse").getPath());
        fileSystem.mkdirs(new Path("/", "warehouse"), new FsPermission((short) 0777));
      }
    }
    Boolean policyOnHDFS = new Boolean(System.getProperty("sentry.e2etest.policyonhdfs", "false"));
    if (policyOnHDFS) {
      // Initialize "hive.exec.scratchdir", according the description of
      // "hive.exec.scratchdir", the permission should be (733).
      // <description>HDFS root scratch dir for Hive jobs which gets created with write
      // all (733) permission. For each connecting user, an HDFS scratch dir:
      // ${hive.exec.scratchdir}/&lt;username&gt; is created,
      // with ${hive.scratch.dir.permission}.</description>
      fileSystem.mkdirs(new Path("/tmp/hive/"));
      fileSystem.setPermission(new Path("/tmp/hive/"), new FsPermission((short) 0733));
    } else {
      LOGGER.info("Setting an readable path to hive.exec.scratchdir");
      properties.put("hive.exec.scratchdir", new File(baseDir, "scratchdir").getPath());
    }
    if(!properties.containsKey(METASTORE_CONNECTION_URL)) {
      properties.put(METASTORE_CONNECTION_URL,
          String.format("jdbc:derby:;databaseName=%s;create=true",
              new File(baseDir, "metastore").getPath()));
    }
    if(!properties.containsKey(ACCESS_TESTING_MODE)) {
      properties.put(ACCESS_TESTING_MODE, "true");
    }
    if(!properties.containsKey(AUTHZ_PROVIDER_RESOURCE)) {
      LOGGER.info("Policy File location: " + policyFile);
      properties.put(AUTHZ_PROVIDER_RESOURCE, policyFile);
    }
    if(!properties.containsKey(AUTHZ_PROVIDER)) {
      properties.put(AUTHZ_PROVIDER, LocalGroupResourceAuthorizationProvider.class.getName());
    }
    if(!properties.containsKey(AUTHZ_SERVER_NAME)) {
      properties.put(AUTHZ_SERVER_NAME, DEFAULT_AUTHZ_SERVER_NAME);
    }
    if(!properties.containsKey(HS2_PORT)) {
      properties.put(HS2_PORT, String.valueOf(findPort()));
    }
    if(!properties.containsKey(SUPPORT_CONCURRENCY)) {
      properties.put(SUPPORT_CONCURRENCY, "false");
    }
    if(!properties.containsKey(HADOOPBIN)) {
      properties.put(HADOOPBIN, "./target/hadoop");
    }
    properties.put(METASTORE_RAW_STORE_IMPL,
        "org.apache.sentry.binding.metastore.AuthorizingObjectStore");
    if (!properties.containsKey(METASTORE_URI)) {
      if (HiveServer2Type.InternalMetastore.equals(type)) {
        // The configuration sentry.metastore.service.users is for the user who
        // has all access to get the metadata.
        properties.put(METASTORE_BYPASS, "accessAllMetaUser");
        properties.put(METASTORE_URI,
          "thrift://localhost:" + String.valueOf(findPort()));
        if (!properties.containsKey(METASTORE_HOOK)) {
          properties.put(METASTORE_HOOK,
              "org.apache.sentry.binding.metastore.MetastoreAuthzBinding");
        }
        properties.put(ConfVars.METASTORESERVERMINTHREADS.varname, "5");
      }
    }

    // set the SentryMetaStoreFilterHook for HiveServer2 only, not for metastore
    if (!HiveServer2Type.InternalMetastore.equals(type)) {
      properties.put(ConfVars.METASTORE_FILTER_HOOK.varname,
          org.apache.sentry.binding.metastore.SentryMetaStoreFilterHook.class.getName());
    }

    if (!properties.containsKey(METASTORE_BYPASS)) {
      properties.put(METASTORE_BYPASS, "hive,impala," + System.getProperty("user.name", ""));
    } else {
      String tempByPass = properties.get(METASTORE_BYPASS);
      tempByPass = "hive,impala," + System.getProperty("user.name", "") + "," + tempByPass;
      properties.put(METASTORE_BYPASS, tempByPass);
    }

    properties.put(METASTORE_SETUGI, "true");
    properties.put(METASTORE_CLIENT_TIMEOUT, "100");
    properties.put(ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS.varname, "true");

    properties.put(ConfVars.HIVESTATSAUTOGATHER.varname, "false");
    properties.put(ConfVars.HIVE_STATS_COLLECT_SCANCOLS.varname, "true");
    String hadoopBinPath = properties.get(HADOOPBIN);
    Assert.assertNotNull(hadoopBinPath, "Hadoop Bin");
    File hadoopBin = new File(hadoopBinPath);
    if(!hadoopBin.isFile()) {
      Assert.fail("Path to hadoop bin " + hadoopBin.getPath() + " is invalid. "
          + "Perhaps you missed the download-hadoop profile.");
    }

    /*
     * This hack, setting the hiveSiteURL field removes a previous hack involving
     * setting of system properties for each property. Although both are hacks,
     * I prefer this hack because once the system properties are set they can
     * affect later tests unless those tests clear them. This hack allows for
     * a clean switch to a new set of defaults when a new HiveConf object is created.
     */
    Reflection.staticField("hiveSiteURL")
      .ofType(URL.class)
      .in(HiveConf.class)
      .set(null);
    HiveConf hiveConf = new HiveConf();
    HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      LOGGER.info(entry.getKey() + " => " + entry.getValue());
      hiveConf.set(entry.getKey(), entry.getValue());
      authzConf.set(entry.getKey(), entry.getValue());
    }
    File hiveSite = new File(confDir, "hive-site.xml");
    File accessSite = new File(confDir, HiveAuthzConf.AUTHZ_SITE_FILE);
    OutputStream out = new FileOutputStream(accessSite);
    authzConf.writeXml(out);
    out.close();
    // points hive-site.xml at access-site.xml
    hiveConf.set(HiveAuthzConf.HIVE_SENTRY_CONF_URL, "file:///" + accessSite.getPath());

    if(!properties.containsKey(HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK.varname)) {
      hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
        "org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook");
    }
    hiveConf.set(HIVESERVER2_IMPERSONATION, "false");
    out = new FileOutputStream(hiveSite);
    hiveConf.writeXml(out);
    out.close();

    Reflection.staticField("hiveSiteURL")
      .ofType(URL.class)
      .in(HiveConf.class)
      .set(hiveSite.toURI().toURL());

    switch (type) {
    case EmbeddedHiveServer2:
      LOGGER.info("Creating EmbeddedHiveServer");
      return new EmbeddedHiveServer();
    case InternalHiveServer2:
      LOGGER.info("Creating InternalHiveServer");
      return new InternalHiveServer(hiveConf);
    case InternalMetastore:
      LOGGER.info("Creating InternalMetastoreServer");
      return new InternalMetastoreServer(hiveConf);
    case ExternalHiveServer2:
      LOGGER.info("Creating ExternalHiveServer");
      return new ExternalHiveServer(hiveConf, confDir, logDir);
    default:
      throw new UnsupportedOperationException(type.name());
    }
  }
  private static int findPort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  @VisibleForTesting
  public static enum HiveServer2Type {
    EmbeddedHiveServer2,           // Embedded HS2, directly executed by JDBC, without thrift
    InternalHiveServer2,        // Start a thrift HS2 in the same process
    InternalMetastore, // Start a thrift HS2 in the same process
    ExternalHiveServer2,   // start a remote thrift HS2
    UnmanagedHiveServer2      // Use a remote thrift HS2 already running
    ;
  }

  public static boolean isInternalServer(HiveServer2Type hs2Type) {
    return (HiveServer2Type.InternalHiveServer2.equals(hs2Type) || HiveServer2Type.InternalMetastore
        .equals(hs2Type));
  }
}
