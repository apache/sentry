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
package org.apache.access.tests.e2e.hiveserver;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

public class HiveServerFactory {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(HiveServerFactory.class);
  private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String DERBY_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
  public static final String HIVESERVER2_TYPE = "access.e2etest.hiveServer2Type";
  public static final String KEEP_BASEDIR = "access.e2etest.keepBaseDir";
  public static final String METASTORE_CONNECTION_URL = HiveConf.ConfVars.METASTORECONNECTURLKEY.varname;
  public static final String WAREHOUSE_DIR = HiveConf.ConfVars.METASTOREWAREHOUSE.varname;
  public static final String AUTHZ_PROVIDER = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER.getVar();
  public static final String AUTHZ_PROVIDER_RESOURCE = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar();
  public static final String AUTHZ_PROVIDER_FILENAME = "test-authz-provider.ini";
  public static final String AUTHZ_SERVER_NAME = HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME.getVar();
  public static final String ACCESS_TESTING_MODE = HiveAuthzConf.AuthzConfVars.ACCESS_TESTING_MODE.getVar();
  public static final String HS2_PORT = ConfVars.HIVE_SERVER2_THRIFT_PORT.toString();
  public static final String DEFAULT_AUTHZ_SERVER_NAME = "server1";

  static {
    try {
      Assert.assertNotNull(DERBY_DRIVER_NAME + " is null", Class.forName(DERBY_DRIVER_NAME));
      Assert.assertNotNull(HIVE_DRIVER_NAME + " is null", Class.forName(HIVE_DRIVER_NAME));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static HiveServer create(Map<String, String> properties,
      File baseDir, File confDir, File policyFile, FileSystem fileSystem)
          throws Exception {
    String type = properties.get(HIVESERVER2_TYPE);
    if(type == null) {
      type = System.getProperty(HIVESERVER2_TYPE);
    }
    if(type == null) {
      type = HiveServer2Type.InternalHiveServer2.name();
    }
    return create(HiveServer2Type.valueOf(type.trim()), properties,
        baseDir, confDir, policyFile, fileSystem);
  }

  private static HiveServer create(HiveServer2Type type,
      Map<String, String> properties, File baseDir, File confDir,
      File policyFile, FileSystem fileSystem) throws Exception {
    if(!properties.containsKey(WAREHOUSE_DIR)) {
      @SuppressWarnings("static-access")
      String dfsUri = fileSystem.getDefaultUri(fileSystem.getConf()).toString();
      if (fileSystem instanceof DistributedFileSystem) {
        properties.put(WAREHOUSE_DIR, dfsUri + "/data");
      } else {
        properties.put(WAREHOUSE_DIR, new File(baseDir, "warehouse").getPath());
      }
    }
    if(!properties.containsKey(METASTORE_CONNECTION_URL)) {
      properties.put(METASTORE_CONNECTION_URL,
          String.format("jdbc:derby:;databaseName=%s;create=true",
          new File(baseDir, "metastore").getPath()));
    }
    if(policyFile.exists()) {
      LOGGER.info("Policy file " + policyFile + " exists");
    } else {
      LOGGER.info("Creating policy file " + policyFile);
      FileOutputStream to = new FileOutputStream(policyFile);
      Resources.copy(Resources.getResource(AUTHZ_PROVIDER_FILENAME), to);
      to.close();
    }
    if(!properties.containsKey(ACCESS_TESTING_MODE)) {
      properties.put(ACCESS_TESTING_MODE, "true");
    }
    if(!properties.containsKey(AUTHZ_PROVIDER_RESOURCE)) {
      properties.put(AUTHZ_PROVIDER_RESOURCE, policyFile.getPath());
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
    /*
     * The system properties need to be set because we don't have
     * references to the configuration objects which will be created
     * by the servers. However, the child objects need the HiveConf
     * and HiveAuthzConf objects.
     */
    HiveConf conf = new HiveConf();
    HiveAuthzConf authzConf = new HiveAuthzConf();
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      LOGGER.debug(entry.getKey() + " => " + entry.getValue());
      System.setProperty(entry.getKey(), entry.getValue());
      conf.set(entry.getKey(), entry.getValue());
      authzConf.set(entry.getKey(), entry.getValue());
    }
    switch (type) {
    case EmbeddedHiveServer2:
      LOGGER.info("Creating EmbeddedHiveServer");
      return new EmbeddedHiveServer();
    case InternalHiveServer2:
      LOGGER.info("Creating InternalHiveServer");
      return new InternalHiveServer(conf);
    case ExternalHiveServer2:
      LOGGER.info("Creating ExternalHiveServer");
      return new ExternalHiveServer(conf, authzConf, baseDir);
    case UnmanagedHiveServer2:
      LOGGER.info("Creating UnmanagedHiveServer");
      return new UnmanagedHiveServer(conf);
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

  private static enum HiveServer2Type {
    EmbeddedHiveServer2,           // Embedded HS2, directly executed by JDBC, without thrift
    InternalHiveServer2,        // Start a thrift HS2 in the same process
    ExternalHiveServer2,   // start a remote thrift HS2
    UnmanagedHiveServer2      // Use a remote thrift HS2 already running
    ;
  }
}
