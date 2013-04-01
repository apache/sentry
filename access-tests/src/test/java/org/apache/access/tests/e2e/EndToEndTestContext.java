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

package org.apache.access.tests.e2e;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class EndToEndTestContext {
  
  public enum HiveServe2Type {
    Embedded,           // Embedded HS2, directly executed by JDBC, without thrift 
    InternalHS2,        // Start a thrift HS2 in the same process
    StartExternalHS2,   // start a remote thrift HS2
    UseExternalHS2      // Use a remote thrift HS2 already running
  }

  private static final Logger LOGGER = LoggerFactory
      .getLogger(EndToEndTestContext.class);

  public static final String HIVESERVER2_TYPE = "access.e2etest.hiveServer2Type";
  public static final String HIVESERVER2_STARTUP_TIMEOUT = "access.e2etest.hs2StartupTimeout";
  private static final int HIVESERVER2_STARTUP_WAIT_INTERVAL = 250;
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  public static final String WAREHOUSE_DIR = HiveConf.ConfVars.METASTOREWAREHOUSE.varname;
  public static final String METASTORE_CONNECTION_URL = HiveConf.ConfVars.METASTORECONNECTURLKEY.varname;
  public static final String AUTHZ_PROVIDER = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER.getVar();
  public static final String AUTHZ_PROVIDER_RESOURCE = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar();
  public static final String AUTHZ_PROVIDER_FILENAME = "test-authz-provider.ini";
  public static final String AUTHZ_SERVER_NAME = HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME.getVar();
  public static final String DEFAULT_AUTHZ_SERVER_NAME = "server1";
  public static final String AUTHZ_EXCEPTION_SQL_STATE = "42000";
  public static final String AUTHZ_EXCEPTION_ERROR_MSG = "No valid privileges";
 
  private static File baseDir = null;
  private static File confDir;
  private File dataDir;
  private final File policyFile;
  private final Set<Connection> connections;
  private final Set<Statement> statements;
  private final HiveServe2Type standAloneServer;
  private static HiveServer2 hiveServer2 = null;
  private static Process hiveServer2Process = null;

  public EndToEndTestContext() throws Exception {
    this(new HashMap<String, String>());
  }

  public EndToEndTestContext(Map<String, String> properties)
  throws Exception {
    this(HiveServe2Type.valueOf(System.getProperty(HIVESERVER2_TYPE, HiveServe2Type.InternalHS2.toString())),
        properties);
  }

  public EndToEndTestContext(HiveServe2Type serverType, Map<String, String> properties)
      throws Exception {
    this.standAloneServer = serverType;
    connections = Sets.newHashSet();
    statements = Sets.newHashSet();
    setupDirs();
    dataDir = new File(baseDir, "data");
    FileUtils.deleteQuietly(dataDir); // clear the old dataDir if any
    assertTrue("Could not create " + dataDir, dataDir.mkdirs());
    policyFile = new File(confDir, AUTHZ_PROVIDER_FILENAME);
    setupEnv(properties);
    if (serverType.equals(HiveServe2Type.StartExternalHS2)) {
      startExternalHiveServer2();
    } else if (serverType.equals(HiveServe2Type.InternalHS2)) {
      startInternalHiveServer2();
    }
  }

  private static synchronized void setupDirs() {
    if (baseDir == null) {
      baseDir = Files.createTempDir();
      confDir = new File(baseDir, "etc");
      assertTrue("Could not create " + confDir, confDir.mkdirs());
    }
  }

  private void setupEnv(Map<String, String> properties) throws IOException,
        ClassNotFoundException {
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    if(!properties.containsKey(WAREHOUSE_DIR)) {
      System.setProperty(WAREHOUSE_DIR, new File(baseDir, "warehouse").getPath());
    }
    if(!properties.containsKey(METASTORE_CONNECTION_URL)) {
      System.setProperty(METASTORE_CONNECTION_URL,
          String.format("jdbc:derby:;databaseName=%s;create=true", new File(baseDir, "metastore").getPath()));
    }
    if(!properties.containsKey(AUTHZ_PROVIDER_RESOURCE)) {
      FileOutputStream to = new FileOutputStream(policyFile);
      Resources.copy(Resources.getResource(AUTHZ_PROVIDER_FILENAME), to);
      to.close();
      System.setProperty(AUTHZ_PROVIDER_RESOURCE, policyFile.getPath());
    }
    if(!properties.containsKey(AUTHZ_PROVIDER)) {
      System.setProperty(AUTHZ_PROVIDER, LocalGroupResourceAuthorizationProvider.class.getName());
    }
    if(!properties.containsKey(AUTHZ_SERVER_NAME)) {
      System.setProperty(AUTHZ_SERVER_NAME, DEFAULT_AUTHZ_SERVER_NAME);
    }
    assertNotNull(DRIVER_NAME + " is null", Class.forName(DRIVER_NAME));
  }

  public File getPolicyFile() {
    return policyFile;
  }

  public Connection createConnection(String username, String password) throws Exception {
    String url;
    if (standAloneServer.equals(HiveServe2Type.Embedded)) {
      url = "jdbc:hive2://";
    } else {
      url = "jdbc:hive2://" + System.getProperty(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.toString(), "localhost") +
          ":" + Integer.valueOf(System.getProperty(ConfVars.HIVE_SERVER2_THRIFT_PORT.toString(), "10000")) +
          "/default";
    }
    Connection connection =  DriverManager.getConnection(url, username, password);
    connections.add(connection);
    assertNotNull("Connection is null", connection);
    assertFalse("Connection should not be closed", connection.isClosed());
    Statement statement  = connection.createStatement();
    statement.execute("set hive.semantic.analyzer.hook = org.apache.access.binding.hive.HiveAuthzBindingHook");
    statement.close();
    return connection;
  }

  public Statement createStatement(Connection connection)
  throws Exception {
    Statement statement  = connection.createStatement();
    assertNotNull("Statement is null", statement);
    statements.add(statement);
    return statement;
  }

  public void writePolicyFile(String buf) throws IOException {
    FileOutputStream out = new FileOutputStream(policyFile);
    out.write(buf.getBytes(Charsets.UTF_8));
    out.close();
  }

  public void appendToPolicyFileWithNewLine(String buf) throws IOException {
    StringBuffer sb = new StringBuffer(buf);
    sb.append('\n');
    Files.append(sb, policyFile, Charsets.UTF_8);
  }

  public boolean deletePolicyFile() throws IOException {
     return policyFile.delete();
  }

  public void makeNewPolicy(String policyLines[]) throws FileNotFoundException {
    PrintWriter policyWriter = new PrintWriter (policyFile.toString());
    for (String line : policyLines) {
      policyWriter.println(line);
    }
    policyWriter.close();
    assertFalse(policyWriter.checkError());
  }
 
  public void close() {
    for(Statement statement : statements) {
      try {
        statement.close();
      } catch (SQLException exception) {
        LOGGER.warn("Error closing " + statement, exception);
      }
    }
    statements.clear();

    for(Connection connection : connections) {
      try {
        connection.close();
      } catch (SQLException exception) {
        LOGGER.warn("Error closing " + connection, exception);
      }
    }
    connections.clear();
  }

  // verify that the sqlexception is due to authorization failure
  public void verifyAuthzException(SQLException sqlException) throws SQLException{
    if (!sqlException.getSQLState().equals(AUTHZ_EXCEPTION_SQL_STATE) || 
        !sqlException.getMessage().contains(AUTHZ_EXCEPTION_ERROR_MSG)) {
      throw sqlException;
    }
  }

  private synchronized void startExternalHiveServer2() throws Exception {
    if (hiveServer2Process == null) {
      // generate hive-site
      HiveConf hiveConf = new HiveConf();
      hiveConf.writeXml(new FileOutputStream(new File(confDir, "hive-site.xml")));
  
      // generate authz site
      HiveAuthzConf authzConf = new HiveAuthzConf();
      authzConf.writeXml(new FileOutputStream(new File(confDir,HiveAuthzConf.AUTHZ_SITE_FILE)));
      // TODO: add set access dist as HIVE_AUX_JARS_PATH env after we have access dist
      // till then you have to copy the access and shiro jars to $HIVE_HOME/lib
      hiveServer2Process = Runtime.getRuntime().
          exec(new String[]{"/bin/sh", "-c",
              "export HIVE_CONF_DIR=" + confDir + ";" +
              System.getProperty("hive.bin.path", "/usr/bin/hive") +
              " --service hiveserver2 > " + baseDir + "/hive.out 2>&1 & echo $! > " + baseDir + "/hs2.pid" });
      // wait for the server to be online
      waitForHS2Startup();
    }
  }

  private synchronized void startInternalHiveServer2() throws Exception {
    if (hiveServer2 == null) {
      hiveServer2 = new HiveServer2();
      hiveServer2.init(new HiveConf());
      hiveServer2.start();
      waitForHS2Startup();
    }
  }

  private void waitForHS2Startup() throws Exception {
    int startupTimeout = Integer.valueOf(
        System.getProperty(HIVESERVER2_STARTUP_TIMEOUT, "10")) * 1000; // wait time in sec 
    int waitTime = 0;
    do {
      Thread.sleep(HIVESERVER2_STARTUP_WAIT_INTERVAL);
      waitTime += HIVESERVER2_STARTUP_WAIT_INTERVAL;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new HiveServer");
      }
      try {
        createConnection("foo", "bar");
        close();
        break;
      } catch (SQLException e) {
        if (e.getSQLState().trim().equals("08S01")) {
        // Ignore
        } else {
          throw e;
        }
      }
    } while (true);
  }

  public static synchronized void shutdown() {
    if (hiveServer2Process != null) {
      try {
        Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c",
            "kill -9 `cat " + baseDir + "/hs2.pid`"});
      } catch (IOException e) {
        System.out.println(e.getMessage());
      }
      hiveServer2Process.destroy();
      hiveServer2Process = null;
    }
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
      baseDir = null;
    }
  }

  public File getDataDir() {
    return dataDir;
  }
}
