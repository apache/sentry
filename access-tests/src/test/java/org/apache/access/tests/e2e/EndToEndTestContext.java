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
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class EndToEndTestContext {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(EndToEndTestContext.class);
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  public static final String WAREHOUSE_DIR = HiveConf.ConfVars.METASTOREWAREHOUSE.varname;
  public static final String METASTORE_CONNECTION_URL = HiveConf.ConfVars.METASTORECONNECTURLKEY.varname;
  public static final String AUTHZ_PROVIDER = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER.getVar();
  public static final String AUTHZ_PROVIDER_RESOURCE = HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar();
  public static final String AUTHZ_PROVIDER_FILENAME = "test-authz-provider.ini";

  private final File baseDir;
  private final File confDir;
  private final File policyFile;
  private final Set<Connection> connections;
  private final Set<Statement> statements;
  private final boolean standAloneServer;
  public EndToEndTestContext(boolean standAloneServer)
  throws Exception {
    this(standAloneServer, new HashMap<String, String>());
  }
  public EndToEndTestContext(boolean standAloneServer, Map<String, String> properties)
  throws Exception {
    this.standAloneServer = standAloneServer;
    connections = Sets.newHashSet();
    statements = Sets.newHashSet();
    baseDir = Files.createTempDir();
    confDir = new File(baseDir, "etc");
    assertTrue("Could not create " + confDir, confDir.mkdirs());
    policyFile = new File(confDir, AUTHZ_PROVIDER_FILENAME);
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
    assertNotNull(DRIVER_NAME + " is null", Class.forName(DRIVER_NAME));
  }
  public File getPolicyFile() {
    return policyFile;
  }
  public Connection createConnection(String username, String password) throws Exception {
    String url = standAloneServer ? "jdbc:hive2://localhost:10000/default" : "jdbc:hive2://";
    Connection connection =  DriverManager.getConnection(url, username, password);
    connections.add(connection);
    assertNotNull("Connection is null", connection);
    assertFalse("Connection should not be closed", connection.isClosed());
    return connection;
  }

  public Statement createStatement(Connection connection)
  throws Exception {
    Statement statement  = connection.createStatement();
    assertNotNull("Statement is null", statement);
    statements.add(statement);
    statement.execute("show tables");
    statement.execute("set hive.support.concurrency = false");
    statement.execute("set hive.semantic.analyzer.hook = org.apache.access.binding.hive.HiveAuthzBindingHook");
    return statement;
  }

  public void close() {
    for(Statement statement : statements) {
      try {
        statement.close();
      } catch (SQLException exception) {
        LOGGER.warn("Error closing " + statement, exception);
      }
    }
    for(Connection connection : connections) {
      try {
        connection.close();
      } catch (SQLException exception) {
        LOGGER.warn("Error closing " + connection, exception);
      }
    }
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }
}