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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.apache.access.tests.e2e.hiveserver.HiveServer;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class Context {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(Context.class);

  public static final String AUTHZ_EXCEPTION_SQL_STATE = "42000";
  public static final String AUTHZ_EXCEPTION_ERROR_MSG = "No valid privileges";

  private final HiveServer hiveServer;
  private final FileSystem fileSystem;
  private final File baseDir;
  private final File dataDir;

  private final File policyFile;
  private final Set<Connection> connections;
  private final Set<Statement> statements;


  public Context(HiveServer hiveServer, FileSystem fileSystem,
      File baseDir, File confDir, File dataDir, File policyFile) throws Exception {
    this.hiveServer = hiveServer;
    this.fileSystem = fileSystem;
    this.baseDir = baseDir;
    this.dataDir = dataDir;
    this.policyFile = policyFile;
    connections = Sets.newHashSet();
    statements = Sets.newHashSet();
  }

  public Connection createConnection(String username, String password) throws Exception {
    String url = hiveServer.getURL();
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
    if (!AUTHZ_EXCEPTION_SQL_STATE.equals(sqlException.getSQLState())) {
      throw sqlException;
    }
  }
  public File getBaseDir() {
    return baseDir;
  }

  public File getDataDir() {
    return dataDir;
  }

  public File getPolicyFile() {
    return policyFile;
  }

  @SuppressWarnings("static-access")
  public URI getDFSUri() throws IOException {
    return fileSystem.getDefaultUri(fileSystem.getConf());
  }
}