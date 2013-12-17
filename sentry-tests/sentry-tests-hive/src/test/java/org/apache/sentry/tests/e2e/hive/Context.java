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

package org.apache.sentry.tests.e2e.hive;

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

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class Context {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(Context.class);

  public static final String AUTHZ_EXCEPTION_SQL_STATE = "42000";
  public static final String AUTHZ_EXEC_HOOK_EXCEPTION_SQL_STATE = "08S01";
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

  public Connection createConnection(String username) throws Exception {

    String password = username;
    Connection connection =  hiveServer.createConnection(username, password);
    connections.add(connection);
    assertNotNull("Connection is null", connection);
    assertFalse("Connection should not be closed", connection.isClosed());
    Statement statement  = connection.createStatement();
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
  /**
   * Deprecated} use append()
   */
  public void writePolicyFile(String buf) throws IOException {
    FileOutputStream out = new FileOutputStream(policyFile);
    out.write(buf.getBytes(Charsets.UTF_8));
    out.close();
  }
  /**
   * Deprecated} use append()
   */
  @Deprecated
  public void appendToPolicyFileWithNewLine(String line) throws IOException {
    append(line);
  }
  public void append(String...lines) throws IOException {
    StringBuffer buffer = new StringBuffer();
    for(String line : lines) {
      buffer.append(line).append("\n");
    }
    Files.append(buffer, policyFile, Charsets.UTF_8);
  }

  public boolean deletePolicyFile() throws IOException {
     return policyFile.delete();
  }
  /**
   * Deprecated} use append()
   */
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

  public void assertAuthzException(Statement statement, String query)
      throws SQLException {
    try {
      statement.execute(query);
      Assert.fail("Expected SQLException for '" + query + "'");
    } catch (SQLException e) {
      verifyAuthzException(e);
    }
  }

  public void assertAuthzExecHookException(Statement statement, String query)
      throws SQLException {
    try {
      statement.execute(query);
      Assert.fail("Expected SQLException for '" + query + "'");
    } catch (SQLException e) {
      verifyAuthzExecHookException(e);
    }
  }


  // verify that the sqlexception is due to authorization failure
  public void verifyAuthzException(SQLException sqlException) throws SQLException{
    verifyAuthzExceptionForState(sqlException, AUTHZ_EXCEPTION_SQL_STATE);
  }

  // verify that the sqlexception is due to authorization failure due to exec hooks
  public void verifyAuthzExecHookException(SQLException sqlException) throws SQLException{
    verifyAuthzExceptionForState(sqlException, AUTHZ_EXEC_HOOK_EXCEPTION_SQL_STATE);
  }

  // verify that the sqlexception is due to authorization failure
  private void verifyAuthzExceptionForState(SQLException sqlException,
        String expectedSqlState) throws SQLException {
    if (!expectedSqlState.equals(sqlException.getSQLState())) {
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

  public String getProperty(String propName) {
    return hiveServer.getProperty(propName);
  }
}