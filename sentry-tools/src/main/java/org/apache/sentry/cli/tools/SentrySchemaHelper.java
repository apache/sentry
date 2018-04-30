/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.cli.tools;

import java.util.IllegalFormatException;

public final class SentrySchemaHelper {
  public static final String DB_DERBY = "derby";
  public static final String DB_MYSQL = "mysql";
  public static final String DB_POSTGRACE = "postgres";
  public static final String DB_ORACLE = "oracle";
  public static final String DB_DB2 = "db2";

  public interface NestedScriptParser {

    public enum CommandType {
      PARTIAL_STATEMENT,
      TERMINATED_STATEMENT,
      COMMENT
    }

    String DEFAUTL_DELIMITER = ";";
    /***
     * Find the type of given command
     * @param dbCommand
     * @return
     */
    boolean isPartialCommand(String dbCommand) throws IllegalArgumentException;

    /** Parse the DB specific nesting format and extract the inner script name if any
     * @param dbCommand command from parent script
     * @return
     * @throws IllegalFormatException
     */
    String getScriptName(String dbCommand) throws IllegalArgumentException;

    /***
     * Find if the given command is a nested script execution
     * @param dbCommand
     * @return
     */
    boolean isNestedScript(String dbCommand);

    /***
     * Find if the given command is should be passed to DB
     * @param dbCommand
     * @return
     */
    boolean isNonExecCommand(String dbCommand);

    /***
     * Get the SQL statement delimiter
     * @return
     */
    String getDelimiter();

    /***
     * Clear any client specific tags
     * @return
     */
    String cleanseCommand(String dbCommand);

    /***
     * Does the DB required table/column names quoted
     * @return
     */
    boolean needsQuotedIdentifier();

    /***
     * Set DB specific options if any
     * @param dbOps
     */
    void setDbOpts(String dbOps);
  }


  /***
   * Base implemenation of NestedScriptParser
   * abstractCommandParser.
   *
   */
  private static abstract class AbstractCommandParser implements NestedScriptParser {
    private String dbOpts = null;

    @Override
    public boolean isPartialCommand(String dbCommand) throws IllegalArgumentException{
      if (dbCommand == null || dbCommand.isEmpty()) {
        throw new IllegalArgumentException("invalid command line " + dbCommand);
      }
      String trimmedDbCommand = dbCommand.trim();
      return !(trimmedDbCommand.endsWith(getDelimiter()) || isNonExecCommand(trimmedDbCommand));
    }

    @Override
    public boolean isNonExecCommand(String dbCommand) {
      return dbCommand.startsWith("--") || dbCommand.startsWith("#");
    }

    @Override
    public String getDelimiter() {
      return DEFAUTL_DELIMITER;
    }

    @Override
    public String cleanseCommand(String dbCommand) {
      // strip off the delimiter
      if (dbCommand.endsWith(getDelimiter())) {
        dbCommand = dbCommand.substring(0,
            dbCommand.length() - getDelimiter().length());
      }
      return dbCommand;
    }

    @Override
    public boolean needsQuotedIdentifier() {
      return false;
    }

    @Override
    public void setDbOpts(String dbOpts) {
      this.dbOpts = dbOpts;
    }

    protected String getDbOpts() {
      return dbOpts;
    }
  }


  // Derby commandline parser
  public static class DerbyCommandParser extends AbstractCommandParser {
    private static final String DERBY_NESTING_TOKEN = "RUN";

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {

      if (!isNestedScript(dbCommand)) {
        throw new IllegalArgumentException("Not a script format " + dbCommand);
      }
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      return tokens[1].replace(";", "").replaceAll("'", "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      // Derby script format is RUN '<file>'
     return dbCommand.startsWith(DERBY_NESTING_TOKEN);
    }
  }


  // MySQL parser
  public static class MySqlCommandParser extends AbstractCommandParser {
    private static final String MYSQL_NESTING_TOKEN = "SOURCE";
    private static final String DELIMITER_TOKEN = "DELIMITER";
    private String delimiter = DEFAUTL_DELIMITER;

    @Override
    public boolean isPartialCommand(String dbCommand) throws IllegalArgumentException{
      boolean isPartial = super.isPartialCommand(dbCommand);
      // if this is a delimiter directive, reset our delimiter
      if (dbCommand.startsWith(DELIMITER_TOKEN)) {
        String[] tokens = dbCommand.split(" ");
        if (tokens.length != 2) {
          throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
        }
        delimiter = tokens[1];
      }
      return isPartial;
    }

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      // remove ending ';'
      return tokens[1].replace(";", "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      return dbCommand.startsWith(MYSQL_NESTING_TOKEN);
    }

    @Override
    public String getDelimiter() {
      return delimiter;
    }

    @Override
    public boolean isNonExecCommand(String dbCommand) {
      return super.isNonExecCommand(dbCommand) ||
          dbCommand.startsWith("/*") && dbCommand.endsWith("*/") ||
          dbCommand.startsWith(DELIMITER_TOKEN);
    }

    @Override
    public String cleanseCommand(String dbCommand) {
      return super.cleanseCommand(dbCommand).replaceAll("/\\*.*?\\*/[^;]", "");
    }

  }

  // Postgres specific parser
  public static class PostgresCommandParser extends AbstractCommandParser {
    public static final String POSTGRES_STRING_COMMAND_FILTER = "SET standard_conforming_strings";
    public static final String POSTGRES_STRING_CLIENT_ENCODING = "SET client_encoding";
    public static final String POSTGRES_SKIP_STANDARD_STRING = "postgres.filter.81";
    private static final String POSTGRES_NESTING_TOKEN = "\\i";

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      // remove ending ';'
      return tokens[1].replace(";", "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      return dbCommand.startsWith(POSTGRES_NESTING_TOKEN);
    }

    @Override
    public boolean needsQuotedIdentifier() {
      return true;
    }

    @Override
    public boolean isNonExecCommand(String dbCommand) {
      // Skip "standard_conforming_strings" command which is not supported in older postgres
      if (POSTGRES_SKIP_STANDARD_STRING.equalsIgnoreCase(getDbOpts()) 
        && (dbCommand.startsWith(POSTGRES_STRING_COMMAND_FILTER) || dbCommand.startsWith(POSTGRES_STRING_CLIENT_ENCODING))) {
        return true;
      }
      return super.isNonExecCommand(dbCommand);
    }
  }

  //Oracle specific parser
  public static class OracleCommandParser extends AbstractCommandParser {
    private static final String ORACLE_NESTING_TOKEN = "@";
    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
      if (!isNestedScript(dbCommand)) {
        throw new IllegalArgumentException("Not a nested script format " + dbCommand);
      }
      // remove ending ';' and starting '@'
      return dbCommand.replace(";", "").replace(ORACLE_NESTING_TOKEN, "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      return dbCommand.startsWith(ORACLE_NESTING_TOKEN);
    }
  }

  // DB2 commandline parser
  public static class DB2CommandParser extends AbstractCommandParser {

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
        //DB2 does not support nesting script
        throw new IllegalArgumentException("DB2 does not support nesting script " + dbCommand);
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
        //DB2 does not support nesting script
     return false;
    }
  }

  public static NestedScriptParser getDbCommandParser(String dbName) {
    if (dbName.equalsIgnoreCase(DB_DERBY)) {
      return new DerbyCommandParser();
    } else if (dbName.equalsIgnoreCase(DB_MYSQL)) {
      return new MySqlCommandParser();
    } else if (dbName.equalsIgnoreCase(DB_POSTGRACE)) {
      return new PostgresCommandParser();
    } else if (dbName.equalsIgnoreCase(DB_ORACLE)) {
        return new OracleCommandParser();
    } else if (dbName.equalsIgnoreCase(DB_DB2)) {
      return new DB2CommandParser();
    } else {
      throw new IllegalArgumentException("Unknown dbType " + dbName);
    }
  }
  
  private SentrySchemaHelper() {
    // Make constructor private to avoid instantiation
  }
}
