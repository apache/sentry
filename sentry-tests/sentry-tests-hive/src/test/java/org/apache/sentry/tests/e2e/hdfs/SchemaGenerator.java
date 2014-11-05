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
package org.apache.sentry.tests.e2e.hdfs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * generate test schema
 * # dbs
 * # partitioned tables per db,  # non-partitioned tables per db,
 * # partitions per table
 * # of roles
 *
 * modify test schema
 * # dbs
 * # seconds to run the updaters
 */
public class SchemaGenerator {
  
  public static class Builder {
    private static final long NUM_DBS = 100;
    private static final long NUM_NON_PARTITIONED_TABLES = 100;
    private static final long NUM_PARTITIONED_TABLES = 100;
    private static final long NUM_PARTITIONS = 100;
    private static final long NUM_ROLES = 100;
    private static final long UPDATER_DURATION = 5*60*60; // 5mins
    private long numDbs = NUM_DBS;
    private long numNonPartitionedTables = NUM_NON_PARTITIONED_TABLES;
    private long numPartitionedTables = NUM_PARTITIONED_TABLES;
    private long numPartitions = NUM_PARTITIONS;
    private long numRoles = NUM_ROLES;
    private long updaterDuration = UPDATER_DURATION;
    private String hiveConnectionUrl;
    private String userName = "";
    private String password = "";
    private String groupName = "";

    public Builder (String hiveConnectionUrl) {
      if (hiveConnectionUrl == null || hiveConnectionUrl.isEmpty()) {
        throw new IllegalArgumentException("Connection URL can not be empty");
      }
      this.hiveConnectionUrl = hiveConnectionUrl;
    }

    public Builder withNumDbs(long numDbs) {
      this.numDbs = numDbs;
      return this;
    }
    public Builder withNumNonPartitionedTables(long numNonPartitionedTables) {
      this.numNonPartitionedTables = numNonPartitionedTables;
      return this;
    }
    public void withNumPartitionedTables(long numPartitionedTables) {
      this.numPartitionedTables = numPartitionedTables;
    }
    public Builder withNumPartitions(long numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }
    public Builder withNumRoles(long numRoles) {
      this.numRoles = numRoles;
      return this;
    }
    public Builder withUserName(String userName) {
      this.userName = userName;
      return this;
    }
    public Builder withPassWord(String passWord) {
      this.password = passWord;
      return this;
    }
    public Builder withGroupName(String groupName) {
      this.groupName = groupName;
      return this;
    }
    public Builder withUpdaterDuration (long updaterDuration) {
      this.updaterDuration = updaterDuration * 1000; // Milliseconds
      return this;
    }

    public SchemaGenerator build() throws Exception {
      if (userName.isEmpty() || groupName.isEmpty()) {
        throw new IllegalArgumentException("User or group name not set");
      }
      return new SchemaGenerator(hiveConnectionUrl, userName, password, groupName, numDbs,
          numNonPartitionedTables, numPartitionedTables, numPartitions, numRoles, updaterDuration);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGenerator.class);
  private static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
  private static final String ADMIN_ROLE = "schema_test_admin_role";
  private static final String DB_NAME_PREFIX = "db_";
  private static final String TAB_NAME_PREFIX = "tab_";
  private static final String PART_TAB_NAME_PREFIX = "ptab_";
  private static final String PARTITION_PREFIX = "part_";
  private static final String ROLE_NAME_PREFIX = "role_";
  private static final String GROUP_NAME_PREFIX = "group_";
  private static final String UPDATER_TAB_PREFIX = "utab_";

  private static final String HELP_SHORT = "H";
  private static final String HELP_LONG = "help";
  private static final String NUM_DB_OPT_SHORT = "d";
  private static final String NUM_DB_OPT_LONG = "databases";
  private static final String NUM_TAB_OPT_SHORT = "t";
  private static final String NUM_TAB_OPT_LONG = "tables";
  private static final String NUM_PART_OPT_SHORT = "p";
  private static final String NUM_PART_OPT_LONG = "partitions";
  private static final String NUM_ROLE_OPT_SHORT = "r";
  private static final String NUM_ROLE_OPT_LONG = "roles";
  private static final String UPDATE_INTERVAL_OPT_SHORT = "i";
  private static final String UPDATE_INTERVAL_OPT_LONG = "updaterInterval";
  private static final String USERNAME_OPT_SHORT = "n";
  private static final String USERNAME_OPT_LONG = "userName";
  private static final String PASSWORD_OPT_SHORT = "w";
  private static final String PASSWORD_OPT_LONG = "passWord";
  private static final String GROUP_OPT_SHORT = "g";
  private static final String GROUP_OPT_LONG = "group";
  private static final String JDBC_URL_OPT_SHORT = "j";
  private static final String JDBC_URL_OPT_LONG = "jdbcUrl";
  private static final String LOAD_OPT_SHORT = "l";
  private static final String LOAD_OPT_LONG = "load";
  private static final String UPDATE_OPT_SHORT = "m";
  private static final String UPDATE_OPT_LONG = "modify";

  final private long numDbs;
  final private long numNonPartitionedTables;
  final private long numPartitionedTables;
  final private long numPartitions;
  final private long numRoles;
  final private long updaterId;
  final private long updaterDuration;
  final private String hiveConnectionUrl;
  final private String userName;
  final private String passWord;
  final private String groupName;
  private Connection hiveConnection;

  private SchemaGenerator(String hiveConnectionUrl, String userName, String passWord, String groupName, 
      long numDbs, long numNonPartitionedTables, long numPartitionedTables,
      long numPartitions, long numRoles, long updaterDuration) {
    this.hiveConnectionUrl = hiveConnectionUrl;
    this.userName = userName;
    this.passWord = passWord;
    this.groupName = groupName;
    this.numDbs = numDbs;
    this.numNonPartitionedTables = numNonPartitionedTables;
    this.numPartitionedTables = numPartitionedTables;
    this.numPartitions = numPartitions;
    this.numRoles = numRoles;
    this.updaterDuration = updaterDuration;
    this.updaterId = getPID();
  }

  public void loadSchema() throws Exception {
   Class.forName(DRIVER_CLASS);
   hiveConnection = DriverManager.getConnection(hiveConnectionUrl, userName, passWord);
   setupAdmin();
   createSchemas();
   createRoles();
   hiveConnection.close();
  }

  public void modifySchema() throws Exception {
    Class.forName(DRIVER_CLASS);
    hiveConnection = DriverManager.getConnection(hiveConnectionUrl, userName, passWord);
    changeSchema();
    hiveConnection.close();    
  }
  
  // Setup admin role if needed
  private void setupAdmin() throws Exception {
    Statement stmt = hiveConnection.createStatement();
    if (!executeAndIgnoreError(stmt, " SET ROLE " + ADMIN_ROLE)) {
      stmt.execute("CREATE ROLE " + ADMIN_ROLE);
      stmt.execute("GRANT ALL ON SERVER server1 TO ROLE " + ADMIN_ROLE);
    }
    stmt.execute("GRANT ROLE " + ADMIN_ROLE + " TO GROUP " + groupName);
    stmt.close();
  }

  /**
   * create roles and add table privileges in roles
   * @throws Exception
   */
  private void createSchemas() throws Exception {
    Statement stmt = hiveConnection.createStatement();
    for (long numDb=1; numDb <= numDbs; numDb++) {
      stmt.execute("DROP DATABASE IF EXISTS " + DB_NAME_PREFIX + numDb + " CASCADE");
      stmt.execute("CREATE DATABASE " + DB_NAME_PREFIX + numDb);

      stmt.execute("USE " + DB_NAME_PREFIX + numDb);
      for (long tabNum=1; tabNum <= numNonPartitionedTables; tabNum++) {
        stmt.execute("CREATE TABLE " +  TAB_NAME_PREFIX + tabNum + " (id INT, name STRING)");
      }

      for (long tabNum=1; tabNum <= numPartitionedTables; tabNum++) {
        stmt.execute("CREATE TABLE " +  PART_TAB_NAME_PREFIX + tabNum + " (id INT, name STRING) partitioned by (key string)");
        for (long partNum=1; partNum <= numPartitions; partNum++) {
          stmt.execute("ALTER TABLE " +  PART_TAB_NAME_PREFIX + tabNum + " ADD PARTITION (key = '" + PARTITION_PREFIX + partNum + "')");
        }
      }
    }
    stmt.close();
  }

  /**
   * create roles and add table privileges in roles
   * @throws Exception
   */
  private void createRoles() throws Exception {
    Statement stmt = hiveConnection.createStatement();
    for (long numRole=1; numRole <= numDbs; numRole++) {
      executeAndIgnoreError(stmt, "DROP ROLE " +  ROLE_NAME_PREFIX + numRole);
      stmt.execute("CREATE ROLE " +  ROLE_NAME_PREFIX + numRole);
      stmt.execute("CREATE ROLE " +  ROLE_NAME_PREFIX + numRole + " TO GROUP " + GROUP_NAME_PREFIX + numRole);
    }
    long roleNum = 1;
    for (long numDb=1; numDb <= numDbs; numDb++) {
      stmt.execute("USE " + DB_NAME_PREFIX + numDb);
      for (long tabNum=1; tabNum <= numNonPartitionedTables; tabNum++) {
        stmt.execute("GRANT SELECT ON TABLE " + TAB_NAME_PREFIX + tabNum + " TO ROLE " + ROLE_NAME_PREFIX + roleNum);
        roleNum = (++roleNum % numRoles) +1;
      }
      roleNum = 1;
      for (long tabNum=1; tabNum <= numPartitionedTables; tabNum++) {
        stmt.execute("GRANT SELECT ON TABLE " + PART_TAB_NAME_PREFIX + tabNum + " TO ROLE " + ROLE_NAME_PREFIX + roleNum);
        roleNum = (++roleNum % numRoles) +1;
      }
    }
    stmt.close();
  }

  private void changeSchema() throws Exception {
    Statement stmt = hiveConnection.createStatement();
    int tabNum = 1;
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < updaterDuration) {
      for (long numDb=1; numDb <= numDbs; numDb++) {
        stmt.execute("USE " + DB_NAME_PREFIX + numDb);
        stmt.execute("DROP TABLE IF EXISTS " + UPDATER_TAB_PREFIX + updaterId + "_" + tabNum);
        stmt.execute("CREATE TABLE " + UPDATER_TAB_PREFIX + updaterId + "_" + tabNum + "(id INT)");
        LOGGER.debug("Created table " + DB_NAME_PREFIX + numDb + "." +
              UPDATER_TAB_PREFIX + updaterId + "_" + tabNum);
        tabNum++;
      }
    }
  }

  private boolean executeAndIgnoreError(Statement stmt, String sql) throws Exception {
    try {
      stmt.execute(sql);
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  private  long getPID() {
    String processName =
        java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      return Long.parseLong(processName.split("@")[0]);
    }

  /**
   * Invoke the schema build with followig arguments
   -d,--databases <arg>         Number of databases
   -g,--group <arg>             admin group
   -H,--help                    Print this help text
   -i,--updaterInterval <arg>   updater duration
   -j,--jdbcUrl <arg>           jdbc connection URL
   -l,--load <arg>              load schema
   -m,--modify <arg>            modify schema
   -n,--userName <arg>          connecting user name
   -p,--partitions <arg>        Number of partitions per tables
   -r,--roles <arg>             Number of roles
   -t,--tables <arg>            Number of tables per Database
   -w,--passWord <arg>          user password
   */
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(NUM_DB_OPT_SHORT, NUM_DB_OPT_LONG, true, "Number of databases");
    options.addOption(NUM_TAB_OPT_SHORT, NUM_TAB_OPT_LONG, true, "Number of tables per Database");
    options.addOption(NUM_PART_OPT_SHORT, NUM_PART_OPT_LONG, true, "Number of partitions per tables");
    options.addOption(NUM_ROLE_OPT_SHORT, NUM_ROLE_OPT_LONG, true, "Number of roles");
    options.addOption(UPDATE_INTERVAL_OPT_SHORT, UPDATE_INTERVAL_OPT_LONG, true, "updater duration");
    options.addOption(USERNAME_OPT_SHORT, USERNAME_OPT_LONG, true, "connecting user name");
    options.addOption(PASSWORD_OPT_SHORT, PASSWORD_OPT_LONG, true, "user password");
    options.addOption(GROUP_OPT_SHORT, GROUP_OPT_LONG, true, "admin group");
    options.addOption(JDBC_URL_OPT_SHORT, JDBC_URL_OPT_LONG, true, "jdbc connection URL");
    OptionGroup optGroup = new OptionGroup();
    optGroup.addOption(new Option(LOAD_OPT_SHORT, LOAD_OPT_LONG, false, "load schema"));
    optGroup.addOption(new Option(UPDATE_OPT_SHORT, UPDATE_OPT_LONG, false, "modify schema"));
    optGroup.addOption(new Option(HELP_SHORT, HELP_LONG, false, "Print this help text"));
    optGroup.setRequired(true);
    options.addOptionGroup(optGroup);

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args, true);
    if (commandLine.hasOption(HELP_SHORT)) {
      new HelpFormatter().printHelp("Help", options);
      System.exit(0);
    }
    Builder schemaBuilder = new Builder(commandLine.getOptionValue(JDBC_URL_OPT_SHORT));
    for (Option passedOpts : commandLine.getOptions()) {
      if (passedOpts.getLongOpt().equals(NUM_DB_OPT_LONG)) {
        schemaBuilder.withNumDbs(Long.valueOf(passedOpts.getValue()));
      } else if (passedOpts.getLongOpt().equals(NUM_TAB_OPT_LONG)) {
        schemaBuilder.withNumNonPartitionedTables(Long.valueOf(passedOpts.getValue())).
          withNumPartitionedTables(Long.valueOf(passedOpts.getValue()));
      } else if (passedOpts.getLongOpt().equals(NUM_PART_OPT_LONG)) {
        schemaBuilder.withNumPartitions(Long.valueOf(passedOpts.getValue()));
      } else if (passedOpts.getLongOpt().equals(NUM_ROLE_OPT_LONG)) {
        schemaBuilder.withNumRoles(Long.valueOf(passedOpts.getValue()));
      } else if (passedOpts.getLongOpt().equals(UPDATE_INTERVAL_OPT_LONG)) {
        schemaBuilder.withUpdaterDuration(Long.valueOf(passedOpts.getValue()));
      } else if (passedOpts.getLongOpt().equals(USERNAME_OPT_LONG)) {
        schemaBuilder.withUserName(passedOpts.getValue());
      } else if (passedOpts.getLongOpt().equals(PASSWORD_OPT_LONG)) {
        schemaBuilder.withPassWord(passedOpts.getValue());
      } else if (passedOpts.getLongOpt().equals(GROUP_OPT_LONG)) {
        schemaBuilder.withGroupName(passedOpts.getValue());
      }
    }
    SchemaGenerator generator = schemaBuilder.build();
    if (commandLine.hasOption(LOAD_OPT_LONG)) {
      generator.loadSchema();
    } else if (commandLine.hasOption(UPDATE_OPT_LONG)) {
      generator.modifySchema();
    } else {
      new HelpFormatter().printHelp("Help", options);
      System.exit(1);
    }
  }
}
