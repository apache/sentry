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
package org.apache.sentry.provider.db.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.beeline.BeeLine;
import org.apache.sentry.Command;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.SentryStoreSchemaInfo;
import org.apache.sentry.provider.db.tools.SentrySchemaHelper.NestedScriptParser;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;

public class SentrySchemaTool {
  private static final String SENTRY_SCRIP_DIR = File.separatorChar + "scripts"
      + File.separatorChar + "sentrystore" + File.separatorChar + "upgrade";
  private String userName = null;
  private String passWord = null;
  private boolean dryRun = false;
  private boolean verbose = false;
  private String dbOpts = null;
  private final Configuration sentryConf;
  private final String dbType;
  private final SentryStoreSchemaInfo SentryStoreSchemaInfo;

  public SentrySchemaTool(Configuration sentryConf, String dbType)
      throws SentryUserException {
    this(System.getenv("SENTRY_HOME") + SENTRY_SCRIP_DIR, sentryConf, dbType);
  }

  public SentrySchemaTool(String sentryScripPath, Configuration sentryConf,
      String dbType) throws SentryUserException {
    if (sentryScripPath == null || sentryScripPath.isEmpty()) {
      throw new SentryUserException("No Sentry script dir provided");
    }
    this.sentryConf = sentryConf;
    this.dbType = dbType;
    this.SentryStoreSchemaInfo = new SentryStoreSchemaInfo(sentryScripPath,
        dbType);
    userName = sentryConf
        .get(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_USER);
    passWord = sentryConf
        .get(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_PASS);
  }

  public Configuration getConfiguration() {
    return sentryConf;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public void setPassWord(String passWord) {
    this.passWord = passWord;
  }

  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public String getDbOpts() {
    return dbOpts;
  }

  public void setDbOpts(String dbOpts) {
    this.dbOpts = dbOpts;
  }

  private static void printAndExit(Options cmdLineOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("schemaTool", cmdLineOptions);
    System.exit(1);
  }

  /***
   * Print Hive version and schema version
   * @throws MetaException
   */
  public void showInfo() throws SentryUserException {
    Connection sentryStoreConn = getConnectionToMetastore(true);
    System.out.println("Sentry distribution version:\t "
        + SentryStoreSchemaInfo.getSentryVersion());
    System.out.println("SentryStore schema version:\t "
        + getMetaStoreSchemaVersion(sentryStoreConn));
  }

  // read schema version from sentry store
  private String getMetaStoreSchemaVersion(Connection sentryStoreConn)
      throws SentryUserException {
    String versionQuery;
    if (SentrySchemaHelper.getDbCommandParser(dbType).needsQuotedIdentifier()) {
      versionQuery = "select t.\"SCHEMA_VERSION\" from \"SENTRY_VERSION\" t";
    } else {
      versionQuery = "select t.SCHEMA_VERSION from SENTRY_VERSION t";
    }
    try {
      Statement stmt = sentryStoreConn.createStatement();
      ResultSet res = stmt.executeQuery(versionQuery);
      if (!res.next()) {
        throw new SentryUserException("Didn't find version data in sentry store");
      }
      String currentSchemaVersion = res.getString(1);
      sentryStoreConn.close();
      return currentSchemaVersion;
    } catch (SQLException e) {
      throw new SentryUserException("Failed to get schema version.", e);
    }
  }

  // test the connection sentry store using the config property
  private void testConnectionToMetastore() throws SentryUserException {
    Connection conn = getConnectionToMetastore(true);
    try {
      conn.close();
    } catch (SQLException e) {
      throw new SentryUserException("Failed to close sentry store connection", e);
    }
  }

  /***
   * get JDBC connection to sentry store db
   *
   * @param printInfo print connection parameters
   * @return
   * @throws MetaException
   */
  private Connection getConnectionToMetastore(boolean printInfo)
      throws SentryUserException {
    try {
      String connectionURL = getValidConfVar(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_URL);
      String driver = getValidConfVar("javax.jdo.option.ConnectionDriverName");
      if (printInfo) {
        System.out.println("Metastore connection URL:\t " + connectionURL);
        System.out.println("Metastore Connection Driver :\t " + driver);
        System.out.println("Metastore connection User:\t " + userName);
      }
      if ((userName == null) || userName.isEmpty()) {
        throw new SentryUserException("UserName empty ");
      }

      // load required JDBC driver
      Class.forName(driver);

      // Connect using the JDBC URL and user/pass from conf
      return DriverManager.getConnection(connectionURL, userName, passWord);
    } catch (IOException e) {
      throw new SentryUserException("Failed to get schema version.", e);
    } catch (SQLException e) {
      throw new SentryUserException("Failed to get schema version.", e);
    } catch (ClassNotFoundException e) {
      throw new SentryUserException("Failed to load driver", e);
    }
  }

  /**
   * check if the current schema version in sentry store matches the Hive version
   * @throws MetaException
   */
  public void verifySchemaVersion() throws SentryUserException {
    // don't check version if its a dry run
    if (dryRun) {
      return;
    }
    String newSchemaVersion =
        getMetaStoreSchemaVersion(getConnectionToMetastore(false));
    // verify that the new version is added to schema
    if (!SentryStoreSchemaInfo.getSentrySchemaVersion().equalsIgnoreCase(
        newSchemaVersion)) {
      throw new SentryUserException("Found unexpected schema version "
          + newSchemaVersion);
    }
  }

  /**
   * Perform sentry store schema upgrade. extract the current schema version from sentry store
   * @throws MetaException
   */
  public void doUpgrade() throws SentryUserException {
    String fromVersion = getMetaStoreSchemaVersion(getConnectionToMetastore(false));
    if (fromVersion == null || fromVersion.isEmpty()) {
      throw new SentryUserException(
          "Schema version not stored in the sentry store. "
              +
          "Metastore schema is too old or corrupt. Try specifying the version manually");
    }
    doUpgrade(fromVersion);
  }

  /**
   * Perform sentry store schema upgrade
   *
   * @param fromSchemaVer
   *          Existing version of the sentry store. If null, then read from the sentry store
   * @throws MetaException
   */
  public void doUpgrade(String fromSchemaVer) throws SentryUserException {
    if (SentryStoreSchemaInfo.getSentrySchemaVersion().equals(fromSchemaVer)) {
      System.out.println("No schema upgrade required from version " + fromSchemaVer);
      return;
    }
    // Find the list of scripts to execute for this upgrade
    List<String> upgradeScripts =
        SentryStoreSchemaInfo.getUpgradeScripts(fromSchemaVer);
    testConnectionToMetastore();
    System.out.println("Starting upgrade sentry store schema from version " +
 fromSchemaVer + " to "
        + SentryStoreSchemaInfo.getSentrySchemaVersion());
    String scriptDir = SentryStoreSchemaInfo.getSentryStoreScriptDir();
    try {
      for (String scriptFile : upgradeScripts) {
        System.out.println("Upgrade script " + scriptFile);
        if (!dryRun) {
          runBeeLine(scriptDir, scriptFile);
          System.out.println("Completed " + scriptFile);
        }
      }
    } catch (IOException eIO) {
      throw new SentryUserException(
          "Upgrade FAILED! Metastore state would be inconsistent !!", eIO);
    }

    // Revalidated the new version after upgrade
    verifySchemaVersion();
  }

  /**
   * Initialize the sentry store schema to current version
   *
   * @throws MetaException
   */
  public void doInit() throws SentryUserException {
    doInit(SentryStoreSchemaInfo.getSentrySchemaVersion());

    // Revalidated the new version after upgrade
    verifySchemaVersion();
  }

  /**
   * Initialize the sentry store schema
   *
   * @param toVersion
   *          If null then current hive version is used
   * @throws MetaException
   */
  public void doInit(String toVersion) throws SentryUserException {
    testConnectionToMetastore();
    System.out.println("Starting sentry store schema initialization to " + toVersion);

    String initScriptDir = SentryStoreSchemaInfo.getSentryStoreScriptDir();
    String initScriptFile = SentryStoreSchemaInfo.generateInitFileName(toVersion);

    try {
      System.out.println("Initialization script " + initScriptFile);
      if (!dryRun) {
        runBeeLine(initScriptDir, initScriptFile);
        System.out.println("Initialization script completed");
      }
    } catch (IOException e) {
      throw new SentryUserException("Schema initialization FAILED!"
          + " Metastore state would be inconsistent !!", e);
    }
  }

  // Flatten the nested upgrade script into a buffer
  public static String buildCommand(NestedScriptParser dbCommandParser,
        String scriptDir, String scriptFile) throws IllegalFormatException, IOException {

    BufferedReader bfReader =
        new BufferedReader(new FileReader(scriptDir + File.separatorChar + scriptFile));
    String currLine;
    StringBuilder sb = new StringBuilder();
    String currentCommand = null;
    while ((currLine = bfReader.readLine()) != null) {
      currLine = currLine.trim();
      if (currLine.isEmpty()) {
        continue; // skip empty lines
      }

      if (currentCommand == null) {
        currentCommand = currLine;
      } else {
        currentCommand = currentCommand + " " + currLine;
      }
      if (dbCommandParser.isPartialCommand(currLine)) {
        // if its a partial line, continue collecting the pieces
        continue;
      }

      // if this is a valid executable command then add it to the buffer
      if (!dbCommandParser.isNonExecCommand(currentCommand)) {
        currentCommand = dbCommandParser.cleanseCommand(currentCommand);

        if (dbCommandParser.isNestedScript(currentCommand)) {
          // if this is a nested sql script then flatten it
          String currScript = dbCommandParser.getScriptName(currentCommand);
          sb.append(buildCommand(dbCommandParser, scriptDir, currScript));
        } else {
          // Now we have a complete statement, process it
          // write the line to buffer
          sb.append(currentCommand);
          sb.append(System.getProperty("line.separator"));
        }
      }
      currentCommand = null;
    }
    bfReader.close();
    return sb.toString();
  }

  // run beeline on the given sentry store scrip, flatten the nested scripts into single file
  private void runBeeLine(String scriptDir, String scriptFile) throws IOException {
    NestedScriptParser dbCommandParser =
        SentrySchemaHelper.getDbCommandParser(dbType);
    dbCommandParser.setDbOpts(getDbOpts());
    // expand the nested script
    String sqlCommands = buildCommand(dbCommandParser, scriptDir, scriptFile);
    File tmpFile = File.createTempFile("schematool", ".sql");
    tmpFile.deleteOnExit();

    // write out the buffer into a file. Add beeline commands for autocommit and close
    FileWriter fstream = new FileWriter(tmpFile.getPath());
    BufferedWriter out = new BufferedWriter(fstream);

    out.write("!set Silent " + verbose + System.getProperty("line.separator"));
    out.write("!autocommit on" + System.getProperty("line.separator"));
    out.write("!set Isolation TRANSACTION_READ_COMMITTED"
        + System.getProperty("line.separator"));
    out.write("!set AllowMultiLineCommand false"
        + System.getProperty("line.separator"));
    out.write(sqlCommands);
    out.write("!closeall" + System.getProperty("line.separator"));
    out.close();
    runBeeLine(tmpFile.getPath());
  }

  // Generate the beeline args per hive conf and execute the given script
  public void runBeeLine(String sqlScriptFile) throws IOException {
    List<String> argList = new ArrayList<String>();
    argList.add("-u");
    argList.add(getValidConfVar(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_URL));
    argList.add("-d");
    argList.add(getValidConfVar("javax.jdo.option.ConnectionDriverName"));
    argList.add("-n");
    argList.add(userName);
    argList.add("-p");
    argList.add(passWord);
    argList.add("-f");
    argList.add(sqlScriptFile);

    BeeLine beeLine = new BeeLine();
    if (!verbose) {
      beeLine.setOutputStream(new PrintStream(new NullOutputStream()));
      // beeLine.getOpts().setSilent(true);
    }
    // beeLine.getOpts().setAllowMultiLineCommand(false);
    // beeLine.getOpts().setIsolation("TRANSACTION_READ_COMMITTED");
    int status = beeLine.begin(argList.toArray(new String[0]), null);
    if (status != 0) {
      throw new IOException("Schema script failed, errorcode " + status);
    }
  }

  private String getValidConfVar(String confVar) throws IOException {
    String confVarKey = confVar;
    if (confVar.startsWith(ServerConfig.SENTRY_DATANUCLEUS_PROPERTY_PREFIX)) {
      confVarKey = StringUtils.removeStart(confVar,
          ServerConfig.SENTRY_DB_PROPERTY_PREFIX);
    }
    String confVarValue = sentryConf.get(confVarKey);
    if (confVarValue == null || confVarValue.isEmpty()) {
      throw new IOException("Empty " + confVar);
    }
    return confVarValue;
  }

  // Create the required command line options
  @SuppressWarnings("static-access")
  private static void initOptions(Options cmdLineOptions) {
    Option help = new Option("help", "print this message");
    Option upgradeOpt = new Option("upgradeSchema", "Schema upgrade");
    Option upgradeFromOpt = OptionBuilder.withArgName("upgradeFrom").hasArg().
                withDescription("Schema upgrade from a version").
                create("upgradeSchemaFrom");
    Option initOpt = new Option("initSchema", "Schema initialization");
    Option initToOpt = OptionBuilder.withArgName("initTo").hasArg().
                withDescription("Schema initialization to a version").
                create("initSchemaTo");
    Option infoOpt = new Option("info", "Show config and schema details");

    OptionGroup optGroup = new OptionGroup();
    optGroup.addOption(upgradeOpt).addOption(initOpt).
                addOption(help).addOption(upgradeFromOpt).
                addOption(initToOpt).addOption(infoOpt);
    optGroup.setRequired(true);

    Option userNameOpt = OptionBuilder.withArgName("user")
                .hasArgs()
                .withDescription("Override config file user name")
                .create("userName");
    Option passwdOpt = OptionBuilder.withArgName("password")
                .hasArgs()
                 .withDescription("Override config file password")
                 .create("passWord");
    Option dbTypeOpt = OptionBuilder.withArgName("databaseType")
                .hasArgs().withDescription("Metastore database type")
                .create("dbType");
    Option dbOpts = OptionBuilder.withArgName("databaseOpts")
                .hasArgs().withDescription("Backend DB specific options")
                .create("dbOpts");

    Option dryRunOpt = new Option("dryRun", "list SQL scripts (no execute)");
    Option verboseOpt = new Option("verbose", "only print SQL statements");

    Option configOpt = OptionBuilder.withArgName("confName").hasArgs()
        .withDescription("Sentry Service configuration file").isRequired(true)
        .create(ServiceConstants.ServiceArgs.CONFIG_FILE_LONG);

    cmdLineOptions.addOption(help);
    cmdLineOptions.addOption(dryRunOpt);
    cmdLineOptions.addOption(userNameOpt);
    cmdLineOptions.addOption(passwdOpt);
    cmdLineOptions.addOption(dbTypeOpt);
    cmdLineOptions.addOption(verboseOpt);
    cmdLineOptions.addOption(dbOpts);
    cmdLineOptions.addOption(configOpt);
    cmdLineOptions.addOptionGroup(optGroup);
  }

  public static class CommandImpl implements Command {
    @Override
    public void run(String[] args) throws Exception {
      CommandLineParser parser = new GnuParser();
      CommandLine line = null;
      String dbType = null;
      String schemaVer = null;
      Options cmdLineOptions = new Options();
      String configFileName = null;

      // Argument handling
      initOptions(cmdLineOptions);
      try {
        line = parser.parse(cmdLineOptions, args);
      } catch (ParseException e) {
        System.err.println("SentrySchemaTool:Parsing failed.  Reason: "
            + e.getLocalizedMessage());
        printAndExit(cmdLineOptions);
      }

      if (line.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("schemaTool", cmdLineOptions);
        return;
      }

      if (line.hasOption("dbType")) {
        dbType = line.getOptionValue("dbType");
        if ((!dbType.equalsIgnoreCase(SentrySchemaHelper.DB_DERBY)
            && !dbType.equalsIgnoreCase(SentrySchemaHelper.DB_MYSQL)
            && !dbType.equalsIgnoreCase(SentrySchemaHelper.DB_POSTGRACE) && !dbType
              .equalsIgnoreCase(SentrySchemaHelper.DB_ORACLE))) {
          System.err.println("Unsupported dbType " + dbType);
          printAndExit(cmdLineOptions);
        }
      } else {
        System.err.println("no dbType supplied");
        printAndExit(cmdLineOptions);
      }
      if (line.hasOption(ServiceConstants.ServiceArgs.CONFIG_FILE_LONG)) {
        configFileName = line
            .getOptionValue(ServiceConstants.ServiceArgs.CONFIG_FILE_LONG);
      } else {
        System.err.println("no config file specified");
        printAndExit(cmdLineOptions);
      }

      try {
        SentrySchemaTool schemaTool = new SentrySchemaTool(
            SentryService.loadConfig(configFileName), dbType);

        if (line.hasOption("userName")) {
          schemaTool.setUserName(line.getOptionValue("userName"));
        }
        if (line.hasOption("passWord")) {
          schemaTool.setPassWord(line.getOptionValue("passWord"));
        }
        if (line.hasOption("dryRun")) {
          schemaTool.setDryRun(true);
        }
        if (line.hasOption("verbose")) {
          schemaTool.setVerbose(true);
        }
        if (line.hasOption("dbOpts")) {
          schemaTool.setDbOpts(line.getOptionValue("dbOpts"));
        }

        if (line.hasOption("info")) {
          schemaTool.showInfo();
        } else if (line.hasOption("upgradeSchema")) {
          schemaTool.doUpgrade();
        } else if (line.hasOption("upgradeSchemaFrom")) {
          schemaVer = line.getOptionValue("upgradeSchemaFrom");
          schemaTool.doUpgrade(schemaVer);
        } else if (line.hasOption("initSchema")) {
          schemaTool.doInit();
        } else if (line.hasOption("initSchemaTo")) {
          schemaVer = line.getOptionValue("initSchemaTo");
          schemaTool.doInit(schemaVer);
        } else {
          System.err.println("no valid option supplied");
          printAndExit(cmdLineOptions);
        }
      } catch (SentryUserException | MalformedURLException e) {
        System.err.println(e);
        if (line.hasOption("verbose")) {
          e.printStackTrace();
        }
        System.err.println("*** Sentry schemaTool failed ***");
        System.exit(1);
      }
      System.out.println("Sentry schemaTool completed");
    }
  }

}
