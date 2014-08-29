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

package org.apache.sentry.binding.hive.authz;

import com.google.common.collect.Table;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.sentry.Command;
import org.apache.sentry.binding.hive.HiveAuthzBindingHook;
import org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;

import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.policy.db.DBModelAuthorizables;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.file.KeyValue;
import org.apache.sentry.provider.file.PolicyFileConstants;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;

import java.security.CodeSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class SentryConfigTool {
  private String sentrySiteFile = null;
  private String policyFile = null;
  private String query = null;
  private String jdbcURL = null;
  private String user = null;
  private String passWord = null;
  private boolean listPrivs = false;
  private boolean validate = false;
  private boolean importPolicy = false;
  private HiveConf hiveConf = null;
  private HiveAuthzConf authzConf = null;
  private AuthorizationProvider sentryProvider = null;

  public SentryConfigTool() {

  }

  public AuthorizationProvider getSentryProvider() {
    return sentryProvider;
  }

  public void setSentryProvider(AuthorizationProvider sentryProvider) {
    this.sentryProvider = sentryProvider;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public void setHiveConf(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public HiveAuthzConf getAuthzConf() {
    return authzConf;
  }

  public void setAuthzConf(HiveAuthzConf authzConf) {
    this.authzConf = authzConf;
  }

  public boolean isValidate() {
    return validate;
  }

  public void setValidate(boolean validate) {
    this.validate = validate;
  }

  public boolean isImportPolicy() {
    return importPolicy;
  }

  public void setImportPolicy(boolean importPolicy) {
    this.importPolicy = importPolicy;
  }

  public String getSentrySiteFile() {
    return sentrySiteFile;
  }

  public void setSentrySiteFile(String sentrySiteFile) {
    this.sentrySiteFile = sentrySiteFile;
  }

  public String getPolicyFile() {
    return policyFile;
  }

  public void setPolicyFile(String policyFile) {
    this.policyFile = policyFile;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getJdbcURL() {
    return jdbcURL;
  }

  public void setJdbcURL(String jdbcURL) {
    this.jdbcURL = jdbcURL;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassWord() {
    return passWord;
  }

  public void setPassWord(String passWord) {
    this.passWord = passWord;
  }

  public boolean isListPrivs() {
    return listPrivs;
  }

  public void setListPrivs(boolean listPrivs) {
    this.listPrivs = listPrivs;
  }

  /**
   * set the required system property to be read by HiveConf and AuthzConf
   * @throws Exception
   */
  public void setupConfig() throws Exception {
    System.out.println("Configuration: ");
    CodeSource src = SentryConfigTool.class.getProtectionDomain()
        .getCodeSource();
    if (src != null) {
      System.out.println("Sentry package jar: " + src.getLocation());
    }

    if (getPolicyFile() != null) {
      System.setProperty(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
          getPolicyFile());
    }
    System.setProperty(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");
    setHiveConf(new HiveConf(SessionState.class));
    getHiveConf().setVar(ConfVars.SEMANTIC_ANALYZER_HOOK,
        HiveAuthzBindingHook.class.getName());
    try {
      System.out.println("Hive config: " + getHiveConf().getHiveSiteLocation());
    } catch (NullPointerException e) {
      // Hack, hiveConf doesn't provide a reliable way check if it found a valid
      // hive-site
      throw new SentryConfigurationException("Didn't find a hive-site.xml");

    }

    if (getSentrySiteFile() != null) {
      getHiveConf()
          .set(HiveAuthzConf.HIVE_SENTRY_CONF_URL, getSentrySiteFile());
    }

    setAuthzConf(HiveAuthzConf.getAuthzConf(getHiveConf()));
    System.out.println("Sentry config: "
        + getAuthzConf().getHiveAuthzSiteFile());
    System.out.println("Sentry Policy: "
        + getAuthzConf().get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar()));
    System.out.println("Sentry server: "
        + getAuthzConf().get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));

    setSentryProvider(getAuthorizationProvider());
  }

  // load auth provider
  private AuthorizationProvider getAuthorizationProvider()
      throws IllegalStateException, SentryConfigurationException {
    String serverName = new Server(getAuthzConf().get(
        AuthzConfVars.AUTHZ_SERVER_NAME.getVar())).getName();
    // get the configured sentry provider
    AuthorizationProvider sentryProvider = null;
    try {
      sentryProvider = HiveAuthzBinding.getAuthProvider(getHiveConf(),
          authzConf, serverName);
    } catch (SentryConfigurationException eC) {
      printConfigErrors(eC);
    } catch (Exception e) {
      throw new IllegalStateException("Couldn't load sentry provider ", e);
    }
    return sentryProvider;
  }

  // validate policy files
  public void validatePolicy() throws Exception {
    try {
      getSentryProvider().validateResource(true);
    } catch (SentryConfigurationException e) {
      printConfigErrors(e);
    }
    System.out.println("No errors found in the policy file");
  }

  // import policy files
  public void importPolicy() throws Exception {
    final String requestorUserName = "hive";
    SimpleFileProviderBackend policyFileBackend;
    SentryPolicyServiceClient client;

    policyFileBackend = new SimpleFileProviderBackend(getAuthzConf(),
        getAuthzConf().get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar()));
    ProviderBackendContext context = new ProviderBackendContext();
    context.setAllowPerDatabase(true);
    policyFileBackend.initialize(context);
    client = new SentryPolicyServiceClient(getAuthzConf());
    Set<String> roles = new HashSet<String>();
    for (TSentryRole sentryRole : client.listRoles(requestorUserName)) {
      roles.add(sentryRole.getRoleName());
    }

    Table<String, String, Set<String>> groupRolePrivilegeTable =
        policyFileBackend.getGroupRolePrivilegeTable();
    for(String groupName : groupRolePrivilegeTable.rowKeySet()) {
      for(String roleName : groupRolePrivilegeTable.columnKeySet()) {
        if (!roles.contains(roleName)) {
          client.createRole(requestorUserName, roleName);
          System.out.println(String.format("CREATE ROLE %s;", roleName));
          roles.add(roleName);
        }

        Set<String> privileges = groupRolePrivilegeTable.get(groupName, roleName);
        if (privileges == null) {
          continue;
        }
        client.grantRoleToGroup(requestorUserName, groupName, roleName);
        System.out.println(String.format("GRANT ROLE %s TO GROUP %s;",
            roleName, groupName));

        for (String permission : privileges) {
          String server = null;
          String database = null;
          String table = null;
          String uri = null;
          String action = AccessConstants.ALL;
          for (String authorizable : PolicyFileConstants.AUTHORIZABLE_SPLITTER.
              trimResults().split(permission)) {
            KeyValue kv = new KeyValue(authorizable);
            DBModelAuthorizable a = DBModelAuthorizables.from(kv);
            if (a == null) {
              action = kv.getValue();
              continue;
            }

            switch (a.getAuthzType()) {
              case Server:
                server = a.getName();
                break;
              case Db:
                database = a.getName();
                break;
              case Table:
              case View:
                table = a.getName();
                break;
              case URI:
                uri = a.getName();
                break;
              default:
                break;
            }
          }

          if (uri != null) {
            System.out.println(String.format(
                "# server=%s",
                server));
            System.out.println(String.format(
                "GRANT ALL ON URI %s TO ROLE %s;",
                uri, roleName));

            client.grantURIPrivilege(requestorUserName, roleName, server, uri);
          } else if (table != null && !AccessConstants.ALL.equals(table)) {
            System.out.println(String.format(
                "# server=%s, database=%s",
                server, database));
            System.out.println(String.format(
                "GRANT %s ON TABLE %s TO ROLE %s;",
                "*".equals(action) ? "ALL" : action.toUpperCase(), table,
                roleName));

            client.grantTablePrivilege(requestorUserName, roleName, server,
                database, table, action);
          } else if (database != null && !AccessConstants.ALL.equals(database)) {
            System.out.println(String.format(
                "# server=%s",
                server));
            System.out.println(String.format(
                "GRANT %s ON DATABASE %s TO ROLE %s;",
                "*".equals(action) ? "ALL" : action.toUpperCase(),
                database, roleName));

            client.grantDatabasePrivilege(requestorUserName, roleName, server,
                database, action);
          } else if (server != null) {
            System.out.println(String.format("GRANT ALL ON SERVER %s TO ROLE %s;",
                server, roleName));

            client.grantServerPrivilege(requestorUserName, roleName, server);
          } else {
            System.out.println(String.format("No grant for permission %s",
                permission));
          }
        }
      }
    }
  }

  // list permissions for given user
  public void listPrivs() throws Exception {
    getSentryProvider().validateResource(true);
    System.out.println("Available privileges for user " + getUser() + ":");
    Set<String> permList = getSentryProvider().listPrivilegesForSubject(
        new Subject(getUser()));
    for (String perms : permList) {
      System.out.println("\t" + perms);
    }
    if (permList.isEmpty()) {
      System.out.println("\t*** No permissions available ***");
    }
  }

  // Verify the given query
  public void verifyLocalQuery(String queryStr) throws Exception {
    // setup Hive driver
    SessionState session = new SessionState(getHiveConf());
    SessionState.start(session);
    Driver driver = new Driver(session.getConf(), getUser(), null);

    // compile the query
    CommandProcessorResponse compilerStatus = driver
        .compileAndRespond(queryStr);
    if (compilerStatus.getResponseCode() != 0) {
      String errMsg = compilerStatus.getErrorMessage();
      if (errMsg.contains(HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE)) {
        printMissingPerms(getHiveConf().get(
            HiveAuthzConf.HIVE_SENTRY_AUTH_ERRORS));
      }
      throw new SemanticException("Compilation error: "
          + compilerStatus.getErrorMessage());
    }
    driver.close();
    System.out
        .println("User " + getUser() + " has privileges to run the query");
  }

  // connect to remote HS2 and run mock query
  public void verifyRemoteQuery(String queryStr) throws Exception {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    Connection conn = DriverManager.getConnection(getJdbcURL(), getUser(),
        getPassWord());
    Statement stmt = conn.createStatement();
    if (!isSentryEnabledOnHiveServer(stmt)) {
      throw new IllegalStateException("Sentry is not enabled on HiveServer2");
    }
    stmt.execute("set " + HiveAuthzConf.HIVE_SENTRY_MOCK_COMPILATION + "=true");
    try {
      stmt.execute(queryStr);
    } catch (SQLException e) {
      String errMsg = e.getMessage();
      if (errMsg.contains(HiveAuthzConf.HIVE_SENTRY_MOCK_ERROR)) {
        System.out.println("User "
            + readConfig(stmt, HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME)
            + " has privileges to run the query");
        return;
      } else if (errMsg
          .contains(HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE)) {
        printMissingPerms(readConfig(stmt,
            HiveAuthzConf.HIVE_SENTRY_AUTH_ERRORS));
        throw e;
      } else {
        throw e;
      }
    } finally {
      if (!stmt.isClosed()) {
        stmt.close();
      }
      conn.close();
    }

  }

  // verify senty session hook is set
  private boolean isSentryEnabledOnHiveServer(Statement stmt)
      throws SQLException {
    return HiveAuthzBindingSessionHook.class.getName().equalsIgnoreCase(
        readConfig(stmt, HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK.varname));
  }

  // read a config value using 'set' statement
  private String readConfig(Statement stmt, String configKey)
      throws SQLException {
    ResultSet res = stmt.executeQuery("set " + configKey);
    if (!res.next()) {
      return null;
    }
    // parse key=value result format
    String result = res.getString(1);
    res.close();
    return result.substring(result.indexOf("=") + 1);
  }

  // print configuration/policy file errors and warnings
  private void printConfigErrors(SentryConfigurationException configException)
      throws SentryConfigurationException {
    System.out.println(" *** Found configuration problems *** ");
    for (String errMsg : configException.getConfigErrors()) {
      System.out.println("ERROR: " + errMsg);
    }
    for (String warnMsg : configException.getConfigWarnings()) {
      System.out.println("Warning: " + warnMsg);
    }
    throw configException;
  }

  // extract the authorization errors from config property and print
  private void printMissingPerms(String errMsg) {
    if (errMsg == null || errMsg.isEmpty()) {
      return;
    }
    System.out.println("*** Query compilation failed ***");
    String perms[] = errMsg.replaceFirst(
        ".*" + HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE, "")
        .split(";");
    System.out.println("Required privileges for given query:");
    for (int count = 0; count < perms.length; count++) {
      System.out.println(" \t " + perms[count]);
    }
  }

  // print usage
  private void usage(Options sentryOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("sentry --command config-tool", sentryOptions);
    System.exit(-1);
  }

  /**
   *  parse arguments
   * <pre>
   *   -d,--debug                  Enable debug output
   *   -e,--query <arg>            Query privilege verification, requires -u
   *   -h,--help                   Print usage
   *   -i,--policyIni <arg>        Policy file path
   *   -j,--jdbcURL <arg>          JDBC URL
   *   -l,--listPrivs,--listPerms  List privilges for given user, requires -u
   *   -p,--password <arg>         Password
   *   -s,--sentry-site <arg>      sentry-site file path
   *   -u,--user <arg>             user name
   *   -v,--validate               Validate policy file
   *   -I,--import                 Import policy file
   * </pre>
   * @param args
   */
  private void parseArgs(String[] args) {
    boolean enableDebug = false;

    Options sentryOptions = new Options();

    Option helpOpt = new Option("h", "help", false, "Print usage");
    helpOpt.setRequired(false);

    Option validateOpt = new Option("v", "validate", false,
        "Validate policy file");
    validateOpt.setRequired(false);

    Option queryOpt = new Option("e", "query", true,
        "Query privilege verification, requires -u");
    queryOpt.setRequired(false);

    Option listPermsOpt = new Option("l", "listPerms", false,
        "list permissions for given user, requires -u");
    listPermsOpt.setRequired(false);
    Option listPrivsOpt = new Option("listPrivs", false,
        "list privileges for given user, requires -u");
    listPrivsOpt.setRequired(false);

    Option importOpt = new Option("I", "import", false,
        "Import policy file");

    // required args
    OptionGroup sentryOptGroup = new OptionGroup();
    sentryOptGroup.addOption(helpOpt);
    sentryOptGroup.addOption(validateOpt);
    sentryOptGroup.addOption(queryOpt);
    sentryOptGroup.addOption(listPermsOpt);
    sentryOptGroup.addOption(listPrivsOpt);
    sentryOptGroup.addOption(importOpt);
    sentryOptGroup.setRequired(true);
    sentryOptions.addOptionGroup(sentryOptGroup);

    // optional args
    Option jdbcArg = new Option("j", "jdbcURL", true, "JDBC URL");
    jdbcArg.setRequired(false);
    sentryOptions.addOption(jdbcArg);

    Option sentrySitePath = new Option("s", "sentry-site", true,
        "sentry-site file path");
    sentrySitePath.setRequired(false);
    sentryOptions.addOption(sentrySitePath);

    Option globalPolicyPath = new Option("i", "policyIni", true,
        "Policy file path");
    globalPolicyPath.setRequired(false);
    sentryOptions.addOption(globalPolicyPath);

    Option userOpt = new Option("u", "user", true, "user name");
    userOpt.setRequired(false);
    sentryOptions.addOption(userOpt);

    Option passWordOpt = new Option("p", "password", true, "Password");
    userOpt.setRequired(false);
    sentryOptions.addOption(passWordOpt);

    Option debugOpt = new Option("d", "debug", false, "enable debug output");
    debugOpt.setRequired(false);
    sentryOptions.addOption(debugOpt);

    try {
      Parser parser = new GnuParser();
      CommandLine cmd = parser.parse(sentryOptions, args);

      for (Option opt : cmd.getOptions()) {
        if (opt.getOpt().equals("s")) {
          setSentrySiteFile(opt.getValue());
        } else if (opt.getOpt().equals("i")) {
          setPolicyFile(opt.getValue());
        } else if (opt.getOpt().equals("e")) {
          setQuery(opt.getValue());
        } else if (opt.getOpt().equals("j")) {
          setJdbcURL(opt.getValue());
        } else if (opt.getOpt().equals("u")) {
          setUser(opt.getValue());
        } else if (opt.getOpt().equals("p")) {
          setPassWord(opt.getValue());
        } else if (opt.getOpt().equals("l") || opt.getOpt().equals("listPrivs")) {
          setListPrivs(true);
        } else if (opt.getOpt().equals("v")) {
          setValidate(true);
        } else if (opt.getOpt().equals("I")) {
          setImportPolicy(true);
        } else if (opt.getOpt().equals("h")) {
          usage(sentryOptions);
        } else if (opt.getOpt().equals("d")) {
          enableDebug = true;
        }
      }

      if (isListPrivs() && (getUser() == null)) {
        throw new ParseException("Can't use -l without -u ");
      }
      if ((getQuery() != null) && (getUser() == null)) {
        throw new ParseException("Must use -u with -e ");
      }
    } catch (ParseException e1) {
      usage(sentryOptions);
    }

    if (!enableDebug) {
      // turn off log
      LogManager.getRootLogger().setLevel(Level.OFF);
    }
  }

  public static class CommandImpl implements Command {
    @Override
    public void run(String[] args) throws Exception {
      SentryConfigTool sentryTool = new SentryConfigTool();

      try {
        // parse arguments
        sentryTool.parseArgs(args);

        // load configuration
        sentryTool.setupConfig();

        // validate configuration
        if (sentryTool.isValidate()) {
          sentryTool.validatePolicy();
        }

        if (sentryTool.isImportPolicy()) {
          sentryTool.importPolicy();
        }

        // list permissions for give user
        if (sentryTool.isListPrivs()) {
          sentryTool.listPrivs();
        }

        // verify given query
        if (sentryTool.getQuery() != null) {
          if (sentryTool.getJdbcURL() != null) {
            sentryTool.verifyRemoteQuery(sentryTool.getQuery());
          } else {
            sentryTool.verifyLocalQuery(sentryTool.getQuery());
          }
        }
      } catch (Exception e) {
        System.out.println("Sentry tool reported Errors: " + e.getMessage());
        e.printStackTrace(System.out);
        System.exit(1);
      }
    }
  }
}
