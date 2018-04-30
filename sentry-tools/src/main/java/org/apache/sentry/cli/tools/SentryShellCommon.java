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

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.lang.StringUtils;

/**
 * SentryShellCommon provides the function for parsing the argument.
 * For hive model and generic model, child class should be implemented as a sentry admin tool.
 */
abstract public class SentryShellCommon {

  public enum TYPE { kafka, hive, solr, sqoop };

  public static final String OPTION_DESC_HELP = "Shell usage";
  public static final String OPTION_DESC_CONF = "sentry-site file path";
  public static final String OPTION_DESC_ROLE_NAME = "Role name";
  public static final String OPTION_DESC_GROUP_NAME = "Group name";
  public static final String OPTION_DESC_PRIVILEGE = "Privilege string";
  public final static String OPTION_DESC_SERVICE = "Name of the service being managed";
  public static final String PREFIX_MESSAGE_MISSING_OPTION = "Missing required option: ";

  public static final String GROUP_SPLIT_CHAR = ",";

  protected String roleName;
  protected String serviceName;
  protected String groupName;
  protected String privilegeStr;
  protected String confPath;
  // flag for the command
  protected boolean isCreateRole;
  protected boolean isDropRole;
  protected boolean isAddRoleGroup;
  protected boolean isDeleteRoleGroup;
  protected boolean isGrantPrivilegeRole;
  protected boolean isRevokePrivilegeRole;
  protected boolean isListRole;
  protected boolean isListPrivilege;
  protected boolean isListGroup;
  protected boolean isPrintHelp;
  // flag for the parameter check
  protected boolean roleNameRequired;
  protected boolean groupNameRequired;
  protected boolean privilegeStrRequired;
  protected TYPE type;

  /**
   * parse arguments
   *
   * <pre>
   *   -conf,--sentry_conf             <filepath>                 sentry config file path
   *   -cr,--create_role            -r <rolename>                 create role
   *   -dr,--drop_role              -r <rolename>                 drop role
   *   -arg,--add_role_group        -r <rolename>  -g <groupname> add role to group
   *   -drg,--delete_role_group     -r <rolename>  -g <groupname> delete role from group
   *   -gpr,--grant_privilege_role  -r <rolename>  -p <privilege> grant privilege to role
   *   -rpr,--revoke_privilege_role -r <rolename>  -p <privilege> revoke privilege from role
   *   -lr,--list_role              -g <groupname>                list roles for group
   *   -lp,--list_privilege         -r <rolename>                 list privilege for role
   *   -lg,--list_group                                           list all groups associated with all roles
   *   -t,--type                    <typename>                    the shell for hive model or generic model
   * </pre>
   *
   * @param args
   */
  protected boolean parseArgs(String[] args) {
    Options simpleShellOptions = new Options();

    setupOptions(simpleShellOptions);



    // help option
    Option helpOpt = new Option("h", "help", false, OPTION_DESC_HELP);
    helpOpt.setRequired(false);
    simpleShellOptions.addOption(helpOpt);

    // this Options is parsed first for help option
    Options helpOptions = new Options();
    helpOptions.addOption(helpOpt);

    try {
      Parser parser = new GnuParser();

      // parse help option first
      CommandLine cmd = parser.parse(helpOptions, args, true);
      for (Option opt : cmd.getOptions()) {
        if (opt.getOpt().equals("h")) {
          // get the help option, print the usage and exit
          usage(simpleShellOptions);
          return false;
        }
      }

      // without help option
      cmd = parser.parse(simpleShellOptions, args);

      parseOptions(cmd);
    } catch (ParseException pe) {
      System.out.println(pe.getMessage());
      usage(simpleShellOptions);
      return false;
    }
    return true;
  }

  protected void setupOptions(Options simpleShellOptions) {
    OptionGroup simpleShellOptGroup = getMainOptions();
    simpleShellOptions.addOptionGroup(simpleShellOptGroup);

    Option sOpt = new Option("s", "service", true, OPTION_DESC_SERVICE);
    sOpt.setRequired(false);
    simpleShellOptions.addOption(sOpt);

    // optional args
    Option pOpt = new Option("p", "privilege", true, OPTION_DESC_PRIVILEGE);
    pOpt.setRequired(false);
    simpleShellOptions.addOption(pOpt);

    Option gOpt = new Option("g", "groupname", true, OPTION_DESC_GROUP_NAME);
    gOpt.setRequired(false);
    simpleShellOptions.addOption(gOpt);

    Option rOpt = new Option("r", "rolename", true, OPTION_DESC_ROLE_NAME);
    rOpt.setRequired(false);
    simpleShellOptions.addOption(rOpt);

    // this argument should also be parsed in the bin/sentryShell
    Option tOpt = new Option("t", "type", true, "[hive|solr|sqoop|.....]");
    tOpt.setRequired(false);
    simpleShellOptions.addOption(tOpt);

    // file path of sentry-site
    Option sentrySitePathOpt = new Option("conf", "sentry_conf", true, OPTION_DESC_CONF);
    sentrySitePathOpt.setRequired(true);
    simpleShellOptions.addOption(sentrySitePathOpt);
  }

  protected OptionGroup getMainOptions() {
    OptionGroup simpleShellOptGroup = new OptionGroup();
    Option crOpt = new Option("cr", "create_role", false, "Create role");
    crOpt.setRequired(false);

    Option drOpt = new Option("dr", "drop_role", false, "Drop role");
    drOpt.setRequired(false);

    Option argOpt = new Option("arg", "add_role_group", false, "Add role to group");
    argOpt.setRequired(false);

    Option drgOpt = new Option("drg", "delete_role_group", false, "Delete role from group");
    drgOpt.setRequired(false);

    Option gprOpt = new Option("gpr", "grant_privilege_role", false, "Grant privilege to role");
    gprOpt.setRequired(false);

    Option rprOpt = new Option("rpr", "revoke_privilege_role", false, "Revoke privilege from role");
    rprOpt.setRequired(false);

    Option lrOpt = new Option("lr", "list_role", false, "List role");
    lrOpt.setRequired(false);

    Option lpOpt = new Option("lp", "list_privilege", false, "List privilege");
    lpOpt.setRequired(false);

    Option lgOpt = new Option("lg", "list_group", false, "List groups");
    lgOpt.setRequired(false);


    // required args group
    simpleShellOptGroup.addOption(crOpt);
    simpleShellOptGroup.addOption(drOpt);
    simpleShellOptGroup.addOption(argOpt);
    simpleShellOptGroup.addOption(drgOpt);
    simpleShellOptGroup.addOption(gprOpt);
    simpleShellOptGroup.addOption(rprOpt);
    simpleShellOptGroup.addOption(lrOpt);
    simpleShellOptGroup.addOption(lpOpt);
    simpleShellOptGroup.addOption(lgOpt);
    simpleShellOptGroup.setRequired(true);
    return simpleShellOptGroup;
  }

  protected void parseOptions(CommandLine cmd) throws ParseException {
    for (Option opt : cmd.getOptions()) {
      if (opt.getOpt().equals("p")) {
        privilegeStr = opt.getValue();
      } else if (opt.getOpt().equals("g")) {
        groupName = opt.getValue();
      } else if (opt.getOpt().equals("r")) {
        roleName = opt.getValue();
      } else if (opt.getOpt().equals("s")) {
        serviceName = opt.getValue();
      } else if (opt.getOpt().equals("cr")) {
        isCreateRole = true;
        roleNameRequired = true;
      } else if (opt.getOpt().equals("dr")) {
        isDropRole = true;
        roleNameRequired = true;
      } else if (opt.getOpt().equals("arg")) {
        isAddRoleGroup = true;
        roleNameRequired = true;
        groupNameRequired = true;
      } else if (opt.getOpt().equals("drg")) {
        isDeleteRoleGroup = true;
        roleNameRequired = true;
        groupNameRequired = true;
      } else if (opt.getOpt().equals("gpr")) {
        isGrantPrivilegeRole = true;
        roleNameRequired = true;
        privilegeStrRequired = true;
      } else if (opt.getOpt().equals("rpr")) {
        isRevokePrivilegeRole = true;
        roleNameRequired = true;
        privilegeStrRequired = true;
      } else if (opt.getOpt().equals("lr")) {
        isListRole = true;
      } else if (opt.getOpt().equals("lp")) {
        isListPrivilege = true;
        roleNameRequired = true;
      } else if (opt.getOpt().equals("lg")) {
        isListGroup = true;
      } else if (opt.getOpt().equals("conf")) {
        confPath = opt.getValue();
      } else if (opt.getOpt().equals("t")) {
        type = TYPE.valueOf(opt.getValue());
      }
    }
    checkRequiredParameter(roleNameRequired, roleName, OPTION_DESC_ROLE_NAME);
    checkRequiredParameter(groupNameRequired, groupName, OPTION_DESC_GROUP_NAME);
    checkRequiredParameter(privilegeStrRequired, privilegeStr, OPTION_DESC_PRIVILEGE);
  }

  protected void checkRequiredParameter(boolean isRequired, String paramValue, String paramName) throws ParseException {
    if (isRequired && StringUtils.isEmpty(paramValue)) {
      throw new ParseException(PREFIX_MESSAGE_MISSING_OPTION + paramName);
    }
  }

  // print usage
  private void usage(Options sentryOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("sentryShell", sentryOptions);
  }

  // hive model and generic model should implement this method
  public abstract void run() throws Exception;

  @VisibleForTesting
  public boolean executeShell(String[] args) throws Exception {
    boolean result = true;
    if (parseArgs(args)) {
      run();
    } else {
      result = false;
    }
    return result;
  }
}
