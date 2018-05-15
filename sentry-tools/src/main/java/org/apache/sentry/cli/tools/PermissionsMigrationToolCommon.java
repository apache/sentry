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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.apache.sentry.core.common.utils.Version;
import org.apache.sentry.policy.common.PrivilegeUtils;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClient;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.api.generic.thrift.TSentryPrivilege;
import org.apache.sentry.api.generic.thrift.TSentryRole;
import org.apache.sentry.api.tools.GenericPrivilegeConverter;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.apache.shiro.config.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

/**
 * This class provides basic framework required to migrate permissions between different Sentry
 * versions. Individual components (e.g. SOLR, KAFKA) needs to override the this class
 * to provide component specific migration functionality.
 */
public abstract class PermissionsMigrationToolCommon {
  private static final Logger LOGGER = LoggerFactory.getLogger(PermissionsMigrationToolCommon.class);
  public static final String SOLR_SERVICE_NAME = "sentry.service.client.solr.service.name";

  private Version sourceVersion;
  private Optional<String> confPath = Optional.empty();
  private Optional<String> policyFile = Optional.empty();
  private Optional<String> outputFile = Optional.empty();
  private boolean dryRun = false;

  /**
   * @return version of Sentry for which the privileges need to be migrated.
   */
  public final Version getSourceVersion() {
    return sourceVersion;
  }

  /**
   * This method returns the name of the component for the migration purpose.
   * @param conf The Sentry configuration
   * @return the name of the component
   */
  protected abstract String getComponent(Configuration conf);


  /**
   * This method returns the name of the service name for the migration purpose.
   *
   * @param conf The Sentry configuration
   * @return the name of the service
   */
  protected abstract String getServiceName(Configuration conf);

  /**
   * Migrate the privileges specified via <code>privileges</code>.
   *
   * @param privileges A collection of privileges to be migrated.
   * @return A collection of migrated privileges
   *         An empty collection if migration is not necessary for the specified privileges.
   */
  protected abstract Collection<String> transformPrivileges (Collection<String> privileges);

  /**
   *  parse arguments
   * <pre>
   *   -s,--source                   Sentry source version
   *   -c,--sentry_conf <filepath>   sentry config file path
   *   -p --policy_file <filepath>   sentry (source) policy file path
   *   -o --output      <filepath>   sentry (target) policy file path
   *   -d --dry_run                  provides the output the migration for inspection without
   *                                 making any configuration changes.
   *   -h,--help                     print usage
   * </pre>
   * @param args
   */
  protected boolean parseArgs(String [] args) {
    Options options = new Options();

    Option sourceVersionOpt = new Option("s", "source", true, "Source Sentry version");
    sourceVersionOpt.setRequired(true);
    options.addOption(sourceVersionOpt);

    Option sentryConfPathOpt = new Option("c", "sentry_conf", true,
        "sentry-site.xml file path (only required in case of Sentry service)");
    sentryConfPathOpt.setRequired(false);
    options.addOption(sentryConfPathOpt);

    Option sentryPolicyFileOpt = new Option("p", "policy_file", true,
        "sentry (source) policy file path (only in case of file based Sentry configuration)");
    sentryPolicyFileOpt.setRequired(false);
    options.addOption(sentryPolicyFileOpt);

    Option sentryOutputFileOpt = new Option("o", "output", true,
        "sentry (target) policy file path (only in case of file based Sentry configuration)");
    sentryOutputFileOpt.setRequired(false);
    options.addOption(sentryOutputFileOpt);

    Option dryRunOpt = new Option("d", "dry_run", false,
        "provides the output the migration for inspection without making actual configuration changes");
    dryRunOpt.setRequired(false);
    options.addOption(dryRunOpt);

    // help option
    Option helpOpt = new Option("h", "help", false, "Shell usage");
    helpOpt.setRequired(false);
    options.addOption(helpOpt);

    // this Option is parsed first for help option
    Options helpOptions = new Options();
    helpOptions.addOption(helpOpt);

    try {
      Parser parser = new GnuParser();

      // parse help option first
      CommandLine cmd = parser.parse(helpOptions, args, true);
      for (Option opt : cmd.getOptions()) {
        if (opt.getOpt().equals("h")) {
          // get the help option, print the usage and exit
          usage(options);
          return false;
        }
      }

      // without help option
      cmd = parser.parse(options, args);

      String sourceVersionStr = null;

      for (Option opt : cmd.getOptions()) {
        if (opt.getOpt().equals("s")) {
          sourceVersionStr = opt.getValue();
        } else if (opt.getOpt().equals("c")) {
          confPath = Optional.of(opt.getValue());
        } else if (opt.getOpt().equals("p")) {
          policyFile = Optional.of(opt.getValue());
        }  else if (opt.getOpt().equals("o")) {
          outputFile = Optional.of(opt.getValue());
        }  else if (opt.getOpt().equals("d")) {
          dryRun = true;
        }
      }

      sourceVersion = Version.parse(sourceVersionStr);

      if (!(confPath.isPresent() || policyFile.isPresent())) {
        System.out.println("Please select either file-based Sentry configuration (-p and -o flags)"
            + " or Sentry service (-c flag) for migration.");
        usage(options);
        return false;
      }

      if (confPath.isPresent() && (policyFile.isPresent() || outputFile.isPresent())) {
        System.out.println("In order to migrate service based Sentry configuration,"
            + " do not specify either -p or -o parameters");
        usage(options);
        return false;
      }

      if (!confPath.isPresent() && (policyFile.isPresent() ^ outputFile.isPresent())) {
        System.out.println("In order to migrate file based Sentry configuration,"
            + " please make sure to specify both -p and -o parameters.");
        usage(options);
        return false;
      }

    } catch (ParseException | java.text.ParseException pe) {
      System.out.println(pe.getMessage());
      usage(options);
      return false;
    }
    return true;
  }

  // print usage
  private void usage(Options sentryOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("sentryMigrationTool", sentryOptions);
  }

  public void run() throws Exception {
    if (policyFile.isPresent()) {
      migratePolicyFile();
    } else {
      migrateSentryServiceConfig();
    }
  }

  private void migrateSentryServiceConfig() throws Exception {
    Configuration conf = getSentryConf();
    String component = getComponent(conf);
    String serviceName = getServiceName(conf);
    GenericPrivilegeConverter converter = new GenericPrivilegeConverter(component, serviceName, false);

    // instantiate a client for sentry service.  This sets the ugi, so must
    // be done before getting the ugi below.
    try(SentryGenericServiceClient client =
                SentryGenericServiceClientFactory.create(getSentryConf())) {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      String requestorName = ugi.getShortUserName();

      for (TSentryRole r : client.listAllRoles(requestorName, component)) {
        for (TSentryPrivilege p : client.listAllPrivilegesByRoleName(requestorName,
            r.getRoleName(), component, serviceName)) {

          String privilegeStr = converter.toString(p);
          Collection<String> privileges = Collections.singleton(privilegeStr);
          Collection<String> migrated = transformPrivileges(privileges);
          if (!migrated.isEmpty()) {
            LOGGER.info("{} For role {} migrating privileges from {} to {}", getDryRunMessage(), r.getRoleName(),
                privileges, migrated);

            /*
             * Note that it is not possible to provide transactional (all-or-nothing) behavior for these configuration
             * changes since the Sentry client/server protocol does not support. e.g. under certain failure conditions
             * like crash of Sentry server or network disconnect between client/server, it is possible that the migration
             * can not complete but can also not be rolled back. Hence this migration tool relies on the fact that privilege
             * grant/revoke operations are idempotent and hence re-execution of the migration tool will fix any inconsistency
             * due to such failures.
             **/
            boolean originalPermPresent = false;
            for (String perm : migrated) {
              if (perm.equalsIgnoreCase(privilegeStr)) {
                originalPermPresent = true;
                continue;
              }
              TSentryPrivilege x = converter.fromString(perm);
              LOGGER.info("{} GRANT permission {}", getDryRunMessage(), perm);
              if (!dryRun) {
                client.grantPrivilege(requestorName, r.getRoleName(), component, x);
              }
            }

            // Revoke old permission (only if not part of migrated permissions)
            if (!originalPermPresent) {
              LOGGER.info("{} REVOKE permission {}", getDryRunMessage(), privilegeStr);
              if (!dryRun) {
                client.revokePrivilege(requestorName, r.getRoleName(), component, p);
              }
            }
          }
        }
      }
    }
  }

  private void migratePolicyFile () throws Exception {
    Configuration conf = getSentryConf();
    Path sourceFile = new Path (policyFile.get());
    SimpleFileProviderBackend policyFileBackend = new SimpleFileProviderBackend(conf, sourceFile);
    ProviderBackendContext ctx = new ProviderBackendContext();
    policyFileBackend.initialize(ctx);

    Set<String> roles = Sets.newHashSet();
    Table<String, String, Set<String>> groupRolePrivilegeTable =
        policyFileBackend.getGroupRolePrivilegeTable();

    Ini output = PolicyFiles.loadFromPath(sourceFile.getFileSystem(conf), sourceFile);
    Ini.Section rolesSection = output.get(PolicyFileConstants.ROLES);

    for (String groupName : groupRolePrivilegeTable.rowKeySet()) {
      for (String roleName : policyFileBackend.getRoles(Collections.singleton(groupName), ActiveRoleSet.ALL)) {
        if (!roles.contains(roleName)) {
          // Do the actual migration
          Set<String> privileges = groupRolePrivilegeTable.get(groupName, roleName);
          Collection<String> migrated = transformPrivileges(privileges);

          if (!migrated.isEmpty()) {
            LOGGER.info("{} For role {} migrating privileges from {} to {}", getDryRunMessage(),
                roleName, privileges, migrated);
            if (!dryRun) {
              rolesSection.put(roleName, PrivilegeUtils.fromPrivilegeStrings(migrated));
            }
          }

          roles.add(roleName);
        }
      }
    }

    if (!dryRun) {
      Path targetFile = new Path (outputFile.get());
      PolicyFiles.writeToPath(output, targetFile.getFileSystem(conf), targetFile);
      LOGGER.info("Successfully saved migrated Sentry policy file at {}", outputFile.get());
    }
  }

  private String getDryRunMessage() {
    return dryRun ? "[Dry Run]" : "";
  }

  private Configuration getSentryConf() {
    Configuration conf = new Configuration();
    if (confPath.isPresent()) {
      conf.addResource(new Path(confPath.get()), true);
    }
    return conf;
  }

  @VisibleForTesting
  public boolean executeConfigTool(String [] args) throws Exception {
    boolean result = true;
    if (parseArgs(args)) {
      run();
    } else {
      result = false;
    }
    return result;
  }
}
