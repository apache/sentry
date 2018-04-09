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

package org.apache.sentry.provider.db.generic.tools;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.exception.SentryConfigurationException;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.model.indexer.IndexerPrivilegeModel;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.provider.common.AuthorizationComponent.HBASE_INDEXER;
import static org.apache.sentry.service.thrift.ServiceConstants.ClientConfig.SERVICE_NAME;

/**
 * SentryConfigToolIndexer is an administrative tool used to parse a HBase Indexer policy file
 * and add the role, group mappings, and privileges therein to the Sentry service.
 */
public class SentryConfigToolIndexer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryConfigToolIndexer.class);

  private String policyFile;
  private boolean validate;
  private boolean importPolicy;
  private boolean checkCompat;
  private String confPath;

  private String serviceName;



  public String getPolicyFile() { return policyFile; }

  public boolean getValidate() { return validate; }
  public boolean getImportPolicy() { return importPolicy; }
  public boolean getCheckCompat() { return checkCompat; }
  public String getConfPath() { return confPath; }
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Adds command line options for the tool to the passed Options object. Used to extend existing options.
   * @param options
   */
  public void setupOptions(Options options) {
    Option globalPolicyPath = new Option("f", "policy_ini", true,
            "Policy file path");
    globalPolicyPath.setRequired(false);
    options.addOption(globalPolicyPath);

    Option validateOpt = new Option("v", "validate", false,
            "Validate policy file");
    validateOpt.setRequired(false);
    options.addOption(validateOpt);

    Option checkCompatOpt = new Option("c","checkcompat",false,
            "Check compatibility with Sentry Service");
    checkCompatOpt.setRequired(false);
    options.addOption(checkCompatOpt);

    Option importOpt = new Option("i", "import", false,
            "Import policy file");
    importOpt.setRequired(false);
    options.addOption(importOpt);

  }

  /**
   * Parses and processes the arguments from the given command line object.
   * @param cmd
   */
  public void parseOptions(CommandLine cmd) {
    boolean isToolActive = false;
    for (Option opt : cmd.getOptions()) {
      if (opt.getOpt().equals("mgr")) {
        isToolActive = true;
      }
    }
    if (!isToolActive) {
      return;
    }
    for (Option opt : cmd.getOptions()) {
      if (opt.getOpt().equals("f")) {
        policyFile = opt.getValue();
      } else if (opt.getOpt().equals("v")) {
        validate = true;
      } else if (opt.getOpt().equals("i")) {
        importPolicy = true;
      } else if (opt.getOpt().equals("c")) {
        checkCompat = true;
      } else if (opt.getOpt().equals("conf")) {
        confPath = opt.getValue();
      } else if (opt.getOpt().equals("s")) {
        serviceName = opt.getValue();
      }
    }
    if (policyFile == null) {
      throw new IllegalArgumentException("Missing required option: f");
    }
    if (!validate && !importPolicy) {
      throw new IllegalArgumentException("No action specified; at least one of action or import must be specified");
    }
  }


  /**
   * Processes the necessary command based on the arguments parsed earlier.
   * @throws Exception
   */
  public void run() throws Exception {
    String component = HBASE_INDEXER;
    Configuration conf = getSentryConf();

    String service = conf.get(SERVICE_NAME, getServiceName());

    if (service == null) {
      throw new IllegalArgumentException("Service was not defined. Please, use -s command option, or sentry.provider.backend.generic.service-name configuration entry.");
    }

    LOGGER.info(String.format("Context: component=%s, service=%s", component, service));
    // instantiate a solr client for sentry service.  This sets the ugi, so must
    // be done before getting the ugi below.
    try(SentryGenericServiceClient client =
                SentryGenericServiceClientFactory.create(conf)) {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      String requestorName = ugi.getShortUserName();

      convertINIToSentryServiceCmds(component, service, requestorName, conf, client,
              getPolicyFile(), getValidate(), getImportPolicy(), getCheckCompat());
    }
  }

  private Configuration getSentryConf() {
    Configuration conf = new Configuration();
    conf.addResource(new Path(getConfPath()), true);
    return conf;
  }

  private void convertINIToSentryServiceCmds(String component,
      String service, String requestorName,
      Configuration conf, SentryGenericServiceClient client,
      String policyFile, boolean validate, boolean importPolicy,
      boolean checkCompat) throws Exception {

    //instantiate a file providerBackend for parsing
    LOGGER.info("Reading policy file at: " + policyFile);
    SimpleFileProviderBackend policyFileBackend =
        new SimpleFileProviderBackend(conf, policyFile);
    ProviderBackendContext context = new ProviderBackendContext();
    context.setValidators(IndexerPrivilegeModel.getInstance().getPrivilegeValidators());
    policyFileBackend.initialize(context);
    if (validate) {
      validatePolicy(policyFileBackend);
    }

    if (checkCompat) {
      checkCompat(policyFileBackend);
    }

    //import the relations about group,role and privilege into the DB store
    Set<String> roles = Sets.newHashSet();
    Table<String, String, Set<String>> groupRolePrivilegeTable =
        policyFileBackend.getGroupRolePrivilegeTable();
    GenericPrivilegeConverter converter = new GenericPrivilegeConverter(component, service, false);

    for (String groupName : groupRolePrivilegeTable.rowKeySet()) {
      for (String roleName : groupRolePrivilegeTable.columnKeySet()) {
        if (!roles.contains(roleName)) {
          LOGGER.info(dryRunMessage(importPolicy) + "Creating role: " + roleName.toLowerCase(Locale.US));
          if (importPolicy) {
            client.createRoleIfNotExist(requestorName, roleName, component);
          }
          roles.add(roleName);
        }

        Set<String> privileges = groupRolePrivilegeTable.get(groupName, roleName);
        if (privileges == null) {
          continue;
        }
        LOGGER.info(dryRunMessage(importPolicy) + "Adding role: " + roleName.toLowerCase(Locale.US) + " to group: " + groupName);
        if (importPolicy) {
          client.grantRoleToGroups(requestorName, roleName, component, Sets.newHashSet(groupName));
        }

        for (String permission : privileges) {
          String action = null;

          for (String authorizable : AUTHORIZABLE_SPLITTER.
              trimResults().split(permission)) {
            KeyValue kv = new KeyValue(authorizable);
            String key = kv.getKey();
            String value = kv.getValue();
            if ("action".equalsIgnoreCase(key)) {
              action = value;
            }
          }

          // Service doesn't support not specifying action
          if (action == null) {
            permission += "->action=" + Action.ALL;
          }
          LOGGER.info(dryRunMessage(importPolicy) + "Adding permission: " + permission + " to role: " + roleName.toLowerCase(Locale.US));
          if (importPolicy) {
            client.grantPrivilege(requestorName, roleName, component, converter.fromString(permission));
          }
        }
      }
    }
  }

  private void validatePolicy(ProviderBackend backend) throws Exception {
    try {
      backend.validatePolicy(true);
    } catch (SentryConfigurationException e) {
      printConfigErrorsWarnings(e);
      throw e;
    }
  }

  private void printConfigErrorsWarnings(SentryConfigurationException configException) {
    System.out.println(" *** Found configuration problems *** ");
    for (String errMsg : configException.getConfigErrors()) {
      System.out.println("ERROR: " + errMsg);
    }
    for (String warnMsg : configException.getConfigWarnings()) {
      System.out.println("Warning: " + warnMsg);
    }
  }

  private void checkCompat(SimpleFileProviderBackend backend) throws Exception {
    Map<String, Set<String>> rolesCaseMapping = new HashMap<String, Set<String>>();
    Table<String, String, Set<String>> groupRolePrivilegeTable =
      backend.getGroupRolePrivilegeTable();

    for (String roleName : groupRolePrivilegeTable.columnKeySet()) {
      String roleNameLower = roleName.toLowerCase(Locale.US);
      if (!roleName.equals(roleNameLower)) {
        if (!rolesCaseMapping.containsKey(roleNameLower)) {
          rolesCaseMapping.put(roleNameLower, Sets.newHashSet(roleName));
        } else {
          rolesCaseMapping.get(roleNameLower).add(roleName);
        }
      }
    }

    List<String> errors = new LinkedList<String>();
    StringBuilder warningString = new StringBuilder();
    if (!rolesCaseMapping.isEmpty()) {
      warningString.append("The following roles names will be lower cased when added to the Sentry Service.\n");
      warningString.append("This will cause document-level security to fail to match the role tokens.\n");
      warningString.append("Role names: ");
    }
    boolean firstWarning = true;

    for (Map.Entry<String, Set<String>> entry : rolesCaseMapping.entrySet()) {
      Set<String> caseMapping = entry.getValue();
      if (caseMapping.size() > 1) {
        StringBuilder errorString = new StringBuilder();
        errorString.append("The following (cased) roles map to the same role in the sentry service: ");
        boolean first = true;
        for (String casedRole : caseMapping) {
          errorString.append(first ? "" : ", ");
          errorString.append(casedRole);
          first = false;
        }
        errorString.append(".  Role in service: ").append(entry.getKey());
        errors.add(errorString.toString());
      }

      for (String casedRole : caseMapping) {
        warningString.append(firstWarning? "" : ", ");
        warningString.append(casedRole);
        firstWarning = false;
      }
    }

    for (String error : errors) {
      System.out.println("ERROR: " + error);
    }
    System.out.println("\n");

    System.out.println("Warning: " + warningString.toString());
    if (errors.size() > 0) {
      SentryConfigurationException ex =
          new SentryConfigurationException("Compatibility check failure");
      ex.setConfigErrors(errors);
      ex.setConfigWarnings(Lists.<String>asList(warningString.toString(), new String[0]));
      throw ex;
    }
  }

  private String dryRunMessage(boolean importPolicy) {
    if (importPolicy) {
      return "";
    } else {
      return "[Dry Run] ";
    }
  }

}
