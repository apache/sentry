/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.cli.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sentry.provider.common.AuthorizationComponent.HBASE_INDEXER;
import static org.apache.sentry.service.thrift.ServiceConstants.ClientConfig.SERVICE_NAME;

/**
 * SentryShellIndexer is an admin tool, and responsible for the management of repository.
 * The following commands are supported:
 * create role, drop role, add group to role, grant privilege to role,
 * revoke privilege from role, list roles, list privilege for role.
 */
public class SentryShellIndexer extends SentryShellGeneric {

  protected boolean isMigration = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryShellIndexer.class);

  private final SentryConfigToolIndexer configTool = new SentryConfigToolIndexer();

  @Override
  protected void setupOptions(Options simpleShellOptions) {
    super.setupOptions(simpleShellOptions);
    configTool.setupOptions(simpleShellOptions);
  }

  @Override
  protected void parseOptions(CommandLine cmd) throws ParseException {
    super.parseOptions(cmd);
    configTool.parseOptions(cmd);
    for (Option opt : cmd.getOptions()) {
      if (opt.getOpt().equals("mgr")) {
        isMigration = true;
      }
    }
  }

  @Override
  protected OptionGroup getMainOptions() {
    OptionGroup mainOptions = super.getMainOptions();
    Option mgrOpt = new Option("mgr", "migrate", false, "Migrate ini file to Sentry service");
    mgrOpt.setRequired(false);
    mainOptions.addOption(mgrOpt);
    return mainOptions;
  }

  /**
   * Processes the necessary command based on the arguments parsed earlier.
   * @throws Exception
   */
  @Override
  public void run() throws Exception {

    if (isMigration) {
      configTool.run();
      return;
    }

    super.run();
  }

  @Override
  protected String getComponent() throws Exception {
    return HBASE_INDEXER;
  }

  @Override
  protected String getService(Configuration conf) throws Exception {
    String service = conf.get(SERVICE_NAME, serviceName);
    if (service == null) {
      throw new IllegalArgumentException("Service was not defined. Please, use -s command option, or sentry.provider.backend.generic.service-name configuration entry.");
    }
    return service;
  }

  /**
   * Entry-point for Hbase indexer cli tool.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    SentryShellIndexer sentryShell = new SentryShellIndexer();
    try {
      sentryShell.executeShell(args);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      Throwable current = e;
      // find the first printable message;
      while (current != null && current.getMessage() == null) {
        current = current.getCause();
      }
      System.out.println("The operation failed." +
              (current.getMessage() == null ? "" : "  Message: " + current.getMessage()));
      System.exit(1);
    }
  }

}
