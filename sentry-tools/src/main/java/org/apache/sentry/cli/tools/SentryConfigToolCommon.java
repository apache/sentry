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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

abstract public class SentryConfigToolCommon {
  private String policyFile;
  private boolean validate;
  private boolean importPolicy;
  private boolean checkCompat;
  private String confPath;

 /**
   *  parse arguments
   * <pre>
   *   -conf,--sentry_conf <filepath>     sentry config file path
   *   -p,--policy_ini     <arg>          policy file path
   *   -v,--validate                      validate policy file
   *   -c,--checkcompat                   check compatibility with service
   *   -i,--import                        import policy file
   *   -h,--help                          print usage
   * </pre>
   * @param args
   */
  protected boolean parseArgs(String [] args) {
    Options options = new Options();

    Option globalPolicyPath = new Option("p", "policy_ini", true,
        "Policy file path");
    globalPolicyPath.setRequired(true);
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

    // file path of sentry-site
    Option sentrySitePathOpt = new Option("conf", "sentry_conf", true, "sentry-site file path");
    sentrySitePathOpt.setRequired(true);
    options.addOption(sentrySitePathOpt);

    // help option
    Option helpOpt = new Option("h", "help", false, "Shell usage");
    helpOpt.setRequired(false);
    options.addOption(helpOpt);

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
          usage(options);
          return false;
        }
      }

      // without help option
      cmd = parser.parse(options, args);

      for (Option opt : cmd.getOptions()) {
        if (opt.getOpt().equals("p")) {
          policyFile = opt.getValue();
        } else if (opt.getOpt().equals("v")) {
          validate = true;
        } else if (opt.getOpt().equals("i")) {
          importPolicy = true;
        } else if (opt.getOpt().equals("c")) {
          checkCompat = true;
        } else if (opt.getOpt().equals("conf")) {
          confPath = opt.getValue();
        }
      }

      if (!validate && !importPolicy) {
        throw new IllegalArgumentException("No action specified; at least one of action or import must be specified");
      }
    } catch (ParseException pe) {
      System.out.println(pe.getMessage());
      usage(options);
      return false;
    }
    return true;
  }

  // print usage
  private void usage(Options sentryOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("sentryConfigTool", sentryOptions);
  }

  public abstract void run() throws Exception;

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

  public String getPolicyFile() { return policyFile; }
  public boolean getValidate() { return validate; }
  public boolean getImportPolicy() { return importPolicy; }
  public boolean getCheckCompat() { return checkCompat; }
  public String getConfPath() { return confPath; }
}
