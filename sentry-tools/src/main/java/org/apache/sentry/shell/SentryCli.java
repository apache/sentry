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

package org.apache.sentry.shell;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClient;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.api.common.ApiConstants;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.sentry.service.common.ServiceConstants.ServerConfig.SECURITY_MODE;
import static org.apache.sentry.service.common.ServiceConstants.ServerConfig.SECURITY_MODE_NONE;

/**
 * Sentry interactive tool
 */
public class SentryCli {
  private static final String LOG4J_CONF = "log4jConf";
  private Options options = new Options();
  private CommandLine cmd;

  private static final String localhost = "localhost";
  private static final String defaultPort = "8038";

  private static final String configOpt = "config";
  private static final String userOpt = "user";
  private static final String hostOpt = "host";

  private static final String configEnv = "SENTRY_CONFIG";
  private static final String hostEnv = "SENTRY_HOST";
  private static final String userEnv = "SENTRY_USER";


  private SentryPolicyServiceClient sentryClient;
  private SentryGenericServiceClient sentryGenericClient;

  public SentryPolicyServiceClient getSentryClient() {
    return sentryClient;
  }

  public SentryGenericServiceClient getSentryGenericClient() {
    return sentryGenericClient;
  }

  public String getRequestorName() {
    return requestorName;
  }

  private String requestorName;

  public static void main(String[] args) {
    SentryCli cli = new SentryCli(args);
    // Create interactive shell and run it
    TopLevelShell shell = new TopLevelShell(cli.getSentryClient(),
        cli.getSentryGenericClient(),
        cli.getRequestorName());
    shell.run();
  }

  /**
   * Construct SentryCli from arguments
   * @param args command-line arguments
   */
  public SentryCli(String[] args) {
    options.addOption("h", "help", false, "show help");
    // file path of sentry-site
    options.addOption("U", userOpt, true, "auth user");
    options.addOption("H", hostOpt, true, "host address");
    options.addOption("c", configOpt, true, "sentry configuration");
    options.addOption("L", LOG4J_CONF, true, "Location of log4j properties file");
    CommandLineParser parser = new GnuParser();
    try {
      this.cmd = parser.parse(options, args);
    } catch (ParseException e) {
      help();
    }
    if (cmd.hasOption("h")) {
      help();
    }
    init();
  }

  /**
   * Initialize CLI
   */
  private void init() {
    Map<String, String> env = System.getenv();
    String log4jconf = cmd.getOptionValue(LOG4J_CONF);
    if (log4jconf != null && log4jconf.length() > 0) {
      Properties log4jProperties = new Properties();

      // Firstly load log properties from properties file
      try (FileInputStream istream = new FileInputStream(log4jconf)) {
        log4jProperties.load(istream);
      } catch (IOException e) {
        e.printStackTrace();
      }

      PropertyConfigurator.configure(log4jProperties);
    }

    String host = cmd.getOptionValue(hostOpt);
    if (host == null) {
      host = env.get(hostEnv);
    }

    String pathConf = cmd.getOptionValue(configOpt);
    if (pathConf == null) {
      pathConf = env.get(configEnv);
    }
    if (host == null && pathConf == null) {
      host = localhost + ":" + defaultPort;
    }

    Configuration conf = new Configuration();

    if (pathConf != null) {
      conf.addResource(new Path(pathConf), true);
    } else {
      conf.set(SECURITY_MODE, SECURITY_MODE_NONE);
    }

    if (host != null) {
      conf.set(ApiConstants.ClientConfig.SERVER_RPC_ADDRESS, host);
    }

    requestorName = cmd.getOptionValue(userOpt);
    if (requestorName == null) {
      requestorName = env.get(userEnv);
    }
    if (requestorName == null) {

      UserGroupInformation ugi = null;
      try {
        ugi = UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        e.printStackTrace();
      }
      requestorName = ugi.getShortUserName();
    }

    try {
      sentryClient = SentryServiceClientFactory.create(conf);
      sentryGenericClient = SentryGenericServiceClientFactory.create(conf);
    } catch (Exception e) {
      System.out.println("Failed to connect to Sentry server: " + e.toString());
    }
  }

  private void help() {
    // This prints out some help
    HelpFormatter formater = new HelpFormatter();
    formater.printHelp("sentrycli", options);
    System.exit(0);
  }

}
