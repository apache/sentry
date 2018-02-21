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
package org.apache.sentry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sentry.binding.hive.authz.SentryConfigTool;
import org.apache.sentry.provider.db.tools.SentrySchemaTool;
import org.apache.sentry.service.thrift.SentryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SentryMain {
    private static final String HELP_SHORT = "h";
    private static final String HELP_LONG = "help";
    private static final String VERSION_SHORT = "v";
    private static final String VERSION_LONG = "version";
    private static final String COMMAND = "command";
    private static final String HIVE_CONF = "hiveconf";
    private static final String LOG4J_CONF = "log4jConf";
    private static final String LOG4J_DATANUCLEUS = "log4j.category.DataNucleus.Query";
    private static final List<String> COMMANDS = Arrays.asList("service", "config-tool", "schema-tool");

    private SentryMain() {
        // Make constructor private to avoid instantiation
    }

    public static void main(String[] args)
            throws Exception {
        CommandLineParser parser = new GnuParser();
        Options options = new Options();
        options.addOption(HELP_SHORT, HELP_LONG, false, "Print this help text");
        options.addOption(VERSION_SHORT, VERSION_LONG, false,
                "Print Sentry version");
        options.addOption(HIVE_CONF, true, "Set hive configuration variables");
        options.addOption(null, COMMAND, true, "Command to run. Options: " + COMMANDS);
        options.addOption(null, LOG4J_CONF, true, "Location of log4j properties file");
        //Ignore unrecognized options: service and config-tool options
        CommandLine commandLine = parser.parse(options, args, true);

        String log4jconf = commandLine.getOptionValue(LOG4J_CONF);
        if (log4jconf != null && log4jconf.length() > 0) {
            Properties log4jProperties = new Properties();

            // Firstly load log properties from properties file
            try (InputStream istream = Files.newInputStream(Paths.get(log4jconf))) {
                log4jProperties.load(istream);
            }

            // Set the log level of DataNucleus.Query to INFO only if it is not set in the
            // properties file
            if (!log4jProperties.containsKey(LOG4J_DATANUCLEUS)) {
                log4jProperties.setProperty(LOG4J_DATANUCLEUS, "INFO");

                // Enable debug log for DataNucleus.Query only when log.threshold is TRACE
                String logThreshold = log4jProperties.getProperty("log.threshold");
                if (logThreshold != null && logThreshold.equalsIgnoreCase("TRACE")) {
                    log4jProperties.setProperty(LOG4J_DATANUCLEUS, "DEBUG");
                }
            }

            PropertyConfigurator.configure(log4jProperties);
            Logger sentryLogger = LoggerFactory.getLogger(SentryMain.class);
            sentryLogger.info("Configuring log4j to use [" + log4jconf + "]");
        }


        //Print sentry help only if commandName was not given,
        // otherwise we assume the help is for the sub command
        String commandName = commandLine.getOptionValue(COMMAND);
        if (commandName == null && (commandLine.hasOption(HELP_SHORT) ||
                commandLine.hasOption(HELP_LONG))) {
            printHelp(options, "Command name is missing.");
        } else if (commandLine.hasOption(VERSION_SHORT) ||
                commandLine.hasOption(VERSION_LONG)) {
            printVersion();
        }

        Command command = null;
        switch (commandName){
            case "service":
                command = new SentryService.CommandImpl();
                break;
            case "config-tool":
                command = new SentryConfigTool.CommandImpl();
                break;
            case "schema-tool":
                command = new SentrySchemaTool.CommandImpl();
                break;
            default:
                printHelp(options, "Unknown command " + commandName + "\n");
                break;
        }
        ((Command)command).run(commandLine.getArgs());
    }

    private static void printVersion() {
        System.out.println(SentryVersionInfo.getBuildVersion());
        System.exit(0);
    }

    private static void printHelp(Options options, String msg) {
        String sentry = "sentry";
        if (msg != null) {
            sentry = msg + sentry;
        }
        (new HelpFormatter()).printHelp(sentry, options);
        System.exit(1);
    }
}


