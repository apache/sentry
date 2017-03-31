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
package org.apache.sentry.tests.e2e.tools;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;

/**
 * This class runs all test tools for sentry-hive integration:
 *   e.g. scale test tool, long haul tests, stress tests;
 *   To run it:
 *    hadoop jar test-tools.jar --help
 */
public class TestTools {
  private static ImmutableMap<String, String> COMMANDS = ImmutableMap.of(
      "help", "help",
      "scale", "scale",
      "cleanUpScaleData", "clean-scale"
      );

  private TestTools() {
    // Make constructor private to avoid instantiation
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption(COMMANDS.get("help").substring(0,1), COMMANDS.get("help"),
        false, "Print this help text.");
    options.addOption(COMMANDS.get("scale").substring(0,1), COMMANDS.get("scale"),
        false, "Run scale test tool to create test data.");
    options.addOption(COMMANDS.get("cleanUpScaleData").substring(0,1), COMMANDS.get("cleanUpScaleData"),
        false, "Clean up scale test data.");
    CommandLine commandLine = parser.parse(options, args, true);
    if (commandLine.hasOption(COMMANDS.get("help").substring(0,1))
        || commandLine.hasOption(COMMANDS.get("help"))) {
      printHelp(options, null);
    } else if (commandLine.hasOption(COMMANDS.get("scale").substring(0,1))
        || commandLine.hasOption(COMMANDS.get("scale"))) {
      CreateSentryTestScaleData createSentryTestScaleData = new CreateSentryTestScaleData();
      CreateSentryTestScaleData.TestStatus testStatus = createSentryTestScaleData.create();
      if (testStatus != null && testStatus.testDataStats != null && testStatus.privilegeStatus != null) {
        System.out.println("Test results:");
        System.out.println(testStatus.toString());
      }
    } else if (commandLine.hasOption(COMMANDS.get("cleanUpScaleData").substring(0,1))
        || commandLine.hasOption(COMMANDS.get("cleanUpScaleData"))) {
      CreateSentryTestScaleData createSentryTestScaleData = new CreateSentryTestScaleData();
      createSentryTestScaleData.cleanUpScaleData();
    } else {
      printHelp(options, null);
    }
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
