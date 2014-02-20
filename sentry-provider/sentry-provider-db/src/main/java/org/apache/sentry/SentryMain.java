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
import org.apache.sentry.service.thrift.SentryService;

import com.google.common.collect.ImmutableMap;

public class SentryMain {
  private static final String HELP_SHORT = "h";
  private static final String HELP_LONG = "help";
  private static final String COMMAND = "command";
  private static final ImmutableMap<String, Command> COMMANDS = ImmutableMap
      .<String, Command>builder()
      .put("service", new SentryService.CommandImpl())
      .build();
  public static void main(String[] args)
      throws Exception {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption(HELP_SHORT, HELP_LONG, false, "Print this help text");
    options.addOption(null, COMMAND, true, "Command to run. Options: " + COMMANDS.keySet());
    CommandLine commandLine = parser.parse(options, args);
    String commandName = commandLine.getOptionValue(COMMAND);
    if (commandName == null || options.hasOption(HELP_SHORT) || options.hasOption(HELP_LONG)) {
      printHelp(options);
    }
    Command command = COMMANDS.get(commandName);
    if (command == null) {
      printHelp(options);
    }
    command.run(commandLine.getArgs());
  }
  private static void printHelp(Options options) {
    (new HelpFormatter()).printHelp("sentry.sh --" + COMMAND + "=" + COMMANDS.keySet(),
        options);
    System.exit(1);
  }
}
