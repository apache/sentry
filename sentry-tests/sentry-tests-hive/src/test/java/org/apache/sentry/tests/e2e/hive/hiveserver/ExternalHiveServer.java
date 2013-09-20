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
package org.apache.sentry.tests.e2e.hive.hiveserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;


public class ExternalHiveServer extends AbstractHiveServer {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ExternalHiveServer.class);
  private final File confDir;
  private final File logDir;
  private Process process;

  public ExternalHiveServer(HiveConf hiveConf, File confDir, File logDir) throws Exception {
    super(hiveConf, getHostname(hiveConf), getPort(hiveConf));
    this.confDir = confDir;
    this.logDir = logDir;
  }


  @Override
  public synchronized void start() throws Exception {
    String hiveCommand = System.getProperty("hive.bin.path", "./target/hive/bin/hive");
    String hadoopHome = System.getProperty("hadoop.home", "./target/hadoop");
    String hadoopClasspath = getHadoopClasspath();
    String command = "export ";
    command += String.format("HIVE_CONF_DIR=\"%s\" HADOOP_HOME=\"%s\" ", confDir.getPath(), hadoopHome);
    command += String.format("HADOOP_CLASSPATH=\"%s:%s\" ", confDir.getPath(), hadoopClasspath);
    command += "HADOOP_CLIENT_OPTS=\"-Dhive.log.dir=./target/\"";
    command += "; ";
    command += String.format("%s --service hiveserver2 >%s/hs2.out 2>&1 & echo $! > %s/hs2.pid",
        hiveCommand, logDir.getPath(), logDir.getPath());
    LOGGER.info("Executing " + command);
    process = Runtime.getRuntime().
        exec(new String[]{"/bin/sh", "-c", command});
    waitForStartup(this);
  }

  @Override
  public synchronized void shutdown() throws Exception {
    if(process != null) {
      process.destroy();
      process = null;
      String pid = Strings.nullToEmpty(Files.readFirstLine(new File(logDir, "hs2.pid"), Charsets.UTF_8)).trim();
      if(!pid.isEmpty()) {
        LOGGER.info("Killing " + pid);
        Process killCommand = Runtime.getRuntime().
            exec(new String[]{"/bin/sh", "-c", "kill " + pid});
        // TODO this isn't strictly correct but kill won't output much data
        String error = read(killCommand.getErrorStream());
        String output = read(killCommand.getInputStream());
        LOGGER.info("Kill exit code " + killCommand.waitFor() +
            ", output = '" + output + "', error = '" + error + "'");
      }
    }
  }

  private String read(InputStream is) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    StringBuffer buffer = new StringBuffer();
    try {
      String line;
      while((line = reader.readLine()) != null) {
        buffer.append(line);
      }
      return buffer.toString();
    } finally {
      reader.close();
    }

  }

  private String getHadoopClasspath() {
    List<String> result = Lists.newArrayList();
    String clazzPath = Preconditions.checkNotNull(System.getProperty("java.class.path"), "java.class.path");
    String sep = Preconditions.checkNotNull(System.getProperty("path.separator"), "path.separator");
    for(String item : Splitter.on(sep).omitEmptyStrings().trimResults().split(clazzPath)) {
      if(item.endsWith("/sentry-tests/target/classes") ||
          item.endsWith("/sentry-tests/target/test-classes")) {
        result.add(item);
      } else {
        File clazzPathItem = new File(item);
        String fileName = clazzPathItem.getName();
        if(clazzPathItem.isFile() && fileName.startsWith("sentry-") && fileName.endsWith(".jar")) {
          result.add(item);
        }
      }
    }
    return Joiner.on(sep).join(result);
  }

}
