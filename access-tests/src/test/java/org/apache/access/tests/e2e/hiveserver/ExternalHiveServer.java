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
package org.apache.access.tests.e2e.hiveserver;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Files;


public class ExternalHiveServer extends AbstractHiveServer {

  private final File confDir;
  private final File logDir;
  private Process process;

  public ExternalHiveServer(HiveConf hiveConf, HiveAuthzConf authzConf, File baseDir) throws Exception {
    super(hiveConf, getHostname(hiveConf), getPort(hiveConf));
    confDir = new File(baseDir, "etc");
    assertTrue("Could not create " + confDir, confDir.isDirectory() || confDir.mkdirs());
    logDir = new File(baseDir, "logs");
    assertTrue("Could not create " + logDir, logDir.isDirectory() || logDir.mkdirs());
    hiveConf.writeXml(new FileOutputStream(new File(confDir, "hive-site.xml")));
    authzConf.writeXml(new FileOutputStream(new File(confDir,HiveAuthzConf.AUTHZ_SITE_FILE)));
  }


  @Override
  public synchronized void start() throws Exception {
    String hiveCommand = System.getProperty("hive.bin.path", "/usr/bin/hive");
    String command = String.format(
        "export HIVE_CONF_DIR=%s; %s --service hiveserver2 > %s>hs2.out 2>&1 & echo $! > %s/hs2.pid",
        confDir.getPath(), hiveCommand, logDir.getPath(), logDir.getPath());
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
        Process killCommand = Runtime.getRuntime().
            exec(new String[]{"/bin/sh", "-c", "kill", pid});
        killCommand.waitFor();
      }
    }
  }
}
