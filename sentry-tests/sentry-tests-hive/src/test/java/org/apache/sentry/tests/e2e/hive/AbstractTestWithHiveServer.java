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
package org.apache.sentry.tests.e2e.hive;

import java.io.File;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServer;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public abstract class AbstractTestWithHiveServer {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AbstractTestWithHiveServer.class);
  protected File baseDir;
  protected File logDir;
  protected File confDir;
  protected File dataDir;
  protected File policyFile;
  protected HiveServer hiveServer;
  protected FileSystem fileSystem;

  public Context createContext(Map<String, String> properties)
      throws Exception {
    fileSystem = FileSystem.get(new Configuration());
    baseDir = Files.createTempDir();
    LOGGER.info("BaseDir = " + baseDir);
    logDir = assertCreateDir(new File(baseDir, "log"));
    confDir = assertCreateDir(new File(baseDir, "etc"));
    dataDir = assertCreateDir(new File(baseDir, "data"));
    policyFile = new File(confDir, HiveServerFactory.AUTHZ_PROVIDER_FILENAME);
    hiveServer = HiveServerFactory.create(properties, baseDir, confDir, logDir, policyFile, fileSystem);
    hiveServer.start();
    return new Context(hiveServer, getFileSystem(),
        baseDir, confDir, dataDir, policyFile);
  }

  protected static File assertCreateDir(File dir) {
    if(!dir.isDirectory()) {
      Assert.assertTrue("Failed creating " + dir, dir.mkdirs());
    }
    return dir;
  }

  protected FileSystem getFileSystem() {
    return fileSystem;
  }

  @After
  public void tearDownWithHiveServer() throws Exception {
    if(hiveServer != null) {
      hiveServer.shutdown();
      hiveServer = null;
    }
    if(baseDir != null) {
      if(System.getProperty(HiveServerFactory.KEEP_BASEDIR) == null) {
        FileUtils.deleteQuietly(baseDir);
      }
      baseDir = null;
    }
  }
}
