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
package org.apache.access.tests.e2e;

import java.io.File;
import java.util.Map;

import junit.framework.Assert;

import org.apache.access.tests.e2e.hiveserver.HiveServer;
import org.apache.access.tests.e2e.hiveserver.HiveServerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

public abstract class AbstractTestWithStaticHiveServer {

  protected static File baseDir;
  protected static File confDir;
  protected static File dataDir;
  protected static File policyFile;
  protected static HiveServer hiveServer;
  protected static FileSystem fileSystem;

  public Context createContext() throws Exception {
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

  @BeforeClass
  public static void setupTestWithHiveServer()
      throws Exception {
    fileSystem = FileSystem.get(new Configuration());
    baseDir = Files.createTempDir();
    confDir = assertCreateDir(new File(baseDir, "etc"));
    dataDir = assertCreateDir(new File(baseDir, "data"));
    policyFile = new File(confDir, HiveServerFactory.AUTHZ_PROVIDER_FILENAME);
    Map<String, String> properties = Maps.newHashMap();
    hiveServer = HiveServerFactory.create(properties, baseDir, confDir, policyFile, fileSystem);
    hiveServer.start();
  }

  @AfterClass
  public static void tearDownTestWithHiveServer() throws Exception {
    if(hiveServer != null) {
      hiveServer.shutdown();
      hiveServer = null;
    }
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
      baseDir = null;
    }
  }
}
