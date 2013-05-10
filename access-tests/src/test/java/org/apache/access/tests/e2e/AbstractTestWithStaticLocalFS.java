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

import org.apache.access.tests.e2e.hiveserver.HiveServerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestWithStaticLocalFS extends AbstractTestWithStaticConfiguration {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AbstractTestWithStaticLocalFS.class);
  @BeforeClass
  public static void setupTestWithStaticHiveServer()
      throws Exception {
    fileSystem = FileSystem.get(new Configuration());
    hiveServer = HiveServerFactory.create(properties, baseDir, confDir, policyFile, fileSystem);
    hiveServer.start();
  }

  @AfterClass
  public static void tearDownTestWithStaticHiveServer() throws Exception {
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
