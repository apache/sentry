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
package org.apache.sentry.tests.e2e.hive.fs;

import junit.framework.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public abstract class AbstractDFS implements DFS {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AbstractDFS.class);
  protected static final String TEST_USER = System.getProperty("sentry.e2etest.hive.test.user", "hive");
  protected static final String KEYTAB_LOCATION = System.getProperty("sentry.e2e.hive.keytabs.location");
  protected static FileSystem fileSystem;
  protected static Path dfsBaseDir;
  public Path sentryDir;

  @Override
  public FileSystem getFileSystem(){
    return fileSystem;
  }

  @Override
  public void tearDown() throws Exception {
    cleanBaseDir();
  }

  @Override
  public void createBaseDir() throws Exception {
    Assert.assertTrue(dfsBaseDir.toString(), fileSystem.delete(dfsBaseDir, true));
    Assert.assertTrue(dfsBaseDir.toString(), fileSystem.mkdirs(dfsBaseDir));
  }

  @Override
  public Path assertCreateDir(String path) throws Exception{
    return assertCreateDfsDir( new Path(dfsBaseDir + path));
  }

  @Override
  public Path getBaseDir(){
    return dfsBaseDir;
  }

  @Override
  public void writePolicyFile(File srcFile) throws IOException {
    String policyFileName = srcFile.getName();
    Path destPath = new Path(sentryDir, policyFileName);
    fileSystem.copyFromLocalFile(true, true, new Path(srcFile.getAbsolutePath()), destPath);
    LOGGER.info("Copied file to HDFS: " + destPath.toString());
  }

  protected void cleanBaseDir() throws Exception {
   cleanDir(dfsBaseDir);
  }

  protected void cleanDir(Path dir) throws Exception {
    if(dir != null) {
      Assert.assertTrue(dir.toString(), fileSystem.delete(dir, true));
    }
  }

  protected Path assertCreateDfsDir(Path dir) throws IOException {
    if(!fileSystem.isDirectory(dir)) {
      Assert.assertTrue("Failed creating " + dir, fileSystem.mkdirs(dir));
    }
    return dir;
  }

  @Override
  public String getTestUser() {return TEST_USER;}

  @Override
  public String getKeytabLocation() {return KEYTAB_LOCATION;}
}
