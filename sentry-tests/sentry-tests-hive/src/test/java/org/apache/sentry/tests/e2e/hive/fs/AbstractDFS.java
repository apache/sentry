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

import java.io.IOException;

public abstract class AbstractDFS implements DFS{
  protected static FileSystem fileSystem;
  protected static Path dfsBaseDir;

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

  protected void cleanBaseDir() throws Exception {
    Assert.assertTrue(dfsBaseDir.toString(), fileSystem.delete(dfsBaseDir, true));
  }

  protected Path assertCreateDfsDir(Path dir) throws IOException {
    if(!fileSystem.isDirectory(dir)) {
      Assert.assertTrue("Failed creating " + dir, fileSystem.mkdirs(dir));
    }
    return dir;
  }

}