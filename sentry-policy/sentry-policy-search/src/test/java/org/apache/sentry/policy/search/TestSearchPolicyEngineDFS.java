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
package org.apache.sentry.policy.search;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestSearchPolicyEngineDFS extends AbstractTestSearchPolicyEngine {

  private static MiniDFSCluster dfsCluster;
  private static FileSystem fileSystem;
  private static Path root;
  private static Path etc;

  @BeforeClass
  public static void setupLocalClazz() throws IOException {
    File baseDir = getBaseDir();
    Assert.assertNotNull(baseDir);
    File dfsDir = new File(baseDir, "dfs");
    Assert.assertTrue(dfsDir.isDirectory() || dfsDir.mkdirs());
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fileSystem = dfsCluster.getFileSystem();
    root = new Path(fileSystem.getUri().toString());
    etc = new Path(root, "/etc");
    fileSystem.mkdirs(etc);
  }

  @AfterClass
  public static void teardownLocalClazz() {
    if(dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Override
  protected void  afterSetup() throws IOException {
    fileSystem.delete(etc, true);
    fileSystem.mkdirs(etc);
    PolicyFiles.copyToDir(fileSystem, etc, "test-authz-provider.ini");
    setPolicy(new SearchPolicyFileBackend(new Path(etc, "test-authz-provider.ini").toString()));
  }

  @Override
  protected void beforeTeardown() throws IOException {
    fileSystem.delete(etc, true);
  }
}
