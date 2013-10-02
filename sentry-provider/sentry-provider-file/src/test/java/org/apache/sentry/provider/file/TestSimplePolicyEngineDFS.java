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
package org.apache.sentry.provider.file;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.Database;
import org.apache.sentry.core.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestSimplePolicyEngineDFS extends AbstractTestSimplePolicyEngine {

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
    PolicyFiles.copyToDir(fileSystem, etc, "test-authz-provider.ini", "test-authz-provider-other-group.ini");
    setPolicy(new SimplePolicyEngine(new Path(etc, "test-authz-provider.ini").toString(), "server1"));
  }
  @Override
  protected void beforeTeardown() throws IOException {
    fileSystem.delete(etc, true);
  }

  @Test
  public void testMultiFSPolicy() throws Exception {
    File globalPolicyFile = new File(Files.createTempDir(), "global-policy.ini");
    File dbPolicyFile = new File(Files.createTempDir(), "db11-policy.ini");

    // Create global policy file
    PolicyFile dbPolicy = new PolicyFile()
      .addPermissionsToRole("db11_role", "server=server1->db=db11")
      .addRolesToGroup("group1", "db11_role");

    dbPolicy.write(dbPolicyFile);
    Path dbPolicyPath = new Path(etc, "db11-policy.ini");

    // create per-db policy file
    PolicyFile globalPolicy = new PolicyFile()
      .addPermissionsToRole("admin_role", "server=server1")
      .addRolesToGroup("admin_group", "admin_role")
      .addGroupsToUser("hive", "admin_group");
    globalPolicy.addDatabase("db11", dbPolicyPath.toUri().toString());
    globalPolicy.write(globalPolicyFile);


    PolicyFiles.copyFilesToDir(fileSystem, etc, globalPolicyFile);
    PolicyFiles.copyFilesToDir(fileSystem, etc, dbPolicyFile);
    SimplePolicyEngine multiFSEngine =
        new SimplePolicyEngine(globalPolicyFile.getPath(), "server1");

    List<Authorizable> dbAuthorizables = Lists.newArrayList();
    dbAuthorizables.add(new Server("server1"));
    dbAuthorizables.add(new Database("db11"));
    List<String> dbGroups = Lists.newArrayList();
    dbGroups.add("group1");
    ImmutableSetMultimap <String, String> dbPerms =
        multiFSEngine.getPermissions(dbAuthorizables, dbGroups);
    Assert.assertEquals("No DB permissions found", 1, dbPerms.size());
  }
}
