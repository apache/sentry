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
package org.apache.sentry.policy.db;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestSimpleDBPolicyEngineDFS extends AbstractTestSimplePolicyEngine {

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
    setPolicy(new DBPolicyFileBackend("server1",
        new Path(etc, "test-authz-provider.ini").toString()));
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
      .addGroupsToUser("db", "admin_group");
    globalPolicy.addDatabase("db11", dbPolicyPath.toUri().toString());
    globalPolicy.write(globalPolicyFile);


    PolicyFiles.copyFilesToDir(fileSystem, etc, globalPolicyFile);
    PolicyFiles.copyFilesToDir(fileSystem, etc, dbPolicyFile);
    DBPolicyFileBackend multiFSEngine =
        new DBPolicyFileBackend("server1", globalPolicyFile.getPath());

    Set<String> dbGroups = Sets.newHashSet();
    dbGroups.add("group1");
    ImmutableSet<String> dbPerms =
        multiFSEngine.getAllPrivileges(dbGroups, ActiveRoleSet.ALL);
    Assert.assertEquals("No DB permissions found", 1, dbPerms.size());
  }
}
