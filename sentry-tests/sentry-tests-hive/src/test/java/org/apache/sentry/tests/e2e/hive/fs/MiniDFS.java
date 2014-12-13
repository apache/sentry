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

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory.HiveServer2Type;

import com.google.common.collect.Lists;

public class MiniDFS extends AbstractDFS {
  // mock user group mapping that maps user to same group
  public static class PseudoGroupMappingService implements
      GroupMappingServiceProvider {

    @Override
    public List<String> getGroups(String user) {
      return Lists.newArrayList(user, System.getProperty("user.name"));
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      // no-op
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      // no-op
    }
  }

  private static MiniDFSCluster dfsCluster;

  MiniDFS(File baseDir, String serverType) throws Exception {
    Configuration conf = new Configuration();
    if (HiveServer2Type.InternalMetastore.name().equalsIgnoreCase(serverType)) {
      // set the test group mapping that maps user to a group of same name
      conf.set("hadoop.security.group.mapping",
          "org.apache.sentry.tests.e2e.hive.fs.MiniDFS$PseudoGroupMappingService");
      // set umask for metastore test client can create tables in the warehouse dir
      conf.set("fs.permissions.umask-mode", "000");
      Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf);
    }
    File dfsDir = assertCreateDir(new File(baseDir, "dfs"));
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
    conf.set("hadoop.security.group.mapping",
        MiniDFS.PseudoGroupMappingService.class.getName());
    Configuration.addDefaultResource("test.xml");
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fileSystem = dfsCluster.getFileSystem();
    String policyDir = System.getProperty("sentry.e2etest.hive.policy.location", "/user/hive/sentry");
    sentryDir = super.assertCreateDfsDir(new Path(fileSystem.getUri() + policyDir));
    dfsBaseDir = assertCreateDfsDir(new Path(new Path(fileSystem.getUri()), "/base"));
  }

  @Override
  public void tearDown() throws Exception {
    if(dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  //Utilities
  private static File assertCreateDir(File dir) {
    if(!dir.isDirectory()) {
      Assert.assertTrue("Failed creating " + dir, dir.mkdirs());
    }
    return dir;
  }
}
