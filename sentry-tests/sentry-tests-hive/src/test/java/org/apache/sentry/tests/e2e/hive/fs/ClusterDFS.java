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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.Random;

public class ClusterDFS extends AbstractDFS{
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ClusterDFS.class);
  public static final String TEST_USER = "sentry.e2etest.hive.test.user";
  private static final String testUser = System.getProperty(TEST_USER, "hive");
  private static final String KEYTAB_LOCATION = System.getProperty("sentry.e2e.hive.keytabs.location");
  private Path sentryDir;

  ClusterDFS() throws Exception{
    Configuration conf = new Configuration();
    fileSystem = FileSystem.get(conf);
    LOGGER.info("Creating basedir as user : " + testUser);
    kinit(testUser);
    String policyDir = System.getProperty("sentry.e2etest.hive.policy.location", "/user/hive/sentry");
    sentryDir = assertCreateDir(policyDir);
    dfsBaseDir = assertCreateDir("/tmp/" + (new Random()).nextInt());
  }

  @Override
  public Path assertCreateDir(String path) throws Exception{
    kinit(testUser);
    if(path.startsWith("/")){
      return super.assertCreateDfsDir(new Path(path));
    }else {
      return super.assertCreateDfsDir( new Path(dfsBaseDir + path));
    }
  }

  @Override
  protected void cleanBaseDir() throws Exception {
    kinit(testUser);
    super.cleanBaseDir();
    super.cleanDir(sentryDir);
  }
  @Override
  public void createBaseDir() throws Exception {
    kinit(testUser);
    super.createBaseDir();
  }
  public void kinit(String user) throws Exception{
    UserGroupInformation.loginUserFromKeytab(user, KEYTAB_LOCATION + "/" + user + ".keytab");
  }
}
