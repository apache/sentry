/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.generic.service.persistent;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.io.Files;

public abstract class SentryStoreIntegrationBase {
  protected final static String[] adminGroups = { "adminGroup" };
  private static File dataDir;
  private static File policyFilePath;
  protected static Configuration conf;
  protected static DelegateSentryStore sentryStore;
  protected static PolicyFile policyFile;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration(false);
    setup(conf);
    sentryStore = new DelegateSentryStore(conf);
  }

  private static void setup(Configuration conf) throws Exception {
    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.setStrings(ServerConfig.ADMIN_GROUPS, adminGroups);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);

    policyFilePath = new File(Files.createTempDir(), "local_policy_file.ini");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
  }

  @After
  public void clearData() throws Exception{
    sentryStore.clearAllTables();
  }

  @AfterClass
  public static void teardown() {
    if (sentryStore != null) {
      sentryStore.close();
    }
    if (dataDir != null) {
      FileUtils.deleteQuietly(dataDir);
    }
    if (policyFilePath != null) {
      FileUtils.deleteQuietly(policyFilePath);
    }
  }

  public static void addGroupsToUser(String user, String... groupNames) {
    policyFile.addGroupsToUser(user, groupNames);
  }

  public static void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }

  public String[] getAdminGroups() {
    return adminGroups;
  }
}
