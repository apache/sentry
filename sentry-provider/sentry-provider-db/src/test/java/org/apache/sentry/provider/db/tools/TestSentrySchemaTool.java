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

package org.apache.sentry.provider.db.tools;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.SentryStoreSchemaInfo;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestSentrySchemaTool {
  private Configuration sentryConf;
  private SentrySchemaTool schemaTool;

  @Before
  public void defaultSetup() throws Exception {
    sentryConf = new Configuration();
    File dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    sentryConf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    schemaTool = new SentrySchemaTool("./src/main/resources", sentryConf,
        "derby");
  }

  private void nonDefaultsetup() throws Exception {
    sentryConf = new Configuration();
    File dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    sentryConf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    schemaTool = new SentrySchemaTool("./src/main/resources", sentryConf,
        "derby");
  }

  @Test
  public void testInitNonDefault() throws Exception {
    nonDefaultsetup();
    schemaTool.doInit();
    schemaTool.verifySchemaVersion();
  }

  @Test
  public void testInit() throws Exception {
    schemaTool.doInit();
    schemaTool.verifySchemaVersion();
  }

  @Test
  public void testInitTo() throws Exception {
    schemaTool.doInit(SentryStoreSchemaInfo.getSentryVersion());
    schemaTool.verifySchemaVersion();
  }

  @Test(expected = SentryUserException.class)
  public void testDryRun() throws Exception {
    schemaTool.setDryRun(true);
    schemaTool.doInit();
    schemaTool.setDryRun(false);
    // verification should fail since dryRun didn't create the actual schema
    schemaTool.verifySchemaVersion();
  }

  @Test
  public void testUpgrade() throws Exception {
    schemaTool.doInit(SentryStoreSchemaInfo.getSentryVersion());
    schemaTool.doUpgrade();
    schemaTool.verifySchemaVersion();
  }

}
