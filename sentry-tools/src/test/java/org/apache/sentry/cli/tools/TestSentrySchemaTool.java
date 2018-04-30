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

package org.apache.sentry.cli.tools;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.SentryStoreSchemaInfo;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestSentrySchemaTool {
  private Configuration sentryConf;
  private SentrySchemaTool schemaTool;

  private static final String OLDEST_INIT_VERSION = "1.4.0";
  private static final String sentry_db_resources = findSentryProviderDBDir()+"/sentry-provider/sentry-provider-db/src/main/resources";

  @Before
  public void defaultSetup() throws Exception {
    System.out.println(System.getProperty("user.dir"));
    sentryConf = new Configuration();
    File dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    sentryConf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    sentryConf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    schemaTool = new SentrySchemaTool(sentry_db_resources, sentryConf,
        "derby");
  }

  private void nonDefaultsetup() throws Exception {
    System.out.println(System.getProperty("user.dir"));
    sentryConf = new Configuration();
    File dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    sentryConf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    sentryConf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    schemaTool = new SentrySchemaTool(sentry_db_resources, sentryConf,
        "derby");
  }

  /**This is a pretty ugly hack.  Since the derby files are in sentry-provider-db,
   * we need to go back a few directories to get to it.  Running in Intellij defaults
   * to the root dir of the project, while running the unit tests on the command line
   * defaults to the root/sentry-tools dir.  Using the .class.getResource() defaults to
   * the target/test-classes/blah dir.  So that's not usable.
   *
   * @return
   */
  private static String findSentryProviderDBDir() {

    String pathToSentryProject = System.getProperty("user.dir");
    if(pathToSentryProject.endsWith("/sentry-tools")) {
      return pathToSentryProject.substring(0, pathToSentryProject.length()- "/sentry-tools".length());
    }
    return pathToSentryProject;
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
    schemaTool.doInit(OLDEST_INIT_VERSION);
    schemaTool.doUpgrade();
    schemaTool.verifySchemaVersion();
  }

}
