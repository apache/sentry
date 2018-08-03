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

package org.apache.sentry.provider.db.service.persistent;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.api.common.SentryServiceUtil;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestSentryVersion {

  private File dataDir;
  private Configuration conf;

  @Before
  public void setup() throws Exception {
    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf = new Configuration(true);
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL, "jdbc:derby:;databaseName="
        + dataDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
  }

  /**
   * Create the schema using auto creation Create new sentry store without
   * implicit schema creation on the same backend db and make sure it starts
   * 
   * @throws Exception
   */
  @Test
  public void testVerifySentryVersionCheck() throws Exception {
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    boolean hdfsSyncEnabled = SentryServiceUtil.isHDFSSyncEnabled(conf);
    SentryStore sentryStore = new SentryStore(conf);
    sentryStore.setPersistUpdateDeltas(hdfsSyncEnabled);
    sentryStore.stop();
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "true");
    sentryStore = new SentryStore(conf);
    sentryStore.setPersistUpdateDeltas(hdfsSyncEnabled);
  }

  /**
   * Verify that store is not initialized by default without schema pre-created
   *
   * @throws Exception
   */
  @Test(expected = SentryNoSuchObjectException.class)
  public void testNegSentrySchemaDefault() throws Exception {
    new SentryStore(conf);
  }

  /**
   * With schema verification turned off, Sentry Store should autoCreate the
   * schema
   * @throws Exception
   */
  @Test
  public void testSentryImplicitVersion() throws Exception {
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    boolean hdfsSyncEnabled = SentryServiceUtil.isHDFSSyncEnabled(conf);
    SentryStore sentryStore = new SentryStore(conf);
    sentryStore.setPersistUpdateDeltas(hdfsSyncEnabled);
    assertEquals(SentryStoreSchemaInfo.getSentryVersion(),
        sentryStore.getSentryVersion());
  }

}
