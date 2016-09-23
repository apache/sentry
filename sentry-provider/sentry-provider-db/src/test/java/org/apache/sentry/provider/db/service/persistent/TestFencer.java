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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

public class TestFencer {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestFencer.class);

  private static class DatabaseContext implements Closeable {
    private final Configuration conf;
    private final File dataDir;

    DatabaseContext() {
      this.conf = new Configuration();
      this.dataDir = new File(Files.createTempDir(), "sentry_policy_db");
      this.conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
          "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
      this.conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    }

    @Override
    public void close() throws IOException {
      FileUtils.deleteQuietly(dataDir);
    }

    public Configuration getConf() {
      return conf;
    }
  }

  @Test(timeout = 60000)
  public void testInvokingFencer() throws Exception {
    DatabaseContext dbCtx = null;
    PersistenceManagerFactory pmf = null;
    try {
      dbCtx = new DatabaseContext();
      Properties prop = SentryStore.getDataNucleusProperties(dbCtx.getConf());
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      Fencer fencer = new Fencer("abc", pmf);
      fencer.fence(pmf);
      fencer.unfence(pmf);
    } finally {
      IOUtils.cleanup(null, dbCtx);
      if (pmf != null) {
        try {
          pmf.close();
        } catch (Exception e) {
          LOGGER.error("error closing pmf" , e);
        }
      }
    }
  }
}
