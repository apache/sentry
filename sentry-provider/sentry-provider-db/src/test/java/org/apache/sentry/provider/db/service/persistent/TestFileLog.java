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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreOp;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestFileLog {

  private String logDir;

  @Before
  public void setup() {
    logDir = Files.createTempDir().getAbsolutePath();
    System.out.println("Creating dir : [" + logDir + "]");
  }

  @After
  public void tearDown() {
    File l = new File(logDir);
    for (File f : l.listFiles()) {
      f.delete();
    }
    l.delete();
  }

  @Test
  public void testReadWriteLog() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir);
    FileLog fileLog = new FileLog(conf);
    fileLog.log(1, new TSentryStoreRecord(TSentryStoreOp.CREATE_ROLE));
    fileLog.log(2, new TSentryStoreRecord(TSentryStoreOp.GRANT_PRIVILEGES));
    fileLog.log(3, new TSentryStoreRecord(TSentryStoreOp.REVOKE_PRVILEGES));
    fileLog.log(4, new TSentryStoreRecord(TSentryStoreOp.ADD_GROUPS));
    fileLog.log(5, new TSentryStoreRecord(TSentryStoreOp.DEL_GROUPS));
    fileLog.close();

    fileLog = new FileLog(conf);
    Assert.assertTrue(fileLog.hasNext());
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, fileLog.next().record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    Assert.assertEquals(TSentryStoreOp.GRANT_PRIVILEGES, fileLog.next().record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    Assert.assertEquals(TSentryStoreOp.REVOKE_PRVILEGES, fileLog.next().record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    Assert.assertEquals(TSentryStoreOp.ADD_GROUPS, fileLog.next().record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    Assert.assertEquals(TSentryStoreOp.DEL_GROUPS, fileLog.next().record.getStoreOp());
    fileLog.close();
  }
}
