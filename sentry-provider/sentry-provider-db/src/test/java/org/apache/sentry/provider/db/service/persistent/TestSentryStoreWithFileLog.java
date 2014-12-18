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
import org.apache.sentry.provider.db.service.persistent.FileLog.Entry;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreOp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestSentryStoreWithFileLog {

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
  public void testBasicStoreOperations() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir);
    SentryStoreWithFileLog store = new SentryStoreWithFileLog(new InMemSentryStore(conf));
    store.createSentryRole("role1");
    store.createSentryRole("role2");
    store.createSentryRole("role3");
    store.stop();

    FileLog fileLog = new FileLog(conf);
    Assert.assertTrue(fileLog.hasNext());
    Entry next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertEquals("role1", next.record.getRoleName());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertEquals("role2", next.record.getRoleName());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertEquals("role3", next.record.getRoleName());
    Assert.assertFalse(fileLog.hasNext());
    fileLog.close();

    store = new SentryStoreWithFileLog(new InMemSentryStore(conf));
    Assert.assertEquals(3, store.getRoleCount());
    store.dropSentryRole("role3");
    store.dropSentryRole("role2");
    store.dropSentryRole("role1");
    store.stop();

    fileLog = new FileLog(conf);
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.DROP_ROLE, next.record.getStoreOp());
    Assert.assertEquals("role3", next.record.getRoleName());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.DROP_ROLE, next.record.getStoreOp());
    Assert.assertEquals("role2", next.record.getRoleName());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.DROP_ROLE, next.record.getStoreOp());
    Assert.assertEquals("role1", next.record.getRoleName());
    Assert.assertFalse(fileLog.hasNext());
    fileLog.close();

    store = new SentryStoreWithFileLog(new InMemSentryStore(conf));
    Assert.assertEquals(0, store.getRoleCount());
    store.stop();
  }

  @Test
  public void testLogSnapshotting() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt(SentryStoreWithFileLog.SENTRY_STORE_FILE_LOG_SNAPSHOT_THRESHOLD, 2);
    conf.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir);
    SentryStoreWithFileLog store = new SentryStoreWithFileLog(new InMemSentryStore(conf));
    store.createSentryRole("role1");
    store.createSentryRole("role2");
    store.createSentryRole("role3");
    store.createSentryRole("role4");
    store.createSentryRole("role5");
    store.stop();

    FileLog fileLog = new FileLog(conf);
    Assert.assertTrue(fileLog.hasNext());
    Entry next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.SNAPSHOT, next.record.getStoreOp());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertFalse(fileLog.hasNext());
    fileLog.close();

    store = new SentryStoreWithFileLog(new InMemSentryStore(conf));
    Assert.assertEquals(5, store.getRoleCount());
    store.dropSentryRole("role3");
    store.dropSentryRole("role2");
    store.dropSentryRole("role1");
    store.stop();

    fileLog = new FileLog(conf);
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.SNAPSHOT, next.record.getStoreOp());
    Assert.assertFalse(fileLog.hasNext());
    fileLog.close();

    store = new SentryStoreWithFileLog(new InMemSentryStore(conf));
    Assert.assertEquals(2, store.getRoleCount());
    store.stop();
  }
}