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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.FileLog.Entry;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreOp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.hazelcast.core.HazelcastInstance;

public class TestSentryStoreWithReplicatedLog {

  private String logDir1;
  private String logDir2;
  private String logDir3;
  
  @Before
  public void setup() {
    logDir1 = Files.createTempDir().getAbsolutePath();
    System.out.println("Creating dir1 : [" + logDir1 + "]");
    logDir2 = Files.createTempDir().getAbsolutePath();
    System.out.println("Creating dir2 : [" + logDir2 + "]");
    logDir3 = Files.createTempDir().getAbsolutePath();
    System.out.println("Creating dir3 : [" + logDir2 + "]");
  }

  @After
  public void tearDown() {
    for (String s : new String[]{logDir1, logDir2, logDir3}) {
      File l = new File(s);
      for (File f : l.listFiles()) {
        f.delete();
      }
      l.delete();
    }
  }

  @Test
  public void testSimpleCase() throws Exception {
    Configuration conf1 = new Configuration(false);
    conf1.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir1);
    HazelcastInstance hInst1 = DistributedUtils.getHazelcastInstance(conf1, true);
    SentryStoreWithReplicatedLog store1 = new SentryStoreWithReplicatedLog(new InMemSentryStore(conf1), hInst1);

    Configuration conf2 = new Configuration(false);
    conf2.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir2);
    HazelcastInstance hInst2 = DistributedUtils.getHazelcastInstance(conf2, true);
    SentryStoreWithReplicatedLog store2 = new SentryStoreWithReplicatedLog(new InMemSentryStore(conf2), hInst2);

    store1.createSentryRole("role1");
    store1.alterSentryRoleAddGroups("admin", "role1",
        Sets.newHashSet(
            new TSentryGroup("group1"),
            new TSentryGroup("group2")));
    store1.alterSentryRoleDeleteGroups("role1",
        Sets.newHashSet(new TSentryGroup("group2")));

    store2.waitForReplicattionToComplete(2000);
    FileLog fileLog = new FileLog(conf2);
    Assert.assertTrue(fileLog.hasNext());
    Entry next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.CREATE_ROLE, next.record.getStoreOp());
    Assert.assertEquals("role1", next.record.getRoleName());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.ADD_GROUPS, next.record.getStoreOp());
    Assert.assertEquals("role1", next.record.getRoleName());
    Assert.assertEquals(Sets.newHashSet("group1", "group2"), next.record.getGroups());
    Assert.assertTrue(fileLog.hasNext());
    next = fileLog.next();
    Assert.assertEquals(TSentryStoreOp.DEL_GROUPS, next.record.getStoreOp());
    Assert.assertEquals("role1", next.record.getRoleName());
    Assert.assertEquals(Sets.newHashSet("group2"), next.record.getGroups());
    Assert.assertFalse(fileLog.hasNext());
    fileLog.close();

    Assert.assertEquals(1, store2.getRoleCount());
    Assert.assertEquals(Sets.newHashSet("group1"), store2.getGroupsForRole("role1"));
    hInst1.shutdown();
    hInst2.shutdown();
  }

  @Test
  public void testSecondNodeAfterAWhile() throws Exception {
    Configuration conf1 = new Configuration(false);
    conf1.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir1);
    HazelcastInstance hInst1 = DistributedUtils.getHazelcastInstance(conf1, true);
    SentryStoreWithReplicatedLog store1 = new SentryStoreWithReplicatedLog(new InMemSentryStore(conf1), hInst1);
    store1.createSentryRole("role1");
    store1.createSentryRole("role2");
    store1.createSentryRole("role3");

    FileLog fileLog = new FileLog(conf1);
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

    Configuration conf2 = new Configuration(false);
    conf2.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir2);
    HazelcastInstance hInst2 = DistributedUtils.getHazelcastInstance(conf2, true);
    SentryStoreWithReplicatedLog store2 = new SentryStoreWithReplicatedLog(new InMemSentryStore(conf2), hInst2);
    Assert.assertEquals(3, store2.getRoleCount());
    hInst1.shutdown();
    hInst2.shutdown();
  }

  @Test
  public void testThreeNodes() throws Exception {
    Configuration conf1 = new Configuration(false);
    conf1.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir1);
    HazelcastInstance hInst1 = DistributedUtils.getHazelcastInstance(conf1, true);
    SentryStoreWithReplicatedLog store1 = new SentryStoreWithReplicatedLog(new InMemSentryStore(conf1), hInst1);

    Configuration conf2 = new Configuration(false);
    conf2.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir2);
    HazelcastInstance hInst2 = DistributedUtils.getHazelcastInstance(conf2, true);
    SentryStoreWithReplicatedLog store2 = new SentryStoreWithReplicatedLog(new InMemSentryStore(conf2), hInst2);

    store1.createSentryRole("role1");
    store1.alterSentryRoleAddGroups("admin", "role1",
        Sets.newHashSet(
            new TSentryGroup("group1"),
            new TSentryGroup("group2")));
    store1.alterSentryRoleDeleteGroups("role1",
        Sets.newHashSet(new TSentryGroup("group2")));

    Configuration conf3 = new Configuration(false);
    conf3.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir3);
    HazelcastInstance hInst3 = DistributedUtils.getHazelcastInstance(conf3, true);
    SentryStoreWithReplicatedLog store3 = new SentryStoreWithReplicatedLog(new InMemSentryStore(conf3), hInst3);

    store1.createSentryRole("role2");
    store1.alterSentryRoleAddGroups("admin", "role2",
        Sets.newHashSet(new TSentryGroup("group3")));

    Thread.sleep(2000);
    Assert.assertEquals(2, store1.getRoleCount());
    Assert.assertEquals(Sets.newHashSet("group1"), store1.getGroupsForRole("role1"));
    Assert.assertEquals(Sets.newHashSet("group3"), store1.getGroupsForRole("role2"));
    Assert.assertEquals(2, store2.getRoleCount());
    Assert.assertEquals(Sets.newHashSet("group1"), store2.getGroupsForRole("role1"));
    Assert.assertEquals(Sets.newHashSet("group3"), store2.getGroupsForRole("role2"));
    Assert.assertEquals(2, store3.getRoleCount());
    Assert.assertEquals(Sets.newHashSet("group1"), store3.getGroupsForRole("role1"));
    Assert.assertEquals(Sets.newHashSet("group3"), store3.getGroupsForRole("role2"));
    hInst1.shutdown();
    hInst2.shutdown();
    hInst3.shutdown();
  }
}
