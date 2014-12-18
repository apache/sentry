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

import static junit.framework.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestFileLoggingSentryStore extends TestInMemSentryStore{
  
  private String logDir;

  @Override
  public void setup() throws Exception {
    super.setup();
    logDir = Files.createTempDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir);
    sentryStore = new SentryStoreWithLocalLock(new SentryStoreWithFileLog(sentryStore));
  }

  @Test
  public void testPersistence() throws Exception {
    String roleName1 = "list-privs-r1", roleName2 = "list-privs-r2";
    String groupName1 = "list-privs-g1", groupName2 = "list-privs-g2";
    String grantor = "g1";
    long seqId = sentryStore.createSentryRole(roleName1).getSequenceId();
    assertEquals(seqId + 1, sentryStore.createSentryRole(roleName2).getSequenceId());
    TSentryPrivilege privilege1 = new TSentryPrivilege();
    privilege1.setPrivilegeScope("TABLE");
    privilege1.setServerName("server1");
    privilege1.setDbName("db1");
    privilege1.setTableName("tbl1");
    privilege1.setAction("SELECT");
    privilege1.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1)
        .getSequenceId());
    assertEquals(seqId + 3, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege1)
        .getSequenceId());
    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("SERVER");
    privilege2.setServerName("server1");
    privilege2.setAction(AccessConstants.ALL);
    privilege2.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 4, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege2)
        .getSequenceId());
    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName(groupName1);
    groups.add(group);
    assertEquals(seqId + 5, sentryStore.alterSentryRoleAddGroups(grantor,
        roleName1, groups).getSequenceId());
    groups.clear();
    group = new TSentryGroup();
    group.setGroupName(groupName2);
    groups.add(group);
    // group 2 has both roles 1 and 2
    assertEquals(seqId + 6, sentryStore.alterSentryRoleAddGroups(grantor,
        roleName1, groups).getSequenceId());
    assertEquals(seqId + 7, sentryStore.alterSentryRoleAddGroups(grantor,
        roleName2, groups).getSequenceId());
    verifyStore(roleName1, roleName2, groupName1, groupName2);

    // KILL The store and restart using same directory..
    Configuration conf = new Configuration(false);
    conf.set(FileLog.SENTRY_FILE_LOG_STORE_LOCATION, logDir);
    sentryStore = new SentryStoreWithLocalLock(new SentryStoreWithFileLog(sentryStore));

    verifyStore(roleName1, roleName2, groupName1, groupName2);
  }

  private void verifyStore(String roleName1, String roleName2,
      String groupName1, String groupName2) throws SentryInvalidInputException {
    // group1 all roles
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(true, new HashSet<String>()))));
    // one active role
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName1)))));
    // unknown active role
    assertEquals(Sets.newHashSet(),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(false, Sets.newHashSet("not a role")))));
    // no active roles
    assertEquals(Sets.newHashSet(),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(false, new HashSet<String>()))));

    // group2 all roles
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(true, new HashSet<String>()))));
    // one active role
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName1)))));
    assertEquals(Sets.newHashSet(
        "server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName2)))));
    // unknown active role
    assertEquals(Sets.newHashSet(),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet("not a role")))));
    // no active roles
    assertEquals(Sets.newHashSet(),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, new HashSet<String>()))));

    // both groups, all active roles
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(true, new HashSet<String>()))));
    // one active role
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName1)))));
    assertEquals(Sets.newHashSet(
        "server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName2)))));
    // unknown active role
    assertEquals(Sets.newHashSet(),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet("not a role")))));
//    // no active roles
    assertEquals(Sets.newHashSet(),
        StoreUtils.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, new HashSet<String>()))));
  }
}
