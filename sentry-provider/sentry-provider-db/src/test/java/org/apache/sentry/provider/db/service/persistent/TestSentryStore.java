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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestSentryStore {

  private File dataDir;
  private SentryStore sentryStore;
  private String[] adminGroups = {"adminGroup1"};
  private PolicyFile policyFile;
  private File policyFilePath;

  @Before
  public void setup() throws Exception {
    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    Configuration conf = new Configuration(false);
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
    conf.setStrings(ServerConfig.ADMIN_GROUPS, adminGroups);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    policyFilePath = new File(dataDir, "local_policy_file.ini");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    policyFile = new PolicyFile();
    sentryStore = new SentryStore(conf);

    String adminUser = "g1";
    addGroupsToUser(adminUser, adminGroups);
    writePolicyFile();
  }

  @After
  public void teardown() {
    if (sentryStore != null) {
      sentryStore.stop();
    }
    if (dataDir != null) {
      FileUtils.deleteQuietly(dataDir);
    }
  }
  @Test
  public void testCaseInsensitiveRole() throws Exception {
    String roleName = "newRole";
    String grantor = "g1";
    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName("test-groups-g1");
    groups.add(group);

    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName("server1");
    privilege.setDbName("default");
    privilege.setTableName("table1");
    privilege.setAction(AccessConstants.ALL);
    privilege.setGrantorPrincipal(grantor);
    privilege.setCreateTime(System.currentTimeMillis());

    long seqId = sentryStore.createSentryRole(roleName, grantor).getSequenceId();
    assertEquals(seqId + 1, sentryStore.alterSentryRoleAddGroups(grantor, roleName, groups).getSequenceId());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleDeleteGroups(roleName, groups).getSequenceId());
    assertEquals(seqId + 3, sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege).getSequenceId());
    assertEquals(seqId + 4, sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege).getSequenceId());
  }
  @Test
  public void testURI() throws Exception {
    String roleName = "test-dup-role";
    String grantor = "g1";
    String uri = "file:///var/folders/dt/9zm44z9s6bjfxbrm4v36lzdc0000gp/T/1401860678102-0/data/kv1.dat";
    sentryStore.createSentryRole(roleName, grantor);
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege("URI", "server1", "ALL");
    tSentryPrivilege.setURI(uri);
    tSentryPrivilege.setGrantorPrincipal(grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, tSentryPrivilege);

    TSentryAuthorizable tSentryAuthorizable = new TSentryAuthorizable();
    tSentryAuthorizable.setUri(uri);
    tSentryAuthorizable.setServer("server1");

    Set<TSentryPrivilege> privileges =
        sentryStore.getTSentryPrivileges(new HashSet<String>(Arrays.asList(roleName)), tSentryAuthorizable);

    assertTrue(privileges.size() == 1);

    Set<TSentryGroup> tSentryGroups = new HashSet<TSentryGroup>();
    tSentryGroups.add(new TSentryGroup("group1"));
    sentryStore.alterSentryRoleAddGroups(grantor, roleName, tSentryGroups);

    TSentryActiveRoleSet thriftRoleSet = new TSentryActiveRoleSet(true, new HashSet<String>(Arrays.asList(roleName)));

    Set<String> privs =
        sentryStore.listSentryPrivilegesForProvider(new HashSet<String>(Arrays.asList("group1")), thriftRoleSet, tSentryAuthorizable);

    assertTrue(privs.size()==1);
    assertTrue(privs.contains("server=server1->uri=" + uri + "->action=all"));
  }

  @Test
  public void testCreateDuplicateRole() throws Exception {
    String roleName = "test-dup-role";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName, grantor);
    try {
      sentryStore.createSentryRole(roleName, grantor);
      fail("Expected SentryAlreadyExistsException");
    } catch(SentryAlreadyExistsException e) {
      // expected
    }
  }
  @Test
  public void testCaseSensitiveScope() throws Exception {
    String roleName = "role1";
    String grantor = "g1";
    long seqId = sentryStore.createSentryRole(roleName, grantor).getSequenceId();
    TSentryPrivilege sentryPrivilege = new TSentryPrivilege("Database", "server1", "all");
    sentryPrivilege.setDbName("db1");
    sentryPrivilege.setGrantorPrincipal(grantor);
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(roleName, sentryPrivilege).getSequenceId());
  }
  @Test
  public void testCreateDropRole() throws Exception {
    String roleName = "test-drop-role";
    String grantor = "g1";
    long seqId = sentryStore.createSentryRole(roleName, grantor).getSequenceId();
    assertEquals(seqId + 1, sentryStore.dropSentryRole(roleName).getSequenceId());
  }

  @Test(expected = SentryNoSuchObjectException.class)
  public void testAddDeleteGroupsNonExistantRole()
      throws Exception {
    String roleName = "non-existant-role";
    String grantor = "g1";
    Set<TSentryGroup> groups = Sets.newHashSet();
    sentryStore.alterSentryRoleAddGroups(grantor, roleName, groups);
  }

  @Test
  public void testAddDeleteGroups() throws Exception {
    String roleName = "test-groups";
    String grantor = "g1";
    long seqId = sentryStore.createSentryRole(roleName, grantor).getSequenceId();
    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName("test-groups-g1");
    groups.add(group);
    group = new TSentryGroup();
    group.setGroupName("test-groups-g2");
    groups.add(group);
    assertEquals(seqId + 1, sentryStore.alterSentryRoleAddGroups(grantor,
        roleName, groups).getSequenceId());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleDeleteGroups(roleName, groups)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    assertEquals(Collections.emptySet(), role.getGroups());
  }

  @Test
  public void testGrantRevokePrivilege() throws Exception {
    String roleName = "test-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    long seqId = sentryStore.createSentryRole(roleName, grantor).getSequenceId();
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setGrantorPrincipal(grantor);
    privilege.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    privilege.setAction(AccessConstants.SELECT);
    assertEquals(seqId + 2, sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege)
        .getSequenceId());
    // after having ALL and revoking SELECT, we should have INSERT
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    MSentryPrivilege mPrivilege = Iterables.get(privileges, 0);
    assertEquals(server, mPrivilege.getServerName());
    assertEquals(db, mPrivilege.getDbName());
    assertEquals(table, mPrivilege.getTableName());
    assertEquals(AccessConstants.INSERT, mPrivilege.getAction());
    assertFalse(mPrivilege.getGrantOption());
  }

  @Test
  public void testGrantRevokePrivilegeWithGrantOption() throws Exception {
    String roleName = "test-grantOption-table";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    TSentryGrantOption grantOption = TSentryGrantOption.TRUE;
    long seqId = sentryStore.createSentryRole(roleName, grantor).getSequenceId();
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setGrantorPrincipal(grantor);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(grantOption);
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    assertEquals(Boolean.valueOf(privilege.getGrantOption().toString()), Iterables.get(privileges, 0).getGrantOption());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege)
        .getSequenceId());
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(0, privileges.size());

    roleName = "test-grantOption-db";
    sentryStore.createSentryRole(roleName, grantor);
    privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("DATABASE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setAction(AccessConstants.ALL);
    privilege.setGrantorPrincipal(grantor);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(grantOption);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege);
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());

    privilege.setAction(AccessConstants.SELECT);
    privilege.setGrantOption(TSentryGrantOption.UNSET);
    sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege);
    // after having ALL and revoking SELECT, we should have INSERT
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    MSentryPrivilege mPrivilege = Iterables.get(privileges, 0);
    assertEquals(server, mPrivilege.getServerName());
    assertEquals(db, mPrivilege.getDbName());
    assertEquals(AccessConstants.INSERT, mPrivilege.getAction());
  }

  @Test
  public void testGrantCheckWithGrantOption() throws Exception {
    // 1. set local group mapping
    // user0->group0->role0
    // user1->group1->role1
    // user2->group2->role2
    // user3->group3->role3
    // user4->group4->role4
    String grantor = "g1";
    String[] users = {"user0","user1","user2","user3","user4"};
    String[] roles = {"role0","role1","role2","role3","role4"};
    String[] groups = {"group0","group1","group2","group3","group4"};
    for (int i = 0; i < users.length; i++) {
      addGroupsToUser(users[i], groups[i]);
      sentryStore.createSentryRole(roles[i], grantor);
      Set<TSentryGroup> tGroups = Sets.newHashSet();
      TSentryGroup tGroup = new TSentryGroup(groups[i]);
      tGroups.add(tGroup);
      sentryStore.alterSentryRoleAddGroups(grantor, roles[i], tGroups);
    }
    writePolicyFile();

    // 2. g1 grant all on database db1 to role0 with grant option
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String roleName = roles[0];
    grantor = "g1";
    TSentryPrivilege privilege1 = new TSentryPrivilege();
    privilege1.setPrivilegeScope("DATABASE");
    privilege1.setServerName(server);
    privilege1.setDbName(db);
    privilege1.setAction(AccessConstants.ALL);
    privilege1.setGrantorPrincipal(grantor);
    privilege1.setCreateTime(System.currentTimeMillis());
    privilege1.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege1);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());

    // 3. user0 grant select on database db1 to role1, with grant option
    roleName = roles[1];
    grantor = users[0];
    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("DATABASE");
    privilege2.setServerName(server);
    privilege2.setDbName(db);
    privilege2.setAction(AccessConstants.SELECT);
    privilege2.setGrantorPrincipal(grantor);
    privilege2.setCreateTime(System.currentTimeMillis());
    privilege2.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege2);

    // 4. user0 grant all on table tb1 to role2, no grant option
    roleName = roles[2];
    grantor = users[0];
    TSentryPrivilege privilege3 = new TSentryPrivilege();
    privilege3.setPrivilegeScope("TABLE");
    privilege3.setServerName(server);
    privilege3.setDbName(db);
    privilege3.setTableName(table);
    privilege3.setAction(AccessConstants.ALL);
    privilege3.setGrantorPrincipal(grantor);
    privilege3.setCreateTime(System.currentTimeMillis());
    privilege3.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege3);

    // 5. user1 has role1, no insert privilege,
    // grant insert to role3, will throw no grant exception
    roleName = roles[3];
    grantor = users[1];
    TSentryPrivilege privilege4 = new TSentryPrivilege();
    privilege4.setPrivilegeScope("DATABASE");
    privilege4.setServerName(server);
    privilege4.setDbName(db);
    privilege4.setAction(AccessConstants.INSERT);
    privilege4.setGrantorPrincipal(grantor);
    privilege4.setCreateTime(System.currentTimeMillis());
    privilege4.setGrantOption(TSentryGrantOption.FALSE);
    boolean isGrantOptionException = false;
    try {
      sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege4);
    } catch (SentryGrantDeniedException e) {
      isGrantOptionException = true;
      System.err.println(e.getMessage());
    }
    assertTrue(isGrantOptionException);

    // 6. user2 has role2, no grant option,
    // grant insert to role4, will throw no grant exception
    roleName = roles[4];
    grantor = users[2];
    TSentryPrivilege privilege5 = new TSentryPrivilege();
    privilege5.setPrivilegeScope("TABLE");
    privilege5.setServerName(server);
    privilege5.setDbName(db);
    privilege5.setTableName(table);
    privilege5.setAction(AccessConstants.INSERT);
    privilege5.setGrantorPrincipal(grantor);
    privilege5.setCreateTime(System.currentTimeMillis());
    privilege5.setGrantOption(TSentryGrantOption.FALSE);
    isGrantOptionException = false;
    try {
      sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege5);
    } catch (SentryGrantDeniedException e) {
      isGrantOptionException = true;
      System.err.println(e.getMessage());
    }
    assertTrue(isGrantOptionException);
  }

  @Test
  public void testRevokeCheckWithGrantOption() throws Exception {
    // 1. set local group mapping
    // user0->group0->role0
    // user1->group1->role1
    // user2->group2->role2
    String grantor = "g1";
    String[] users = {"user0","user1","user2"};
    String[] roles = {"role0","role1","role2"};
    String[] groups = {"group0","group1","group2"};
    for (int i = 0; i < users.length; i++) {
      addGroupsToUser(users[i], groups[i]);
      sentryStore.createSentryRole(roles[i], grantor);
      Set<TSentryGroup> tGroups = Sets.newHashSet();
      TSentryGroup tGroup = new TSentryGroup(groups[i]);
      tGroups.add(tGroup);
      sentryStore.alterSentryRoleAddGroups(grantor, roles[i], tGroups);
    }
    writePolicyFile();

    // 2. g1 grant select on database db1 to role0, with grant option
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String roleName = roles[0];
    grantor = "g1";
    TSentryPrivilege privilege1 = new TSentryPrivilege();
    privilege1.setPrivilegeScope("DATABASE");
    privilege1.setServerName(server);
    privilege1.setDbName(db);
    privilege1.setAction(AccessConstants.SELECT);
    privilege1.setGrantorPrincipal(grantor);
    privilege1.setCreateTime(System.currentTimeMillis());
    privilege1.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege1);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());

    // 3. g1 grant all on table tb1 to role1, no grant option
    roleName = roles[1];
    grantor = "g1";
    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("TABLE");
    privilege2.setServerName(server);
    privilege2.setDbName(db);
    privilege2.setTableName(table);
    privilege2.setAction(AccessConstants.ALL);
    privilege2.setGrantorPrincipal(grantor);
    privilege2.setCreateTime(System.currentTimeMillis());
    privilege2.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege2);

    // 4. g1 grant select on table tb1 to role2, no grant option
    roleName = roles[2];
    grantor = "g1";
    TSentryPrivilege privilege3 = new TSentryPrivilege();
    privilege3.setPrivilegeScope("TABLE");
    privilege3.setServerName(server);
    privilege3.setDbName(db);
    privilege3.setTableName(table);
    privilege3.setAction(AccessConstants.SELECT);
    privilege3.setGrantorPrincipal(grantor);
    privilege3.setCreateTime(System.currentTimeMillis());
    privilege3.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege3);

    // 5. user1 has role1, no grant option,
    // revoke from role2 will throw no grant exception
    roleName = roles[2];
    grantor = users[1];
    privilege3.setGrantorPrincipal(grantor);
    boolean isGrantOptionException = false;
    try {
      sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege3);
    } catch (SentryGrantDeniedException e) {
      isGrantOptionException = true;
      System.err.println(e.getMessage());
    }
    assertTrue(isGrantOptionException);

    // 6. user0 has role0, only have select,
    // revoke all from role1 will throw no grant exception
    roleName = roles[1];
    grantor = users[0];
    privilege2.setGrantorPrincipal(grantor);
    try {
      sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege2);
    } catch (SentryGrantDeniedException e) {
      isGrantOptionException = true;
      System.err.println(e.getMessage());
    }
    assertTrue(isGrantOptionException);

    // 7. user0 has role0, has select and grant option,
    // revoke select from role2
    roleName = roles[2];
    grantor = users[0];
    privilege3.setGrantorPrincipal(grantor);
    sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege3);
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(0, privileges.size());
  }

  @Test
  public void testRevokeAllGrantOption() throws Exception {
    // 1. set local group mapping
    // user0->group0->role0
    String grantor = "g1";
    String[] users = {"user0"};
    String[] roles = {"role0"};
    String[] groups = {"group0"};
    for (int i = 0; i < users.length; i++) {
      addGroupsToUser(users[i], groups[i]);
      sentryStore.createSentryRole(roles[i], grantor);
      Set<TSentryGroup> tGroups = Sets.newHashSet();
      TSentryGroup tGroup = new TSentryGroup(groups[i]);
      tGroups.add(tGroup);
      sentryStore.alterSentryRoleAddGroups(grantor, roles[i], tGroups);
    }
    writePolicyFile();

    // 2. g1 grant select on table tb1 to role0, with grant option
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String roleName = roles[0];
    grantor = "g1";
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.SELECT);
    privilege.setGrantorPrincipal(grantor);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege);

    // 3. g1 grant select on table tb1 to role0, no grant option
    roleName = roles[0];
    grantor = "g1";
    privilege.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege);

    // 4. g1 revoke all privilege from role0
    roleName = roles[0];
    grantor = "g1";
    privilege.setGrantOption(TSentryGrantOption.UNSET);
    sentryStore.alterSentryRoleRevokePrivilege(roleName, privilege);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 0, privileges.size());
  }

  @Test
  public void testListSentryPrivilegesForProvider() throws Exception {
    String roleName1 = "list-privs-r1", roleName2 = "list-privs-r2";
    String groupName1 = "list-privs-g1", groupName2 = "list-privs-g2";
    String grantor = "g1";
    long seqId = sentryStore.createSentryRole(roleName1, grantor).getSequenceId();
    assertEquals(seqId + 1, sentryStore.createSentryRole(roleName2, grantor).getSequenceId());
    TSentryPrivilege privilege1 = new TSentryPrivilege();
    privilege1.setPrivilegeScope("TABLE");
    privilege1.setServerName("server1");
    privilege1.setDbName("db1");
    privilege1.setTableName("tbl1");
    privilege1.setAction("SELECT");
    privilege1.setGrantorPrincipal(grantor);
    privilege1.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleGrantPrivilege(roleName1, privilege1)
        .getSequenceId());
    assertEquals(seqId + 3, sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege1)
        .getSequenceId());
    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("SERVER");
    privilege2.setServerName("server1");
    privilege2.setGrantorPrincipal(grantor);
    privilege2.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 4, sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege2)
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
    // group1 all roles
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(true, new HashSet<String>()))));
    // one active role
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName1)))));
    // unknown active role
    assertEquals(Sets.newHashSet(),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(false, Sets.newHashSet("not a role")))));
    // no active roles
    assertEquals(Sets.newHashSet(),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName1),
            new TSentryActiveRoleSet(false, new HashSet<String>()))));

    // group2 all roles
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(true, new HashSet<String>()))));
    // one active role
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName1)))));
    assertEquals(Sets.newHashSet(
        "server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName2)))));
    // unknown active role
    assertEquals(Sets.newHashSet(),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet("not a role")))));
    // no active roles
    assertEquals(Sets.newHashSet(),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.newHashSet(groupName2),
            new TSentryActiveRoleSet(false, new HashSet<String>()))));

    // both groups, all active roles
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(true, new HashSet<String>()))));
    // one active role
    assertEquals(Sets.newHashSet("server=server1->db=db1->table=tbl1->action=select"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName1)))));
    assertEquals(Sets.newHashSet(
        "server=server1->db=db1->table=tbl1->action=select", "server=server1"),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet(roleName2)))));
    // unknown active role
    assertEquals(Sets.newHashSet(),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, Sets.newHashSet("not a role")))));
    // no active roles
    assertEquals(Sets.newHashSet(),
        SentryStore.toTrimedLower(sentryStore.listAllSentryPrivilegesForProvider(Sets.
            newHashSet(groupName1, groupName2),
            new TSentryActiveRoleSet(false, new HashSet<String>()))));
  }

  @Test
  public void testListRole() throws Exception {
    String roleName1 = "role1", roleName2 = "role2", roleName3 = "role3";
    String group1 = "group1", group2 = "group2";
    String grantor = "g1";

    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.createSentryRole(roleName3, grantor);

    sentryStore.alterSentryRoleAddGroups(grantor, roleName1, Sets.newHashSet(new TSentryGroup(group1)));
    sentryStore.alterSentryRoleAddGroups(grantor, roleName2, Sets.newHashSet(new TSentryGroup(group2)));
    sentryStore.alterSentryRoleAddGroups(grantor, roleName3,
        Sets.newHashSet(new TSentryGroup(group1), new TSentryGroup(group2)));

    assertEquals(2, sentryStore.getTSentryRolesByGroupName(Sets.newHashSet(group1), false).size());
    assertEquals(2, sentryStore.getTSentryRolesByGroupName(Sets.newHashSet(group2), false).size());
    assertEquals(3, sentryStore.getTSentryRolesByGroupName(Sets.newHashSet(group1,group2), false).size());
    assertEquals(0,
        sentryStore.getTSentryRolesByGroupName(Sets.newHashSet("foo"), true)
            .size());
  }

  /**
   * Assign multiple table and SERVER privileges to roles
   * drop privilege for the object verify that it's removed correctl
   * @throws Exception
   */
  @Test
  public void testDropDbObject() throws Exception {
    String roleName1 = "list-privs-r1", roleName2 = "list-privs-r2", roleName3 = "list-privs-r3";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.createSentryRole(roleName3, grantor);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName("tbl1");
    privilege_tbl1.setGrantorPrincipal(grantor);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege1 = new TSentryPrivilege(privilege_tbl1);
    privilege1.setAction("SELECT");

    TSentryPrivilege privilege2_1 = new TSentryPrivilege(privilege_tbl1);
    privilege2_1.setAction("INSERT");
    TSentryPrivilege privilege3_1 = new TSentryPrivilege(privilege_tbl1);
    privilege3_1.setAction("*");

    TSentryPrivilege privilege_server = new TSentryPrivilege();
    privilege_server.setPrivilegeScope("SERVER");
    privilege_server.setServerName("server1");
    privilege_server.setGrantorPrincipal(grantor);
    privilege_server.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl2 = new TSentryPrivilege();
    privilege_tbl2.setPrivilegeScope("TABLE");
    privilege_tbl2.setServerName("server1");
    privilege_tbl2.setDbName("db1");
    privilege_tbl2.setTableName("tbl2");
    privilege_tbl2.setGrantorPrincipal(grantor);
    privilege_tbl2.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege2_3 = new TSentryPrivilege(privilege_tbl2);
    privilege2_3.setAction("SELECT");

    TSentryPrivilege privilege3_2 = new TSentryPrivilege(privilege_tbl2);
    privilege3_2.setAction("INSERT");

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, privilege1);

    sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege2_1);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege_server);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege2_3);

    sentryStore.alterSentryRoleGrantPrivilege(roleName3, privilege3_1);
    sentryStore.alterSentryRoleGrantPrivilege(roleName3, privilege3_2);

    sentryStore.dropPrivilege(toTSentryAuthorizable(privilege_tbl1));
    assertEquals(0, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1)
        .size());
    assertEquals(2, sentryStore.getAllTSentryPrivilegesByRoleName(roleName2)
        .size());
    assertEquals(1, sentryStore.getAllTSentryPrivilegesByRoleName(roleName3)
        .size());

    sentryStore.dropPrivilege(toTSentryAuthorizable(privilege_tbl2));
    assertEquals(0, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1)
        .size());
    assertEquals(1, sentryStore.getAllTSentryPrivilegesByRoleName(roleName2)
        .size());
    assertEquals(0, sentryStore.getAllTSentryPrivilegesByRoleName(roleName3)
        .size());
  }

  @Test
  public void testDropOverlappedPrivileges() throws Exception {
    String roleName1 = "list-privs-r1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1, grantor);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName("tbl1");
    privilege_tbl1.setGrantorPrincipal(grantor);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction("INSERT");

    TSentryPrivilege privilege_tbl1_all = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_all.setAction("*");

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, privilege_tbl1_insert);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, privilege_tbl1_all);

    sentryStore.dropPrivilege(toTSentryAuthorizable(privilege_tbl1));
    assertEquals(0, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1)
        .size());
  }

  private TSentryAuthorizable toTSentryAuthorizable(
      TSentryPrivilege tSentryPrivilege) {
    TSentryAuthorizable tSentryAuthorizable = new TSentryAuthorizable();
    tSentryAuthorizable.setServer(tSentryPrivilege.getServerName());
    tSentryAuthorizable.setDb(tSentryPrivilege.getDbName());
    tSentryAuthorizable.setTable(tSentryPrivilege.getTableName());
    tSentryAuthorizable.setUri(tSentryPrivilege.getURI());
    return tSentryAuthorizable;
  }

  /***
   * Create roles and assign privileges for same table rename the privileges for
   * the table and verify the new privileges
   * @throws Exception
   */
  @Test
  public void testRenameTable() throws Exception {
    String roleName1 = "role1", roleName2 = "role2", roleName3 = "role3";
    String grantor = "g1";
    String table1 = "tbl1", table2 = "tbl2";

    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.createSentryRole(roleName3, grantor);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName(table1);
    privilege_tbl1.setGrantorPrincipal(grantor);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction(AccessConstants.INSERT);

    TSentryPrivilege privilege_tbl1_select = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_select.setAction(AccessConstants.SELECT);

    TSentryPrivilege privilege_tbl1_all = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_all.setAction(AccessConstants.ALL);

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, privilege_tbl1_insert);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege_tbl1_select);
    sentryStore.alterSentryRoleGrantPrivilege(roleName3, privilege_tbl1_all);

    TSentryAuthorizable oldTable = toTSentryAuthorizable(privilege_tbl1);
    TSentryAuthorizable newTable = toTSentryAuthorizable(privilege_tbl1);
    newTable.setTable(table2);
    sentryStore.renamePrivilege(oldTable, newTable, System.getProperty("user.name"));

    for (String roleName : Sets.newHashSet(roleName1, roleName2, roleName3)) {
      Set<TSentryPrivilege> privilegeSet = sentryStore
          .getAllTSentryPrivilegesByRoleName(roleName);
      assertEquals(1, privilegeSet.size());
      for (TSentryPrivilege privilege : privilegeSet) {
        assertTrue(table2.equalsIgnoreCase(privilege.getTableName()));
        assertEquals(System.getProperty("user.name"),
            privilege.getGrantorPrincipal());
      }
    }
  }

  protected void addGroupsToUser(String user, String... groupNames) {
    policyFile.addGroupsToUser(user, groupNames);
  }

  protected void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }
}
