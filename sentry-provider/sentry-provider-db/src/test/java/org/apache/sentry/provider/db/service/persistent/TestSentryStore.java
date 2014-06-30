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
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
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

  @Before
  public void setup() throws Exception {
    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    Configuration conf = new Configuration(false);
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
    sentryStore = new SentryStore(conf);
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
    privilege.setPrivilegeName(SentryStore.constructPrivilegeName(privilege));

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
    tSentryPrivilege.setPrivilegeName(SentryStore.constructPrivilegeName(tSentryPrivilege));
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
    assertTrue(privs.contains("server=server1->uri=" + uri + "->action=ALL"));
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
    privilege.setPrivilegeName(SentryStore.constructPrivilegeName(privilege));
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(roleName, privilege)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    assertEquals(privilege.getPrivilegeName(), Iterables.get(privileges, 0).getPrivilegeName());
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
    privilege1.setPrivilegeName(SentryStore.constructPrivilegeName(privilege1));
    assertEquals(seqId + 2, sentryStore.alterSentryRoleGrantPrivilege(roleName1, privilege1)
        .getSequenceId());
    assertEquals(seqId + 3, sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege1)
        .getSequenceId());
    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("SERVER");
    privilege2.setServerName("server1");
    privilege2.setGrantorPrincipal(grantor);
    privilege2.setCreateTime(System.currentTimeMillis());
    privilege2.setPrivilegeName(SentryStore.constructPrivilegeName(privilege2));
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
    privilege_tbl1.setPrivilegeName(SentryStore.constructPrivilegeName(privilege_tbl1));

    TSentryPrivilege privilege1 = new TSentryPrivilege(privilege_tbl1);
    privilege1.setAction("SELECT");
    privilege1.setPrivilegeName(SentryStore.constructPrivilegeName(privilege1));

    TSentryPrivilege privilege2_1 = new TSentryPrivilege(privilege_tbl1);
    privilege2_1.setAction("INSERT");
    privilege2_1.setPrivilegeName(SentryStore.constructPrivilegeName(privilege2_1));
    TSentryPrivilege privilege3_1 = new TSentryPrivilege(privilege_tbl1);
    privilege3_1.setAction("*");
    privilege3_1.setPrivilegeName(SentryStore.constructPrivilegeName(privilege3_1));

    TSentryPrivilege privilege_server = new TSentryPrivilege();
    privilege_server.setPrivilegeScope("SERVER");
    privilege_server.setServerName("server1");
    privilege_server.setGrantorPrincipal(grantor);
    privilege_server.setCreateTime(System.currentTimeMillis());
    privilege_server.setPrivilegeName(SentryStore.constructPrivilegeName(privilege_server));

    TSentryPrivilege privilege_tbl2 = new TSentryPrivilege();
    privilege_tbl2.setPrivilegeScope("TABLE");
    privilege_tbl2.setServerName("server1");
    privilege_tbl2.setDbName("db1");
    privilege_tbl2.setTableName("tbl2");
    privilege_tbl2.setGrantorPrincipal(grantor);
    privilege_tbl2.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege2_3 = new TSentryPrivilege(privilege_tbl2);
    privilege2_3.setAction("SELECT");
    privilege2_3.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege2_3));

    TSentryPrivilege privilege3_2 = new TSentryPrivilege(privilege_tbl2);
    privilege3_2.setAction("INSERT");
    privilege2_3.setPrivilegeName(SentryStore.constructPrivilegeName(privilege2_3));

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
    privilege_tbl1.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege_tbl1));

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction("INSERT");
    privilege_tbl1_insert.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege_tbl1_insert));

    TSentryPrivilege privilege_tbl1_all = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_all.setAction("*");
    privilege_tbl1_all.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege_tbl1_all));

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
    privilege_tbl1.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege_tbl1));

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction(AccessConstants.INSERT);
    privilege_tbl1_insert.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege_tbl1_insert));

    TSentryPrivilege privilege_tbl1_select = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_select.setAction(AccessConstants.SELECT);
    privilege_tbl1_select.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege_tbl1_select));

    TSentryPrivilege privilege_tbl1_all = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_all.setAction(AccessConstants.ALL);
    privilege_tbl1_all.setPrivilegeName(SentryStore
        .constructPrivilegeName(privilege_tbl1_all));

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, privilege_tbl1_insert);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, privilege_tbl1_select);
    sentryStore.alterSentryRoleGrantPrivilege(roleName3, privilege_tbl1_all);

    TSentryAuthorizable oldTable = toTSentryAuthorizable(privilege_tbl1);
    TSentryAuthorizable newTable = toTSentryAuthorizable(privilege_tbl1);
    newTable.setTable(table2);
    sentryStore.renamePrivilege(oldTable, newTable);

    for (String roleName : Sets.newHashSet(roleName1, roleName2, roleName3)) {
      Set<TSentryPrivilege> privilegeSet = sentryStore
          .getAllTSentryPrivilegesByRoleName(roleName);
      assertEquals(1, privilegeSet.size());
      for (TSentryPrivilege privilege : privilegeSet) {
        assertTrue(table2.equalsIgnoreCase(privilege.getTableName()));
      }
    }
  }
}
