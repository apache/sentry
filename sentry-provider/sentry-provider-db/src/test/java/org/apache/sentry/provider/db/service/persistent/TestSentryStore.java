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
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.UserProvider;
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
import org.junit.AfterClass;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestSentryStore {

  private static File dataDir;
  private static SentryStore sentryStore;
  private static String[] adminGroups = { "adminGroup1" };
  private static PolicyFile policyFile;
  private static File policyFilePath;
  final long NUM_PRIVS = 60;  // > SentryStore.PrivCleaner.NOTIFY_THRESHOLD
  private static Configuration conf = null;
  private static char[] passwd = new char[] { '1', '2', '3'};

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration(false);
    final String ourUrl = UserProvider.SCHEME_NAME + ":///";
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);
    CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(ServerConfig.
        SENTRY_STORE_JDBC_PASS, passwd);
    provider.flush();

    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.setStrings(ServerConfig.ADMIN_GROUPS, adminGroups);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    policyFilePath = new File(dataDir, "local_policy_file.ini");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    sentryStore = new SentryStore(conf);
  }

  @Before
  public void before() throws Exception {
    policyFile = new PolicyFile();
    String adminUser = "g1";
    addGroupsToUser(adminUser, adminGroups);
    writePolicyFile();
  }

  @After
  public void after() {
    sentryStore.clearAllTables();
  }

  @AfterClass
  public static void teardown() {
    if (sentryStore != null) {
      sentryStore.stop();
    }
    if (dataDir != null) {
      FileUtils.deleteQuietly(dataDir);
    }
  }

  @Test
  public void testCredentialProvider() throws Exception {
    assertArrayEquals(passwd, conf.getPassword(ServerConfig.
        SENTRY_STORE_JDBC_PASS));
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
    privilege.setCreateTime(System.currentTimeMillis());

    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    assertEquals(seqId + 1, sentryStore.alterSentryRoleAddGroups(grantor, roleName, groups).getSequenceId());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleDeleteGroups(roleName, groups).getSequenceId());
    assertEquals(seqId + 3, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege).getSequenceId());
    assertEquals(seqId + 4, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege).getSequenceId());
  }
  @Test
  public void testURI() throws Exception {
    String roleName = "test-dup-role";
    String grantor = "g1";
    String uri = "file:///var/folders/dt/9zm44z9s6bjfxbrm4v36lzdc0000gp/T/1401860678102-0/data/kv1.dat";
    sentryStore.createSentryRole(roleName);
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege("URI", "server1", "ALL");
    tSentryPrivilege.setURI(uri);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, tSentryPrivilege);

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
    sentryStore.createSentryRole(roleName);
    try {
      sentryStore.createSentryRole(roleName);
      fail("Expected SentryAlreadyExistsException");
    } catch(SentryAlreadyExistsException e) {
      // expected
    }
  }

  @Test
  public void testCaseSensitiveScope() throws Exception {
    String roleName = "role1";
    String grantor = "g1";
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    TSentryPrivilege sentryPrivilege = new TSentryPrivilege("Database", "server1", "all");
    sentryPrivilege.setDbName("db1");
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, sentryPrivilege).getSequenceId());
  }
  @Test
  public void testCreateDropRole() throws Exception {
    String roleName = "test-drop-role";
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
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
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
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
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    privilege.setAction(AccessConstants.SELECT);
    assertEquals(seqId + 2, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege)
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

  private void verifyOrphanCleanup() throws Exception {
    boolean success = false;
    int iterations = 30;
    while (!success && iterations > 0) {
      Thread.sleep(1000);
      long numDBPrivs = sentryStore.countMSentryPrivileges();
      if (numDBPrivs < NUM_PRIVS) {
        assertEquals(0, numDBPrivs);
        success = true;
      }
      iterations--;
    }
    assertTrue("Failed to cleanup orphaned privileges", success);
  }

  /**
   * Create several privileges in the database, then delete the role that
   * created them.  This makes them all orphans.  Wait a bit to ensure the
   * cleanup thread runs, and expect them all to be gone from the database.
   * @throws Exception
   */
  @Ignore("Disabled with SENTRY-545 following SENTRY-140 problems")
  @Test
  public void testPrivilegeCleanup() throws Exception {
    final String roleName = "test-priv-cleanup";
    final String grantor = "g1";
    final String server = "server";
    final String dBase = "db";
    final String table = "table-";

    sentryStore.createSentryRole(roleName);

    // Create NUM_PRIVS unique privilege objects in the database
    for (int i = 0; i < NUM_PRIVS; i++) {
      TSentryPrivilege priv = new TSentryPrivilege();
      priv.setPrivilegeScope("TABLE");
      priv.setServerName(server);
      priv.setAction(AccessConstants.ALL);
      priv.setCreateTime(System.currentTimeMillis());
      priv.setTableName(table + i);
      priv.setDbName(dBase);
      sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, priv);
    }

    // Make sure we really have the expected number of privs in the database
    assertEquals(sentryStore.countMSentryPrivileges(), NUM_PRIVS);

    // Now to make a bunch of orphans, we just remove the role that
    // created them.
    sentryStore.dropSentryRole(roleName);

    // Now wait and see if the orphans get cleaned up
    verifyOrphanCleanup();
  }

  /**
   * Much like testPrivilegeCleanup, make a lot of privileges and make sure
   * they get cleaned up.  The difference here is that the privileges are
   * created by granting ALL and then removing SELECT - thus leaving INSERT.
   * This test exists because the revocation plays havoc with the orphan
   * cleanup thread.
   * @throws Exception
   */
  @Ignore("Disabled with SENTRY-545 following SENTRY-140 problems")
  @Test
  public void testPrivilegeCleanup2() throws Exception {
    final String roleName = "test-priv-cleanup";
    final String grantor = "g1";
    final String server = "server";
    final String dBase = "db";
    final String table = "table-";

    sentryStore.createSentryRole(roleName);

    // Create NUM_PRIVS unique privilege objects in the database once more,
    // this time granting ALL and revoking SELECT to make INSERT.
    for (int i=0 ; i < NUM_PRIVS; i++) {
      TSentryPrivilege priv = new TSentryPrivilege();
      priv.setPrivilegeScope("DATABASE");
      priv.setServerName(server);
      priv.setAction(AccessConstants.ALL);
      priv.setCreateTime(System.currentTimeMillis());
      priv.setTableName(table + i);
      priv.setDbName(dBase);
      priv.setGrantOption(TSentryGrantOption.TRUE);
      sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, priv);

      priv.setAction(AccessConstants.SELECT);
      priv.setGrantOption(TSentryGrantOption.UNSET);
      sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, priv);
      // after having ALL and revoking SELECT, we should have INSERT
      MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
      Set<MSentryPrivilege> privileges = role.getPrivileges();
      assertEquals(privileges.toString(), i+1, privileges.size());
      MSentryPrivilege mPrivilege = Iterables.get(privileges, 0);
      assertEquals(AccessConstants.INSERT, mPrivilege.getAction());
    }

    // Drop the role and clean up as before
    sentryStore.dropSentryRole(roleName);
    verifyOrphanCleanup();
  }

  @Test
  public void testGrantRevokeMultiPrivileges() throws Exception {
    String roleName = "test-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String[] columns = {"c1","c2","c3","c4"};
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    Set<TSentryPrivilege> tPrivileges = Sets.newHashSet();
    for (String column : columns) {
      TSentryPrivilege privilege = new TSentryPrivilege();
      privilege.setPrivilegeScope("Column");
      privilege.setServerName(server);
      privilege.setDbName(db);
      privilege.setTableName(table);
      privilege.setColumnName(column);
      privilege.setAction(AccessConstants.SELECT);
      privilege.setCreateTime(System.currentTimeMillis());
      tPrivileges.add(privilege);
    }
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivileges(grantor, roleName, tPrivileges)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 4, privileges.size());

    tPrivileges = Sets.newHashSet();
    for (int i = 0; i < 2; i++) {
      TSentryPrivilege privilege = new TSentryPrivilege();
      privilege.setPrivilegeScope("Column");
      privilege.setServerName(server);
      privilege.setDbName(db);
      privilege.setTableName(table);
      privilege.setColumnName(columns[i]);
      privilege.setAction(AccessConstants.SELECT);
      privilege.setCreateTime(System.currentTimeMillis());
      tPrivileges.add(privilege);
    }
    assertEquals(seqId + 2, sentryStore.alterSentryRoleRevokePrivileges(grantor, roleName, tPrivileges)
        .getSequenceId());
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("Table");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.SELECT);
    privilege.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 3, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege)
        .getSequenceId());
    // After revoking table scope, we will have 0 privileges
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 0, privileges.size());
  }

  /**
   * Regression test for SENTRY-74 and SENTRY-552
   */
  @Test
  public void testGrantRevokePrivilegeWithColumn() throws Exception {
    String roleName = "test-col-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String column1 = "c1";
    String column2 = "c2";
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("COLUMN");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setColumnName(column1);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());

    // Grant ALL on c1 and c2
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege)
        .getSequenceId());
    privilege.setColumnName(column2);
    assertEquals(seqId + 2, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // Revoke SELECT on c2
    privilege.setAction(AccessConstants.SELECT);
    assertEquals(seqId + 3, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege)
        .getSequenceId());

    // At this point c1 has ALL privileges and c2 should have INSERT after revoking SELECT
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());
    for (MSentryPrivilege mPrivilege: privileges) {
      assertEquals(server, mPrivilege.getServerName());
      assertEquals(db, mPrivilege.getDbName());
      assertEquals(table, mPrivilege.getTableName());
      assertFalse(mPrivilege.getGrantOption());
      if (mPrivilege.getColumnName().equals(column1)) {
        assertEquals(AccessConstants.ALL, mPrivilege.getAction());
      } else if (mPrivilege.getColumnName().equals(column2)) {
        assertEquals(AccessConstants.INSERT, mPrivilege.getAction());
      } else {
        fail("Unexpected column name: " + mPrivilege.getColumnName());
      }
    }

    // after revoking INSERT table level privilege will remove privileges from column2
    // and downgrade column1 to SELECT privileges.
    privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.INSERT);
    privilege.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 4, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege)
        .getSequenceId());
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    assertEquals(column1, Iterables.get(privileges, 0).getColumnName());
    assertEquals(AccessConstants.SELECT, Iterables.get(privileges, 0).getAction());

    // Revoke ALL from the table should now remove all the column privileges.
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 5, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege)
        .getSequenceId());
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 0, privileges.size());
  }

  /**
   * Regression test for SENTRY-552
   */
  @Test
  public void testGrantRevokeTablePrivilegeDowngradeByDb() throws Exception {
    String roleName = "test-table-db-downgrade-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table1 = "tbl1";
    String table2 = "tbl2";
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    TSentryPrivilege privilegeTable1 = new TSentryPrivilege();
    privilegeTable1.setPrivilegeScope("TABLE");
    privilegeTable1.setServerName(server);
    privilegeTable1.setDbName(db);
    privilegeTable1.setTableName(table1);
    privilegeTable1.setAction(AccessConstants.ALL);
    privilegeTable1.setCreateTime(System.currentTimeMillis());
    TSentryPrivilege privilegeTable2 = privilegeTable1.deepCopy();;
    privilegeTable2.setTableName(table2);

    // Grant ALL on table1 and table2
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeTable1)
        .getSequenceId());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeTable2)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // Revoke SELECT on table2
    privilegeTable2.setAction(AccessConstants.SELECT);
    assertEquals(seqId + 3, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeTable2)
        .getSequenceId());
    // after having ALL and revoking SELECT, we should have INSERT
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // At this point table1 has ALL privileges and table2 should have INSERT after revoking SELECT
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());
    for (MSentryPrivilege mPrivilege: privileges) {
      assertEquals(server, mPrivilege.getServerName());
      assertEquals(db, mPrivilege.getDbName());
      assertFalse(mPrivilege.getGrantOption());
      if (mPrivilege.getTableName().equals(table1)) {
        assertEquals(AccessConstants.ALL, mPrivilege.getAction());
      } else if (mPrivilege.getTableName().equals(table2)) {
        assertEquals(AccessConstants.INSERT, mPrivilege.getAction());
      } else {
        fail("Unexpected table name: " + mPrivilege.getTableName());
      }
    }

    // Revoke INSERT on Database
    privilegeTable2.setAction(AccessConstants.INSERT);
    privilegeTable2.setPrivilegeScope("DATABASE");
    privilegeTable2.unsetTableName();
    assertEquals(seqId + 4, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeTable2)
        .getSequenceId());
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();

    // after revoking INSERT database level privilege will remove privileges from table2
    // and downgrade table1 to SELECT privileges.
    assertEquals(privileges.toString(), 1, privileges.size());
    MSentryPrivilege mPrivilege = Iterables.get(privileges, 0);
    assertEquals(server, mPrivilege.getServerName());
    assertEquals(db, mPrivilege.getDbName());
    assertEquals(table1, mPrivilege.getTableName());
    assertEquals(AccessConstants.SELECT, mPrivilege.getAction());
    assertFalse(mPrivilege.getGrantOption());
  }

  /**
   * Regression test for SENTRY-552
   */
  @Test
  public void testGrantRevokeColumnPrivilegeDowngradeByDb() throws Exception {
    String roleName = "test-column-db-downgrade-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String column1 = "c1";
    String column2 = "c2";
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    TSentryPrivilege privilegeCol1 = new TSentryPrivilege();
    privilegeCol1.setPrivilegeScope("COLUMN");
    privilegeCol1.setServerName(server);
    privilegeCol1.setDbName(db);
    privilegeCol1.setTableName(table);
    privilegeCol1.setColumnName(column1);
    privilegeCol1.setAction(AccessConstants.ALL);
    privilegeCol1.setCreateTime(System.currentTimeMillis());
    TSentryPrivilege privilegeCol2 = privilegeCol1.deepCopy();;
    privilegeCol2.setColumnName(column2);

    // Grant ALL on column1 and column2
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeCol1)
        .getSequenceId());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeCol2)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // Revoke SELECT on column2
    privilegeCol2.setAction(AccessConstants.SELECT);
    assertEquals(seqId + 3, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeCol2)
        .getSequenceId());
    // after having ALL and revoking SELECT, we should have INSERT
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // At this point column1 has ALL privileges and column2 should have INSERT after revoking SELECT
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());
    for (MSentryPrivilege mPrivilege: privileges) {
      assertEquals(server, mPrivilege.getServerName());
      assertEquals(db, mPrivilege.getDbName());
      assertEquals(table, mPrivilege.getTableName());
      assertFalse(mPrivilege.getGrantOption());
      if (mPrivilege.getColumnName().equals(column1)) {
        assertEquals(AccessConstants.ALL, mPrivilege.getAction());
      } else if (mPrivilege.getColumnName().equals(column2)) {
        assertEquals(AccessConstants.INSERT, mPrivilege.getAction());
      } else {
        fail("Unexpected column name: " + mPrivilege.getColumnName());
      }
    }

    // Revoke INSERT on Database
    privilegeCol2.setAction(AccessConstants.INSERT);
    privilegeCol2.setPrivilegeScope("DATABASE");
    privilegeCol2.unsetTableName();
    privilegeCol2.unsetColumnName();
    assertEquals(seqId + 4, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeCol2)
        .getSequenceId());
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();

    // after revoking INSERT database level privilege will remove privileges from column2
    // and downgrade column1 to SELECT privileges.
    assertEquals(privileges.toString(), 1, privileges.size());
    MSentryPrivilege mPrivilege = Iterables.get(privileges, 0);
    assertEquals(server, mPrivilege.getServerName());
    assertEquals(db, mPrivilege.getDbName());
    assertEquals(table, mPrivilege.getTableName());
    assertEquals(column1, mPrivilege.getColumnName());
    assertEquals(AccessConstants.SELECT, mPrivilege.getAction());
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
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(grantOption);
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    assertEquals(Boolean.valueOf(privilege.getGrantOption().toString()), Iterables.get(privileges, 0).getGrantOption());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege)
        .getSequenceId());
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(0, privileges.size());

    roleName = "test-grantOption-db";
    sentryStore.createSentryRole(roleName);
    privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("DATABASE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setAction(AccessConstants.ALL);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(grantOption);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());

    privilege.setAction(AccessConstants.SELECT);
    privilege.setGrantOption(TSentryGrantOption.UNSET);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
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
      sentryStore.createSentryRole(roles[i]);
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
    privilege1.setCreateTime(System.currentTimeMillis());
    privilege1.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege1);
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
    privilege2.setCreateTime(System.currentTimeMillis());
    privilege2.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege2);

    // 4. user0 grant all on table tb1 to role2, no grant option
    roleName = roles[2];
    grantor = users[0];
    TSentryPrivilege privilege3 = new TSentryPrivilege();
    privilege3.setPrivilegeScope("TABLE");
    privilege3.setServerName(server);
    privilege3.setDbName(db);
    privilege3.setTableName(table);
    privilege3.setAction(AccessConstants.ALL);
    privilege3.setCreateTime(System.currentTimeMillis());
    privilege3.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege3);

    // 5. user1 has role1, no insert privilege,
    // grant insert to role3, will throw no grant exception
    roleName = roles[3];
    grantor = users[1];
    TSentryPrivilege privilege4 = new TSentryPrivilege();
    privilege4.setPrivilegeScope("DATABASE");
    privilege4.setServerName(server);
    privilege4.setDbName(db);
    privilege4.setAction(AccessConstants.INSERT);
    privilege4.setCreateTime(System.currentTimeMillis());
    privilege4.setGrantOption(TSentryGrantOption.FALSE);
    boolean isGrantOptionException = false;
    try {
      sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege4);
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
    privilege5.setCreateTime(System.currentTimeMillis());
    privilege5.setGrantOption(TSentryGrantOption.FALSE);
    isGrantOptionException = false;
    try {
      sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege5);
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
      sentryStore.createSentryRole(roles[i]);
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
    privilege1.setCreateTime(System.currentTimeMillis());
    privilege1.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege1);
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
    privilege2.setCreateTime(System.currentTimeMillis());
    privilege2.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege2);

    // 4. g1 grant select on table tb1 to role2, no grant option
    roleName = roles[2];
    grantor = "g1";
    TSentryPrivilege privilege3 = new TSentryPrivilege();
    privilege3.setPrivilegeScope("TABLE");
    privilege3.setServerName(server);
    privilege3.setDbName(db);
    privilege3.setTableName(table);
    privilege3.setAction(AccessConstants.SELECT);
    privilege3.setCreateTime(System.currentTimeMillis());
    privilege3.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege3);

    // 5. user1 has role1, no grant option,
    // revoke from role2 will throw no grant exception
    roleName = roles[2];
    grantor = users[1];
    boolean isGrantOptionException = false;
    try {
      sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege3);
    } catch (SentryGrantDeniedException e) {
      isGrantOptionException = true;
      System.err.println(e.getMessage());
    }
    assertTrue(isGrantOptionException);

    // 6. user0 has role0, only have select,
    // revoke all from role1 will throw no grant exception
    roleName = roles[1];
    grantor = users[0];
    try {
      sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege2);
    } catch (SentryGrantDeniedException e) {
      isGrantOptionException = true;
      System.err.println(e.getMessage());
    }
    assertTrue(isGrantOptionException);

    // 7. user0 has role0, has select and grant option,
    // revoke select from role2
    roleName = roles[2];
    grantor = users[0];
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege3);
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
      sentryStore.createSentryRole(roles[i]);
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
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);

    // 3. g1 grant select on table tb1 to role0, no grant option
    roleName = roles[0];
    grantor = "g1";
    privilege.setGrantOption(TSentryGrantOption.FALSE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);

    // 4. g1 revoke all privilege from role0
    roleName = roles[0];
    grantor = "g1";
    privilege.setGrantOption(TSentryGrantOption.UNSET);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 0, privileges.size());
  }

  @Test
  public void testGrantCheckWithColumn() throws Exception {
    // 1. set local group mapping
    // user0->group0->role0
    // user1->group1->role1
    String grantor = "g1";
    String[] users = {"user0","user1"};
    String[] roles = {"role0","role1"};
    String[] groups = {"group0","group1"};
    for (int i = 0; i < users.length; i++) {
      addGroupsToUser(users[i], groups[i]);
      sentryStore.createSentryRole(roles[i]);
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
    TSentryPrivilege privilege1 = new TSentryPrivilege();
    privilege1.setPrivilegeScope("TABLE");
    privilege1.setServerName(server);
    privilege1.setDbName(db);
    privilege1.setTableName(table);
    privilege1.setAction(AccessConstants.SELECT);
    privilege1.setCreateTime(System.currentTimeMillis());
    privilege1.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege1);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());

    // 3. user0 grant select on column tb1.c1 to role1, with grant option
    roleName = roles[1];
    grantor = users[0];
    String column = "c1";
    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("COLUMN");
    privilege2.setServerName(server);
    privilege2.setDbName(db);
    privilege2.setTableName(table);
    privilege2.setColumnName(column);
    privilege2.setAction(AccessConstants.SELECT);
    privilege2.setCreateTime(System.currentTimeMillis());
    privilege2.setGrantOption(TSentryGrantOption.TRUE);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege2);

    // 4. user1 revoke table level privilege from user0, will throw grant denied exception
    roleName = roles[0];
    grantor = users[1];
    boolean isGrantOptionException = false;
    try {
      sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege1);
    } catch (SentryGrantDeniedException e) {
      isGrantOptionException = true;
      System.err.println(e.getMessage());
    }
    assertTrue(isGrantOptionException);

    // 5. user0 revoke column level privilege from user1
    roleName = roles[1];
    grantor = users[0];
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege2);
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(0, privileges.size());
  }

  @Test
  public void testGrantDuplicatePrivilege() throws Exception {
    String roleName = "test-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    long seqId = sentryStore.createSentryRole(roleName).getSequenceId();
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    assertEquals(seqId + 1, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege)
        .getSequenceId());
    assertEquals(seqId + 2, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege)
        .getSequenceId());
    privilege.setServerName("Server1");
    privilege.setDbName("DB1");
    privilege.setTableName("TBL1");
    assertEquals(seqId + 3, sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege)
        .getSequenceId());
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
  }

  @Test
  public void testListSentryPrivilegesForProvider() throws Exception {
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

    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);
    sentryStore.createSentryRole(roleName3);

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
    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);
    sentryStore.createSentryRole(roleName3);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName("tbl1");
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
    privilege_server.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl2 = new TSentryPrivilege();
    privilege_tbl2.setPrivilegeScope("TABLE");
    privilege_tbl2.setServerName("server1");
    privilege_tbl2.setDbName("db1");
    privilege_tbl2.setTableName("tbl2");
    privilege_tbl2.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege2_3 = new TSentryPrivilege(privilege_tbl2);
    privilege2_3.setAction("SELECT");

    TSentryPrivilege privilege3_2 = new TSentryPrivilege(privilege_tbl2);
    privilege3_2.setAction("INSERT");

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege2_1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege_server);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege2_3);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName3, privilege3_1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName3, privilege3_2);

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

  /**
   * Regression test for SENTRY-547 and SENTRY-548
   * Use case:
   * GRANT INSERT on TABLE tbl1 to ROLE role1
   * GRANT SELECT on TABLE tbl1 to ROLE role1
   * GRANT ALTER on TABLE tbl1 to ROLE role1
   * GRANT DROP on TABLE tbl1 to ROLE role1
   * DROP TABLE tbl1
   *
   * After drop tbl1, role1 should have 0 privileges
   */
  @Test
  public void testDropTableWithMultiAction() throws Exception {
    String roleName1 = "role1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName("tbl1");
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction(AccessConstants.INSERT);

    TSentryPrivilege privilege_tbl1_select = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_select.setAction(AccessConstants.SELECT);

    TSentryPrivilege privilege_tbl1_alter = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_alter.setAction(AccessConstants.ALTER);

    TSentryPrivilege privilege_tbl1_drop = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_drop.setAction(AccessConstants.DROP);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_insert);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_select);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_alter);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_drop);

    assertEquals(4, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1).size());

    // after drop privilege_tbl1, role1 should have 0 privileges
    sentryStore.dropPrivilege(toTSentryAuthorizable(privilege_tbl1));
    assertEquals(0, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1).size());
  }

  @Test
  public void testDropTableWithColumn() throws Exception {
    String roleName1 = "role1", roleName2 = "role2";
    String grantor = "g1";
    String table1 = "tbl1";

    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName(table1);
    privilege_tbl1.setAction(AccessConstants.SELECT);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_c1 = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_c1.setPrivilegeScope("COLUMN");
    privilege_tbl1_c1.setColumnName("c1");
    privilege_tbl1_c1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_c2 = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_c2.setPrivilegeScope("COLUMN");
    privilege_tbl1_c2.setColumnName("c2");
    privilege_tbl1_c2.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_c3 = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_c3.setPrivilegeScope("COLUMN");
    privilege_tbl1_c3.setColumnName("c3");
    privilege_tbl1_c3.setCreateTime(System.currentTimeMillis());

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_c1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_c2);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege_tbl1_c3);

    Set<TSentryPrivilege> privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(2, privilegeSet.size());
    privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName2);
    assertEquals(1, privilegeSet.size());

    TSentryAuthorizable tableAuthorizable = toTSentryAuthorizable(privilege_tbl1);
    sentryStore.dropPrivilege(tableAuthorizable);

    privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(0, privilegeSet.size());
    privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName2);
    assertEquals(0, privilegeSet.size());
  }

  @Test
  public void testDropOverlappedPrivileges() throws Exception {
    String roleName1 = "list-privs-r1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName("tbl1");
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction("INSERT");

    TSentryPrivilege privilege_tbl1_all = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_all.setAction("*");

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_insert);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_all);

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

    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);
    sentryStore.createSentryRole(roleName3);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName(table1);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction(AccessConstants.INSERT);

    TSentryPrivilege privilege_tbl1_select = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_select.setAction(AccessConstants.SELECT);

    TSentryPrivilege privilege_tbl1_all = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_all.setAction(AccessConstants.ALL);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_insert);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege_tbl1_select);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName3, privilege_tbl1_all);

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

  /**
   * Regression test for SENTRY-550
   * Use case:
   * GRANT INSERT on TABLE tbl1 to ROLE role1
   * GRANT SELECT on TABLE tbl1 to ROLE role1
   * GRANT ALTER on TABLE tbl1 to ROLE role1
   * GRANT DROP on TABLE tbl1 to ROLE role1
   * RENAME TABLE tbl1 to tbl2
   *
   * After rename tbl1 to tbl2, table name of all role1's privileges should be "tbl2"
   */
  @Test
  public void testRenameTableWithMultiAction() throws Exception {
    String roleName1 = "role1";
    String grantor = "g1";
    String table1 = "tbl1", table2 = "tbl2";
    sentryStore.createSentryRole(roleName1);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName(table1);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction(AccessConstants.INSERT);

    TSentryPrivilege privilege_tbl1_select = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_select.setAction(AccessConstants.SELECT);

    TSentryPrivilege privilege_tbl1_alter = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_alter.setAction(AccessConstants.ALTER);

    TSentryPrivilege privilege_tbl1_drop = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_drop.setAction(AccessConstants.DROP);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_insert);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_select);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_alter);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_drop);

    TSentryAuthorizable oldTable = toTSentryAuthorizable(privilege_tbl1);
    TSentryAuthorizable newTable = toTSentryAuthorizable(privilege_tbl1);
    newTable.setTable(table2);
    sentryStore.renamePrivilege(oldTable, newTable);

    // after rename tbl1 to tbl2, all table name of role's privilege will be tbl2
    Set<TSentryPrivilege> privilegeSet = sentryStore
        .getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(4, privilegeSet.size());
    for (TSentryPrivilege privilege : privilegeSet) {
      assertTrue(table2.equalsIgnoreCase(privilege.getTableName()));
    }
  }

  @Test
  public void testSentryRoleSize() throws Exception {
    for( long i = 0; i< 5; i++ ) {
      assertEquals((Long)i, sentryStore.getRoleCountGauge().getValue());
      sentryStore.createSentryRole("role" + i);
    }
  }
  @Test
  public void testSentryPrivilegeSize() throws Exception {
    String role1 = "role1";
    String role2 = "role2";

    sentryStore.createSentryRole(role1);
    sentryStore.createSentryRole(role2);

    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName("server1");
    privilege.setDbName("db1");
    privilege.setTableName("tb1");
    privilege.setCreateTime(System.currentTimeMillis());

    String grantor = "g1";

    assertEquals(new Long(0), sentryStore.getPrivilegeCountGauge().getValue());

    sentryStore.alterSentryRoleGrantPrivilege(grantor, role1, privilege);
    assertEquals(new Long(1), sentryStore.getPrivilegeCountGauge().getValue());

    sentryStore.alterSentryRoleGrantPrivilege(grantor, role2, privilege);
    assertEquals(new Long(1), sentryStore.getPrivilegeCountGauge().getValue());

    privilege.setTableName("tb2");
    sentryStore.alterSentryRoleGrantPrivilege(grantor, role2, privilege);
    assertEquals(new Long(2), sentryStore.getPrivilegeCountGauge().getValue());
  }

  @Test
  public void testSentryGroupsSize() throws Exception {
    String role1 = "role1";
    String role2 = "role2";

    sentryStore.createSentryRole(role1);
    sentryStore.createSentryRole(role2);

    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName("group1");
    groups.add(group);

    String grantor = "g1";

    sentryStore.alterSentryRoleAddGroups(grantor, role1, groups);
    assertEquals(new Long(1), sentryStore.getGroupCountGauge().getValue());

    sentryStore.alterSentryRoleAddGroups(grantor, role2, groups);
    assertEquals(new Long(1), sentryStore.getGroupCountGauge().getValue());

    groups.add(new TSentryGroup("group2"));
    sentryStore.alterSentryRoleAddGroups(grantor, role2, groups);
    assertEquals(new Long(2), sentryStore.getGroupCountGauge().getValue());

  }

  @Test
  public void testRenameTableWithColumn() throws Exception {
    String roleName1 = "role1", roleName2 = "role2";
    String grantor = "g1";
    String table1 = "tbl1", table2 = "tbl2";

    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName(table1);
    privilege_tbl1.setAction(AccessConstants.SELECT);
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_c1 = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_c1.setPrivilegeScope("COLUMN");
    privilege_tbl1_c1.setColumnName("c1");
    privilege_tbl1_c1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_c2 = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_c2.setPrivilegeScope("COLUMN");
    privilege_tbl1_c2.setColumnName("c2");
    privilege_tbl1_c2.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_c3 = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl1_c3.setPrivilegeScope("COLUMN");
    privilege_tbl1_c3.setColumnName("c3");
    privilege_tbl1_c3.setCreateTime(System.currentTimeMillis());

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_c1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_c2);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege_tbl1_c3);

    Set<TSentryPrivilege> privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(2, privilegeSet.size());
    privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName2);
    assertEquals(1, privilegeSet.size());

    TSentryAuthorizable oldTable = toTSentryAuthorizable(privilege_tbl1);
    TSentryAuthorizable newTable = toTSentryAuthorizable(privilege_tbl1);
    newTable.setTable(table2);
    sentryStore.renamePrivilege(oldTable, newTable);

    privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(2, privilegeSet.size());
    for (TSentryPrivilege privilege : privilegeSet) {
      assertTrue(table2.equalsIgnoreCase(privilege.getTableName()));
    }
    privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName2);
    assertEquals(1, privilegeSet.size());
  }

  protected static void addGroupsToUser(String user, String... groupNames) {
    policyFile.addGroupsToUser(user, groupNames);
  }

  protected static void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }

}
