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
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.UserProvider;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.PermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.AfterClass;

import static org.apache.sentry.provider.db.service.persistent.QueryParamBuilder.newQueryParamBuilder;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sentry.hdfs.Updateable.Update;
import javax.jdo.JDODataStoreException;

public class TestSentryStore extends org.junit.Assert {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestSentryStore.class);

  private static File dataDir;
  private static SentryStore sentryStore;
  private static String[] adminGroups = { "adminGroup1" };
  private static PolicyFile policyFile;
  private static File policyFilePath;
  final long NUM_PRIVS = 5;  // > SentryStore.PrivCleaner.NOTIFY_THRESHOLD
  private static Configuration conf = null;
  private static char[] passwd = new char[] { '1', '2', '3'};

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration(false);
    final String ourUrl = UserProvider.SCHEME_NAME + ":///";
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    // THis should be a UserGroupInformation provider
    CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);


    // The user credentials are stored as a static variable by UserGrouoInformation provider.
    // We need to only set the password the first time, an attempt to set it for the second
    // time fails with an exception.
    if(provider.getCredentialEntry(ServerConfig.SENTRY_STORE_JDBC_PASS) == null) {
      provider.createCredentialEntry(ServerConfig.SENTRY_STORE_JDBC_PASS, passwd);
      provider.flush();
    }

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
    conf.setInt(ServerConfig.SENTRY_STORE_TRANSACTION_RETRY, 10);
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

  /**
   * Fail test if role already exists
   * @param roleName Role name to checl
   * @throws Exception
   */
  private void checkRoleDoesNotExist(String roleName) throws Exception {
    try {
      sentryStore.getMSentryRoleByName(roleName);
      fail("Role " + roleName + "already exists");
    } catch (SentryNoSuchObjectException e) {
      // Ok
    }
  }

  /**
   * Fail test if role doesn't exist
   * @param roleName Role name to checl
   * @throws Exception
   */
  private void checkRoleExists(String roleName) throws Exception {
    assertEquals(roleName.toLowerCase(),
            sentryStore.getMSentryRoleByName(roleName).getRoleName());
  }

  /**
   * Create a role with the given name and verify that it is created
   *
   * @param roleName
   * @throws Exception
   */
  private void createRole(String roleName) throws Exception {
    checkRoleDoesNotExist(roleName);
    sentryStore.createSentryRole(roleName);
    checkRoleExists(roleName);
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

    createRole(roleName);
    sentryStore.alterSentryRoleAddGroups(grantor, roleName, groups);
    sentryStore.alterSentryRoleDeleteGroups(roleName, groups);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
  }

  @Test
  public void testURI() throws Exception {
    String roleName = "test-dup-role";
    String grantor = "g1";
    String uri = "file:///var/folders/dt/9zm44z9s6bjfxbrm4v36lzdc0000gp/T/1401860678102-0/data/kv1.dat";
    createRole(roleName);
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
        sentryStore.listSentryPrivilegesForProvider(
        new HashSet<String>(Arrays.asList("group1")), thriftRoleSet, tSentryAuthorizable);

    assertTrue(privs.size()==1);
    assertTrue(privs.contains("server=server1->uri=" + uri + "->action=all"));
  }

  @Test
  public void testCreateDuplicateRole() throws Exception {
    String roleName = "test-dup-role";
    createRole(roleName);
    try {
      sentryStore.createSentryRole(roleName);
      fail("Expected SentryAlreadyExistsException");
    } catch (SentryAlreadyExistsException e) {
      // expected
    }
  }

  @Test
  public void testCaseSensitiveScope() throws Exception {
    String roleName = "role1";
    String grantor = "g1";
    createRole(roleName);
    TSentryPrivilege sentryPrivilege = new TSentryPrivilege("Database", "server1", "all");
    sentryPrivilege.setDbName("db1");
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, sentryPrivilege);
  }

  /**
   * Create a new role and then destroy it
   * @throws Exception
   */
  @Test
  public void testCreateDropRole() throws Exception {
    String roleName = "test-drop-role";
    createRole(roleName);
    sentryStore.dropSentryRole(roleName);
    checkRoleDoesNotExist(roleName);
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
    createRole(roleName);
    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName("test-groups-g1");
    groups.add(group);
    group = new TSentryGroup();
    group.setGroupName("test-groups-g2");
    groups.add(group);
    sentryStore.alterSentryRoleAddGroups(grantor, roleName, groups);
    sentryStore.alterSentryRoleDeleteGroups(roleName, groups);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    assertEquals(Collections.emptySet(), role.getGroups());
  }

  @Test
  public void testGetTSentryRolesForGroups() throws Exception {
    // Test the method getRoleNamesForGroups according to the following test data:
    // group1->r1
    // group2->r2
    // group3->r2
    String grantor = "g1";
    String roleName1 = "r1";
    String roleName2 = "r2";
    String roleName3 = "r3";
    String group1 = "group1";
    String group2 = "group2";
    String group3 = "group3";

    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);
    sentryStore.createSentryRole(roleName3);
    sentryStore.alterSentryRoleAddGroups(grantor, roleName1, Sets.newHashSet(new TSentryGroup(group1)));
    sentryStore.alterSentryRoleAddGroups(grantor, roleName2, Sets.newHashSet(new TSentryGroup(group2),
        new TSentryGroup(group3)));

    Set<String> groupSet1 = Sets.newHashSet(group1, group2, group3);
    Set<String> roleSet1 = Sets.newHashSet(roleName1, roleName2);

    Set<String> groupSet2 = Sets.newHashSet(group1);
    Set<String> roleSet2 = Sets.newHashSet(roleName1);

    Set<String> groupSet3 = Sets.newHashSet("foo");
    Set<String> roleSet3 = Sets.newHashSet();

    // Query for multiple groups
    Set<String> roles = sentryStore.getRoleNamesForGroups(groupSet1);
    assertEquals("Returned roles should match the expected roles", 0, Sets.symmetricDifference(roles, roleSet1).size());

    // Query for single group
    roles = sentryStore.getRoleNamesForGroups(groupSet2);
    assertEquals("Returned roles should match the expected roles", 0, Sets.symmetricDifference(roles, roleSet2).size());

    // Query for non-existing group
    roles = sentryStore.getRoleNamesForGroups(groupSet3);
    assertEquals("Returned roles should match the expected roles", 0, Sets.symmetricDifference(roles, roleSet3).size());
  }

  @Test
  public void testGrantRevokePrivilege() throws Exception {
    String roleName = "test-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    createRole(roleName);
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    privilege.setAction(AccessConstants.SELECT);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
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
    long numDBPrivs = sentryStore.countMSentryPrivileges();
    assertEquals("Privilege count", numDBPrivs,1);
  }

  private void verifyOrphanCleanup() throws Exception {
    assertFalse("Failed to cleanup orphaned privileges", sentryStore.findOrphanedPrivileges());
  }

  /**
   * Create several privileges in the database, then delete the role that
   * created them.  This makes them all orphans.  Wait a bit to ensure the
   * cleanup thread runs, and expect them all to be gone from the database.
   * @throws Exception
   */
 // @Ignore("Disabled with SENTRY-545 following SENTRY-140 problems")
  @Test
  public void testPrivilegeCleanup() throws Exception {
    final String roleName = "test-priv-cleanup";
    final String grantor = "g1";
    final String server = "server";
    final String dBase = "db";
    final String table = "table-";

    createRole(roleName);

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

    List<MSentryPrivilege> list = sentryStore.getAllMSentryPrivileges();
    assertEquals(list.size(), 0);
  }

  /**
   * Much like testPrivilegeCleanup, make a lot of privileges and make sure
   * they get cleaned up.  The difference here is that the privileges are
   * created by granting ALL and then removing SELECT - thus leaving INSERT.
   * This test exists because the revocation plays havoc with the orphan
   * cleanup thread.
   * @throws Exception
   */
 // @Ignore("Disabled with SENTRY-545 following SENTRY-140 problems")
  @Test
  public void testPrivilegeCleanup2() throws Exception {
    final String roleName = "test-priv-cleanup";
    final String grantor = "g1";
    final String server = "server";
    final String dBase = "db";
    final String table = "table-";

    createRole(roleName);

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
    //There should not be any Privileges left
    List<MSentryPrivilege> list = sentryStore.getAllMSentryPrivileges();
    assertEquals(list.size(), 0);
  }

  /**
   * This method tries to add ALL privileges on
   * databases to a role and immediately revokes
   * SELECT and INSERT privileges. At the end of
   * each iteration we should not find any privileges
   * for that role. Finally we should not find any
   * privileges, as we are cleaning up orphan privileges
   * immediately.
   * @throws Exception
   */
  @Test
  public void testPrivilegeCleanup3() throws Exception {
    final String roleName = "test-priv-cleanup";
    final String grantor = "g1";
    final String server = "server";
    final String dBase = "db";
    final String table = "table-";

    createRole(roleName);

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

      sentryStore.findOrphanedPrivileges();
      //After having ALL and revoking SELECT, we should have INSERT
      //Remove the INSERT privilege as well.
      //There should not be any more privileges in the sentry store
      priv.setAction(AccessConstants.INSERT);
      sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, priv);

      MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
      assertEquals("Privilege Count", 0, role.getPrivileges().size());

    }

    // Drop the role and clean up as before
    verifyOrphanCleanup();

    //There should not be any Privileges left
    List<MSentryPrivilege> list = sentryStore.getAllMSentryPrivileges();
    assertEquals(list.size(), 0);

  }

  /**
   * This method "n" privileges with action "ALL" on
   * tables to a role. Later we are revoking insert
   * and select privileges to of the tables making the
   * the privilege orphan. Finally we should find only
   * n -1 privileges, as we are cleaning up orphan
   * privileges immediately.
   * @throws Exception
   */
  @Test
  public void testPrivilegeCleanup4 () throws Exception {
    final String roleName = "test-priv-cleanup";
    final String grantor = "g1";
    final String server = "server";
    final String dBase = "db";
    final String table = "table-";

    createRole(roleName);

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

    //Revoking INSERT privilege. This is change the privilege to SELECT
    TSentryPrivilege priv = new TSentryPrivilege();
    priv.setPrivilegeScope("TABLE");
    priv.setServerName(server);
    priv.setAction(AccessConstants.INSERT);
    priv.setCreateTime(System.currentTimeMillis());
    priv.setTableName(table + '0');
    priv.setDbName(dBase);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, priv);

    //There should be SELECT privilege in the sentry store
    priv = new TSentryPrivilege();
    priv.setPrivilegeScope("TABLE");
    priv.setServerName(server);
    priv.setAction(AccessConstants.SELECT);
    priv.setCreateTime(System.currentTimeMillis());
    priv.setTableName(table + '0');
    priv.setDbName(dBase);
    MSentryPrivilege mPriv = sentryStore.findMSentryPrivilegeFromTSentryPrivilege(priv);
    assertNotNull(mPriv);

    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    assertEquals("Privilege Count", NUM_PRIVS, role.getPrivileges().size());

    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, priv);
    role = sentryStore.getMSentryRoleByName(roleName);
    assertEquals("Privilege Count", NUM_PRIVS-1, role.getPrivileges().size());

  }

  /**
   * This method tries to add alter privileges on
   * databases to a role and immediately revokes
   * them. At the end of each iteration we should
   * not find any privileges for that role
   * Finally we should not find any privileges, as
   * we are cleaning up orphan privileges immediately.
   * @throws Exception
   */
  @Test
  public void testPrivilegeCleanup5() throws Exception {
    final String roleName = "test-priv-cleanup";
    final String grantor = "g1";
    final String server = "server";
    final String dBase = "db";
    final String table = "table-";

    createRole(roleName);

    // Create NUM_PRIVS unique privilege objects in the database once more,
    // this time granting ALL and revoking SELECT to make INSERT.
    for (int i=0 ; i < NUM_PRIVS; i++) {
      TSentryPrivilege priv = new TSentryPrivilege();
      priv.setPrivilegeScope("DATABASE");
      priv.setServerName(server);
      priv.setAction(AccessConstants.ALTER);
      priv.setCreateTime(System.currentTimeMillis());
      priv.setTableName(table + i);
      priv.setDbName(dBase);
      priv.setGrantOption(TSentryGrantOption.TRUE);
      sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, priv);

      priv.setAction(AccessConstants.ALTER);
      sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, priv);

      MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
      assertEquals("Privilege Count", 0, role.getPrivileges().size());
    }
    //There should not be any Privileges left
    List<MSentryPrivilege> list = sentryStore.getAllMSentryPrivileges();
    assertEquals(list.size(), 0);

  }

  //TODO Use new transaction Manager logic, Instead of

  @Test
  public void testGrantRevokeMultiPrivileges() throws Exception {
    String roleName = "test-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String[] columns = {"c1","c2","c3","c4"};
    createRole(roleName);
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
    sentryStore.alterSentryRoleGrantPrivileges(grantor, roleName, tPrivileges);
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
    sentryStore.alterSentryRoleRevokePrivileges(grantor, roleName, tPrivileges);
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
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
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
    createRole(roleName);
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("COLUMN");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setColumnName(column1);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());

    // Grant ALL on c1 and c2
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    privilege.setColumnName(column2);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // Revoke SELECT on c2
    privilege.setAction(AccessConstants.SELECT);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);

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
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    assertEquals(column1, Iterables.get(privileges, 0).getColumnName());
    assertEquals(AccessConstants.SELECT, Iterables.get(privileges, 0).getAction());

    // Revoke ALL from the table should now remove all the column privileges.
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
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
    createRole(roleName);
    TSentryPrivilege privilegeTable1 = new TSentryPrivilege();
    privilegeTable1.setPrivilegeScope("TABLE");
    privilegeTable1.setServerName(server);
    privilegeTable1.setDbName(db);
    privilegeTable1.setTableName(table1);
    privilegeTable1.setAction(AccessConstants.ALL);
    privilegeTable1.setCreateTime(System.currentTimeMillis());
    TSentryPrivilege privilegeTable2 = privilegeTable1.deepCopy();
    privilegeTable2.setTableName(table2);

    // Grant ALL on table1 and table2
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeTable1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeTable2);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // Revoke SELECT on table2
    privilegeTable2.setAction(AccessConstants.SELECT);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeTable2);
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
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeTable2);
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
    createRole(roleName);
    TSentryPrivilege privilegeCol1 = new TSentryPrivilege();
    privilegeCol1.setPrivilegeScope("COLUMN");
    privilegeCol1.setServerName(server);
    privilegeCol1.setDbName(db);
    privilegeCol1.setTableName(table);
    privilegeCol1.setColumnName(column1);
    privilegeCol1.setAction(AccessConstants.ALL);
    privilegeCol1.setCreateTime(System.currentTimeMillis());
    TSentryPrivilege privilegeCol2 = privilegeCol1.deepCopy();
    privilegeCol2.setColumnName(column2);

    // Grant ALL on column1 and column2
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeCol1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilegeCol2);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 2, privileges.size());

    // Revoke SELECT on column2
    privilegeCol2.setAction(AccessConstants.SELECT);
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeCol2);
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
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilegeCol2);
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
    createRole(roleName);

    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(grantOption);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
    assertEquals(Boolean.valueOf(privilege.getGrantOption().toString()), Iterables.get(privileges, 0).getGrantOption());
    sentryStore.alterSentryRoleRevokePrivilege(grantor, roleName, privilege);
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(0, privileges.size());

    roleName = "test-grantOption-db";

    createRole(roleName);
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
    createRole(roleName);
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("TABLE");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    privilege.setServerName("Server1");
    privilege.setDbName("DB1");
    privilege.setTableName("TBL1");
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName, privilege);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());
  }

  @Test
  public void testListSentryPrivilegesForProvider() throws Exception {
    String roleName1 = "list-privs-r1", roleName2 = "list-privs-r2";
    String groupName1 = "list-privs-g1", groupName2 = "list-privs-g2";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);
    TSentryPrivilege privilege1 = new TSentryPrivilege();
    privilege1.setPrivilegeScope("TABLE");
    privilege1.setServerName("server1");
    privilege1.setDbName("db1");
    privilege1.setTableName("tbl1");
    privilege1.setAction("SELECT");
    privilege1.setCreateTime(System.currentTimeMillis());
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege1);
    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("SERVER");
    privilege2.setServerName("server1");
    privilege2.setCreateTime(System.currentTimeMillis());
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege2);
    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName(groupName1);
    groups.add(group);
    sentryStore.alterSentryRoleAddGroups(grantor, roleName1, groups);
    groups.clear();
    group = new TSentryGroup();
    group.setGroupName(groupName2);
    groups.add(group);
    // group 2 has both roles 1 and 2
    sentryStore.alterSentryRoleAddGroups(grantor, roleName1, groups);
    sentryStore.alterSentryRoleAddGroups(grantor, roleName2, groups);
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

  @Test
  public void testSentryVersionCheck() throws Exception {
    // don't verify version, the current version willll be set in MSentryVersion
    sentryStore.verifySentryStoreSchema(false);
    assertEquals(sentryStore.getSentryVersion(),
        SentryStoreSchemaInfo.getSentryVersion());

    // verify the version with the same value
    sentryStore.verifySentryStoreSchema(true);

    // verify the version with the different value
    sentryStore.setSentryVersion("test-version", "test-version");
    try {
      sentryStore.verifySentryStoreSchema(true);
      fail("SentryAccessDeniedException should be thrown.");
    } catch (SentryAccessDeniedException e) {
      // the excepted exception, recover the version
      sentryStore.verifySentryStoreSchema(false);
    }
  }

  @Test
  public void testRetrieveFullPermssionsImage() throws Exception {

    // Create roles
    String roleName1 = "privs-r1", roleName2 = "privs-r2";
    String groupName1 = "privs-g1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);

    // Grant Privileges to the roles
    TSentryPrivilege privilege1 = new TSentryPrivilege();
    privilege1.setPrivilegeScope("TABLE");
    privilege1.setServerName("server1");
    privilege1.setDbName("db1");
    privilege1.setTableName("tbl1");
    privilege1.setAction("SELECT");
    privilege1.setCreateTime(System.currentTimeMillis());
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege1);
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege1);

    TSentryPrivilege privilege2 = new TSentryPrivilege();
    privilege2.setPrivilegeScope("SERVER");
    privilege2.setServerName("server1");
    privilege1.setDbName("db2");
    privilege1.setAction("ALL");
    privilege2.setCreateTime(System.currentTimeMillis());
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName2, privilege2);

    // Grant roles to the groups
    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName(groupName1);
    groups.add(group);
    sentryStore.alterSentryRoleAddGroups(grantor, roleName1, groups);
    sentryStore.alterSentryRoleAddGroups(grantor, roleName2, groups);

    PermissionsImage permImage = sentryStore.retrieveFullPermssionsImage();
    Map<String, Map<String, String>> privs = permImage.getPrivilegeImage();
    Map<String, List<String>> roles = permImage.getRoleImage();
    assertEquals(2, privs.get("db1.tbl1").size());
    assertEquals(2, roles.size());
  }

  /**
   * Verifies complete snapshot of HMS Paths can be persisted and retrieved properly.
   */
  @Test
  public void testPersistFullPathsImage() throws Exception {
    Map<String, Set<String>> authzPaths = new HashMap<>();
    // Makes sure that authorizable object could be associated
    // with different paths and can be properly persisted into database.
    authzPaths.put("db1.table1", Sets.newHashSet("/user/hive/warehouse/db2.db/table1.1",
                                                "/user/hive/warehouse/db2.db/table1.2"));
    // Makes sure that the same MPaths object could be associated
    // with multiple authorizable and can be properly persisted into database.
    authzPaths.put("db1.table2", Sets.newHashSet("/user/hive/warehouse/db2.db/table2.1",
                                                "/user/hive/warehouse/db2.db/table2.2"));
    authzPaths.put("db2.table2", Sets.newHashSet("/user/hive/warehouse/db2.db/table2.1",
                                                "/user/hive/warehouse/db2.db/table2.3"));
    sentryStore.persistFullPathsImage(authzPaths);
    PathsImage pathsImage = sentryStore.retrieveFullPathsImage();
    Map<String, Set<String>> pathImage = pathsImage.getPathImage();
    assertEquals(3, pathImage.size());
    for (Map.Entry<String, Set<String>> entry : pathImage.entrySet()) {
      assertEquals(2, entry.getValue().size());
    }
    assertEquals(2, pathImage.get("db2.table2").size());
    assertEquals(Sets.newHashSet("/user/hive/warehouse/db2.db/table1.1",
                                "/user/hive/warehouse/db2.db/table1.2"),
                                pathImage.get("db1.table1"));
    assertEquals(Sets.newHashSet("/user/hive/warehouse/db2.db/table2.1",
                                "/user/hive/warehouse/db2.db/table2.2"),
                                pathImage.get("db1.table2"));
    assertEquals(Sets.newHashSet("/user/hive/warehouse/db2.db/table2.1",
                                "/user/hive/warehouse/db2.db/table2.3"),
                                pathImage.get("db2.table2"));
    assertEquals(6, sentryStore.getMPaths().size());
  }

  @Test
  public void testAddDeleteAuthzPathsMapping() throws Exception {
    // Add "db1.table1" authzObj
    Long lastNotificationId = sentryStore.getLastProcessedNotificationID();
    PathsUpdate addUpdate = new PathsUpdate(1, false);
    addUpdate.newPathChange("db1.table").
          addToAddPaths(Arrays.asList("db1", "tbl1"));
    addUpdate.newPathChange("db1.table").
          addToAddPaths(Arrays.asList("db1", "tbl2"));

    sentryStore.addAuthzPathsMapping("db1.table",
          Sets.newHashSet("db1/tbl1", "db1/tbl2"), addUpdate);
    PathsImage pathsImage = sentryStore.retrieveFullPathsImage();
    Map<String, Set<String>> pathImage = pathsImage.getPathImage();
    assertEquals(1, pathImage.size());
    assertEquals(2, pathImage.get("db1.table").size());
    assertEquals(2, sentryStore.getMPaths().size());

    // Query the persisted path change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPathChangeID();
    MSentryPathChange addPathChange = sentryStore.getMSentryPathChangeByID(lastChangeID);
    assertEquals(addUpdate.JSONSerialize(), addPathChange.getPathChange());
    lastNotificationId = sentryStore.getLastProcessedNotificationID();
    assertEquals(1, lastNotificationId.longValue());

    // Delete path 'db1.db/tbl1' from "db1.table1" authzObj.
    PathsUpdate delUpdate = new PathsUpdate(2, false);
    delUpdate.newPathChange("db1.table")
          .addToDelPaths(Arrays.asList("db1", "tbl1"));
    sentryStore.deleteAuthzPathsMapping("db1.table", Sets.newHashSet("db1/tbl1"), delUpdate);
    pathImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(1, pathImage.size());
    assertEquals(1, pathImage.get("db1.table").size());
    assertEquals(1, sentryStore.getMPaths().size());

    // Query the persisted path change and ensure it equals to the original one
    lastChangeID = sentryStore.getLastProcessedPathChangeID();
    MSentryPathChange delPathChange = sentryStore.getMSentryPathChangeByID(lastChangeID);
    assertEquals(delUpdate.JSONSerialize(), delPathChange.getPathChange());
    lastNotificationId = sentryStore.getLastProcessedNotificationID();
    assertEquals(2, lastNotificationId.longValue());

    // Delete "db1.table" authzObj from the authzObj -> [Paths] mapping.
    PathsUpdate delAllupdate = new PathsUpdate(3, false);
    delAllupdate.newPathChange("db1.table")
        .addToDelPaths(Lists.newArrayList(PathsUpdate.ALL_PATHS));
    sentryStore.deleteAllAuthzPathsMapping("db1.table", delAllupdate);
    pathImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(0, pathImage.size());
    assertEquals(0, sentryStore.getMPaths().size());

    // Query the persisted path change and ensure it equals to the original one
    lastChangeID = sentryStore.getLastProcessedPathChangeID();
    MSentryPathChange delAllPathChange = sentryStore.getMSentryPathChangeByID(lastChangeID);
    assertEquals(delAllupdate.JSONSerialize(), delAllPathChange.getPathChange());

    lastNotificationId = sentryStore.getLastProcessedNotificationID();
    assertEquals(3, lastNotificationId.longValue());

  }

  @Test
  public void testRenameUpdateAuthzPathsMapping() throws Exception {
    Map<String, Set<String>> authzPaths = new HashMap<>();
    Long lastNotificationId = sentryStore.getLastProcessedNotificationID();
    authzPaths.put("db1.table1", Sets.newHashSet("user/hive/warehouse/db1.db/table1",
                                                "user/hive/warehouse/db1.db/table1/p1"));
    authzPaths.put("db1.table2", Sets.newHashSet("user/hive/warehouse/db1.db/table2"));
    sentryStore.persistFullPathsImage(authzPaths);
    Map<String, Set<String>> pathsImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(2, pathsImage.size());


    // Rename path of 'db1.table1' from 'db1.table1' to 'db1.newTable1'
    PathsUpdate renameUpdate = new PathsUpdate(1, false);
    renameUpdate.newPathChange("db1.table1")
        .addToDelPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "table1"));
    renameUpdate.newPathChange("db1.newTable1")
        .addToAddPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "newTable1"));
    sentryStore.renameAuthzPathsMapping("db1.table1", "db1.newTable1",
    "user/hive/warehouse/db1.db/table1", "user/hive/warehouse/db1.db/newTable1", renameUpdate);
    pathsImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(2, pathsImage.size());
    assertEquals(3, sentryStore.getMPaths().size());
    assertTrue(pathsImage.containsKey("db1.newTable1"));
    assertEquals(Sets.newHashSet("user/hive/warehouse/db1.db/table1/p1",
                                "user/hive/warehouse/db1.db/newTable1"),
                  pathsImage.get("db1.newTable1"));

    // Query the persisted path change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPathChangeID();
    MSentryPathChange renamePathChange = sentryStore.getMSentryPathChangeByID(lastChangeID);
    assertEquals(renameUpdate.JSONSerialize(), renamePathChange.getPathChange());
    lastNotificationId = sentryStore.getLastProcessedNotificationID();
    assertEquals(1, lastNotificationId.longValue());
    // Rename 'db1.table1' to "db1.table2" but did not change its location.
    renameUpdate = new PathsUpdate(2, false);
    renameUpdate.newPathChange("db1.newTable1")
        .addToDelPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "newTable1"));
    renameUpdate.newPathChange("db1.newTable2")
        .addToAddPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "newTable1"));
    sentryStore.renameAuthzObj("db1.newTable1", "db1.newTable2", renameUpdate);
    pathsImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(2, pathsImage.size());
    assertEquals(3, sentryStore.getMPaths().size());
    assertTrue(pathsImage.containsKey("db1.newTable2"));
    assertEquals(Sets.newHashSet("user/hive/warehouse/db1.db/table1/p1",
                                "user/hive/warehouse/db1.db/newTable1"),
                  pathsImage.get("db1.newTable2"));
    lastNotificationId = sentryStore.getLastProcessedNotificationID();
    assertEquals(2, lastNotificationId.longValue());

    // Query the persisted path change and ensure it equals to the original one
    lastChangeID = sentryStore.getLastProcessedPathChangeID();
    renamePathChange = sentryStore.getMSentryPathChangeByID(lastChangeID);
    assertEquals(renameUpdate.JSONSerialize(), renamePathChange.getPathChange());

    // Update path of 'db1.newTable2' from 'db1.newTable1' to 'db1.newTable2'
    PathsUpdate update = new PathsUpdate(3, false);
    update.newPathChange("db1.newTable1")
        .addToDelPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "newTable1"));
    update.newPathChange("db1.newTable1")
        .addToAddPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "newTable2"));
    sentryStore.updateAuthzPathsMapping("db1.newTable2",
            "user/hive/warehouse/db1.db/newTable1",
            "user/hive/warehouse/db1.db/newTable2",
            update);
    pathsImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(2, pathsImage.size());
    assertEquals(3, sentryStore.getMPaths().size());
    assertTrue(pathsImage.containsKey("db1.newTable2"));
    assertEquals(Sets.newHashSet("user/hive/warehouse/db1.db/table1/p1",
                                "user/hive/warehouse/db1.db/newTable2"),
                  pathsImage.get("db1.newTable2"));

    // Query the persisted path change and ensure it equals to the original one
    lastChangeID = sentryStore.getLastProcessedPathChangeID();
    MSentryPathChange updatePathChange = sentryStore.getMSentryPathChangeByID(lastChangeID);
    assertEquals(update.JSONSerialize(), updatePathChange.getPathChange());
    lastNotificationId = sentryStore.getLastProcessedNotificationID();
    assertEquals(3, lastNotificationId.longValue());
  }

  @Test
  public void testQueryParamBuilder() {
    QueryParamBuilder paramBuilder;
    paramBuilder = newQueryParamBuilder();
    // Try single parameter
    paramBuilder.add("key", "val");
    assertEquals("(this.key == :key)", paramBuilder.toString());
    // Try adding second parameter plus add trimming and conversion
    // to lower case
    paramBuilder.add("key1", " Val1 ", true);
    assertEquals("(this.key == :key && this.key1 == :key1)",
            paramBuilder.toString());
    Map<String, Object> params = paramBuilder.getArguments();
    assertEquals("val", params.get("key"));
    assertEquals("Val1", params.get("key1"));

    paramBuilder = newQueryParamBuilder(QueryParamBuilder.Op.OR);
    paramBuilder.add("key", " Val ", true);
    paramBuilder.addNotNull("notNullField");
    paramBuilder.addNull("nullField");
    assertEquals("(this.key == :key || this.notNullField != \"__NULL__\" || this.nullField == \"__NULL__\")",
            paramBuilder.toString());
    params = paramBuilder.getArguments();
    assertEquals("Val", params.get("key"));

    paramBuilder = newQueryParamBuilder()
            .addNull("var1")
            .addNotNull("var2");
    assertEquals("(this.var1 == \"__NULL__\" && this.var2 != \"__NULL__\")",
            paramBuilder.toString());

    // Test newChild()
    paramBuilder = newQueryParamBuilder();
    paramBuilder
            .addString("e1")
            .addString("e2")
            .newChild()
            .add("v3", "e3")
            .add("v4", "e4")
            .newChild()
            .addString("e5")
            .addString("e6")
    ;
    assertEquals("(e1 && e2 && (this.v3 == :v3 || this.v4 == :v4 || (e5 && e6)))",
            paramBuilder.toString());

    params = paramBuilder.getArguments();
    assertEquals("e3", params.get("v3"));
    assertEquals("e4", params.get("v4"));

    // Test addSet
    paramBuilder = newQueryParamBuilder();
    Set<String>names = new HashSet<>();
    names.add("foo");
    names.add("bar");
    names.add("bob");

    paramBuilder.addSet("prefix == ", names);
    assertEquals("(prefix == :var0 && prefix == :var1 && prefix == :var2)",
            paramBuilder.toString());
    params = paramBuilder.getArguments();

    Set<String>result = new HashSet<>();
    result.add((String)params.get("var0"));
    result.add((String)params.get("var1"));
    result.add((String)params.get("var2"));
    assertTrue(result.containsAll(names));
    assertTrue(names.containsAll(result));
  }

  @Test
  public void testPrivilegesWithPermUpdate() throws Exception {
    String roleName = "test-privilege";
    String grantor = "g1";
    String server = "server1";
    String db = "db1";
    String table = "tbl1";
    String authzObj = "db1.tbl1";
    createRole(roleName);

    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("Column");
    privilege.setServerName(server);
    privilege.setDbName(db);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.SELECT);
    privilege.setCreateTime(System.currentTimeMillis());

    // Generate the permission add update authzObj "db1.tbl1"
    PermissionsUpdate addUpdate = new PermissionsUpdate(0, false);
    addUpdate.addPrivilegeUpdate(authzObj).putToAddPrivileges(roleName, privilege.getAction().toUpperCase());

    // Grant the privilege to role test-privilege and verify it has been persisted.
    Map<TSentryPrivilege, Update> addPrivilegesUpdateMap = Maps.newHashMap();
    addPrivilegesUpdateMap.put(privilege, addUpdate);
    sentryStore.alterSentryRoleGrantPrivileges(grantor, roleName, Sets.newHashSet(privilege), addPrivilegesUpdateMap);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    Set<MSentryPrivilege> privileges = role.getPrivileges();
    assertEquals(privileges.toString(), 1, privileges.size());

    // Query the persisted perm change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPermChangeID();
    long initialID = lastChangeID;
    MSentryPermChange addPermChange = sentryStore.getMSentryPermChangeByID(lastChangeID);
    assertEquals(addUpdate.JSONSerialize(), addPermChange.getPermChange());

    // Generate the permission delete update authzObj "db1.tbl1"
    PermissionsUpdate delUpdate = new PermissionsUpdate(0, false);
    delUpdate.addPrivilegeUpdate(authzObj).putToDelPrivileges(roleName, privilege.getAction().toUpperCase());

    // Revoke the same privilege and verify it has been removed.
    Map<TSentryPrivilege, Update> delPrivilegesUpdateMap = Maps.newHashMap();
    delPrivilegesUpdateMap.put(privilege, delUpdate);
    sentryStore.alterSentryRoleRevokePrivileges(grantor, roleName, Sets.newHashSet(privilege), delPrivilegesUpdateMap);
    role = sentryStore.getMSentryRoleByName(roleName);
    privileges = role.getPrivileges();
    assertEquals(0, privileges.size());

    // Query the persisted perm change and ensure it equals to the original one
    lastChangeID = sentryStore.getLastProcessedPermChangeID();
    MSentryPermChange delPermChange = sentryStore.getMSentryPermChangeByID(lastChangeID);
    assertEquals(delUpdate.JSONSerialize(), delPermChange.getPermChange());

    // Verify getMSentryPermChanges will return all MSentryPermChanges up
    // to the given changeID.
    List<MSentryPermChange> mSentryPermChanges = sentryStore.getMSentryPermChanges(initialID);
    assertEquals(lastChangeID - initialID + 1, mSentryPermChanges.size());

    // Verify ifPermChangeExists will return true for persisted MSentryPermChange.
    assertTrue(sentryStore.permChangeExists(1));
  }

  @Test
  public void testAddDeleteGroupsWithPermUpdate() throws Exception {
    String roleName = "test-groups";
    String grantor = "g1";
    createRole(roleName);

    Set<TSentryGroup> groups = Sets.newHashSet();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName("test-groups-g1");
    groups.add(group);
    group = new TSentryGroup();
    group.setGroupName("test-groups-g2");
    groups.add(group);

    // Generate the permission add update for role "test-groups"
    PermissionsUpdate addUpdate = new PermissionsUpdate(0, false);
    TRoleChanges addrUpdate = addUpdate.addRoleUpdate(roleName);
    for (TSentryGroup g : groups) {
      addrUpdate.addToAddGroups(g.getGroupName());
    }

    // Assign the role "test-groups" to the groups and verify.
    sentryStore.alterSentryRoleAddGroups(grantor, roleName, groups, addUpdate);
    MSentryRole role = sentryStore.getMSentryRoleByName(roleName);
    assertEquals(2, role.getGroups().size());

    // Query the persisted perm change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPermChangeID();
    MSentryPermChange addPermChange = sentryStore.getMSentryPermChangeByID(lastChangeID);
    assertEquals(addUpdate.JSONSerialize(), addPermChange.getPermChange());

    // Generate the permission add update for role "test-groups"
    PermissionsUpdate delUpdate = new PermissionsUpdate(0, false);
    TRoleChanges delrUpdate = delUpdate.addRoleUpdate(roleName);
    for (TSentryGroup g : groups) {
      delrUpdate.addToDelGroups(g.getGroupName());
    }

    // Revoke the role "test-groups" to the groups and verify.
    sentryStore.alterSentryRoleDeleteGroups(roleName, groups, delUpdate);
    role = sentryStore.getMSentryRoleByName(roleName);
    assertEquals(Collections.emptySet(), role.getGroups());

    // Query the persisted perm change and ensure it equals to the original one
    MSentryPermChange delPermChange = sentryStore.getMSentryPermChangeByID(lastChangeID + 1);
    assertEquals(delUpdate.JSONSerialize(), delPermChange.getPermChange());
  }

  @Test
  public void testCreateDropRoleWithPermUpdate() throws Exception {
    String roleName = "test-drop-role";
    createRole(roleName);

    // Generate the permission del update for dropping role "test-drop-role"
    PermissionsUpdate delUpdate = new PermissionsUpdate(0, false);
    delUpdate.addPrivilegeUpdate(PermissionsUpdate.ALL_AUTHZ_OBJ).putToDelPrivileges(roleName, PermissionsUpdate.ALL_AUTHZ_OBJ);
    delUpdate.addRoleUpdate(roleName).addToDelGroups(PermissionsUpdate.ALL_GROUPS);

    // Drop the role and verify.
    sentryStore.dropSentryRole(roleName, delUpdate);
    checkRoleDoesNotExist(roleName);

    // Query the persisted perm change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPermChangeID();
    MSentryPermChange delPermChange = sentryStore.getMSentryPermChangeByID(lastChangeID);
    assertEquals(delUpdate.JSONSerialize(), delPermChange.getPermChange());
  }

  @Test
  public void testDropObjWithPermUpdate() throws Exception {
    String roleName1 = "list-privs-r1", roleName2 = "list-privs-r2";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);
    sentryStore.createSentryRole(roleName2);

    String authzObj = "db1.tbl1";
    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName("tbl1");
    privilege_tbl1.setCreateTime(System.currentTimeMillis());
    privilege_tbl1.setAction("SELECT");

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1);

    // Generate the permission drop update for dropping privilege for "db1.tbl1"
    PermissionsUpdate dropUpdate = new PermissionsUpdate(0, false);
    dropUpdate.addPrivilegeUpdate(authzObj).putToDelPrivileges(PermissionsUpdate.ALL_ROLES, PermissionsUpdate.ALL_ROLES);

    // Drop the privilege and verify.
    sentryStore.dropPrivilege(toTSentryAuthorizable(privilege_tbl1), dropUpdate);
    assertEquals(0, sentryStore.getAllTSentryPrivilegesByRoleName(roleName1).size());
    assertEquals(0, sentryStore.getAllTSentryPrivilegesByRoleName(roleName2).size());

    // Query the persisted perm change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPermChangeID();
    MSentryPermChange dropPermChange = sentryStore.getMSentryPermChangeByID(lastChangeID);
    assertEquals(dropUpdate.JSONSerialize(), dropPermChange.getPermChange());
  }

  @Test
  public void testRenameObjWithPermUpdate() throws Exception {
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
    privilege_tbl1.setAction(AccessConstants.ALL);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1);

    // Generate the permission rename update for renaming privilege for "db1.tbl1"
    String oldAuthz = "db1.tbl1";
    String newAuthz = "db1.tbl2";
    PermissionsUpdate renameUpdate = new PermissionsUpdate(0, false);
    TPrivilegeChanges privUpdate = renameUpdate.addPrivilegeUpdate(PermissionsUpdate.RENAME_PRIVS);
    privUpdate.putToAddPrivileges(newAuthz, newAuthz);
    privUpdate.putToDelPrivileges(oldAuthz, oldAuthz);

    // Rename the privilege and verify.
    TSentryAuthorizable oldTable = toTSentryAuthorizable(privilege_tbl1);
    TSentryAuthorizable newTable = toTSentryAuthorizable(privilege_tbl1);
    newTable.setTable(table2);
    sentryStore.renamePrivilege(oldTable, newTable, renameUpdate);

    Set<TSentryPrivilege> privilegeSet = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(1, privilegeSet.size());
    for (TSentryPrivilege privilege : privilegeSet) {
      assertTrue(table2.equalsIgnoreCase(privilege.getTableName()));
    }

    // Query the persisted perm change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPermChangeID();
    MSentryPermChange renamePermChange = sentryStore.getMSentryPermChangeByID(lastChangeID);
    assertEquals(renameUpdate.JSONSerialize(), renamePermChange.getPermChange());
  }

  protected static void addGroupsToUser(String user, String... groupNames) {
    policyFile.addGroupsToUser(user, groupNames);
  }

  protected static void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }

  @Test
  public void testPurgeDeltaChanges() throws Exception {
    String role = "purgeRole";
    String grantor = "g1";
    String table = "purgeTable";

    assertEquals(0, sentryStore.getMSentryPermChanges().size());
    assertEquals(0, sentryStore.getMSentryPathChanges().size());

    sentryStore.createSentryRole(role);
    int privCleanCount = ServerConfig.SENTRY_DELTA_KEEP_COUNT_DEFAULT;
    int extraPrivs = 5;

    final int numPermChanges = extraPrivs + privCleanCount;
    for (int i = 0; i < numPermChanges; i++) {
      TSentryPrivilege privilege = new TSentryPrivilege();
      privilege.setPrivilegeScope("Column");
      privilege.setServerName("server");
      privilege.setDbName("db");
      privilege.setTableName(table);
      privilege.setColumnName("column");
      privilege.setAction(AccessConstants.SELECT);
      privilege.setCreateTime(System.currentTimeMillis());

      PermissionsUpdate update = new PermissionsUpdate(i + 1, false);
      sentryStore.alterSentryRoleGrantPrivilege(grantor, role, privilege, update);
    }
    assertEquals(numPermChanges, sentryStore.getMSentryPermChanges().size());

    sentryStore.purgeDeltaChangeTables();
    assertEquals(privCleanCount, sentryStore.getMSentryPermChanges().size());

    // TODO: verify MSentryPathChange being purged.
    // assertEquals(1, sentryStore.getMSentryPathChanges().size());
  }

  /**
   * This test verifies that in the case of concurrently updating delta change tables, no gap
   * between change ID was made. All the change IDs must be consecutive ({@see SENTRY-1643}).
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testConcurrentUpdateChanges() throws Exception {
    final int numThreads = 20;
    final int numChangesPerThread = 100;
    final TransactionManager tm = sentryStore.getTransactionManager();
    final AtomicLong seqNumGenerator = new AtomicLong(0);
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
          } catch (Exception e) {
            LOGGER.error("Barrier failed to await", e);
            return;
          }
          for (int j = 0; j < numChangesPerThread; j++) {
            List<TransactionBlock<Object>> tbs = new ArrayList<>();
            PermissionsUpdate update =
                new PermissionsUpdate(seqNumGenerator.getAndIncrement(), false);
            tbs.add(new DeltaTransactionBlock(update));
            try {
              tm.executeTransactionBlocksWithRetry(tbs);
            } catch (Exception e) {
              LOGGER.error("Failed to execute permission update transaction", e);
              fail(String.format("Transaction failed: %s", e.getMessage()));
            }
          }
        }
      });
    }
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);

    List<MSentryPermChange> changes = sentryStore.getMSentryPermChanges();
    int actualSize = changes.size();
    if (actualSize != (numThreads * numChangesPerThread)) {
      LOGGER.warn("Detected {} dropped changes", ((numChangesPerThread * numThreads) - actualSize));
    }
    TreeSet<Long> changeIDs = new TreeSet<>();
    for (MSentryPermChange change : changes) {
      changeIDs.add(change.getChangeID());
    }
    assertEquals("duplicated change ID", actualSize, changeIDs.size());

    long prevId = changeIDs.first() - 1;
    for (Long changeId : changeIDs) {
      assertTrue(String.format("Found non-consecutive number: prev=%d cur=%d", prevId, changeId),
          changeId - prevId == 1);
      prevId = changeId;
    }
  }

  @Test
  public void testDuplicateNotification() throws Exception {
    Map<String, Set<String>> authzPaths = new HashMap<>();
    Long lastNotificationId = sentryStore.getLastProcessedNotificationID();
    authzPaths.put("db1.table1", Sets.newHashSet("user/hive/warehouse/db1.db/table1",
      "user/hive/warehouse/db1.db/table1/p1"));
    authzPaths.put("db1.table2", Sets.newHashSet("user/hive/warehouse/db1.db/table2"));
    sentryStore.persistFullPathsImage(authzPaths);
    Map<String, Set<String>> pathsImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(2, pathsImage.size());

    if (lastNotificationId == null) {
      lastNotificationId = SentryStore.EMPTY_NOTIFICATION_ID;
    }

    // Rename path of 'db1.table1' from 'db1.table1' to 'db1.newTable1'
    PathsUpdate renameUpdate = new PathsUpdate(1, false);
    renameUpdate.newPathChange("db1.table1")
      .addToDelPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "table1"));
    renameUpdate.newPathChange("db1.newTable1")
      .addToAddPaths(Arrays.asList("user", "hive", "warehouse", "db1.db", "newTable1"));
    sentryStore.renameAuthzPathsMapping("db1.table1", "db1.newTable1",
      "user/hive/warehouse/db1.db/table1", "user/hive/warehouse/db1.db/newTable1", renameUpdate);
    pathsImage = sentryStore.retrieveFullPathsImage().getPathImage();
    assertEquals(2, pathsImage.size());
    assertEquals(3, sentryStore.getMPaths().size());
    assertTrue(pathsImage.containsKey("db1.newTable1"));
    assertEquals(Sets.newHashSet("user/hive/warehouse/db1.db/table1/p1",
      "user/hive/warehouse/db1.db/newTable1"),
      pathsImage.get("db1.newTable1"));

    // Query the persisted path change and ensure it equals to the original one
    long lastChangeID = sentryStore.getLastProcessedPathChangeID();
    MSentryPathChange renamePathChange = sentryStore.getMSentryPathChangeByID(lastChangeID);
    assertEquals(renameUpdate.JSONSerialize(), renamePathChange.getPathChange());
    lastNotificationId = sentryStore.getLastProcessedNotificationID();
    assertEquals(1, lastNotificationId.longValue());


    // Process the notificaiton second time
    try {
      sentryStore.renameAuthzPathsMapping("db1.table1", "db1.newTable1",
        "user/hive/warehouse/db1.db/table1", "user/hive/warehouse/db1.db/newTable1", renameUpdate);
    } catch (Exception e) {
      if (!(e.getCause() instanceof JDODataStoreException)) {
        fail("Unexpected failure occured while processing duplicate notification");
      }
    }
  }

  @Test
  public void testIsAuthzPathsMappingEmpty() throws Exception {
    // Add "db1.table1" authzObj
    Long lastNotificationId = sentryStore.getLastProcessedNotificationID();
    PathsUpdate addUpdate = new PathsUpdate(1, false);
    addUpdate.newPathChange("db1.table").
      addToAddPaths(Arrays.asList("db1", "tbl1"));
    addUpdate.newPathChange("db1.table").
      addToAddPaths(Arrays.asList("db1", "tbl2"));

    assertEquals(sentryStore.isAuthzPathsMappingEmpty(), true);
    sentryStore.addAuthzPathsMapping("db1.table",
      Sets.newHashSet("db1/tbl1", "db1/tbl2"), addUpdate);
    PathsImage pathsImage = sentryStore.retrieveFullPathsImage();
    Map<String, Set<String>> pathImage = pathsImage.getPathImage();
    assertEquals(1, pathImage.size());
    assertEquals(2, pathImage.get("db1.table").size());
    assertEquals(2, sentryStore.getMPaths().size());
    assertEquals(sentryStore.isAuthzPathsMappingEmpty(), false);
    sentryStore.clearAllTables();
    assertEquals(sentryStore.isAuthzPathsMappingEmpty(), true);
  }
}
