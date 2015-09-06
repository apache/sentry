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
 * Unless createRequired by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.thrift;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class TestSentryServiceIntegration extends SentryServiceIntegrationBase {

  @Test
  public void testCreateDropShowRole() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_r";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName);

        client.createRole(requestorUserName, roleName);

        Set<TSentryRole> roles = client.listRoles(requestorUserName);
        assertEquals("Incorrect number of roles", 1, roles.size());

        for (TSentryRole role:roles) {
          assertTrue(role.getRoleName(), role.getRoleName().equalsIgnoreCase(roleName));
        }
        client.dropRole(requestorUserName, roleName);
      }});
  }

  @Test
  public void testGranRevokePrivilegeOnTableForRole() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName1 = "admin_r1";
        String roleName2 = "admin_r2";

        client.dropRoleIfExists(requestorUserName,  roleName1);
        client.createRole(requestorUserName,  roleName1);

        client.grantTablePrivilege(requestorUserName, roleName1, "server", "db1", "table1", "ALL");
        client.grantTablePrivilege(requestorUserName, roleName1, "server", "db1", "table2", "ALL");
        client.grantTablePrivilege(requestorUserName, roleName1, "server", "db2", "table3", "ALL");
        client.grantTablePrivilege(requestorUserName, roleName1, "server", "db2", "table4", "ALL");


        client.dropRoleIfExists(requestorUserName,  roleName2);
        client.createRole(requestorUserName,  roleName2);

        client.grantTablePrivilege(requestorUserName, roleName2, "server", "db1", "table1", "ALL");
        client.grantTablePrivilege(requestorUserName, roleName2, "server", "db1", "table2", "ALL");
        client.grantTablePrivilege(requestorUserName, roleName2, "server", "db2", "table3", "ALL");
        client.grantTablePrivilege(requestorUserName, roleName2, "server", "db2", "table4", "ALL");

        Set<TSentryPrivilege> listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertEquals("Privilege not assigned to role1 !!", 4, listPrivilegesByRoleName.size());

        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertEquals("Privilege not assigned to role2 !!", 4, listPrivilegesByRoleName.size());


        client.revokeTablePrivilege(requestorUserName, roleName1, "server", "db1", "table1", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 3);
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 4);

        client.revokeTablePrivilege(requestorUserName, roleName2, "server", "db1", "table1", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 3);
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 3);

        client.revokeTablePrivilege(requestorUserName, roleName1, "server", "db1", "table2", "ALL");
        client.revokeTablePrivilege(requestorUserName, roleName1, "server", "db2", "table3", "ALL");
        client.revokeTablePrivilege(requestorUserName, roleName1, "server", "db2", "table4", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 0);

        client.revokeTablePrivilege(requestorUserName, roleName2, "server", "db1", "table2", "ALL");
        client.revokeTablePrivilege(requestorUserName, roleName2, "server", "db2", "table3", "ALL");
        client.revokeTablePrivilege(requestorUserName, roleName2, "server", "db2", "table4", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 0);
      }});
  }

  @Test
  public void testMultipleRolesSamePrivilege() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName1 = "admin_r1";
        String roleName2 = "admin_r2";

        client.dropRoleIfExists(requestorUserName, roleName1);
        client.createRole(requestorUserName,  roleName1);

        client.dropRoleIfExists(requestorUserName,  roleName2);
        client.createRole(requestorUserName,  roleName2);

        client.grantTablePrivilege(requestorUserName, roleName1, "server", "db", "table", "ALL");
        Set<TSentryPrivilege> listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not assigned to role1 !!", listPrivilegesByRoleName.size() == 1);

        client.grantTablePrivilege(requestorUserName, roleName2, "server", "db", "table", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertTrue("Privilege not assigned to role2 !!", listPrivilegesByRoleName.size() == 1);
      }});
  }

  @Test
  public void testShowRoleGrant() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_testdb";
        String groupName = "group1";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName);
        client.createRole(requestorUserName, roleName);

        Set<TSentryRole> roles = client.listRoles(requestorUserName);
        assertEquals("Incorrect number of roles", 1, roles.size());

        client.grantRoleToGroup(requestorUserName, groupName, roleName);
        Set<TSentryRole> groupRoles = client.listRolesByGroupName(requestorUserName, groupName);
        assertTrue(groupRoles.size() == 1);
        for (TSentryRole role:groupRoles) {
          assertTrue(role.getRoleName(), role.getRoleName().equalsIgnoreCase(roleName));
          assertTrue(role.getGroups().size() == 1);
          for (TSentryGroup group :role.getGroups()) {
            assertTrue(group.getGroupName(), group.getGroupName().equalsIgnoreCase(groupName));
          }
        }

        client.dropRole(requestorUserName, roleName);
      }});
  }

  @Test
  public void testShowGrant() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_testdb";
        String server = "server1";
        String db = "testDB";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName);
        client.createRole(requestorUserName, roleName);

        Set<TSentryRole> roles = client.listRoles(requestorUserName);
        assertEquals("Incorrect number of roles", 1, roles.size());

        client.grantDatabasePrivilege(requestorUserName, roleName, server, db, AccessConstants.ALL);
        Set<TSentryPrivilege> privileges = client.listAllPrivilegesByRoleName(requestorUserName, roleName);
        assertTrue(privileges.size() == 1);

        client.revokeDatabasePrivilege(requestorUserName, roleName, server, db, AccessConstants.ALL);
        client.dropRole(requestorUserName, roleName);
      }});
  }

  //See SENTRY-166
  @Test
  public void testUriWithEquals() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_testdb";
        String server = "server1";
        String uri = "file://u/w/h/t/partition=value/";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        // Creating associated role
        client.dropRoleIfExists(requestorUserName, roleName);
        client.createRole(requestorUserName, roleName);
        Set<TSentryRole> roles = client.listRoles(requestorUserName);
        assertEquals("Incorrect number of roles", 1, roles.size());

        client.grantURIPrivilege(requestorUserName, roleName, server, uri);
        Set<TSentryPrivilege> privileges = client.listAllPrivilegesByRoleName(requestorUserName, roleName);
        assertTrue(privileges.size() == 1);

        // Revoking the same privilege
        client.revokeURIPrivilege(requestorUserName, roleName, server, uri);
        privileges = client.listAllPrivilegesByRoleName(requestorUserName, roleName);
        assertTrue(privileges.size() == 0);

        // Clean up
        client.dropRole(requestorUserName, roleName);
      }});
  }


  //See SENTRY-181
  @Test
  public void testSameGrantTwice() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName = "admin_r1";

        client.createRole(requestorUserName, roleName);
        client.grantTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL");
        client.grantTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL");
        assertEquals(1, client.listAllPrivilegesByRoleName(requestorUserName, roleName).size());
      }});
  }

  @Test
  public void testGrantRevokeWithGrantOption() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        // Grant a privilege with Grant Option
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName = "admin_r1";
        boolean grantOption = true;
        boolean withoutGrantOption = false;

        client.dropRoleIfExists(requestorUserName,  roleName);
        client.createRole(requestorUserName,  roleName);

        client.grantTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL", grantOption);
        assertEquals(1, client.listAllPrivilegesByRoleName(requestorUserName, roleName).size());

        // Try to revoke the privilege without grantOption and can't revoke the privilege.
        client.revokeTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL", withoutGrantOption);
        assertEquals(1, client.listAllPrivilegesByRoleName(requestorUserName, roleName).size());

        // Try to revoke the privilege with grantOption, the privilege will be revoked.
        client.revokeTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL", grantOption);
        assertEquals(0, client.listAllPrivilegesByRoleName(requestorUserName, roleName).size());
      }});
  }

  @Test
  public void testGrantTwoPrivilegeDiffInGrantOption() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        // Grant a privilege with 'Grant Option'.
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName = "admin_r1";
        boolean grantOption = true;
        boolean withoutGrantOption = false;

        client.dropRoleIfExists(requestorUserName,  roleName);
        client.createRole(requestorUserName,  roleName);

        client.grantTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL", grantOption);
        assertEquals(1, client.listAllPrivilegesByRoleName(requestorUserName, roleName).size());

        // Grant a privilege without 'Grant Option'.
        client.grantTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL", withoutGrantOption);
        assertEquals(2, client.listAllPrivilegesByRoleName(requestorUserName, roleName).size());

        // Use 'grantOption = null', the two privileges will be revoked.
        client.revokeTablePrivilege(requestorUserName, roleName, "server", "db1", "table1", "ALL", null);
        assertEquals(0, client.listAllPrivilegesByRoleName(requestorUserName, roleName).size());
      }});
  }

  @Test
  public void testGranRevokePrivilegeOnColumnForRole() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName1 = "admin_r1";
        String roleName2 = "admin_r2";

        client.dropRoleIfExists(requestorUserName,  roleName1);
        client.createRole(requestorUserName,  roleName1);

        client.grantColumnPrivilege(requestorUserName, roleName1, "server", "db1", "table1", "col1", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName1, "server", "db1", "table1", "col2", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName1, "server", "db1", "table2", "col1", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName1, "server", "db1", "table2", "col2", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName1, "server", "db2", "table1", "col1", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName1, "server", "db2", "table2", "col1", "ALL");


        client.dropRoleIfExists(requestorUserName,  roleName2);
        client.createRole(requestorUserName,  roleName2);

        client.grantColumnPrivilege(requestorUserName, roleName2, "server", "db1", "table1", "col1", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName2, "server", "db1", "table1", "col2", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName2, "server", "db1", "table2", "col1", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName2, "server", "db1", "table2", "col2", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName2, "server", "db2", "table1", "col1", "ALL");
        client.grantColumnPrivilege(requestorUserName, roleName2, "server", "db2", "table2", "col1", "ALL");

        Set<TSentryPrivilege> listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertEquals("Privilege not assigned to role1 !!", 6, listPrivilegesByRoleName.size());

        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertEquals("Privilege not assigned to role2 !!", 6, listPrivilegesByRoleName.size());


        client.revokeColumnPrivilege(requestorUserName, roleName1, "server", "db1", "table1", "col1", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 5);
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 6);

        client.revokeTablePrivilege(requestorUserName, roleName2, "server", "db1", "table1", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 4);
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 5);

        client.revokeDatabasePrivilege(requestorUserName, roleName1, "server", "db1", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 2);
        client.revokeColumnPrivilege(requestorUserName, roleName1, "server", "db2", "table1", "col1", "ALL");
        client.revokeColumnPrivilege(requestorUserName, roleName1, "server", "db2", "table2", "col1", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 0);

        client.revokeColumnPrivilege(requestorUserName, roleName2, "server", "db1", "table2", "col1", "ALL");
        client.revokeColumnPrivilege(requestorUserName, roleName2, "server", "db1", "table2", "col2", "ALL");
        client.revokeColumnPrivilege(requestorUserName, roleName2, "server", "db2", "table1", "col1", "ALL");
        client.revokeColumnPrivilege(requestorUserName, roleName2, "server", "db2", "table2", "col1", "ALL");
        listPrivilegesByRoleName = client.listAllPrivilegesByRoleName(requestorUserName, roleName2);
        assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 0);
      }});
  }

  @Test
  public void testListByAuthDB() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName1 = "role1";
        String roleName2 = "role2";
        Set<String> testRoleSet = Sets.newHashSet(roleName1, roleName2);
        String group1 = "group1";
        String group2 = "group2";
        Set<String> testGroupSet = Sets.newHashSet(group1, group2);
        String server = "server1";
        String db = "testDB";
        String db2 = "testDB2";
        String tab = "testTab";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        String group1user = "group1user";
        setLocalGroupMapping(group1user, Sets.newHashSet(group1));
        String group2user = "group2user";
        setLocalGroupMapping(group2user, Sets.newHashSet(group2));
        setLocalGroupMapping("random", Sets.newHashSet("foo"));
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName1);
        client.createRole(requestorUserName, roleName1);
        client.dropRoleIfExists(requestorUserName, roleName2);
        client.createRole(requestorUserName, roleName2);

        TSentryPrivilege role1db1 = client.grantDatabasePrivilege(
            requestorUserName, roleName1, server, db, AccessConstants.SELECT);
        client.grantTablePrivilege(requestorUserName, roleName1, server, db, tab,
            AccessConstants.ALL);
        client.grantTablePrivilege(requestorUserName, roleName1, server, db2, tab,
            AccessConstants.SELECT);
        client.grantURIPrivilege(requestorUserName, roleName1, server, "hdfs:///fooUri");
        client.grantRoleToGroup(requestorUserName, group1, roleName1);

        TSentryPrivilege role2db1 = client.grantDatabasePrivilege(
            requestorUserName, roleName2, server, db,
            AccessConstants.ALL);
        client.grantDatabasePrivilege(requestorUserName, roleName2, server, db2,
            AccessConstants.SELECT);
        client.grantTablePrivilege(requestorUserName, roleName2, server, db2, tab,
            AccessConstants.ALL);
        client.grantRoleToGroup(requestorUserName, group2, roleName2);

        // build expected output
        TSentryPrivilegeMap db1RoleToPrivMap = new TSentryPrivilegeMap(
            new TreeMap<String, Set<TSentryPrivilege>>());
        db1RoleToPrivMap.getPrivilegeMap()
            .put(roleName1, Sets.newHashSet(role1db1));
        db1RoleToPrivMap.getPrivilegeMap()
            .put(roleName2, Sets.newHashSet(role2db1));
        Map<TSentryAuthorizable, TSentryPrivilegeMap> expectedResults = Maps
            .newTreeMap();
        List<? extends Authorizable> db1Authrizable = Lists.newArrayList(
            new Server(server), new Database(db));
        expectedResults.put(
            SentryPolicyServiceClientDefaultImpl.setupSentryAuthorizable(db1Authrizable),
            db1RoleToPrivMap);

        Set<List<? extends Authorizable>> authorizableSet = Sets.newHashSet();
        authorizableSet.add(db1Authrizable);

        // verify for null group and null roleset
        Map<TSentryAuthorizable, TSentryPrivilegeMap> authPrivMap = client
            .listPrivilegsbyAuthorizable(requestorUserName, authorizableSet, null, null);
        assertEquals(expectedResults, authPrivMap);

        // verify for null group and specific roleset
        authPrivMap = client.listPrivilegsbyAuthorizable(requestorUserName, authorizableSet,
            null, new ActiveRoleSet(testRoleSet));
        assertEquals(expectedResults, authPrivMap);

        // verify for null group and specific roleset
        authPrivMap = client.listPrivilegsbyAuthorizable(requestorUserName, authorizableSet, null,
            ActiveRoleSet.ALL);
        assertEquals(expectedResults, authPrivMap);

        // verify for specific group and null roleset
        authPrivMap = client.listPrivilegsbyAuthorizable(requestorUserName, authorizableSet,
            testGroupSet, null);
        assertEquals(expectedResults, authPrivMap);

        // verify for specific group and specific roleset
        authPrivMap = client.listPrivilegsbyAuthorizable(requestorUserName, authorizableSet,
            testGroupSet, new ActiveRoleSet(testRoleSet));
        assertEquals(expectedResults, authPrivMap);

        // verify for specific group and ALL roleset
        authPrivMap = client.listPrivilegsbyAuthorizable(requestorUserName, authorizableSet,
            testGroupSet, ActiveRoleSet.ALL);
        assertEquals(expectedResults, authPrivMap);

        // verify users not belonging to any group are not shown anything
        authPrivMap = client
            .listPrivilegsbyAuthorizable("random", authorizableSet,
                new HashSet<String>(), ActiveRoleSet.ALL);
        expectedResults.clear();
        expectedResults.put(
            SentryPolicyServiceClientDefaultImpl.setupSentryAuthorizable(db1Authrizable),
            new TSentryPrivilegeMap(new HashMap<String, Set<TSentryPrivilege>>()));
        assertEquals(expectedResults, authPrivMap);
      }});
  }

  @Test
  public void testListByAuthTab() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName1 = "role1";
        String roleName2 = "role2";
        String server = "server1";
        String db = "testDB";
        String db2 = "testDB2";
        String tab = "testTab";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName1);
        client.createRole(requestorUserName, roleName1);
        client.dropRoleIfExists(requestorUserName, roleName2);
        client.createRole(requestorUserName, roleName2);

        client.grantDatabasePrivilege(
            requestorUserName, roleName1, server, db, AccessConstants.SELECT);
        client.grantTablePrivilege(requestorUserName, roleName1, server, db, tab,
            AccessConstants.ALL);
        TSentryPrivilege role1db2tab = client.grantTablePrivilege(
            requestorUserName, roleName1, server, db2, tab,
            AccessConstants.SELECT);

        client.grantDatabasePrivilege(
            requestorUserName, roleName2, server, db,
            AccessConstants.ALL);
        client.grantDatabasePrivilege(requestorUserName, roleName2, server, db2,
            AccessConstants.SELECT);
        TSentryPrivilege role2db2tab = client.grantTablePrivilege(
            requestorUserName, roleName2, server, db2, tab,
            AccessConstants.ALL);
        client.grantURIPrivilege(requestorUserName, roleName1, server,
            "hdfs:///fooUri");

        // build expected output
        TSentryPrivilegeMap db1RoleToPrivMap = new TSentryPrivilegeMap(
            new TreeMap<String, Set<TSentryPrivilege>>());
        db1RoleToPrivMap.getPrivilegeMap().put(roleName1,
            Sets.newHashSet(role1db2tab));
        db1RoleToPrivMap.getPrivilegeMap().put(roleName2,
            Sets.newHashSet(role2db2tab));
        Map<TSentryAuthorizable, TSentryPrivilegeMap> expectedResults = Maps
            .newTreeMap();
        List<? extends Authorizable> db2TabAuthrizable = Lists.newArrayList(
            new Server(server), new Database(db2), new Table(tab));
        expectedResults.put(
            SentryPolicyServiceClientDefaultImpl.setupSentryAuthorizable(db2TabAuthrizable),
            db1RoleToPrivMap);

        Set<List<? extends Authorizable>> authorizableSet = Sets.newHashSet();
        authorizableSet.add(db2TabAuthrizable);
        Map<TSentryAuthorizable, TSentryPrivilegeMap> authPrivMap = client
            .listPrivilegsbyAuthorizable(requestorUserName, authorizableSet, null, null);

        assertEquals(expectedResults, authPrivMap);
      }});
  }

  @Test
  public void testListByAuthUri() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName1 = "role1";
        String roleName2 = "role2";
        String server = "server1";
        String db = "testDB";
        String db2 = "testDB2";
        String tab = "testTab";
        String uri1 = "hdfs:///fooUri";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName1);
        client.createRole(requestorUserName, roleName1);
        client.dropRoleIfExists(requestorUserName, roleName2);
        client.createRole(requestorUserName, roleName2);

        client.grantDatabasePrivilege(requestorUserName, roleName1, server, db,
            AccessConstants.SELECT);
        client.grantTablePrivilege(requestorUserName, roleName1, server, db, tab,
            AccessConstants.ALL);
        client.grantTablePrivilege(requestorUserName, roleName1, server, db2, tab,
            AccessConstants.SELECT);
        TSentryPrivilege role1uri1 = client.grantURIPrivilege(requestorUserName,
            roleName1, server, uri1);

        client.grantDatabasePrivilege(requestorUserName, roleName2, server, db,
            AccessConstants.ALL);
        client.grantDatabasePrivilege(requestorUserName, roleName2, server, db2,
            AccessConstants.SELECT);
        client.grantTablePrivilege(requestorUserName, roleName2, server, db2, tab,
            AccessConstants.ALL);
        TSentryPrivilege role2uri2 = client.grantURIPrivilege(requestorUserName,
            roleName2, server, uri1);

        // build expected output
        TSentryPrivilegeMap db1RoleToPrivMap = new TSentryPrivilegeMap(
            new TreeMap<String, Set<TSentryPrivilege>>());
        db1RoleToPrivMap.getPrivilegeMap().put(roleName1,
            Sets.newHashSet(role1uri1));
        db1RoleToPrivMap.getPrivilegeMap().put(roleName2,
            Sets.newHashSet(role2uri2));
        Map<TSentryAuthorizable, TSentryPrivilegeMap> expectedResults = Maps
            .newTreeMap();
        List<? extends Authorizable> uri1Authrizable = Lists.newArrayList(
            new Server(server), new AccessURI(uri1));
        expectedResults.put(
            SentryPolicyServiceClientDefaultImpl.setupSentryAuthorizable(uri1Authrizable),
            db1RoleToPrivMap);

        Set<List<? extends Authorizable>> authorizableSet = Sets.newHashSet();
        authorizableSet.add(uri1Authrizable);
        Map<TSentryAuthorizable, TSentryPrivilegeMap> authPrivMap = client
            .listPrivilegsbyAuthorizable(requestorUserName, authorizableSet, null, null);

        assertEquals(expectedResults, authPrivMap);
      }});
  }

  /**
   * List privileges by authorizables executed by non-admin user
   * Test various positive and negative cases for non-admin user
   * @throws Exception
   */
  @Test
  public void testListByAuthTabForNonAdmin() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        String user1 = "user1";
        String group1 = "group1";
        String group2 = "group2";
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        Set<String> userGroupNames1 = Sets.newHashSet(group1);
        Set<String> userGroupNames2 = Sets.newHashSet(group2);
        String roleName1 = "role1";
        String roleName2 = "role2";
        String server = "server1";
        String db = "testDB";
        String db2 = "testDB2";
        String tab = "testTab";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        setLocalGroupMapping(user1, userGroupNames1);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName1);
        client.createRole(requestorUserName, roleName1);
        client.dropRoleIfExists(requestorUserName, roleName2);
        client.createRole(requestorUserName, roleName2);

        client.grantDatabasePrivilege(requestorUserName, roleName1, server, db,
            AccessConstants.SELECT);
        client.grantTablePrivilege(requestorUserName, roleName1, server, db, tab,
            AccessConstants.ALL);
        TSentryPrivilege role1db2tab = client.grantTablePrivilege(
            requestorUserName, roleName1, server, db2, tab, AccessConstants.SELECT);
        client.grantRoleToGroup(requestorUserName, group1, roleName1);

        client.grantDatabasePrivilege(requestorUserName, roleName2, server, db,
            AccessConstants.ALL);
        client.grantDatabasePrivilege(requestorUserName, roleName2, server, db2,
            AccessConstants.SELECT);
        client.grantTablePrivilege(requestorUserName, roleName2, server, db2, tab,
            AccessConstants.ALL);
        client.grantURIPrivilege(requestorUserName, roleName1, server,
            "hdfs:///fooUri");

        // build expected output. user1 should see privileges on tab1 from role1
        TSentryPrivilegeMap db1RoleToPrivMap = new TSentryPrivilegeMap(
            new TreeMap<String, Set<TSentryPrivilege>>());
        db1RoleToPrivMap.getPrivilegeMap().put(roleName1, Sets.newHashSet(role1db2tab));
        Map<TSentryAuthorizable, TSentryPrivilegeMap> expectedResults = Maps.newTreeMap();
        List<? extends Authorizable> db2TabAuthorizable = Lists.newArrayList(
            new Server(server), new Database(db2), new Table(tab));
        expectedResults.put(
            SentryPolicyServiceClientDefaultImpl.setupSentryAuthorizable(db2TabAuthorizable),
            db1RoleToPrivMap);

        Set<List<? extends Authorizable>> authorizableSet = Sets.newHashSet();
        authorizableSet.add(db2TabAuthorizable);

        // list privileges with null group and roles
        Map<TSentryAuthorizable, TSentryPrivilegeMap> authPrivMap = client
            .listPrivilegsbyAuthorizable(user1, authorizableSet, null, null);
        assertEquals(expectedResults, authPrivMap);

        // list privileges with empty group set and null roles
        authPrivMap = client.listPrivilegsbyAuthorizable(user1, authorizableSet,
            new HashSet<String>(), null);
        assertEquals(expectedResults, authPrivMap);

        // list privileges with null group set and ALL roleset
        authPrivMap = client.listPrivilegsbyAuthorizable(user1, authorizableSet,
            null, new ActiveRoleSet(true));
        assertEquals(expectedResults, authPrivMap);

        // list privileges with user1's group set and null roles
        authPrivMap = client.listPrivilegsbyAuthorizable(user1, authorizableSet,
            userGroupNames1, null);
        assertEquals(expectedResults, authPrivMap);

        // list privileges with user1's group set and ALL roles
        authPrivMap = client.listPrivilegsbyAuthorizable(user1, authorizableSet,
            userGroupNames1, new ActiveRoleSet(true));
        assertEquals(expectedResults, authPrivMap);

        // list privileges with null group and user's specific roles with uppercase name
        authPrivMap = client.listPrivilegsbyAuthorizable(user1, authorizableSet,
            null, new ActiveRoleSet(Sets.newHashSet(roleName1.toUpperCase())));
        assertEquals(expectedResults, authPrivMap);

        // verify that user1 can't query group2
        try {
          client.listPrivilegsbyAuthorizable(user1, authorizableSet, userGroupNames2, null);
          fail("listPrivilegsbyAuthorizable() should fail for user1 accessing " + group2);
        } catch (SentryAccessDeniedException e) {
          // expected
        }

        // verify that user1 can't query role2
        ActiveRoleSet roleSet2 = new ActiveRoleSet(Sets.newHashSet(roleName2));
        try {
          client.listPrivilegsbyAuthorizable(user1, authorizableSet, null, roleSet2);
          fail("listPrivilegsbyAuthorizable() should fail for user1 accessing " + roleName2);
        } catch (SentryAccessDeniedException e) {
          // expected
        }
      }});
  }

  /**
   * Attempt to access a configuration value that is forbidden in getConfigVal
   * @param configVal The banned value
   * @param defaultVal A default to pass to getConfigValue
   * @throws Exception
   */
  private void checkBannedConfigVal(final String configVal, final String defaultVal)
          throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        try {
            client.getConfigValue(configVal, defaultVal);
            fail("Attempt to access " + configVal + " succeeded");
          } catch (SentryAccessDeniedException e) {
            assertTrue(e.toString().contains("was denied"));
            assertTrue(e.toString().contains(configVal));
          }
      }});
  }

  @Test
  public void testGetConfigVal() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        String val;

        // Basic success case
        val = client.getConfigValue("sentry.service.admin.group", "xxx");
        assertEquals(val, "admin_group");

        // Undefined value gets the default back
        val = client.getConfigValue("sentry.this.is.not.defined", "hello");
        assertEquals(val, "hello");

        // Undefined value and null default gets null back
        val = client.getConfigValue("sentry.this.is.not.defined", null);
        assertEquals(val, null);

        // Known config value with null default works as expected
        val = client.getConfigValue("sentry.service.admin.group", null);
        assertEquals(val, "admin_group");

        // Value that is forbidden (anything not starting with "sentry") dies
        checkBannedConfigVal("notsentry", "xxx");

        // Ditto with a null default
        checkBannedConfigVal("notsentry", null);

        // Values with .jdbc. are forbidden
        checkBannedConfigVal("sentry.xxx.jdbc.xxx", null);

        // Values with password are forbidden
        checkBannedConfigVal("sentry.xxx.password", null);

        // Attempt to get the location of the keytab also fails
        checkBannedConfigVal("sentry.service.server.keytab", null);

      }});
  }

  /* SENTRY-841 */
  @Test
  public void testGranRevokePrivilegeOnServerForRole() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        String roleName1 = "admin_r1";

        client.dropRoleIfExists(requestorUserName, roleName1);
        client.createRole(requestorUserName, roleName1);

        client.grantServerPrivilege(requestorUserName, roleName1, "server", false);

        Set<TSentryPrivilege> listPrivs = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege should be all:",listPrivs.iterator().next().getAction().equals("*"));

        client.revokeServerPrivilege(requestorUserName, roleName1, "server", false);
        listPrivs = client.listAllPrivilegesByRoleName(requestorUserName, roleName1);
        assertTrue("Privilege not correctly revoked !!", listPrivs.size() == 0);

      }});
  }
}
