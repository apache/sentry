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
package org.apache.sentry.provider.db.generic.service.thrift;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.Field;
import org.apache.sentry.core.model.search.SearchConstants;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestSentryGenericServiceIntegration extends SentryGenericServiceIntegrationBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestSentryGenericServiceIntegration.class);

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

        client.dropRoleIfExists(requestorUserName, roleName, SOLR);

        client.createRole(requestorUserName, roleName, SOLR);

        client.addRoleToGroups(requestorUserName, roleName, SOLR, Sets.newHashSet(requestorUserGroupNames));

        Set<TSentryRole> roles = client.listUserRoles(requestorUserName,SOLR);
        assertEquals("Incorrect number of roles", 1, roles.size());
        for (TSentryRole role:roles) {
          assertTrue(role.getRoleName(), role.getRoleName().equalsIgnoreCase(roleName));
        }
        client.dropRole(requestorUserName, roleName, SOLR);
      }});
  }

  @Test
  public void testAddDeleteRoleToGroup() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String testGroupName = "g1";
        String roleName = "admin_r";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        setLocalGroupMapping(requestorUserName, Sets.newHashSet(testGroupName));
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName, SOLR);

        client.createRole(requestorUserName, roleName, SOLR);

        client.addRoleToGroups(requestorUserName, roleName, SOLR, Sets.newHashSet(testGroupName));

        Set<TSentryRole> roles = client.listUserRoles(requestorUserName,SOLR);
        assertEquals("Incorrect number of roles", 1, roles.size());
        for (TSentryRole role:roles) {
          assertTrue(role.getRoleName(), role.getRoleName().equalsIgnoreCase(roleName));
          assertTrue(role.getGroups().size() == 1);
          for (String group :role.getGroups()) {
            assertEquals(testGroupName, group);
          }
        }

        client.deleteRoleToGroups(requestorUserName, roleName, SOLR, Sets.newHashSet(testGroupName));
        roles = client.listUserRoles(requestorUserName,SOLR);
        assertEquals("Incorrect number of roles", 0, roles.size());

        client.dropRole(requestorUserName, roleName, SOLR);
      }});
  }

  @Test
  public void testGranRevokePrivilege() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName1 = "admin_r1";
        String roleName2 = "admin_r2";

        client.dropRoleIfExists(requestorUserName,  roleName1, SOLR);
        client.createRole(requestorUserName,  roleName1, SOLR);

        client.dropRoleIfExists(requestorUserName,  roleName2, SOLR);
        client.createRole(requestorUserName,  roleName2, SOLR);

        TSentryPrivilege queryPrivilege = new TSentryPrivilege(SOLR, "service1",
                                              fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
                                              SearchConstants.QUERY);

        TSentryPrivilege updatePrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.UPDATE);

        client.grantPrivilege(requestorUserName, roleName1, SOLR, queryPrivilege);
        client.grantPrivilege(requestorUserName, roleName2, SOLR, updatePrivilege);

        client.revokePrivilege(requestorUserName, roleName1, SOLR, queryPrivilege);
        client.revokePrivilege(requestorUserName, roleName2, SOLR, updatePrivilege);
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

        client.dropRoleIfExists(requestorUserName, roleName1, SOLR);
        client.createRole(requestorUserName,  roleName1, SOLR);

        client.dropRoleIfExists(requestorUserName,  roleName2, SOLR);
        client.createRole(requestorUserName,  roleName2, SOLR);

        TSentryPrivilege queryPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.QUERY);

        client.grantPrivilege(requestorUserName, roleName1, SOLR, queryPrivilege);
        Set<TSentryPrivilege> listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, roleName1, SOLR, "service1");
        assertTrue("Privilege not assigned to role1 !!", listPrivilegesByRoleName.size() == 1);

        client.grantPrivilege(requestorUserName, roleName2, SOLR, queryPrivilege);
        listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, roleName2, SOLR, "service1");
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
        String roleName = "admin_r1";
        String groupName = "group1";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        setLocalGroupMapping(requestorUserName, Sets.newHashSet(groupName));
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName, SOLR);
        client.createRole(requestorUserName, roleName, SOLR);
        client.addRoleToGroups(requestorUserName, roleName, SOLR, Sets.newHashSet(groupName));

        Set<TSentryRole> groupRoles = client.listRolesByGroupName(requestorUserName, groupName,SOLR);
        assertTrue(groupRoles.size() == 1);
        for (TSentryRole role:groupRoles) {
          assertTrue(role.getRoleName(), role.getRoleName().equalsIgnoreCase(roleName));
          assertTrue(role.getGroups().size() == 1);
          for (String group :role.getGroups()) {
            assertEquals(groupName, group);
          }
        }

        client.dropRole(requestorUserName, roleName, SOLR);
      }});
  }

  @Test
  public void testShowGrant() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_r1";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName, SOLR);
        client.createRole(requestorUserName, roleName, SOLR);

        TSentryPrivilege queryPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.QUERY);

        TSentryPrivilege updatePrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.UPDATE);

        client.grantPrivilege(requestorUserName, roleName, SOLR, updatePrivilege);
        client.grantPrivilege(requestorUserName, roleName, SOLR, queryPrivilege);
        Set<TSentryPrivilege> privileges = client.listPrivilegesByRoleName(requestorUserName, roleName, SOLR, "service1");
        assertTrue(privileges.size() == 2);

        client.revokePrivilege(requestorUserName, roleName, SOLR, updatePrivilege);
        privileges = client.listPrivilegesByRoleName(requestorUserName, roleName, SOLR, "service1");
        assertTrue(privileges.size() == 1);
      }});
  }

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

        client.createRole(requestorUserName, roleName, SOLR);

        TSentryPrivilege queryPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.QUERY);

        client.grantPrivilege(requestorUserName, roleName, SOLR, queryPrivilege);
        assertEquals(1, client.listPrivilegesByRoleName(requestorUserName, roleName, SOLR, "service1").size());
      }});
  }

  @Test
  public void testGrantRevokeWithGrantOption() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String adminUser = ADMIN_USER;
        Set<String> adminGroup = Sets.newHashSet(ADMIN_GROUP);
        String grantOptionUser = "user1";
        Set<String> grantOptionGroup = Sets.newHashSet("group1");
        String noGrantOptionUser = "user2";
        Set<String> noGrantOptionGroup = Sets.newHashSet("group2");

        setLocalGroupMapping(adminUser, adminGroup);
        setLocalGroupMapping(grantOptionUser, grantOptionGroup);
        setLocalGroupMapping(noGrantOptionUser, noGrantOptionGroup);
        writePolicyFile();

        String grantRole = "grant_r";
        String noGrantRole = "no_grant_r";
        String testRole = "test_role";

        client.createRole(adminUser, grantRole, SOLR);
        client.createRole(adminUser, noGrantRole, SOLR);
        client.createRole(adminUser, testRole, SOLR);

        TSentryPrivilege grantPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"))),
            SearchConstants.QUERY);
        grantPrivilege.setGrantOption(TSentryGrantOption.TRUE);

        TSentryPrivilege noGrantPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"))),
            SearchConstants.QUERY);
        noGrantPrivilege.setGrantOption(TSentryGrantOption.FALSE);

        TSentryPrivilege testPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.QUERY);
        testPrivilege.setGrantOption(TSentryGrantOption.FALSE);

        client.grantPrivilege(adminUser, grantRole, SOLR, grantPrivilege);
        client.grantPrivilege(adminUser, noGrantRole, SOLR, noGrantPrivilege);

        client.addRoleToGroups(adminUser, grantRole, SOLR, grantOptionGroup);
        client.addRoleToGroups(adminUser, noGrantRole, SOLR, noGrantOptionGroup);

        try {
          client.grantPrivilege(grantOptionUser,testRole,SOLR, testPrivilege);
        } catch (SentryUserException e) {
          fail("grantOptionUser failed grant privilege to user");
        }

        try {
          client.grantPrivilege(noGrantOptionUser, testRole, SOLR, testPrivilege);
          fail("noGrantOptionUser can't grant privilege to user");
        } catch (SentryUserException e) {
        }

        try {
          client.revokePrivilege(grantOptionUser, testRole, SOLR, testPrivilege);
        } catch(SentryUserException e) {
          fail("grantOptionUser failed revoke privilege to user");
        }

        try {
          client.revokePrivilege(noGrantOptionUser, testRole, SOLR, testPrivilege);
          fail("noGrantOptionUser can't revoke privilege to user");
        } catch (SentryUserException e) {
        }
      }});
  }

  @Test
  public void testGetPrivilegeByHierarchy() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String adminUser = ADMIN_USER;
        Set<String> adminGroup = Sets.newHashSet(ADMIN_GROUP);
        String testRole = "role1";
        Set<String> testGroup = Sets.newHashSet("group1");
        String testUser = "user1";
        setLocalGroupMapping(adminUser, adminGroup);
        setLocalGroupMapping(testUser, testGroup);
        writePolicyFile();


        client.createRole(adminUser, testRole, SOLR);
        client.addRoleToGroups(adminUser, testRole, SOLR, testGroup);

        TSentryPrivilege queryPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.QUERY);

        TSentryPrivilege updatePrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c2"), new Field("f2"))),
            SearchConstants.UPDATE);

        client.grantPrivilege(adminUser, testRole, SOLR, queryPrivilege);
        client.grantPrivilege(adminUser, testRole, SOLR, updatePrivilege);

        assertEquals(2, client.listPrivilegesByRoleName(testUser, testRole, SOLR, "service1").size());

        assertEquals(1, client.listPrivilegesByRoleName(testUser, testRole,
            SOLR, "service1", Arrays.asList(new Collection("c1"))).size());

        assertEquals(1, client.listPrivilegesByRoleName(testUser, testRole,
            SOLR, "service1", Arrays.asList(new Collection("c2"))).size());

        assertEquals(1, client.listPrivilegesByRoleName(testUser, testRole,
            SOLR, "service1", Arrays.asList(new Collection("c1"), new Field("f1"))).size());

        assertEquals(1, client.listPrivilegesByRoleName(testUser, testRole,
            SOLR, "service1", Arrays.asList(new Collection("c2"), new Field("f2"))).size());

       //test listPrivilegesForProvider by group(testGroup)
        ActiveRoleSet roleSet = ActiveRoleSet.ALL;

        assertEquals(1, client.listPrivilegesForProvider(SOLR, "service1", roleSet,
            testGroup, Arrays.asList(new Collection("c1"))).size());

        assertEquals(1, client.listPrivilegesForProvider(SOLR, "service1", roleSet,
            testGroup, Arrays.asList(new Collection("c2"))).size());

        assertEquals(1, client.listPrivilegesForProvider(SOLR, "service1", roleSet,
            testGroup, Arrays.asList(new Collection("c1"), new Field("f1"))).size());

        assertEquals(1, client.listPrivilegesForProvider(SOLR, "service1", roleSet,
            testGroup, Arrays.asList(new Collection("c2"), new Field("f2"))).size());
      }});
  }

  @Test
  public void testGetPrivilegeByAuthorizable() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String adminUser = ADMIN_USER;
        Set<String> adminGroup = Sets.newHashSet(ADMIN_GROUP);
        String testRole = "role1";
        Set<String> testGroup = Sets.newHashSet("group1");
        String testUser = "user1";
        setLocalGroupMapping(adminUser, adminGroup);
        setLocalGroupMapping(testUser, testGroup);
        writePolicyFile();

        client.createRole(adminUser, testRole, SOLR);
        client.addRoleToGroups(adminUser, testRole, SOLR, adminGroup);

        TSentryPrivilege queryPrivilege = new TSentryPrivilege(SOLR, "service1",
        fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
        SearchConstants.QUERY);

        TSentryPrivilege updatePrivilege = new TSentryPrivilege(SOLR, "service1",
        fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f2"))),
        SearchConstants.UPDATE);

        client.grantPrivilege(adminUser, testRole, SOLR, queryPrivilege);
        client.grantPrivilege(adminUser, testRole, SOLR, updatePrivilege);

        //test listPrivilegsbyAuthorizable without requested group and active role set.
        assertEquals(1, client.listPrivilegsbyAuthorizable(SOLR, "service1", adminUser,
            Sets.newHashSet(new String("Collection=c1->Field=f1")), null, null).size());

        //test listPrivilegsbyAuthorizable with requested group (testGroup)
        Map<String, TSentryPrivilegeMap> privilegeMap = client.listPrivilegsbyAuthorizable(SOLR,
            "service1", adminUser, Sets.newHashSet(new String("Collection=c1->Field=f1")), testGroup, null);
        TSentryPrivilegeMap actualMap = privilegeMap.get(new String("Collection=c1->Field=f1"));
        assertEquals(0, actualMap.getPrivilegeMap().size());

        //test listPrivilegsbyAuthorizable with active role set.
        ActiveRoleSet roleSet = ActiveRoleSet.ALL;
        assertEquals(1, client.listPrivilegsbyAuthorizable(SOLR, "service1", adminUser,
            Sets.newHashSet(new String("Collection=c1->Field=f1")), null, roleSet).size());
        privilegeMap = client.listPrivilegsbyAuthorizable(SOLR,
          "service1", adminUser, Sets.newHashSet(new String("Collection=c1->Field=f1")), null, roleSet);
        actualMap = privilegeMap.get(new String("Collection=c1->Field=f1"));
        assertEquals(1, actualMap.getPrivilegeMap().size());

        privilegeMap = client.listPrivilegsbyAuthorizable(SOLR,
            "service1", testUser, Sets.newHashSet(new String("Collection=c1->Field=f1")), null, roleSet);
        actualMap = privilegeMap.get(new String("Collection=c1->Field=f1"));
        assertEquals(0, actualMap.getPrivilegeMap().size());

        // grant tesRole to testGroup.
        client.addRoleToGroups(adminUser, testRole, SOLR, testGroup);

        privilegeMap = client.listPrivilegsbyAuthorizable(SOLR,
            "service1", testUser, Sets.newHashSet(new String("Collection=c1")), null, roleSet);
        actualMap = privilegeMap.get(new String("Collection=c1"));
        assertEquals(1, actualMap.getPrivilegeMap().size());
        assertEquals(2, actualMap.getPrivilegeMap().get(testRole).size());
      }});
  }

  @Test
  public void testDropAndRenamePrivilege() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName = "admin_r1";

        client.createRole(requestorUserName, roleName, SOLR);

        TSentryPrivilege queryPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c1"), new Field("f1"))),
            SearchConstants.QUERY);
        client.grantPrivilege(requestorUserName, roleName, SOLR, queryPrivilege);

        assertEquals(1, client.listPrivilegesByRoleName(requestorUserName, roleName,
            SOLR, "service1", Arrays.asList(new Collection("c1"), new Field("f1"))).size());

        assertEquals(0, client.listPrivilegesByRoleName(requestorUserName, roleName,
            SOLR, "service1", Arrays.asList(new Collection("c2"), new Field("f2"))).size());

        client.renamePrivilege(requestorUserName, SOLR, "service1", Arrays.asList(new Collection("c1"), new Field("f1")),
            Arrays.asList(new Collection("c2"), new Field("f2")));

        assertEquals(0, client.listPrivilegesByRoleName(requestorUserName, roleName,
            SOLR, "service1", Arrays.asList(new Collection("c1"), new Field("f1"))).size());

        assertEquals(1, client.listPrivilegesByRoleName(requestorUserName, roleName,
            SOLR, "service1", Arrays.asList(new Collection("c2"), new Field("f2"))).size());

        TSentryPrivilege dropPrivilege = new TSentryPrivilege(SOLR, "service1",
            fromAuthorizable(Arrays.asList(new Collection("c2"), new Field("f2"))),
            SearchConstants.QUERY);

        client.dropPrivilege(requestorUserName, SOLR, dropPrivilege);

        assertEquals(0, client.listPrivilegesByRoleName(requestorUserName, roleName,
            SOLR, "service1", Arrays.asList(new Collection("c2"), new Field("f2"))).size());
      }});
  }

  private List<TAuthorizable> fromAuthorizable(List<? extends Authorizable> authorizables) {
    List<TAuthorizable> tAuthorizables = Lists.newArrayList();
    for (Authorizable authorizable : authorizables) {
      tAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
    }
    return tAuthorizables;
  }
}
