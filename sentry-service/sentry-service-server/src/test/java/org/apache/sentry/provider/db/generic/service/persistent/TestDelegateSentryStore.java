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
package org.apache.sentry.provider.db.generic.service.persistent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.sentry.api.generic.thrift.TSentryRole;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestDelegateSentryStore extends SentryStoreIntegrationBase{
  private static final String SEARCH = "solr";

  @Before
  public void configure() throws Exception {
    /**
     * add the admin user to admin groups
     */
    policyFile = new PolicyFile();
    addGroupsToUser("admin", getAdminGroups());
    writePolicyFile();
  }

  @Test
  public void testCreateDropRole() throws Exception {
    String roleName = "test-drop-role";
    String grantor = "grantor";
    sentryStore.createRole(SEARCH, roleName, grantor);
    sentryStore.dropRole(SEARCH, roleName, grantor);
  }

  @Test
  public void testCaseInsensitiveCreateDropRole() throws Exception {
    String roleName1 = "test";
    String roleName2 = "TeSt";
    String grantor = "grantor";
    sentryStore.createRole(SEARCH, roleName1, grantor);
    try {
      sentryStore.createRole(SEARCH, roleName2, grantor);
      fail("Fail to throw Exception");
    } catch (SentryAlreadyExistsException e) {
      //ignore the exception
    }

    try {
      sentryStore.dropRole(SEARCH, roleName2, grantor);
    } catch (SentryNoSuchObjectException e) {
      fail("Shouldn't throw SentryNoSuchObjectException");
    }
  }

  @Test(expected=Exception.class)
  public void testCreateDuplicateRole() throws Exception {
    String roleName = "test-dup-role";
    String grantor = "grantor";
    sentryStore.createRole(SEARCH, roleName, grantor);
    sentryStore.createRole(SEARCH, roleName, grantor);
  }

  @Test(expected=SentryNoSuchObjectException.class)
  public void testDropNotExistRole() throws Exception {
    String roleName = "not-exist";
    String grantor = "grantor";
    sentryStore.dropRole(SEARCH, roleName, grantor);
  }

  @Test(expected = SentryNoSuchObjectException.class)
  public void testAddGroupsNonExistantRole()
      throws Exception {
    String roleName = "non-existant-role";
    String grantor = "grantor";
    sentryStore.alterRoleAddGroups(SEARCH, roleName, Sets.newHashSet("g1"), grantor);
  }

  @Test(expected = SentryNoSuchObjectException.class)
  public void testDeleteGroupsNonExistantRole()
      throws Exception {
    String roleName = "non-existant-role";
    String grantor = "grantor";
    sentryStore.alterRoleDeleteGroups(SEARCH, roleName, Sets.newHashSet("g1"), grantor);
  }

  @Test
  public void testAddDeleteRoleToGroups() throws Exception {
    String role1 = "r1", role2 = "r2", role3 = "r3";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    Set<String> oneGroup = Sets.newHashSet("g3");
    String grantor = "grantor";

    sentryStore.createRole(SEARCH, role1, grantor);
    sentryStore.createRole(SEARCH, role2, grantor);
    sentryStore.createRole(SEARCH, role3, grantor);

    sentryStore.alterRoleAddGroups(SEARCH, role1, twoGroups, grantor);
    Set<TSentryRole> tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, twoGroups);
    assertEquals(1, tRoles.size());
    for(TSentryRole tRole:tRoles) {
      assertEquals(role1, tRole.getRoleName());
      assertEquals(twoGroups, tRole.getGroups());
    }

    assertEquals(Sets.newHashSet(role1), sentryStore.getRolesByGroups(SEARCH, twoGroups));

    sentryStore.alterRoleAddGroups(SEARCH, role2, oneGroup, grantor);
    tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, oneGroup);
    assertEquals(1, tRoles.size());
    for(TSentryRole tRole:tRoles) {
      assertEquals(role2, tRole.getRoleName());
      assertEquals(oneGroup, tRole.getGroups());
    }
    //Test with null group added
    Set<String>tempGroups = Sets.newHashSet();
    tempGroups.add(null);
    tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, tempGroups);
    assertEquals(3, tRoles.size()); //Should get all roles
    assertEquals(Sets.newHashSet(role1, role2, role3), sentryStore.getRolesByGroups(SEARCH, tempGroups));

    sentryStore.alterRoleDeleteGroups(SEARCH, role1, Sets.newHashSet("g1"), grantor);
    tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, twoGroups);
    assertEquals(1, tRoles.size());
    for(TSentryRole tRole:tRoles) {
      assertEquals(role1, tRole.getRoleName());
      assertEquals(Sets.newHashSet("g2"), tRole.getGroups());
    }
    sentryStore.alterRoleDeleteGroups(SEARCH, role2, oneGroup, grantor);
    tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, oneGroup);
    assertEquals(0, tRoles.size());
  }

  @Test
  public void testGetRolesByGroupNames() throws Exception {
    String role1 = "r1", role2 = "r2";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    String grantor = "grantor";

    sentryStore.createRole(SEARCH, role1, grantor);
    sentryStore.createRole(SEARCH, role2, grantor);

    sentryStore.alterRoleAddGroups(SEARCH, role1, twoGroups, grantor);
    sentryStore.alterRoleAddGroups(SEARCH, role2, twoGroups, grantor);

    assertEquals(Sets.newHashSet(role1,role2), sentryStore.getRolesByGroups(SEARCH, twoGroups));
  }

  @Test
  public void testGetGroupsByRoleNames() throws Exception {
    String role1 = "r1", role2 = "r2", role3 = "r3";
    Set<String> groups1 = Sets.newHashSet("g1", "g2", "g3", "g4");
    Set<String> groups2 = Sets.newHashSet("g1", "g3", "g5", "g6", "g7");
    Set<String> allGroups = Sets.newHashSet("g1", "g2", "g3", "g4", "g5", "g6", "g7");
    String grantor = "grantor";

    sentryStore.createRole(SEARCH, role1, grantor);
    sentryStore.createRole(SEARCH, role2, grantor);
    sentryStore.createRole(SEARCH, role3, grantor);

    sentryStore.alterRoleAddGroups(SEARCH, role1, groups1, grantor);
    sentryStore.alterRoleAddGroups(SEARCH, role2, groups2, grantor);

    Set<TSentryRole> tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, Sets.newHashSet(groups1));
    assertEquals(2, tRoles.size());
    for(TSentryRole tRole:tRoles) {
      if(tRole.getRoleName().equals(role1)) {
        assertEquals(groups1, tRole.getGroups());
      } else if(tRole.getRoleName().equals(role2)) {
        assertEquals(groups2, tRole.getGroups());
      }
    }

    //Get the same output as before
    tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, Sets.newHashSet(groups2));
    assertEquals(2, tRoles.size());
    for(TSentryRole tRole:tRoles) {
      if(tRole.getRoleName().equals(role1)) {
        assertEquals(groups1, tRole.getGroups());
      } else if(tRole.getRoleName().equals(role2)) {
        assertEquals(groups2, tRole.getGroups());
      }
    }

    //Again get the same output as before
    tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, allGroups);
    assertEquals(2, tRoles.size());
    for(TSentryRole tRole:tRoles) {
      if(tRole.getRoleName().equals(role1)) {
        assertEquals(groups1, tRole.getGroups());
      } else if(tRole.getRoleName().equals(role2)) {
        assertEquals(groups2, tRole.getGroups());
      }
    }

    sentryStore.alterRoleAddGroups(SEARCH, role3, groups1, grantor);
    tRoles = sentryStore.getTSentryRolesByGroupName(SEARCH, allGroups);
    assertEquals(3, tRoles.size());
    for(TSentryRole tRole:tRoles) {
      if(tRole.getRoleName().equals(role3)) {
        assertEquals(groups1, tRole.getGroups());
      }
    }
  }

  @Test
  public void testGetAllRoles() throws Exception {
    String role1 = "r1", role2 = "r2";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    String grantor = "grantor";

    sentryStore.createRole(SEARCH, role1, grantor);
    sentryStore.createRole(SEARCH, role2, grantor);

    sentryStore.alterRoleAddGroups(SEARCH, role1, twoGroups, grantor);
    sentryStore.alterRoleAddGroups(SEARCH, role2, twoGroups, grantor);

    //test get all roles by groupName=null
    String groupName = null;
    Set<String> groups = Sets.newHashSet(groupName);
    assertEquals(Sets.newHashSet(role1,role2), sentryStore.getRolesByGroups(SEARCH, groups));

    groups.clear();
    assertEquals(0, sentryStore.getRolesByGroups(SEARCH, groups).size());
  }
}
