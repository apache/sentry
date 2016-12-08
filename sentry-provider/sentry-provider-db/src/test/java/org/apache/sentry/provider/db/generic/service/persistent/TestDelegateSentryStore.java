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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.util.Set;

import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
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

  @Test(expected=SentryAlreadyExistsException.class)
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
    String role1 = "r1", role2 = "r2";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    Set<String> oneGroup = Sets.newHashSet("g3");
    String grantor = "grantor";

    sentryStore.createRole(SEARCH, role1, grantor);
    sentryStore.createRole(SEARCH, role2, grantor);

    sentryStore.alterRoleAddGroups(SEARCH, role1, twoGroups, grantor);
    assertEquals(twoGroups, sentryStore.getGroupsByRoles(SEARCH,Sets.newHashSet(role1)));

    assertEquals(Sets.newHashSet(role1), sentryStore.getRolesByGroups(SEARCH, twoGroups));

    sentryStore.alterRoleAddGroups(SEARCH, role2, oneGroup, grantor);
    assertEquals(oneGroup, sentryStore.getGroupsByRoles(SEARCH, Sets.newHashSet(role2)));

    sentryStore.alterRoleDeleteGroups(SEARCH, role1, Sets.newHashSet("g1"), grantor);
    assertEquals(Sets.newHashSet("g2"), sentryStore.getGroupsByRoles(SEARCH, Sets.newHashSet(role1)));

    sentryStore.alterRoleDeleteGroups(SEARCH, role2, oneGroup, grantor);
    assertEquals(Sets.newHashSet(), sentryStore.getGroupsByRoles(SEARCH, Sets.newHashSet(role2)));
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
    String role1 = "r1", role2 = "r2";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    String grantor = "grantor";

    sentryStore.createRole(SEARCH, role1, grantor);
    sentryStore.createRole(SEARCH, role2, grantor);

    sentryStore.alterRoleAddGroups(SEARCH, role1, twoGroups, grantor);
    sentryStore.alterRoleAddGroups(SEARCH, role2, twoGroups, grantor);

    assertEquals(twoGroups, sentryStore.getGroupsByRoles(SEARCH, Sets.newHashSet(role1)));
    assertEquals(twoGroups, sentryStore.getGroupsByRoles(SEARCH, Sets.newHashSet(role2)));
    assertEquals(twoGroups, sentryStore.getGroupsByRoles(SEARCH, Sets.newHashSet(role1,role2)));
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
