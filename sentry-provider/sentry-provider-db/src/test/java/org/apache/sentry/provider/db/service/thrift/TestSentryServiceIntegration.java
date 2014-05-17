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

import com.google.common.collect.Sets;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.Test;

import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestSentryServiceIntegration extends SentryServiceIntegrationBase {

  @Test
  public void testCreateDropShowRole() throws Exception {
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    String roleName = "admin_r";

    client.dropRoleIfExists(requestorUserName, requestorUserGroupNames, roleName);

    client.createRole(requestorUserName, requestorUserGroupNames, roleName);

    Set<TSentryRole> roles = client.listRoles(requestorUserName, requestorUserGroupNames);
    assertEquals("Incorrect number of roles", 1, roles.size());

    for (TSentryRole role:roles) {
      assertTrue(role.getRoleName(), role.getRoleName().equalsIgnoreCase(roleName));
    }
    client.dropRole(requestorUserName, requestorUserGroupNames, roleName);
  }
  
  @Test
  public void testGranRevokePrivilegeOnTableForRole() throws Exception {
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    String roleName = "admin_r";

    client.dropRoleIfExists(requestorUserName, requestorUserGroupNames, roleName);
    client.createRole(requestorUserName, requestorUserGroupNames, roleName);
    
    client.grantTablePrivilege(requestorUserName, requestorUserGroupNames, roleName, "server", "db", "table", "ALL");    
    Set<TSentryPrivilege> listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, requestorUserGroupNames, roleName);
    assertTrue("Privilege not assigned to role !!", listPrivilegesByRoleName.size() == 1);
    
    client.revokeTablePrivilege(requestorUserName, requestorUserGroupNames, roleName, "server", "db", "table", "ALL");
    listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, requestorUserGroupNames, roleName);
    assertTrue("Privilege not correctly revoked !!", listPrivilegesByRoleName.size() == 0);
  }  

  @Test
  public void testShowRoleGrant() throws Exception {
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    String roleName = "admin_testdb";
    String groupName = "group1";

    client.dropRoleIfExists(requestorUserName, requestorUserGroupNames, roleName);
    client.createRole(requestorUserName, requestorUserGroupNames, roleName);

    Set<TSentryRole> roles = client.listRoles(requestorUserName, requestorUserGroupNames);
    assertEquals("Incorrect number of roles", 1, roles.size());

    client.grantRoleToGroup(requestorUserName, requestorUserGroupNames, groupName, roleName);
    Set<TSentryRole> groupRoles = client.listRolesByGroupName(requestorUserName,
        requestorUserGroupNames, groupName);
    assertTrue(groupRoles.size() == 1);
    for (TSentryRole role:groupRoles) {
      assertTrue(role.getRoleName(), role.getRoleName().equalsIgnoreCase(roleName));
      assertTrue(role.getGroups().size() == 1);
      for (TSentryGroup group :role.getGroups()) {
        assertTrue(group.getGroupName(), group.getGroupName().equalsIgnoreCase(groupName));
      }
    }

    client.dropRole(requestorUserName, requestorUserGroupNames, roleName);
  }

  @Test
  public void testShowGrant() throws Exception {
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    String roleName = "admin_testdb";
    String server = "server1";
    String db = "testDB";

    client.dropRoleIfExists(requestorUserName, requestorUserGroupNames, roleName);
    client.createRole(requestorUserName, requestorUserGroupNames, roleName);

    Set<TSentryRole> roles = client.listRoles(requestorUserName, requestorUserGroupNames);
    assertEquals("Incorrect number of roles", 1, roles.size());

    client.grantDatabasePrivilege(requestorUserName, requestorUserGroupNames, roleName, server, db);
    Set<TSentryPrivilege> privileges = client.listPrivilegesByRoleName(requestorUserName,
        requestorUserGroupNames, roleName);
    assertTrue(privileges.size() == 1);
    for (TSentryPrivilege privilege:privileges) {
      assertTrue(privilege.getPrivilegeName(),
        privilege.getPrivilegeName().equalsIgnoreCase(SentryStore.constructPrivilegeName(privilege)));
    }

    client.revokeDatabasePrivilege(requestorUserName, requestorUserGroupNames, roleName, server, db);
    client.dropRole(requestorUserName, requestorUserGroupNames, roleName);
  }

}