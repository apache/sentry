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
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestSentryServerWithoutKerberos extends SentryServiceIntegrationBase {

  @Override
  public void beforeSetup() throws Exception {
    this.kerberos = false;
  }

  @Test
  public void testCreateRole() throws Exception {
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
    writePolicyFile();
    String roleName = "admin_r";
    client.dropRoleIfExists(requestorUserName, roleName);
    client.createRole(requestorUserName, roleName);
    client.dropRole(requestorUserName, roleName);
  }

  @Test
  public void testQueryPushDown() throws Exception {
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
    writePolicyFile();

    String roleName1 = "admin_r1";
    String roleName2 = "admin_r2";

    String group1 = "g1";
    String group2 = "g2";

    client.dropRoleIfExists(requestorUserName, roleName1);
    client.createRole(requestorUserName, roleName1);
    client.grantRoleToGroup(requestorUserName, group1, roleName1);

    client.grantTablePrivilege(requestorUserName, roleName1, "server", "db1", "table1", "ALL");
    client.grantTablePrivilege(requestorUserName, roleName1, "server", "db1", "table2", "ALL");
    client.grantTablePrivilege(requestorUserName, roleName1, "server", "db2", "table3", "ALL");
    client.grantTablePrivilege(requestorUserName, roleName1, "server", "db2", "table4", "ALL");


    client.dropRoleIfExists(requestorUserName, roleName2);
    client.createRole(requestorUserName, roleName2);
    client.grantRoleToGroup(requestorUserName, group1, roleName2);
    client.grantRoleToGroup(requestorUserName, group2, roleName2);

    client.grantTablePrivilege(requestorUserName, roleName2, "server", "db1", "table1", "ALL");
    client.grantTablePrivilege(requestorUserName, roleName2, "server", "db1", "table2", "ALL");
    client.grantTablePrivilege(requestorUserName, roleName2, "server", "db2", "table3", "ALL");
    client.grantTablePrivilege(requestorUserName, roleName2, "server", "db2", "table4", "ALL");
    client.grantTablePrivilege(requestorUserName, roleName2, "server", "db3", "table5", "ALL");

    Set<TSentryPrivilege> listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, roleName2, Lists.newArrayList(new Server("server"), new Database("db1")));
    assertEquals("Privilege not assigned to role2 !!", 2, listPrivilegesByRoleName.size());

    listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, roleName2, Lists.newArrayList(new Server("server"), new Database("db2"), new Table("table1")));
    assertEquals("Privilege not assigned to role2 !!", 0, listPrivilegesByRoleName.size());

    listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, roleName2, Lists.newArrayList(new Server("server"), new Database("db1"), new Table("table1")));
    assertEquals("Privilege not assigned to role2 !!", 1, listPrivilegesByRoleName.size());

    listPrivilegesByRoleName = client.listPrivilegesByRoleName(requestorUserName, roleName2, Lists.newArrayList(new Server("server"), new Database("db3")));
    assertEquals("Privilege not assigned to role2 !!", 1, listPrivilegesByRoleName.size());

    Set<String> listPrivilegesForProvider = client.listPrivilegesForProvider(Sets.newHashSet(group1, group2), ActiveRoleSet.ALL, new Server("server"), new Database("db2"));
    Assert.assertEquals("Privilege not correctly assigned to roles !!",
        Sets.newHashSet("server=server->db=db2->table=table4->action=ALL", "server=server->db=db2->table=table3->action=ALL"),
        listPrivilegesForProvider);

    listPrivilegesForProvider = client.listPrivilegesForProvider(Sets.newHashSet(group1, group2), ActiveRoleSet.ALL, new Server("server"), new Database("db3"));
    Assert.assertEquals("Privilege not correctly assigned to roles !!", Sets.newHashSet("server=server->db=db3->table=table5->action=ALL"), listPrivilegesForProvider);

    listPrivilegesForProvider = client.listPrivilegesForProvider(Sets.newHashSet(group1, group2), new ActiveRoleSet(Sets.newHashSet(roleName1)), new Server("server"), new Database("db3"));
    Assert.assertEquals("Privilege not correctly assigned to roles !!", Sets.newHashSet("server=+"), listPrivilegesForProvider);

    listPrivilegesForProvider = client.listPrivilegesForProvider(Sets.newHashSet(group1, group2), new ActiveRoleSet(Sets.newHashSet(roleName1)), new Server("server1"));
    Assert.assertEquals("Privilege not correctly assigned to roles !!", new HashSet<String>(), listPrivilegesForProvider);
  }



  /**
   * Create role, add privileges and grant it to a group drop the role and
   * verify the privileges are no longer visible recreate the role with same
   * name and verify the privileges again.
   * @throws Exception
   */
  @Test
  public void testDropRole() throws Exception {
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
    writePolicyFile();
    String roleName = "admin_r";

    // create role and add privileges
    client.dropRoleIfExists(requestorUserName, roleName);
    client.createRole(requestorUserName, roleName);
    client.grantRoleToGroup(requestorUserName, ADMIN_GROUP, roleName);
    client.grantDatabasePrivilege(requestorUserName, roleName, "server1", "db2", AccessConstants.ALL);
    client.grantTablePrivilege(requestorUserName, roleName, "server1", "db3", "tab3", "ALL");
    assertEquals(2, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());

    // drop role and verify privileges
    client.dropRole(requestorUserName, roleName);
    assertEquals(0, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());

    // recreate the role
    client.createRole(requestorUserName, roleName);
    client.grantRoleToGroup(requestorUserName, ADMIN_GROUP, roleName);
    assertEquals(0, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());

    // grant different privileges and verify
    client.grantDatabasePrivilege(requestorUserName, roleName, "server1", "db2", AccessConstants.ALL);
    assertEquals(1, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());
    client.dropRole(requestorUserName, roleName);
    assertEquals(0, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());
    assertEquals(0, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());
  }
}