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
import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.Test;

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
    client.grantDatabasePrivilege(requestorUserName, roleName, "server1", "db2");
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
    client.grantDatabasePrivilege(requestorUserName, roleName, "server1", "db2");
    assertEquals(1, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());
    client.dropRole(requestorUserName, roleName);
    assertEquals(0, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());
    assertEquals(0, client.listPrivilegesForProvider(requestorUserGroupNames,
            ActiveRoleSet.ALL).size());
  }
}