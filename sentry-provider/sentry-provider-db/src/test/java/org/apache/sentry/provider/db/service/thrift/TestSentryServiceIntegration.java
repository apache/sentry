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

import java.util.HashSet;
import java.util.Set;

import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.SimpleDBProviderBackend;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.apache.sentry.service.thrift.ServiceConstants.ThriftConstants;
import org.junit.Test;

import com.google.common.collect.Sets;


public class TestSentryServiceIntegration extends SentryServiceIntegrationBase {

  @Test
  public void testCreateRole() throws Exception {
    String requestorUserName = "user_1";
    Set<String> requestorUserGroupNames = new HashSet<String>();
    String roleName = "admin_r";

    client.dropRoleIfExists(requestorUserName, requestorUserGroupNames, roleName);

    client.createRole(requestorUserName, requestorUserGroupNames, roleName);

    TListSentryRolesRequest listReq = new TListSentryRolesRequest();
    listReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    listReq.setRoleName(roleName);
    listReq.setRequestorUserName(requestorUserName);
    TListSentryRolesResponse listResp = client.listRoleByName(listReq);
    Set<TSentryRole> roles = listResp.getRoles();
    assertEquals("Incorrect number of roles:" + roles, 1, roles.size());

    client.dropRole(requestorUserName, requestorUserGroupNames, roleName);
  }

  @Test
  public void testGrantRevokePrivilege() throws Exception {
    String server = "server1";
    String requestorUserName = "server_admin";
    Set<String> requestorUserGroupNames = new HashSet<String>();
    String roleName = "admin_testdb";
    String db = "testDB";
    String group = "group1";

    client.dropRoleIfExists(requestorUserName, requestorUserGroupNames, roleName);
    client.createRole(requestorUserName, requestorUserGroupNames, roleName);

    TListSentryRolesRequest listReq = new TListSentryRolesRequest();
    listReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    listReq.setRoleName("admin_testdb");
    listReq.setRequestorUserName(requestorUserName);
    TListSentryRolesResponse listResp = client.listRoleByName(listReq);
    Set<TSentryRole> roles = listResp.getRoles();
    assertEquals("Incorrect number of roles:" + roles, 1, roles.size());

    client.grantDatabasePrivilege(requestorUserName, requestorUserGroupNames, roleName, server, db);

    // verify we can get the privileges from the backend
    SimpleDBProviderBackend dbBackend = new SimpleDBProviderBackend(client);
    dbBackend.initialize(new ProviderBackendContext());
    assertEquals(Sets.newHashSet(), dbBackend.getPrivileges(Sets.newHashSet(group),
        new ActiveRoleSet(true)));
    client.grantRoleToGroup(requestorUserName, requestorUserGroupNames, group, roleName);
    assertEquals(Sets.newHashSet(), dbBackend.getPrivileges(Sets.newHashSet(group),
        new ActiveRoleSet(new HashSet<String>())));
    assertEquals(Sets.newHashSet("server="+ server + "->db=" + db + "->action=*"),
        dbBackend.getPrivileges(Sets.newHashSet("group1"),
        new ActiveRoleSet(true)));
    assertEquals(Sets.newHashSet("server="+ server + "->db=" + db + "->action=*"),
        dbBackend.getPrivileges(Sets.newHashSet(group),
        new ActiveRoleSet(Sets.newHashSet(roleName))));

    client.revokeDatabasePrivilege(requestorUserName, requestorUserGroupNames, roleName, server, db);
    client.dropRole(requestorUserName, requestorUserGroupNames, roleName);
  }

}
