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
import java.util.HashSet;
import java.util.Set;

import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.apache.sentry.service.thrift.ServiceConstants.ThriftConstants;
import org.apache.sentry.service.thrift.Status;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TestSentryServiceIntegration extends SentryServiceIntegrationBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestSentryServiceIntegration.class);

  @Test
  public void testCreateRole() throws Exception {
    Set<String> groupSet = new HashSet<String>();
    TDropSentryRoleRequest dropReq = new TDropSentryRoleRequest();
    dropReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    dropReq.setRoleName("admin_r");
    dropReq.setRequestorUserName("user_1");
    groupSet.add("admin");
    dropReq.setRequestorGroupName(groupSet);
    TDropSentryRoleResponse dropResp = client.dropRole(dropReq);
    assertStatus(Status.NO_SUCH_OBJECT, dropResp.getStatus());
    LOGGER.info("Successfully dropped role: admin_r");
    groupSet.clear();

    TCreateSentryRoleRequest createReq = new TCreateSentryRoleRequest();
    createReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    createReq.setRequestorUserName("user_1");
    groupSet.add("admin");
    createReq.setRequestorGroupName(groupSet);
    TSentryRole role = new TSentryRole();
    role.setRoleName("admin_r");
    role.setCreateTime(System.currentTimeMillis());
    role.setGrantorPrincipal("test");
    role.setPrivileges(new HashSet<TSentryPrivilege>());
    createReq.setRole(role);
    TCreateSentryRoleResponse createResp = client.createRole(createReq);
    assertOK(createResp.getStatus());
    LOGGER.info("Successfully create role: admin_r");
    groupSet.clear();

    TListSentryRolesRequest listReq = new TListSentryRolesRequest();
    listReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    listReq.setRoleName("admin_r");
    listReq.setRequestorUserName("user_1");
    groupSet.add("admin");
    listReq.setRequestorGroupName(groupSet);
    TListSentryRolesResponse listResp = client.listRoleByName(listReq);
    Set<TSentryRole> roles = listResp.getRoles();
    Preconditions.checkArgument(roles.size() == 1, "Incorrect number of roles");
    groupSet.clear();

    dropReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    dropReq.setRoleName("admin_r");
    dropReq.setRequestorUserName("user_1");
    groupSet.add("admin");
    dropReq.setRequestorGroupName(groupSet);
    dropResp = client.dropRole(dropReq);
    assertOK(dropResp.getStatus());
    LOGGER.info("Successfully dropped role: admin_r");
    groupSet.clear();
  }

  @Test
  public void testGrantRevokePrivilege() throws Exception {
    Set<String> groupSet = new HashSet<String>();
    TDropSentryRoleRequest dropReq = new TDropSentryRoleRequest();
    dropReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    dropReq.setRoleName("admin_testdb");
    dropReq.setRequestorUserName("server_admin");
    groupSet.add("admin");
    dropReq.setRequestorGroupName(groupSet);
    TDropSentryRoleResponse dropResp = client.dropRole(dropReq);
    assertStatus(Status.NO_SUCH_OBJECT, dropResp.getStatus());
    LOGGER.info("Successfully dropped role: admin_testdb");
    groupSet.clear();

    TCreateSentryRoleRequest createReq = new TCreateSentryRoleRequest();
    createReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    createReq.setRequestorUserName("server_admin");
    groupSet.add("admin");
    createReq.setRequestorGroupName(groupSet);
    TSentryRole role = new TSentryRole();
    role.setRoleName("admin_testdb");
    role.setCreateTime(System.currentTimeMillis());
    role.setGrantorPrincipal("server_admin");
    role.setPrivileges(new HashSet<TSentryPrivilege>());
    createReq.setRole(role);
    TCreateSentryRoleResponse createResp = client.createRole(createReq);
    assertOK(createResp.getStatus());
    LOGGER.info("Successfully create role: admin_testdb");
    groupSet.clear();

    TListSentryRolesRequest listReq = new TListSentryRolesRequest();
    listReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    listReq.setRoleName("admin_testdb");
    listReq.setRequestorUserName("server_admin");
    groupSet.add("admin");
    listReq.setRequestorGroupName(groupSet);
    TListSentryRolesResponse listResp = client.listRoleByName(listReq);
    Set<TSentryRole> roles = listResp.getRoles();
    Preconditions.checkArgument(roles.size() == 1, "Incorrect number of roles");
    groupSet.clear();

    TAlterSentryRoleGrantPrivilegeRequest grantReq = new TAlterSentryRoleGrantPrivilegeRequest();
    grantReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    grantReq.setRoleName("admin_testdb");
    grantReq.setRequestorUserName("server_admin");
    groupSet.add("admin");
    grantReq.setRequestorGroupName(groupSet);
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope("DB");
    privilege.setServerName("server1");
    privilege.setDbName("testDB");
    privilege.setAction("ALL");
    privilege.setGrantorPrincipal("server_admin");
    privilege.setCreateTime(System.currentTimeMillis());
    grantReq.setPrivilege(privilege);
    TAlterSentryRoleGrantPrivilegeResponse grantResp = client.grantPrivilege(grantReq);
    assertOK(grantResp.getStatus());
    LOGGER.info("Successfully granted privilege: " + privilege.toString());
    groupSet.clear();

    TAlterSentryRoleRevokePrivilegeRequest revokeReq = new TAlterSentryRoleRevokePrivilegeRequest();
    revokeReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    revokeReq.setRoleName("admin_testdb");
    revokeReq.setRequestorUserName("server_admin");
    groupSet.add("admin");
    revokeReq.setRequestorGroupName(groupSet);
    revokeReq.setPrivilege(privilege);
    TAlterSentryRoleRevokePrivilegeResponse revokeResp = client.revokePrivilege(revokeReq);
    assertOK(revokeResp.getStatus());
    LOGGER.info("Successfully revoked privilege: " + privilege.toString());
    groupSet.clear();

    dropReq.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    dropReq.setRoleName("admin_testdb");
    dropReq.setRequestorUserName("server_admin");
    groupSet.add("admin");
    dropReq.setRequestorGroupName(groupSet);
    dropResp = client.dropRole(dropReq);
    assertOK(dropResp.getStatus());
    LOGGER.info("Successfully dropped role: admin_testdb");
    groupSet.clear();
  }

}
