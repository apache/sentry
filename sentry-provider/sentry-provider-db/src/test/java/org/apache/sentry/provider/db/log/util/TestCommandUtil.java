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

package org.apache.sentry.provider.db.log.util;

import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.junit.Test;

public class TestCommandUtil extends TestCase {

  @Test
  public void testCreateCmdForCreateOrDropRole() {
    String roleName = "testRole";

    String createRoleCmdResult = CommandUtil.createCmdForCreateOrDropRole(
        roleName, true);
    String dropRoleCmdResult = CommandUtil.createCmdForCreateOrDropRole(
        roleName, false);
    String createRoleCmdExcepted = "CREATE ROLE testRole";
    String dropRoleCmdExcepted = "DROP ROLE testRole";

    assertEquals(createRoleCmdExcepted, createRoleCmdResult);
    assertEquals(dropRoleCmdResult, dropRoleCmdExcepted);
  }

  @Test
  public void testCreateCmdForRoleAddOrDeleteGroup1() {

    TAlterSentryRoleAddGroupsRequest requestAdd = getRoleAddGroupsRequest();
    TAlterSentryRoleDeleteGroupsRequest requestDelete = getRoleDeleteGroupsRequest();

    Set<TSentryGroup> groups = getGroups(1);
    requestAdd.setGroups(groups);
    requestDelete.setGroups(groups);

    String createRoleAddGroupCmdResult = CommandUtil
        .createCmdForRoleAddGroup(requestAdd);
    String createRoleAddGroupCmdExcepted = "GRANT ROLE testRole TO GROUP testGroup1";
    String createRoleDeleteGroupCmdResult = CommandUtil
        .createCmdForRoleDeleteGroup(requestDelete);
    String createRoleDeleteGroupCmdExcepted = "REVOKE ROLE testRole FROM GROUP testGroup1";

    assertEquals(createRoleAddGroupCmdExcepted, createRoleAddGroupCmdResult);
    assertEquals(createRoleDeleteGroupCmdExcepted,
        createRoleDeleteGroupCmdResult);
  }

  @Test
  public void testCreateCmdForRoleAddOrDeleteGroup2() {

    TAlterSentryRoleAddGroupsRequest requestAdd = getRoleAddGroupsRequest();
    TAlterSentryRoleDeleteGroupsRequest requestDelete = getRoleDeleteGroupsRequest();

    Set<TSentryGroup> groups = getGroups(3);
    requestAdd.setGroups(groups);
    requestDelete.setGroups(groups);

    String createRoleAddGroupCmdResult = CommandUtil
        .createCmdForRoleAddGroup(requestAdd);
    String createRoleAddGroupCmdExcepted = "GRANT ROLE testRole TO GROUP testGroup1, testGroup2, testGroup3";
    String createRoleDeleteGroupCmdResult = CommandUtil
        .createCmdForRoleDeleteGroup(requestDelete);
    String createRoleDeleteGroupCmdExcepted = "REVOKE ROLE testRole FROM GROUP testGroup1, testGroup2, testGroup3";

    assertEquals(createRoleAddGroupCmdExcepted, createRoleAddGroupCmdResult);
    assertEquals(createRoleDeleteGroupCmdExcepted,
        createRoleDeleteGroupCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege1() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.ALL,
        PrivilegeScope.DATABASE.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil
        .createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT ALL ON DATABASE dbTest TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE ALL ON DATABASE dbTest FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted,
        createRevokePrivilegeCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege2() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.INSERT,
        PrivilegeScope.DATABASE.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil
        .createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT INSERT ON DATABASE dbTest TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE INSERT ON DATABASE dbTest FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted,
        createRevokePrivilegeCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege3() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.SELECT,
        PrivilegeScope.DATABASE.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil
        .createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT SELECT ON DATABASE dbTest TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE SELECT ON DATABASE dbTest FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted,
        createRevokePrivilegeCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege4() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(null,
        PrivilegeScope.DATABASE.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil
        .createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT null ON DATABASE dbTest TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE null ON DATABASE dbTest FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted,
        createRevokePrivilegeCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege5() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.SELECT,
        PrivilegeScope.TABLE.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil
        .createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT SELECT ON TABLE tableTest TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE SELECT ON TABLE tableTest FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted,
        createRevokePrivilegeCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege6() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.SELECT,
        PrivilegeScope.SERVER.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil
        .createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT SELECT ON SERVER serverTest TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE SELECT ON SERVER serverTest FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted,
        createRevokePrivilegeCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege7() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.SELECT,
        PrivilegeScope.URI.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil
        .createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT SELECT ON URI hdfs://namenode:port/path/to/dir TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE SELECT ON URI hdfs://namenode:port/path/to/dir FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted,
        createRevokePrivilegeCmdResult);
  }

  private TAlterSentryRoleAddGroupsRequest getRoleAddGroupsRequest() {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest();
    request.setRoleName("testRole");
    return request;
  }

  private TAlterSentryRoleDeleteGroupsRequest getRoleDeleteGroupsRequest() {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest();
    request.setRoleName("testRole");
    return request;
  }

  private Set<TSentryGroup> getGroups(int num) {
    Set<TSentryGroup> groups = new LinkedHashSet<TSentryGroup>();
    for (int i = 0; i < num; i++) {
      TSentryGroup group = new TSentryGroup();
      group.setGroupName("testGroup" + (i + 1));
      groups.add(group);
    }
    return groups;
  }

  private TAlterSentryRoleGrantPrivilegeRequest getGrantPrivilegeRequest() {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setRoleName("testRole");
    return request;
  }

  private TAlterSentryRoleRevokePrivilegeRequest getRevokePrivilegeRequest() {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    request.setRoleName("testRole");
    return request;
  }

  private TSentryPrivilege getPrivilege(String action, String privilegeScope,
      String dbName, String tableName, String serverName, String URI) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setAction(action);
    privilege.setPrivilegeScope(privilegeScope);
    privilege.setDbName(dbName);
    privilege.setTableName(tableName);
    privilege.setServerName(serverName);
    privilege.setURI(URI);
    return privilege;
  }
}
