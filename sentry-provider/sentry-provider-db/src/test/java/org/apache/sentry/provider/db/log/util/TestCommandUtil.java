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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.api.generic.thrift.TAuthorizable;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.api.service.thrift.TSentryGrantOption;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestCommandUtil extends Assert {

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

    String createRoleAddGroupCmdResult = CommandUtil.createCmdForRoleAddGroup("testRole",
        getGroupStr(1));
    String createRoleAddGroupCmdExcepted = "GRANT ROLE testRole TO GROUP testGroup1";
    String createRoleDeleteGroupCmdResult = CommandUtil.createCmdForRoleDeleteGroup("testRole",
        getGroupStr(1));
    String createRoleDeleteGroupCmdExcepted = "REVOKE ROLE testRole FROM GROUP testGroup1";

    assertEquals(createRoleAddGroupCmdExcepted, createRoleAddGroupCmdResult);
    assertEquals(createRoleDeleteGroupCmdExcepted,
        createRoleDeleteGroupCmdResult);
  }

  @Test
  public void testCreateCmdForRoleAddOrDeleteGroup2() {
    String createRoleAddGroupCmdResult = CommandUtil.createCmdForRoleAddGroup("testRole",
        getGroupStr(3));
    String createRoleAddGroupCmdExcepted = "GRANT ROLE testRole TO GROUP testGroup1, testGroup2, testGroup3";
    String createRoleDeleteGroupCmdResult = CommandUtil.createCmdForRoleDeleteGroup("testRole",
        getGroupStr(3));
    String createRoleDeleteGroupCmdExcepted = "REVOKE ROLE testRole FROM GROUP testGroup1, testGroup2, testGroup3";

    assertEquals(createRoleAddGroupCmdExcepted, createRoleAddGroupCmdResult);
    assertEquals(createRoleDeleteGroupCmdExcepted,
        createRoleDeleteGroupCmdResult);
  }

  @Test
  public void testCreateCmdForRoleAddOrDeleteUser1() {
    String createRoleAddGroupCmdResult =
        CommandUtil.createCmdForRoleAddUser("testRole", getUserStr(1));
    String createRoleAddGroupCmdExcepted = "GRANT ROLE testRole TO USER testUser1";
    String createRoleDeleteGroupCmdResult =
        CommandUtil.createCmdForRoleDeleteUser("testRole", getUserStr(1));
    String createRoleDeleteGroupCmdExcepted = "REVOKE ROLE testRole FROM USER testUser1";

    assertEquals(createRoleAddGroupCmdExcepted, createRoleAddGroupCmdResult);
    assertEquals(createRoleDeleteGroupCmdExcepted, createRoleDeleteGroupCmdResult);
  }

  @Test
  public void testCreateCmdForRoleAddOrDeleteUser2() {
    String createRoleAddGroupCmdResult =
        CommandUtil.createCmdForRoleAddUser("testRole", getUserStr(3));
    String createRoleAddGroupCmdExcepted =
        "GRANT ROLE testRole TO USER testUser1, testUser2, testUser3";
    String createRoleDeleteGroupCmdResult =
        CommandUtil.createCmdForRoleDeleteUser("testRole", getUserStr(3));
    String createRoleDeleteGroupCmdExcepted =
        "REVOKE ROLE testRole FROM USER testUser1, testUser2, testUser3";

    assertEquals(createRoleAddGroupCmdExcepted, createRoleAddGroupCmdResult);
    assertEquals(createRoleDeleteGroupCmdExcepted, createRoleDeleteGroupCmdResult);
  }

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege1() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.ALL,
        PrivilegeScope.DATABASE.name(), "dbTest", "tableTest", "serverTest",
        "hdfs://namenode:port/path/to/dir");
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

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
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

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
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

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
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

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
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

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
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

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
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

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

  @Test
  public void testCreateCmdForGrantOrRevokePrivilege8() {
    TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantPrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokePrivilegeRequest();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.SELECT, PrivilegeScope.SERVER.name(),
        "dbTest", "tableTest", "serverTest", "hdfs://namenode:port/path/to/dir");
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    grantRequest.setPrivileges(privileges);
    revokeRequest.setPrivileges(privileges);

    String createGrantPrivilegeCmdResult = CommandUtil.createCmdForGrantPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT SELECT ON SERVER serverTest TO ROLE testRole WITH GRANT OPTION";
    String createRevokePrivilegeCmdResult = CommandUtil.createCmdForRevokePrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE SELECT ON SERVER serverTest FROM ROLE testRole WITH GRANT OPTION";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted, createRevokePrivilegeCmdResult);
  }

  // generate the command without grant option
  @Test
  public void testCreateCmdForGrantOrRevokeGMPrivilege1() {
    org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantGMPrivilegeRequest();
    org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokeGMPrivilegeRequest();
    org.apache.sentry.api.generic.thrift.TSentryPrivilege privilege = getGMPrivilege();
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil.createCmdForGrantGMPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 TO ROLE testRole";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokeGMPrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 FROM ROLE testRole";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted, createRevokePrivilegeCmdResult);
  }

  // generate the command with grant option
  @Test
  public void testCreateCmdForGrantOrRevokeGMPrivilege2() {
    org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest grantRequest = getGrantGMPrivilegeRequest();
    org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest revokeRequest = getRevokeGMPrivilegeRequest();
    org.apache.sentry.api.generic.thrift.TSentryPrivilege privilege = getGMPrivilege();
    privilege
        .setGrantOption(org.apache.sentry.api.generic.thrift.TSentryGrantOption.TRUE);
    grantRequest.setPrivilege(privilege);
    revokeRequest.setPrivilege(privilege);

    String createGrantPrivilegeCmdResult = CommandUtil.createCmdForGrantGMPrivilege(grantRequest);
    String createGrantPrivilegeCmdExcepted = "GRANT ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 TO ROLE testRole WITH GRANT OPTION";
    String createRevokePrivilegeCmdResult = CommandUtil
        .createCmdForRevokeGMPrivilege(revokeRequest);
    String createRevokePrivilegeCmdExcepted = "REVOKE ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 FROM ROLE testRole WITH GRANT OPTION";

    assertEquals(createGrantPrivilegeCmdExcepted, createGrantPrivilegeCmdResult);
    assertEquals(createRevokePrivilegeCmdExcepted, createRevokePrivilegeCmdResult);
  }

  private String getGroupStr(int num) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < num; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("testGroup" + (i + 1));
    }
    return sb.toString();
  }

  private String getUserStr(int num) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < num; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("testUser" + (i + 1));
    }
    return sb.toString();
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

  private org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest getGrantGMPrivilegeRequest() {
    org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest request = new org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest();
    request.setRoleName("testRole");
    return request;
  }

  private org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest getRevokeGMPrivilegeRequest() {
    org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest request = new org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest();
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

  private org.apache.sentry.api.generic.thrift.TSentryPrivilege getGMPrivilege() {
    org.apache.sentry.api.generic.thrift.TSentryPrivilege privilege = new org.apache.sentry.api.generic.thrift.TSentryPrivilege();
    privilege.setAction("ACTION");
    privilege.setComponent("COMPONENT");
    List<TAuthorizable> authorizables = new ArrayList<TAuthorizable>();
    authorizables.add(new TAuthorizable("resourceType1", "resourceName1"));
    authorizables.add(new TAuthorizable("resourceType2", "resourceName2"));
    privilege.setAuthorizables(authorizables);
    return privilege;
  }
}
