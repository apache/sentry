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

package org.apache.sentry.provider.db.log.entity;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleAddGroupsResponse;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleDeleteGroupsResponse;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleGrantPrivilegeResponse;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleRevokePrivilegeResponse;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TCreateSentryRoleRequest;
import org.apache.sentry.provider.db.generic.service.thrift.TCreateSentryRoleResponse;
import org.apache.sentry.provider.db.generic.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.provider.db.generic.service.thrift.TDropSentryRoleResponse;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.Status;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJsonLogEntityFactoryGM {

  private static Configuration conf;
  private static String TEST_IP = "localhost/127.0.0.1";
  private static String TEST_IMPERSONATOR = "impersonator";
  private static String TEST_ROLE_NAME = "testRole";
  private static String TEST_USER_NAME = "requestUser";
  private static String TEST_GROUP = "testGroup";
  private static String TEST_ACTION = "action";
  private static String TEST_COMPONENT = "component";
  private static Map<String, String> TEST_PRIVILEGES_MAP = new HashMap<String, String>();

  @BeforeClass
  public static void init() {
    conf = new Configuration();
    conf.set(ServerConfig.SENTRY_SERVICE_NAME, ServerConfig.SENTRY_SERVICE_NAME_DEFAULT);
    ThriftUtil.setIpAddress(TEST_IP);
    ThriftUtil.setImpersonator(TEST_IMPERSONATOR);
    TEST_PRIVILEGES_MAP.put("resourceType1", "resourceName1");
    TEST_PRIVILEGES_MAP.put("resourceType2", "resourceName2");
    TEST_PRIVILEGES_MAP.put("resourceType3", "resourceName3");
  }

  @Test
  public void testCreateRole() {
    TCreateSentryRoleRequest request = new TCreateSentryRoleRequest();
    TCreateSentryRoleResponse response = new TCreateSentryRoleResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    response.setStatus(Status.OK());
    GMAuditMetadataLogEntity amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_CREATE_ROLE, "CREATE ROLE testRole",
        Constants.OBJECT_TYPE_ROLE, new HashMap<String, String>());

    response.setStatus(Status.InvalidInput("", null));
    amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance().createJsonLogEntity(
        request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_CREATE_ROLE, "CREATE ROLE testRole",
        Constants.OBJECT_TYPE_ROLE, new HashMap<String, String>());
  }

  @Test
  public void testDropRole() {
    TDropSentryRoleRequest request = new TDropSentryRoleRequest();
    TDropSentryRoleResponse response = new TDropSentryRoleResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    response.setStatus(Status.OK());
    GMAuditMetadataLogEntity amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory
        .getInstance().createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_DROP_ROLE, "DROP ROLE testRole",
        Constants.OBJECT_TYPE_ROLE, new HashMap<String, String>());

    response.setStatus(Status.InvalidInput("", null));
    amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance().createJsonLogEntity(
        request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_DROP_ROLE, "DROP ROLE testRole",
        Constants.OBJECT_TYPE_ROLE, new HashMap<String, String>());
  }

  @Test
  public void testGrantRole() {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);

    TAlterSentryRoleGrantPrivilegeResponse response = new TAlterSentryRoleGrantPrivilegeResponse();

    TSentryPrivilege privilege = getPrivilege();
    request.setPrivilege(privilege);
    response.setStatus(Status.OK());
    GMAuditMetadataLogEntity amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(
        request, response, conf);
    assertCommon(
        amle,
        Constants.TRUE,
        Constants.OPERATION_GRANT_PRIVILEGE,
        "GRANT ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 resourceType3 resourceName3 TO ROLE testRole",
        Constants.OBJECT_TYPE_PRINCIPAL, TEST_PRIVILEGES_MAP);

    response.setStatus(Status.InvalidInput("", null));
    amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance().createJsonLogEntity(
        request, response, conf);
    assertCommon(
        amle,
        Constants.FALSE,
        Constants.OPERATION_GRANT_PRIVILEGE,
        "GRANT ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 resourceType3 resourceName3 TO ROLE testRole",
        Constants.OBJECT_TYPE_PRINCIPAL, TEST_PRIVILEGES_MAP);
  }

  @Test
  public void testRevokeRole() {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeResponse response = new TAlterSentryRoleRevokePrivilegeResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);

    TSentryPrivilege privilege = getPrivilege();
    request.setPrivilege(privilege);
    response.setStatus(Status.OK());
    GMAuditMetadataLogEntity amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, response, conf);
    assertCommon(
        amle,
        Constants.TRUE,
        Constants.OPERATION_REVOKE_PRIVILEGE,
        "REVOKE ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 resourceType3 resourceName3 FROM ROLE testRole",
        Constants.OBJECT_TYPE_PRINCIPAL, TEST_PRIVILEGES_MAP);

    response.setStatus(Status.InvalidInput("", null));
    amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance().createJsonLogEntity(
        request, response, conf);

    assertCommon(
        amle,
        Constants.FALSE,
        Constants.OPERATION_REVOKE_PRIVILEGE,
        "REVOKE ACTION ON resourceType1 resourceName1 resourceType2 resourceName2 resourceType3 resourceName3 FROM ROLE testRole",
        Constants.OBJECT_TYPE_PRINCIPAL, TEST_PRIVILEGES_MAP);
  }

  @Test
  public void testAddRole() {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest();
    TAlterSentryRoleAddGroupsResponse response = new TAlterSentryRoleAddGroupsResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    request.setGroups(getGroups());
    response.setStatus(Status.OK());
    GMAuditMetadataLogEntity amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_ADD_ROLE,
        "GRANT ROLE testRole TO GROUP testGroup", Constants.OBJECT_TYPE_ROLE,
        new HashMap<String, String>());

    response.setStatus(Status.InvalidInput("", null));
    amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance().createJsonLogEntity(
        request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_ADD_ROLE,
        "GRANT ROLE testRole TO GROUP testGroup", Constants.OBJECT_TYPE_ROLE,
        new HashMap<String, String>());
  }

  @Test
  public void testDeleteRole() {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest();
    TAlterSentryRoleDeleteGroupsResponse response = new TAlterSentryRoleDeleteGroupsResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    request.setGroups(getGroups());
    response.setStatus(Status.OK());
    GMAuditMetadataLogEntity amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory
        .getInstance().createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_DELETE_ROLE,
        "REVOKE ROLE testRole FROM GROUP testGroup", Constants.OBJECT_TYPE_ROLE,
        new HashMap<String, String>());

    response.setStatus(Status.InvalidInput("", null));
    amle = (GMAuditMetadataLogEntity) JsonLogEntityFactory.getInstance().createJsonLogEntity(
        request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_DELETE_ROLE,
        "REVOKE ROLE testRole FROM GROUP testGroup", Constants.OBJECT_TYPE_ROLE,
        new HashMap<String, String>());
  }

  private void assertCommon(GMAuditMetadataLogEntity amle, String allowedExcepted,
      String operationExcepted, String operationTextExcepted, String objectTypeExcepted,
      Map<String, String> privilegesExcepted) {
    assertEquals(ServerConfig.SENTRY_SERVICE_NAME_DEFAULT, amle.getServiceName());
    assertEquals(TEST_IP, amle.getIpAddress());
    assertEquals(TEST_USER_NAME, amle.getUserName());
    assertEquals(TEST_IMPERSONATOR, amle.getImpersonator());
    assertEquals(allowedExcepted, amle.getAllowed());
    assertEquals(operationExcepted, amle.getOperation());
    assertEquals(operationTextExcepted, amle.getOperationText());
    assertEquals(objectTypeExcepted, amle.getObjectType());
    assertPrivilegesMap(privilegesExcepted, amle.getPrivilegesMap());
  }

  private void assertPrivilegesMap(Map<String, String> privilegesExcepted,
      Map<String, String> privilegesActual) {
    assertEquals(privilegesExcepted.size(), privilegesActual.size());
    for (Map.Entry<String, String> privilege : privilegesExcepted.entrySet()) {
      assertEquals(privilege.getValue(), privilegesActual.get(privilege.getKey()));
    }
  }

  private TSentryPrivilege getPrivilege() {
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setAction(TEST_ACTION);
    privilege.setComponent(TEST_COMPONENT);
    List<TAuthorizable> authorizables = new ArrayList<TAuthorizable>();
    authorizables.add(new TAuthorizable("resourceType1", "resourceName1"));
    authorizables.add(new TAuthorizable("resourceType2", "resourceName2"));
    authorizables.add(new TAuthorizable("resourceType3", "resourceName3"));
    privilege.setAuthorizables(authorizables);
    return privilege;
  }

  private Set<String> getGroups() {
    Set<String> groups = new HashSet<String>();
    groups.add(TEST_GROUP);
    return groups;
  }
}
