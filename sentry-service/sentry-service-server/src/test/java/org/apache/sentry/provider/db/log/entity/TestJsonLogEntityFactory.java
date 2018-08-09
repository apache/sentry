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

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleAddGroupsResponse;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleDeleteGroupsResponse;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleGrantPrivilegeResponse;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleRevokePrivilegeResponse;
import org.apache.sentry.api.service.thrift.TCreateSentryRoleRequest;
import org.apache.sentry.api.service.thrift.TCreateSentryRoleResponse;
import org.apache.sentry.api.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.api.service.thrift.TDropSentryRoleResponse;
import org.apache.sentry.api.service.thrift.TSentryGroup;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.sentry.api.common.Status;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestJsonLogEntityFactory {

  private static Configuration conf;

  private static String TEST_IP = "localhost/127.0.0.1";
  private static String TEST_IMPERSONATOR = "impersonator";
  private static String TEST_ROLE_NAME = "testRole";
  private static String TEST_USER_NAME = "requestUser";
  private static String TEST_DATABASE_NAME = "testDB";
  private static String TEST_TABLE_NAME = "testTable";
  private static String TEST_GROUP = "testGroup";

  @BeforeClass
  public static void init() {
    conf = new Configuration();
    conf.set(ServerConfig.SENTRY_SERVICE_NAME,
        ServerConfig.SENTRY_SERVICE_NAME_DEFAULT);
    ThriftUtil.setIpAddress(TEST_IP);
    ThriftUtil.setImpersonator(TEST_IMPERSONATOR);
  }

  @Test
  public void testCreateRole() {
    TCreateSentryRoleRequest request = new TCreateSentryRoleRequest();
    TCreateSentryRoleResponse response = new TCreateSentryRoleResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    response.setStatus(Status.OK());
    DBAuditMetadataLogEntity amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory
        .getInstance().createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_CREATE_ROLE,
        "CREATE ROLE testRole", null, null, null, Constants.OBJECT_TYPE_ROLE);

    response.setStatus(Status.InvalidInput("", null));
    amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_CREATE_ROLE,
        "CREATE ROLE testRole", null, null, null, Constants.OBJECT_TYPE_ROLE);
  }

  @Test
  public void testDropRole() {
    TDropSentryRoleRequest request = new TDropSentryRoleRequest();
    TDropSentryRoleResponse response = new TDropSentryRoleResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    response.setStatus(Status.OK());
    DBAuditMetadataLogEntity amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory
        .getInstance().createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_DROP_ROLE,
        "DROP ROLE testRole", null, null, null, Constants.OBJECT_TYPE_ROLE);

    response.setStatus(Status.InvalidInput("", null));
    amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_DROP_ROLE,
        "DROP ROLE testRole", null, null, null, Constants.OBJECT_TYPE_ROLE);
  }

  @Test
  public void testGrantRole() {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);

    TAlterSentryRoleGrantPrivilegeResponse response = new TAlterSentryRoleGrantPrivilegeResponse();

    TSentryPrivilege privilege = getPrivilege(AccessConstants.ALL,
        PrivilegeScope.DATABASE.name(), TEST_DATABASE_NAME, null, null, null);
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    request.setPrivileges(privileges);
    response.setStatus(Status.OK());
    DBAuditMetadataLogEntity amle = new DBAuditMetadataLogEntity();
    Set<JsonLogEntity> amles =  JsonLogEntityFactory
        .getInstance().createJsonLogEntities(request, response, conf);
    assertEquals(amles.size(),1);
    amle = (DBAuditMetadataLogEntity) amles.iterator().next();

    assertCommon(amle, Constants.TRUE, Constants.OPERATION_GRANT_PRIVILEGE,
        "GRANT ALL ON DATABASE testDB TO ROLE testRole", TEST_DATABASE_NAME,
        null, null, Constants.OBJECT_TYPE_PRINCIPAL);

    privilege = getPrivilege(AccessConstants.ALL, PrivilegeScope.TABLE.name(),
        null, TEST_TABLE_NAME, null, null);
    privileges = Sets.newHashSet();
    privileges.add(privilege);
    request.setPrivileges(privileges);
    response.setStatus(Status.InvalidInput("", null));
    amles =  JsonLogEntityFactory.getInstance()
        .createJsonLogEntities(request, response, conf);
    assertEquals(amles.size(),1);
    amle = (DBAuditMetadataLogEntity) amles.iterator().next();

    assertCommon(amle, Constants.FALSE, Constants.OPERATION_GRANT_PRIVILEGE,
        "GRANT ALL ON TABLE testTable TO ROLE testRole", null, TEST_TABLE_NAME,
        null, Constants.OBJECT_TYPE_PRINCIPAL);
  }

  @Test
  public void testRevokeRole() {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeResponse response = new TAlterSentryRoleRevokePrivilegeResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);

    TSentryPrivilege privilege = getPrivilege(AccessConstants.ALL,
        PrivilegeScope.DATABASE.name(), TEST_DATABASE_NAME, null, null, null);
    Set<TSentryPrivilege> privileges = Sets.newHashSet();
    privileges.add(privilege);
    request.setPrivileges(privileges);
    response.setStatus(Status.OK());
    DBAuditMetadataLogEntity amle = new DBAuditMetadataLogEntity();
    Set<JsonLogEntity> amles =  JsonLogEntityFactory
        .getInstance().createJsonLogEntities(request, response, conf);
    assertEquals(amles.size(),1);
    amle = (DBAuditMetadataLogEntity) amles.iterator().next();

    assertCommon(amle, Constants.TRUE, Constants.OPERATION_REVOKE_PRIVILEGE,
        "REVOKE ALL ON DATABASE testDB FROM ROLE testRole", TEST_DATABASE_NAME,
        null, null, Constants.OBJECT_TYPE_PRINCIPAL);

    privilege = getPrivilege(AccessConstants.ALL, PrivilegeScope.TABLE.name(),
        null, TEST_TABLE_NAME, null, null);
    privileges = Sets.newHashSet();
    privileges.add(privilege);
    request.setPrivileges(privileges);
    response.setStatus(Status.InvalidInput("", null));
    amles =  JsonLogEntityFactory.getInstance()
        .createJsonLogEntities(request, response, conf);
    assertEquals(amles.size(),1);
    amle = (DBAuditMetadataLogEntity) amles.iterator().next();

    assertCommon(amle, Constants.FALSE, Constants.OPERATION_REVOKE_PRIVILEGE,
        "REVOKE ALL ON TABLE testTable FROM ROLE testRole", null,
        TEST_TABLE_NAME, null, Constants.OBJECT_TYPE_PRINCIPAL);
  }

  @Test
  public void testAddRole() {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest();
    TAlterSentryRoleAddGroupsResponse response = new TAlterSentryRoleAddGroupsResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    request.setGroups(getGroups());
    response.setStatus(Status.OK());
    DBAuditMetadataLogEntity amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory
        .getInstance().createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_ADD_ROLE,
        "GRANT ROLE testRole TO GROUP testGroup", null, null, null,
        Constants.OBJECT_TYPE_ROLE);

    response.setStatus(Status.InvalidInput("", null));
    amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_ADD_ROLE,
        "GRANT ROLE testRole TO GROUP testGroup", null, null, null,
        Constants.OBJECT_TYPE_ROLE);
  }

  @Test
  public void testDeleteRole() {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest();
    TAlterSentryRoleDeleteGroupsResponse response = new TAlterSentryRoleDeleteGroupsResponse();
    request.setRequestorUserName(TEST_USER_NAME);
    request.setRoleName(TEST_ROLE_NAME);
    request.setGroups(getGroups());
    response.setStatus(Status.OK());
    DBAuditMetadataLogEntity amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory
        .getInstance().createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.TRUE, Constants.OPERATION_DELETE_ROLE,
        "REVOKE ROLE testRole FROM GROUP testGroup", null, null, null,
        Constants.OBJECT_TYPE_ROLE);

    response.setStatus(Status.InvalidInput("", null));
    amle = (DBAuditMetadataLogEntity) JsonLogEntityFactory.getInstance()
        .createJsonLogEntity(request, response, conf);
    assertCommon(amle, Constants.FALSE, Constants.OPERATION_DELETE_ROLE,
        "REVOKE ROLE testRole FROM GROUP testGroup", null, null, null,
        Constants.OBJECT_TYPE_ROLE);
  }

  private void assertCommon(DBAuditMetadataLogEntity amle,
      String allowedExcepted, String operationExcepted,
      String operationTextExcepted, String databaseNameExcepted,
      String tableNameExcepted, String resourcePathExcepted,
      String objectTypeExcepted) {
    assertEquals(ServerConfig.SENTRY_SERVICE_NAME_DEFAULT,
        amle.getServiceName());
    assertEquals(TEST_IP, amle.getIpAddress());
    assertEquals(TEST_USER_NAME, amle.getUserName());
    assertEquals(TEST_IMPERSONATOR, amle.getImpersonator());
    assertEquals(allowedExcepted, amle.getAllowed());
    assertEquals(operationExcepted, amle.getOperation());
    assertEquals(operationTextExcepted, amle.getOperationText());
    assertEquals(tableNameExcepted, amle.getTableName());
    assertEquals(databaseNameExcepted, amle.getDatabaseName());
    assertEquals(resourcePathExcepted, amle.getResourcePath());
    assertEquals(objectTypeExcepted, amle.getObjectType());
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

  private Set<TSentryGroup> getGroups() {
    Set<TSentryGroup> groups = new LinkedHashSet<TSentryGroup>();
    TSentryGroup group = new TSentryGroup();
    group.setGroupName(TEST_GROUP);
    groups.add(group);
    return groups;
  }
}
