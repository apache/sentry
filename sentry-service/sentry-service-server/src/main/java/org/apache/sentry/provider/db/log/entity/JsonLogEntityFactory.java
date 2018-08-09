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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.generic.thrift.TAuthorizable;
import org.apache.sentry.api.service.thrift.TSentryAuthorizable;
import org.apache.sentry.api.service.thrift.TSentryPrincipalType;
import org.apache.sentry.provider.db.log.util.CommandUtil;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleAddGroupsResponse;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleAddUsersRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleAddUsersResponse;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleDeleteGroupsResponse;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleDeleteUsersRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleDeleteUsersResponse;
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
import org.apache.sentry.service.thrift.TSentryResponseStatus;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

public final class JsonLogEntityFactory {

  private static JsonLogEntityFactory factory = new JsonLogEntityFactory();

  private JsonLogEntityFactory() {
  }

  public static JsonLogEntityFactory getInstance() {
    return factory;
  }

  // log entity for hive/impala create role
  public JsonLogEntity createJsonLogEntity(TCreateSentryRoleRequest request,
      TCreateSentryRoleResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(
        request.getRoleName(), true));

    return hamle;
  }

  // log entity for hive/impala drop role
  public JsonLogEntity createJsonLogEntity(TDropSentryRoleRequest request,
      TDropSentryRoleResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(
        request.getRoleName(), false));

    return hamle;
  }

  // log entity for hive/impala grant privilege
  public Set<JsonLogEntity> createJsonLogEntities(
      TAlterSentryRoleGrantPrivilegeRequest request,
      TAlterSentryRoleGrantPrivilegeResponse response, Configuration conf) {
    ImmutableSet.Builder<JsonLogEntity> setBuilder = ImmutableSet.builder();
    if (request.isSetPrivileges()) {
      for (TSentryPrivilege privilege : request.getPrivileges()) {
        JsonLogEntity logEntity = createJsonLogEntity(request, privilege, response, conf);
        setBuilder.add(logEntity);
      }
    }
    return setBuilder.build();
  }

  private JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleGrantPrivilegeRequest request, TSentryPrivilege privilege,
      TAlterSentryRoleGrantPrivilegeResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForGrantPrivilege(request));
    hamle.setDatabaseName(privilege.getDbName());
    hamle.setTableName(privilege.getTableName());
    hamle.setResourcePath(privilege.getURI());
    return hamle;
  }

  // log entity for hive/impala revoke privilege
  public Set<JsonLogEntity> createJsonLogEntities(
      TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response, Configuration conf) {
    ImmutableSet.Builder<JsonLogEntity> setBuilder = ImmutableSet.builder();
    if (request.isSetPrivileges()) {
      for (TSentryPrivilege privilege : request.getPrivileges()) {
        JsonLogEntity logEntity = createJsonLogEntity(request, privilege, response, conf);
        setBuilder.add(logEntity);
      }
    }
    return setBuilder.build();
  }

  private JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleRevokePrivilegeRequest request, TSentryPrivilege privilege,
      TAlterSentryRoleRevokePrivilegeResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForRevokePrivilege(request));
    hamle.setDatabaseName(privilege.getDbName());
    hamle.setTableName(privilege.getTableName());
    hamle.setResourcePath(privilege.getURI());

    return hamle;
  }

  // log entity for hive/impala add role to group
  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleAddGroupsRequest request,
      TAlterSentryRoleAddGroupsResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String groups = getGroupsStr(request.getGroupsIterator());
    hamle.setOperationText(CommandUtil.createCmdForRoleAddGroup(request.getRoleName(), groups));

    return hamle;
  }

  // log entity for hive/impala delete role from group
  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleDeleteGroupsRequest request,
      TAlterSentryRoleDeleteGroupsResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String groups = getGroupsStr(request.getGroupsIterator());
    hamle.setOperationText(CommandUtil.createCmdForRoleDeleteGroup(request.getRoleName(), groups));

    return hamle;
  }

  public JsonLogEntity createJsonLogEntity(String operationType, String objectType,
    String requestorUserName, TSentryResponseStatus status, TSentryAuthorizable authorizable,
    TSentryPrincipalType ownerType, String ownerName, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, status, requestorUserName,
      operationType, objectType);

    if (Constants.OPERATION_GRANT_OWNER_PRIVILEGE.equals(operationType)) {
      hamle.setOperationText(CommandUtil.createCmdForImplicitGrantOwnerPrivilege(ownerType, ownerName, authorizable));
    } else if (Constants.OPERATION_TRANSFER_OWNER_PRIVILEGE.equals(operationType)) {
      hamle.setOperationText(CommandUtil.createCmdForImplicitTransferOwnerPrivilege(ownerType, ownerName, authorizable));
    } else {
      hamle.setOperationText("Unknown operation");
    }

    hamle.setDatabaseName(authorizable.getDb());
    hamle.setTableName(authorizable.getTable());
    return hamle;
  }

  private String getGroupsStr(Iterator<TSentryGroup> iter) {
    StringBuilder groups = new StringBuilder("");
    if (iter != null) {
      boolean commaFlg = false;
      while (iter.hasNext()) {
        if (commaFlg) {
          groups.append(", ");
        } else {
          commaFlg = true;
        }
        groups.append(iter.next().getGroupName());
      }
    }
    return groups.toString();
  }

  public JsonLogEntity createJsonLogEntity(TAlterSentryRoleAddUsersRequest request,
      TAlterSentryRoleAddUsersResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String users = getUsersStr(request.getUsersIterator());
    amle.setOperationText(CommandUtil.createCmdForRoleAddUser(request.getRoleName(), users));

    return amle;
  }

  public JsonLogEntity createJsonLogEntity(TAlterSentryRoleDeleteUsersRequest request,
      TAlterSentryRoleDeleteUsersResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String users = getUsersStr(request.getUsersIterator());
    amle.setOperationText(CommandUtil.createCmdForRoleDeleteUser(request.getRoleName(), users));

    return amle;
  }

  private String getUsersStr(Iterator<String> iter) {
    StringBuilder users = new StringBuilder("");
    if (iter != null) {
      boolean commaFlg = false;
      while (iter.hasNext()) {
        if (commaFlg) {
          users.append(", ");
        } else {
          commaFlg = true;
        }
        users.append(iter.next());
      }
    }
    return users.toString();
  }

  public String isAllowed(TSentryResponseStatus status) {
    if (status.equals(Status.OK())) {
      return Constants.TRUE;
    }
    return Constants.FALSE;
  }

  // log entity for generic model create role
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.api.generic.thrift.TCreateSentryRoleRequest request,
      org.apache.sentry.api.generic.thrift.TCreateSentryRoleResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    gmamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(request.getRoleName(), true));

    return gmamle;
  }

  // log entity for generic model drop role
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.api.generic.thrift.TDropSentryRoleRequest request,
      org.apache.sentry.api.generic.thrift.TDropSentryRoleResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    gmamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(request.getRoleName(), false));

    return gmamle;
  }

  // log entity for generic model grant privilege
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest request,
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    if (request.getPrivilege() != null) {
      List<TAuthorizable> authorizables = request.getPrivilege().getAuthorizables();
      Map<String, String> privilegesMap = new LinkedHashMap<String, String>();
      if (authorizables != null) {
        for (TAuthorizable authorizable : authorizables) {
          privilegesMap.put(authorizable.getType(), authorizable.getName());
        }
      }
      gmamle.setPrivilegesMap(privilegesMap);
    }
    gmamle.setOperationText(CommandUtil.createCmdForGrantGMPrivilege(request));

    return gmamle;
  }

  // log entity for generic model revoke privilege
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest request,
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    if (request.getPrivilege() != null) {
      List<TAuthorizable> authorizables = request.getPrivilege().getAuthorizables();
      Map<String, String> privilegesMap = new LinkedHashMap<String, String>();
      if (authorizables != null) {
        for (TAuthorizable authorizable : authorizables) {
          privilegesMap.put(authorizable.getType(), authorizable.getName());
        }
      }
      gmamle.setPrivilegesMap(privilegesMap);
    }
    gmamle.setOperationText(CommandUtil.createCmdForRevokeGMPrivilege(request));

    return gmamle;
  }

  // log entity for generic model add role to group
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleAddGroupsRequest request,
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleAddGroupsResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    Joiner joiner = Joiner.on(",");
    String groups = joiner.join(request.getGroupsIterator());
    gmamle.setOperationText(CommandUtil.createCmdForRoleAddGroup(request.getRoleName(), groups));

    return gmamle;
  }

  // log entity for hive delete role from group
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleDeleteGroupsRequest request,
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleDeleteGroupsResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    Joiner joiner = Joiner.on(",");
    String groups = joiner.join(request.getGroupsIterator());
    gmamle.setOperationText(CommandUtil.createCmdForRoleDeleteGroup(request.getRoleName(), groups));

    return gmamle;
  }

  private DBAuditMetadataLogEntity createCommonHAMLE(Configuration conf,
      TSentryResponseStatus responseStatus, String userName, String requestClassName) {
    DBAuditMetadataLogEntity hamle = new DBAuditMetadataLogEntity();
    setCommAttrForAMLE(hamle, conf, responseStatus, userName, toOperationType(requestClassName),
      toObjectType(requestClassName));
    return hamle;
  }

  private DBAuditMetadataLogEntity createCommonHAMLE(Configuration conf,
    TSentryResponseStatus responseStatus, String userName, String operationType, String objectType) {
    DBAuditMetadataLogEntity hamle = new DBAuditMetadataLogEntity();
    setCommAttrForAMLE(hamle, conf, responseStatus, userName, operationType, objectType);
    return hamle;
  }

  private GMAuditMetadataLogEntity createCommonGMAMLE(Configuration conf,
      TSentryResponseStatus responseStatus, String userName, String requestClassName,
      String component) {
    GMAuditMetadataLogEntity gmamle = new GMAuditMetadataLogEntity();
    setCommAttrForAMLE(gmamle, conf, responseStatus, userName, toOperationType(requestClassName),
      toObjectType(requestClassName));
    gmamle.setComponent(component);
    return gmamle;
  }

  private void setCommAttrForAMLE(AuditMetadataLogEntity amle, Configuration conf,
      TSentryResponseStatus responseStatus, String userName, String operationType, String objectType) {
    amle.setUserName(userName);
    amle.setServiceName(conf.get(ServerConfig.SENTRY_SERVICE_NAME,
        ServerConfig.SENTRY_SERVICE_NAME_DEFAULT).trim());
    amle.setImpersonator(ThriftUtil.getImpersonator());
    amle.setIpAddress(ThriftUtil.getIpAddress());
    amle.setOperation(operationType);
    amle.setEventTime(Long.toString(System.currentTimeMillis()));
    amle.setAllowed(isAllowed(responseStatus));
    amle.setObjectType(objectType);
  }

  private String toOperationType(String className) {
    return Constants.requestTypeToOperationMap.get(className);
  }

  private String toObjectType(String className) {
    return Constants.requestTypeToObjectTypeMap.get(className);
  }
}
