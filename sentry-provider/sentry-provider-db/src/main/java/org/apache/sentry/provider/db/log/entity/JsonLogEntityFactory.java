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

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.log.util.CommandUtil;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeResponse;
import org.apache.sentry.provider.db.service.thrift.TCreateSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TCreateSentryRoleResponse;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleResponse;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;

public class JsonLogEntityFactory {

  private static JsonLogEntityFactory factory = new JsonLogEntityFactory();

  private JsonLogEntityFactory() {
  };

  public static JsonLogEntityFactory getInstance() {
    return factory;
  }

  public JsonLogEntity createJsonLogEntity(TCreateSentryRoleRequest request,
      TCreateSentryRoleResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    amle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(
        request.getRoleName(), true));

    return amle;
  }

  public JsonLogEntity createJsonLogEntity(TDropSentryRoleRequest request,
      TDropSentryRoleResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    amle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(
        request.getRoleName(), false));

    return amle;
  }

  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleGrantPrivilegeRequest request,
      TAlterSentryRoleGrantPrivilegeResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    amle.setOperationText(CommandUtil.createCmdForGrantPrivilege(request));
    TSentryPrivilege privilege = request.getPrivilege();
    amle.setDatabaseName(privilege.getDbName());
    amle.setTableName(privilege.getTableName());
    amle.setResourcePath(privilege.getURI());

    return amle;
  }

  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    amle.setOperationText(CommandUtil.createCmdForRevokePrivilege(request));
    TSentryPrivilege privilege = request.getPrivilege();
    amle.setDatabaseName(privilege.getDbName());
    amle.setTableName(privilege.getTableName());
    amle.setResourcePath(privilege.getURI());

    return amle;
  }

  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleAddGroupsRequest request,
      TAlterSentryRoleAddGroupsResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    amle.setOperationText(CommandUtil.createCmdForRoleAddGroup(request));

    return amle;
  }

  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleDeleteGroupsRequest request,
      TAlterSentryRoleDeleteGroupsResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    amle.setOperationText(CommandUtil.createCmdForRoleDeleteGroup(request));

    return amle;
  }

  public String isAllowed(TSentryResponseStatus status) {
    if (status.equals(Status.OK())) {
      return Constants.TRUE;
    }
    return Constants.FALSE;
  }

  private AuditMetadataLogEntity createCommonAMLE(Configuration conf,
      TSentryResponseStatus responseStatus, String userName,
      String requestClassName) {
    AuditMetadataLogEntity amle = new AuditMetadataLogEntity();
    amle.setUserName(userName);
    amle.setServiceName(conf.get(ServerConfig.SENTRY_SERVICE_NAME,
        ServerConfig.SENTRY_SERVICE_NAME_DEFAULT).trim());
    amle.setImpersonator(CommandUtil.getImpersonator());
    amle.setIpAddress(CommandUtil.getIpAddress());
    amle.setOperation(Constants.requestTypeToOperationMap.get(requestClassName));
    amle.setEventTime(Long.toString(System.currentTimeMillis()));
    amle.setAllowed(isAllowed(responseStatus));
    amle.setObjectType(Constants.requestTypeToObjectTypeMap
        .get(requestClassName));
    return amle;
  }
}
