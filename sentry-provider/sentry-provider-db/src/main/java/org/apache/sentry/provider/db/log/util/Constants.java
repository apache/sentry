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

import java.util.Map;
import com.google.common.collect.ImmutableMap;

import org.apache.sentry.api.service.thrift.*;

public final class Constants {
  public static final String AUDIT_LOGGER_NAME = "sentry.hive.authorization.ddl.logger";
  public static final String AUDIT_LOGGER_NAME_GENERIC = "sentry.generic.authorization.ddl.logger";

  public static final String LOG_FIELD_SERVICE_NAME = "serviceName";
  public static final String LOG_FIELD_USER_NAME = "userName";
  public static final String LOG_FIELD_IMPERSONATOR = "impersonator";
  public static final String LOG_FIELD_IP_ADDRESS = "ipAddress";
  public static final String LOG_FIELD_OPERATION = "operation";
  public static final String LOG_FIELD_EVENT_TIME = "eventTime";
  public static final String LOG_FIELD_OPERATION_TEXT = "operationText";
  public static final String LOG_FIELD_ALLOWED = "allowed";
  public static final String LOG_FIELD_DATABASE_NAME = "databaseName";
  public static final String LOG_FIELD_TABLE_NAME = "tableName";
  public static final String LOG_FIELD_COLUMN_NAME = "column";
  public static final String LOG_FIELD_RESOURCE_PATH = "resourcePath";
  public static final String LOG_FIELD_OBJECT_TYPE = "objectType";
  public static final String LOG_FIELD_COMPONENT = "component";

  public static final String OPERATION_CREATE_ROLE = "CREATE_ROLE";
  public static final String OPERATION_DROP_ROLE = "DROP_ROLE";
  public static final String OPERATION_ADD_ROLE = "ADD_ROLE_TO_GROUP";
  public static final String OPERATION_DELETE_ROLE = "DELETE_ROLE_FROM_GROUP";
  public static final String OPERATION_ADD_ROLE_USER = "ADD_ROLE_TO_USER";
  public static final String OPERATION_DELETE_ROLE_USER = "DELETE_ROLE_FROM_USER";
  public static final String OPERATION_GRANT_PRIVILEGE = "GRANT_PRIVILEGE";
  public static final String OPERATION_REVOKE_PRIVILEGE = "REVOKE_PRIVILEGE";

  public static final String OBJECT_TYPE_PRINCIPAL = "PRINCIPAL";
  public static final String OBJECT_TYPE_ROLE = "ROLE";

  public static final String TRUE = "true";
  public static final String FALSE = "false";

  public static final Map<String, String> requestTypeToOperationMap = ImmutableMap.<String, String>builder()
    // for hive audit log
    .put(TCreateSentryRoleRequest.class.getName(), Constants.OPERATION_CREATE_ROLE)
    .put(TAlterSentryRoleGrantPrivilegeRequest.class.getName(), Constants.OPERATION_GRANT_PRIVILEGE)
    .put(TAlterSentryRoleRevokePrivilegeRequest.class.getName(), Constants.OPERATION_REVOKE_PRIVILEGE)
    .put(TDropSentryRoleRequest.class.getName(), Constants.OPERATION_DROP_ROLE)
    .put(TAlterSentryRoleAddGroupsRequest.class.getName(), Constants.OPERATION_ADD_ROLE)
    .put(TAlterSentryRoleDeleteGroupsRequest.class.getName(), Constants.OPERATION_DELETE_ROLE)
    .put(TAlterSentryRoleAddUsersRequest.class.getName(), Constants.OPERATION_ADD_ROLE_USER)
    .put(TAlterSentryRoleDeleteUsersRequest.class.getName(), Constants.OPERATION_DELETE_ROLE_USER)

    // for generic model audit log
    .put(org.apache.sentry.api.generic.thrift.TCreateSentryRoleRequest.class.getName(),
        Constants.OPERATION_CREATE_ROLE)
    .put(org.apache.sentry.api.generic.thrift.TDropSentryRoleRequest.class.getName(),
        Constants.OPERATION_DROP_ROLE)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest.class.getName(),
        Constants.OPERATION_GRANT_PRIVILEGE)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest.class.getName(),
        Constants.OPERATION_REVOKE_PRIVILEGE)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleAddGroupsRequest.class.getName(),
        Constants.OPERATION_ADD_ROLE)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleDeleteGroupsRequest.class.getName(),
        Constants.OPERATION_DELETE_ROLE)
    .build();
  
  public static final Map<String, String> requestTypeToObjectTypeMap = ImmutableMap.<String, String>builder()
    // for hive audit log
    .put(TCreateSentryRoleRequest.class.getName(), Constants.OBJECT_TYPE_ROLE)
    .put(TDropSentryRoleRequest.class.getName(), Constants.OBJECT_TYPE_ROLE)
    .put(TAlterSentryRoleAddGroupsRequest.class.getName(), Constants.OBJECT_TYPE_ROLE)
    .put(TAlterSentryRoleDeleteGroupsRequest.class.getName(), Constants.OBJECT_TYPE_ROLE)
    .put(TAlterSentryRoleAddUsersRequest.class.getName(), Constants.OBJECT_TYPE_ROLE)
    .put(TAlterSentryRoleDeleteUsersRequest.class.getName(), Constants.OBJECT_TYPE_ROLE)
    .put(TAlterSentryRoleGrantPrivilegeRequest.class.getName(), Constants.OBJECT_TYPE_PRINCIPAL)
    .put(TAlterSentryRoleRevokePrivilegeRequest.class.getName(), Constants.OBJECT_TYPE_PRINCIPAL)

    // for generic model audit log
    .put(org.apache.sentry.api.generic.thrift.TCreateSentryRoleRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE)
    .put(org.apache.sentry.api.generic.thrift.TDropSentryRoleRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleAddGroupsRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleDeleteGroupsRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest.class.getName(),
        Constants.OBJECT_TYPE_PRINCIPAL)
    .put(org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest.class.getName(),
        Constants.OBJECT_TYPE_PRINCIPAL)
    .build();

  private Constants() {
    // Make constructor private to avoid instantiation
  }

}
