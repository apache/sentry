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

import java.util.HashMap;
import java.util.Map;

import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TCreateSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleRequest;

public class Constants {
  public final static String AUDIT_LOGGER_NAME = "sentry.hive.authorization.ddl.logger";

  public final static String LOG_FIELD_SERVICE_NAME = "serviceName";
  public final static String LOG_FIELD_USER_NAME = "userName";
  public final static String LOG_FIELD_IMPERSONATOR = "impersonator";
  public final static String LOG_FIELD_IP_ADDRESS = "ipAddress";
  public final static String LOG_FIELD_OPERATION = "operation";
  public final static String LOG_FIELD_EVENT_TIME = "eventTime";
  public final static String LOG_FIELD_OPERATION_TEXT = "operationText";
  public final static String LOG_FIELD_ALLOWED = "allowed";
  public final static String LOG_FIELD_DATABASE_NAME = "databaseName";
  public final static String LOG_FIELD_TABLE_NAME = "tableName";
  public final static String LOG_FIELD_RESOURCE_PATH = "resourcePath";
  public final static String LOG_FIELD_OBJECT_TYPE = "objectType";

  public final static String OPERATION_CREATE_ROLE = "CREATE_ROLE";
  public final static String OPERATION_DROP_ROLE = "DROP_ROLE";
  public final static String OPERATION_ADD_ROLE = "ADD_ROLE_TO_GROUP";
  public final static String OPERATION_DELETE_ROLE = "DELETE_ROLE_FROM_GROUP";
  public final static String OPERATION_GRANT_PRIVILEGE = "GRANTE_PRIVILEGE";
  public final static String OPERATION_REVOKE_PRIVILEGE = "REVOKE_PRIVILEGE";

  public final static String OBJECT_TYPE_PRINCIPAL = "PRINCIPAL";
  public final static String OBJECT_TYPE_ROLE = "ROLE";

  public final static String TRUE = "true";
  public final static String FALSE = "false";

  public static final Map<String, String> requestTypeToOperationMap = new HashMap<String, String>();
  public static final Map<String, String> requestTypeToObjectTypeMap = new HashMap<String, String>();

  static {
    requestTypeToOperationMap.put(TCreateSentryRoleRequest.class.getName(),
        Constants.OPERATION_CREATE_ROLE);
    requestTypeToOperationMap.put(
        TAlterSentryRoleGrantPrivilegeRequest.class.getName(),
        Constants.OPERATION_GRANT_PRIVILEGE);
    requestTypeToOperationMap.put(
        TAlterSentryRoleRevokePrivilegeRequest.class.getName(),
        Constants.OPERATION_REVOKE_PRIVILEGE);
    requestTypeToOperationMap.put(TDropSentryRoleRequest.class.getName(),
        Constants.OPERATION_DROP_ROLE);
    requestTypeToOperationMap.put(
        TAlterSentryRoleAddGroupsRequest.class.getName(),
        Constants.OPERATION_ADD_ROLE);
    requestTypeToOperationMap.put(
        TAlterSentryRoleDeleteGroupsRequest.class.getName(),
        Constants.OPERATION_DELETE_ROLE);

    requestTypeToObjectTypeMap.put(TCreateSentryRoleRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE);
    requestTypeToObjectTypeMap.put(TDropSentryRoleRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE);
    requestTypeToObjectTypeMap.put(
        TAlterSentryRoleAddGroupsRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE);
    requestTypeToObjectTypeMap.put(
        TAlterSentryRoleDeleteGroupsRequest.class.getName(),
        Constants.OBJECT_TYPE_ROLE);
    requestTypeToObjectTypeMap.put(
        TAlterSentryRoleGrantPrivilegeRequest.class.getName(),
        Constants.OBJECT_TYPE_PRINCIPAL);
    requestTypeToObjectTypeMap.put(
        TAlterSentryRoleRevokePrivilegeRequest.class.getName(),
        Constants.OBJECT_TYPE_PRINCIPAL);
  }
}
