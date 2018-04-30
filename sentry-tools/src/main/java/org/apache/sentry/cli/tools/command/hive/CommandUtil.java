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
package org.apache.sentry.cli.tools.command.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants;

public final class CommandUtil {

  public static final String SPLIT_CHAR = ",";

  private CommandUtil() {
    // Make constructor private to avoid instantiation
  }

  // check the privilege value for the specific privilege scope
  // eg, for the table scope, server and database can't be empty
  public static void validatePrivilegeHierarchy(TSentryPrivilege tSentryPrivilege) throws IllegalArgumentException {
    String serverName = tSentryPrivilege.getServerName();
    String dbName = tSentryPrivilege.getDbName();
    String tableName = tSentryPrivilege.getTableName();
    String columnName = tSentryPrivilege.getColumnName();
    String uri = tSentryPrivilege.getURI();
    if (ServiceConstants.PrivilegeScope.SERVER.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      if (StringUtils.isEmpty(serverName)) {
        throw new IllegalArgumentException("The hierarchy of privilege is not correct.");
      }
    } else if (ServiceConstants.PrivilegeScope.URI.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      if (StringUtils.isEmpty(serverName) || StringUtils.isEmpty(uri)) {
        throw new IllegalArgumentException("The hierarchy of privilege is not correct.");
      }
    } else if (ServiceConstants.PrivilegeScope.DATABASE.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      if (StringUtils.isEmpty(serverName) || StringUtils.isEmpty(dbName)) {
        throw new IllegalArgumentException("The hierarchy of privilege is not correct.");
      }
    } else if (ServiceConstants.PrivilegeScope.TABLE.toString().equals(tSentryPrivilege.getPrivilegeScope())) {
      if (StringUtils.isEmpty(serverName) || StringUtils.isEmpty(dbName)
              || StringUtils.isEmpty(tableName)) {
        throw new IllegalArgumentException("The hierarchy of privilege is not correct.");
      }
    } else if (ServiceConstants.PrivilegeScope.COLUMN.toString().equals(tSentryPrivilege.getPrivilegeScope())
      && (StringUtils.isEmpty(serverName) || StringUtils.isEmpty(dbName)
              || StringUtils.isEmpty(tableName) || StringUtils.isEmpty(columnName))) {
        throw new IllegalArgumentException("The hierarchy of privilege is not correct.");
    }
  }
}
