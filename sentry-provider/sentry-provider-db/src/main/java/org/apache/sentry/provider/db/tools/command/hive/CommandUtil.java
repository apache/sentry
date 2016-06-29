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
package org.apache.sentry.provider.db.tools.command.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants;

public final class CommandUtil {

  public static final String SPLIT_CHAR = ",";
  
  private CommandUtil() {
    // Make constructor private to avoid instantiation
  }

  // parse the privilege in String and get the TSentryPrivilege as result
  public static TSentryPrivilege convertToTSentryPrivilege(String privilegeStr) throws Exception {
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    for (String authorizable : SentryConstants.AUTHORIZABLE_SPLITTER.split(privilegeStr)) {
      KeyValue tempKV = new KeyValue(authorizable);
      String key = tempKV.getKey();
      String value = tempKV.getValue();

      if (PolicyFileConstants.PRIVILEGE_SERVER_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setServerName(value);
      } else if (PolicyFileConstants.PRIVILEGE_DATABASE_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setDbName(value);
      } else if (PolicyFileConstants.PRIVILEGE_TABLE_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setTableName(value);
      } else if (PolicyFileConstants.PRIVILEGE_COLUMN_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setColumnName(value);
      } else if (PolicyFileConstants.PRIVILEGE_URI_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setURI(value);
        tSentryPrivilege.setAction(AccessConstants.ALL);
      } else if (PolicyFileConstants.PRIVILEGE_ACTION_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setAction(value);
      } else if (PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME.equalsIgnoreCase(key)) {
        TSentryGrantOption grantOption = "true".equalsIgnoreCase(value) ? TSentryGrantOption.TRUE
                : TSentryGrantOption.FALSE;
        tSentryPrivilege.setGrantOption(grantOption);
      }
    }
    tSentryPrivilege.setPrivilegeScope(getPrivilegeScope(tSentryPrivilege));
    validatePrivilegeHierarchy(tSentryPrivilege);
    return tSentryPrivilege;
  }

  // for the different hierarchy for hive:
  // 1: server->url
  // 2: server->database->table->column
  // if both of them are found in the privilege string, the privilege scope will be set as
  // PrivilegeScope.URI
  private static String getPrivilegeScope(TSentryPrivilege tSentryPrivilege) {
    ServiceConstants.PrivilegeScope privilegeScope = ServiceConstants.PrivilegeScope.SERVER;
    if (!StringUtils.isEmpty(tSentryPrivilege.getURI())) {
      privilegeScope = ServiceConstants.PrivilegeScope.URI;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getColumnName())) {
      privilegeScope = ServiceConstants.PrivilegeScope.COLUMN;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getTableName())) {
      privilegeScope = ServiceConstants.PrivilegeScope.TABLE;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getDbName())) {
      privilegeScope = ServiceConstants.PrivilegeScope.DATABASE;
    }
    return privilegeScope.toString();
  }

  // check the privilege value for the specific privilege scope
  // eg, for the table scope, server and database can't be empty
  private static void validatePrivilegeHierarchy(TSentryPrivilege tSentryPrivilege) throws Exception {
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
