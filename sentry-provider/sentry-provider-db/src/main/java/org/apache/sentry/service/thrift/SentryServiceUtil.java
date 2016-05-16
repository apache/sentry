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

package org.apache.sentry.service.thrift;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;

import com.google.common.collect.Lists;

public final class SentryServiceUtil {

  // parse the privilege in String and get the TSentryPrivilege as result
  public static TSentryPrivilege convertToTSentryPrivilege(String privilegeStr) {
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
      } else if (PolicyFileConstants.PRIVILEGE_ACTION_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setAction(value);
      } else if (PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME.equalsIgnoreCase(key)) {
        TSentryGrantOption grantOption = "true".equalsIgnoreCase(value) ? TSentryGrantOption.TRUE
            : TSentryGrantOption.FALSE;
        tSentryPrivilege.setGrantOption(grantOption);
      }
    }
    tSentryPrivilege.setPrivilegeScope(getPrivilegeScope(tSentryPrivilege));
    return tSentryPrivilege;
  }

  /**
   * Parse the object path from string to map.
   * @param objectPath the string format as db=db1->table=tbl1
   * @return Map
   */
  public static Map<String, String> parseObjectPath(String objectPath) {
    Map<String, String> objectMap = new HashMap<String, String>();
    if (StringUtils.isEmpty(objectPath)) {
      return objectMap;
    }
    for (String kvStr : SentryConstants.AUTHORIZABLE_SPLITTER.split(objectPath)) {
      KeyValue kv = new KeyValue(kvStr);
      String key = kv.getKey();
      String value = kv.getValue();

      if (PolicyFileConstants.PRIVILEGE_DATABASE_NAME.equalsIgnoreCase(key)) {
        objectMap.put(PolicyFileConstants.PRIVILEGE_DATABASE_NAME, value);
      } else if (PolicyFileConstants.PRIVILEGE_TABLE_NAME.equalsIgnoreCase(key)) {
        objectMap.put(PolicyFileConstants.PRIVILEGE_TABLE_NAME, value);
      }
    }
    return objectMap;
  }

  // for the different hierarchy for hive:
  // 1: server->url
  // 2: server->database->table->column
  // if both of them are found in the privilege string, the privilege scope will be set as
  // PrivilegeScope.URI
  public static String getPrivilegeScope(TSentryPrivilege tSentryPrivilege) {
    PrivilegeScope privilegeScope = PrivilegeScope.SERVER;
    if (!StringUtils.isEmpty(tSentryPrivilege.getURI())) {
      privilegeScope = PrivilegeScope.URI;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getColumnName())) {
      privilegeScope = PrivilegeScope.COLUMN;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getTableName())) {
      privilegeScope = PrivilegeScope.TABLE;
    } else if (!StringUtils.isEmpty(tSentryPrivilege.getDbName())) {
      privilegeScope = PrivilegeScope.DATABASE;
    }
    return privilegeScope.toString();
  }

  // convert TSentryPrivilege to privilege in string
  public static String convertTSentryPrivilegeToStr(TSentryPrivilege tSentryPrivilege) {
    List<String> privileges = Lists.newArrayList();
    if (tSentryPrivilege != null) {
      String serverName = tSentryPrivilege.getServerName();
      String dbName = tSentryPrivilege.getDbName();
      String tableName = tSentryPrivilege.getTableName();
      String columnName = tSentryPrivilege.getColumnName();
      String uri = tSentryPrivilege.getURI();
      String action = tSentryPrivilege.getAction();
      String grantOption = (tSentryPrivilege.getGrantOption() == TSentryGrantOption.TRUE ? "true"
          : "false");
      if (!StringUtils.isEmpty(serverName)) {
        privileges.add(SentryConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_SERVER_NAME,
            serverName));
        if (!StringUtils.isEmpty(uri)) {
          privileges.add(SentryConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_URI_NAME,
              uri));
        } else if (!StringUtils.isEmpty(dbName)) {
          privileges.add(SentryConstants.KV_JOINER.join(
              PolicyFileConstants.PRIVILEGE_DATABASE_NAME, dbName));
          if (!StringUtils.isEmpty(tableName)) {
            privileges.add(SentryConstants.KV_JOINER.join(
                PolicyFileConstants.PRIVILEGE_TABLE_NAME, tableName));
            if (!StringUtils.isEmpty(columnName)) {
              privileges.add(SentryConstants.KV_JOINER.join(
                  PolicyFileConstants.PRIVILEGE_COLUMN_NAME, columnName));
            }
          }
        }
        if (!StringUtils.isEmpty(action)) {
          privileges.add(SentryConstants.KV_JOINER.join(
              PolicyFileConstants.PRIVILEGE_ACTION_NAME, action));
        }
      }
      // only append the grant option to privilege string if it's true
      if ("true".equals(grantOption)) {
        privileges.add(SentryConstants.KV_JOINER.join(
            PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME, grantOption));
      }
    }
    return SentryConstants.AUTHORIZABLE_JOINER.join(privileges);
  }

  private SentryServiceUtil() {
    // Make constructor private to avoid instantiation
  }

}
