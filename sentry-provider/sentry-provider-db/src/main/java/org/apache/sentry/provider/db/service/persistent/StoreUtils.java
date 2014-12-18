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

package org.apache.sentry.provider.db.service.persistent;

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_JOINER;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.provider.common.ProviderConstants;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class StoreUtils {

  public static String NULL_COL = "__NULL__";

  @VisibleForTesting
  public static String toAuthorizable(MSentryPrivilege privilege) {
    return toAuthorizable(privilege.getServerName(), privilege.getDbName(),
        privilege.getURI(), privilege.getTableName(), privilege.getColumnName(),
        privilege.getAction());
  }

  @VisibleForTesting
  public static String toAuthorizable(String serverName, String dbName,
      String uri, String tableName, String columnName, String action) {
    List<String> authorizable = new ArrayList<String>(4);
    authorizable.add(KV_JOINER.join(AuthorizableType.Server.name().toLowerCase(),
        serverName));
    if (isNULL(uri)) {
      if (!isNULL(dbName)) {
        authorizable.add(KV_JOINER.join(AuthorizableType.Db.name().toLowerCase(),
            dbName));
        if (!isNULL(tableName)) {
          authorizable.add(KV_JOINER.join(AuthorizableType.Table.name().toLowerCase(),
              tableName));
          if (!isNULL(columnName)) {
            authorizable.add(KV_JOINER.join(AuthorizableType.Column.name().toLowerCase(),
                columnName));
          }
        }
      }
    } else {
      authorizable.add(KV_JOINER.join(AuthorizableType.URI.name().toLowerCase(),
          uri));
    }
    if (!isNULL(action)
        && !action.equalsIgnoreCase(AccessConstants.ALL)) {
      authorizable
      .add(KV_JOINER.join(ProviderConstants.PRIVILEGE_NAME.toLowerCase(),
          action));
    }
    return AUTHORIZABLE_JOINER.join(authorizable);
  }

  @VisibleForTesting
  public static Set<String> toTrimedLower(Set<String> s) {
    if (null == s) return new HashSet<String>();
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }
  
  public static String toNULLCol(String s) {
    return Strings.isNullOrEmpty(s) ? NULL_COL : s;
  }

  public static String fromNULLCol(String s) {
    return isNULL(s) ? "" : s;
  }

  public static boolean isNULL(String s) {
    return Strings.isNullOrEmpty(s) || s.equals(NULL_COL);
  }
}
