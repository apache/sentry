/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.sentry.api.service.thrift;

import static org.apache.sentry.core.common.utils.SentryUtils.isNULL;

import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.model.db.AccessConstants;

/**
 * Utilities for Sentry policy store functions.
 */
public class SentryPolicyStoreUtils {
  // Private to avoid instantiate this class
  private SentryPolicyStoreUtils() {
  }

  /**
   * Return true if this privilege implies other privilege (copied from MSentryPrivilege.implies).
   * Otherwise, return false
   *
   * @param privilege, privilege to compare against the other privilege
   * @param other, the other privilege
   * @return True if both privilege are the same; False otherwise.
   */
  static boolean privilegeImplies(TSentryPrivilege privilege, TSentryPrivilege other) {
    // serverName never be null
    if (isNULL(privilege.getServerName()) || isNULL(other.getServerName())) {
      return false;
    } else if (!privilege.getServerName().equals(other.getServerName())) {
      return false;
    }

    // check URI implies
    if (!isNULL(privilege.getURI()) && !isNULL(other.getURI())) {
      if (!PathUtils.impliesURI(privilege.getURI(), other.getURI())) {
        return false;
      }
      // if URI is NULL, check dbName and tableName
    } else if (isNULL(privilege.getURI()) && isNULL(other.getURI())) {
      if (!isNULL(privilege.getDbName())) {
        if (isNULL(other.getDbName())) {
          return false;
        } else if (!privilege.getDbName().equals(other.getDbName())) {
          return false;
        }
      }
      if (!isNULL(privilege.getTableName())) {
        if (isNULL(other.getTableName())) {
          return false;
        } else if (!privilege.getTableName().equals(other.getTableName())) {
          return false;
        }
      }
      if (!isNULL(privilege.getColumnName())) {
        if (isNULL(other.getColumnName())) {
          return false;
        } else if (!privilege.getColumnName().equals(other.getColumnName())) {
          return false;
        }
      }
      // if URI is not NULL, but other's URI is NULL, return false
    } else if (!isNULL(privilege.getURI()) && isNULL(other.getURI())){
      return false;
    }

    // check action implies
    String action = privilege.getAction();
    if (!action.equalsIgnoreCase(AccessConstants.ALL)
      && !action.equalsIgnoreCase(AccessConstants.OWNER)
      && !action.equalsIgnoreCase(other.getAction())
      && !action.equalsIgnoreCase(AccessConstants.ACTION_ALL)) {
      return false;
    }

    return true;
  }
}
