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

import java.util.Iterator;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;

public class CommandUtil {

  public static String createCmdForCreateOrDropRole(String roleName,
      boolean isCreate) {
    if (isCreate) {
      return "CREATE ROLE " + roleName;
    }
    return "DROP ROLE " + roleName;
  }

  public static String createCmdForRoleAddGroup(
      TAlterSentryRoleAddGroupsRequest request) {
    return createCmdForRoleAddOrDeleteGroup(request.getRoleName(),
        request.getGroupsIterator(), true);
  }

  public static String createCmdForRoleDeleteGroup(
      TAlterSentryRoleDeleteGroupsRequest request) {
    return createCmdForRoleAddOrDeleteGroup(request.getRoleName(),
        request.getGroupsIterator(), false);
  }

  private static String createCmdForRoleAddOrDeleteGroup(String roleName,
      Iterator<TSentryGroup> iter, boolean isAddGroup) {
    StringBuilder sb = new StringBuilder();
    if (isAddGroup) {
      sb.append("GRANT ROLE ");
    } else {
      sb.append("REVOKE ROLE ");
    }
    sb.append(roleName);
    if (isAddGroup) {
      sb.append(" TO ");
    } else {
      sb.append(" FROM ");
    }

    if (iter != null) {
      sb.append("GROUP ");
      boolean commaFlg = false;
      while (iter.hasNext()) {
        if (commaFlg) {
          sb.append(", ");
        } else {
          commaFlg = true;
        }
        sb.append(iter.next().getGroupName());
      }
    } else {
      sb = new StringBuilder("Missing group information.");
    }

    return sb.toString();
  }

  public static String createCmdForGrantPrivilege(
      TAlterSentryRoleGrantPrivilegeRequest request) {
    return createCmdForGrantOrRevokePrivilege(request.getRoleName(),
        request.getPrivilege(), true);
  }

  public static String createCmdForRevokePrivilege(
      TAlterSentryRoleRevokePrivilegeRequest request) {
    return createCmdForGrantOrRevokePrivilege(request.getRoleName(),
        request.getPrivilege(), false);
  }

  private static String createCmdForGrantOrRevokePrivilege(String roleName,
      TSentryPrivilege privilege, boolean isGrant) {
    StringBuilder sb = new StringBuilder();
    if (isGrant) {
      sb.append("GRANT ");
    } else {
      sb.append("REVOKE ");
    }

    String action = privilege.getAction();
    String privilegeScope = privilege.getPrivilegeScope();
    if (AccessConstants.ALL.equalsIgnoreCase(action)) {
      sb.append("ALL");
    } else {
      if (action != null) {
        action = action.toUpperCase();
      }
      sb.append(action);
    }

    sb.append(" ON ").append(privilege.getPrivilegeScope()).append(" ");
    if (PrivilegeScope.DATABASE.name().equalsIgnoreCase(privilegeScope)) {
      sb.append(privilege.getDbName());
    } else if (PrivilegeScope.TABLE.name().equalsIgnoreCase(privilegeScope)) {
      sb.append(privilege.getTableName());
    } else if (PrivilegeScope.SERVER.name().equalsIgnoreCase(privilegeScope)) {
      sb.append(privilege.getServerName());
    } else if (PrivilegeScope.URI.name().equalsIgnoreCase(privilegeScope)) {
      sb.append(privilege.getURI());
    }

    if (isGrant) {
      sb.append(" TO ROLE ");
    } else {
      sb.append(" FROM ROLE ");
    }
    sb.append(roleName);

    return sb.toString();
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return "";
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  private static ThreadLocal<String> threadLocalImpersonator = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return "";
    }
  };

  public static void setImpersonator(String impersonator) {
    threadLocalImpersonator.set(impersonator);
  }

  public static String getImpersonator() {
    return threadLocalImpersonator.get();
  }
}
