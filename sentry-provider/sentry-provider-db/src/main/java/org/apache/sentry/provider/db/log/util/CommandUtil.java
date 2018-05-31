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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.api.generic.thrift.TAuthorizable;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.api.service.thrift.TSentryGrantOption;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.datanucleus.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

public final class CommandUtil {
    
  public CommandUtil() {
    // Make constructor private to avoid instantiation
  }

  public static String createCmdForCreateOrDropRole(String roleName,
      boolean isCreate) {
    if (isCreate) {
      return "CREATE ROLE " + roleName;
    }
    return "DROP ROLE " + roleName;
  }

  public static String createCmdForRoleAddGroup(String roleName, String groups) {
    return createCmdForRoleGrant(roleName, groups, true, true);
  }

  public static String createCmdForRoleDeleteGroup(String roleName, String groups) {
    return createCmdForRoleGrant(roleName, groups, false, true);
  }

  private static String createCmdForRoleGrant(String roleName, String principals,
      boolean isGrant, boolean isGroup) {
    StringBuilder sb = new StringBuilder();
    if (isGrant) {
      sb.append("GRANT ROLE ");
    } else {
      sb.append("REVOKE ROLE ");
    }
    sb.append(roleName);
    if (isGrant) {
      sb.append(" TO ");
    } else {
      sb.append(" FROM ");
    }

    String principalType = isGroup ? "GROUP" : "USER";
    if (!StringUtils.isEmpty(principals)) {
      sb.append(principalType).append(" ").append(principals);
    } else {
      sb = new StringBuilder("Missing " + principalType + " information.");
    }

    return sb.toString();
  }

  public static String createCmdForRoleAddUser(String roleName, String users) {
    return createCmdForRoleGrant(roleName, users, true, false);
  }

  public static String createCmdForRoleDeleteUser(String roleName, String users) {
    return createCmdForRoleGrant(roleName, users, false, false);
  }

  public static String createCmdForGrantPrivilege(
      TAlterSentryRoleGrantPrivilegeRequest request) {
    return createCmdForGrantOrRevokePrivileges(request.getRoleName(),
        request.getPrivileges(), true);
  }

  public static String createCmdForRevokePrivilege(
      TAlterSentryRoleRevokePrivilegeRequest request) {
    return createCmdForGrantOrRevokePrivileges(request.getRoleName(),
        request.getPrivileges(), false);
  }

  private static String createCmdForGrantOrRevokePrivileges(String roleName,
      Set<TSentryPrivilege> privileges, boolean isGrant) {
    StringBuilder sb = new StringBuilder();
    if (privileges != null) {
      for (TSentryPrivilege privilege : privileges) {
        sb.append(createCmdForGrantOrRevokePrivilege(roleName, privilege, isGrant));
      }
    }
    return sb.toString();
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

    if (privilege.getGrantOption() == TSentryGrantOption.TRUE) {
      sb.append(" WITH GRANT OPTION");
    }

    return sb.toString();
  }

  public static String createCmdForGrantGMPrivilege(
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleGrantPrivilegeRequest request) {
    return createCmdForGrantOrRevokeGMPrivilege(request.getRoleName(), request.getPrivilege(), true);
  }

  public static String createCmdForRevokeGMPrivilege(
      org.apache.sentry.api.generic.thrift.TAlterSentryRoleRevokePrivilegeRequest request) {
    return createCmdForGrantOrRevokeGMPrivilege(request.getRoleName(), request.getPrivilege(),
        false);
  }

  private static String createCmdForGrantOrRevokeGMPrivilege(String roleName,
      org.apache.sentry.api.generic.thrift.TSentryPrivilege privilege,
      boolean isGrant) {
    StringBuilder sb = new StringBuilder();
    if (isGrant) {
      sb.append("GRANT ");
    } else {
      sb.append("REVOKE ");
    }

    String action = privilege.getAction();
    if (AccessConstants.ALL.equalsIgnoreCase(action)) {
      sb.append("ALL");
    } else {
      if (action != null) {
        action = action.toUpperCase();
      }
      sb.append(action);
    }

    sb.append(" ON");

    List<TAuthorizable> authorizables = privilege.getAuthorizables();
    if (authorizables != null) {
      for (TAuthorizable authorizable : authorizables) {
        sb.append(" ").append(authorizable.getType()).append(" ").append(authorizable.getName());
      }
    }

    if (isGrant) {
      sb.append(" TO ROLE ");
    } else {
      sb.append(" FROM ROLE ");
    }
    sb.append(roleName);

    if (privilege.getGrantOption() == org.apache.sentry.api.generic.thrift.TSentryGrantOption.TRUE) {
      sb.append(" WITH GRANT OPTION");
    }

    return sb.toString();
  }

  // Check if the given IP is one of the local IP.
  @VisibleForTesting
  public static boolean assertIPInAuditLog(String ipInAuditLog) throws Exception {
    if (ipInAuditLog == null) {
      return false;
    }
    Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
    while (netInterfaces.hasMoreElements()) {
      NetworkInterface ni = netInterfaces.nextElement();
      Enumeration<InetAddress> ips = ni.getInetAddresses();
      while (ips.hasMoreElements()) {
        if (ipInAuditLog.indexOf(ips.nextElement().getHostAddress()) != -1) {
          return true;
        }
      }
    }
    return false;
  }
}
