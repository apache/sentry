/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.metastore;

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.core.common.utils.SentryConstants.PRIVILEGE_PREFIX;
import static org.apache.sentry.core.common.utils.SentryConstants.ROLE_SPLITTER;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.DBModelAuthorizables;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.api.service.thrift.TSentryRole;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.StaticUserGroup;
import org.apache.tools.ant.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class SentryPolicyProviderForDb extends PolicyFile {
  private static final Logger LOG = LoggerFactory.getLogger(SentryPolicyProviderForDb.class);
  protected static final Set<String> ADMIN_GROUP_SET = Sets
      .newHashSet(StaticUserGroup.ADMINGROUP);
  private SentryPolicyServiceClient sentryClient;

  protected SentryPolicyServiceClient getSentryClient() {
    return sentryClient;
  }

  public SentryPolicyProviderForDb(SentryPolicyServiceClient sentryClient) {
    this.sentryClient = sentryClient;
  }

  public static SentryPolicyProviderForDb setAdminOnServer1(String admin,
      SentryPolicyServiceClient sentryClient)
      throws Exception {
    SentryPolicyProviderForDb policyFile = new SentryPolicyProviderForDb(
        sentryClient);
    policyFile.addRolesToGroup(admin, "admin_role").addPermissionsToRole(
        "admin_role", "server=server1");
    return policyFile;
  }

  public void write(File file) throws Exception {
    super.write(file);
    if (!usingSentryService()) {
      return;
    }

    // remove existing metadata
    for (TSentryRole tRole : sentryClient.listAllRoles(StaticUserGroup.ADMIN1)) {
      sentryClient.dropRole(StaticUserGroup.ADMIN1, tRole.getRoleName());
    }

    // create roles and add privileges
    for (Entry<String, Collection<String>> roleEntry : getRolesToPermissions()
        .asMap().entrySet()) {
      sentryClient.createRole(StaticUserGroup.ADMIN1, roleEntry.getKey());
      for (String privilege : roleEntry.getValue()) {
        addPrivilege(roleEntry.getKey(), privilege);
      }
    }

    // grant roles to groups
    for (Entry<String, Collection<String>> groupEntry : getGroupsToRoles().asMap()
        .entrySet()) {
      for (String roleNames : groupEntry.getValue()) {
        for (String roleName : roleNames.split(",")) {
          try {
            sentryClient
                .grantRoleToGroup(StaticUserGroup.ADMIN1, groupEntry.getKey(), roleName);
          } catch (SentryUserException e) {
            LOG.warn("Error granting role " + roleName + " to group "
                + groupEntry.getKey());
          }
        }
      }
    }
  }

  private void addPrivilege(String roleName, String privileges)
      throws Exception {
    String serverName = null, dbName = null, tableName = null, columnName = null, uriPath = null;
    String action = AccessConstants.ALL;
    for (String privilege : ROLE_SPLITTER.split(privileges)) {
      for (String section : AUTHORIZABLE_SPLITTER.split(privilege)) {
        // action is not an authorizeable
        if (!section.toLowerCase().startsWith(PRIVILEGE_PREFIX)) {
          DBModelAuthorizable dbAuthorizable = DBModelAuthorizables
              .from(section);
          if (dbAuthorizable == null) {
            throw new IOException("Unknow Auth type " + section);
          }

          if (AuthorizableType.Server.equals(dbAuthorizable.getAuthzType())) {
            serverName = dbAuthorizable.getName();
          } else if (AuthorizableType.Db.equals(dbAuthorizable.getAuthzType())) {
            dbName = dbAuthorizable.getName();
          } else if (AuthorizableType.Table.equals(dbAuthorizable
              .getAuthzType())) {
            tableName = dbAuthorizable.getName();
          } else if (AuthorizableType.URI.equals(dbAuthorizable.getAuthzType())) {
            uriPath = dbAuthorizable.getName();
          } else if (AuthorizableType.Column.equals(dbAuthorizable.getAuthzType())) {
            columnName = dbAuthorizable.getName();
          } else {
            throw new IOException("Unsupported auth type "
                + dbAuthorizable.getName() + " : "
                + dbAuthorizable.getTypeName());
          }
        } else {
          action = DBModelAction
              .valueOf(
                  StringUtils.removePrefix(section, PRIVILEGE_PREFIX)
                      .toUpperCase()).toString();
        }
      }

      if (columnName != null) {
        sentryClient.grantColumnPrivilege(StaticUserGroup.ADMIN1, roleName, serverName, dbName,
            tableName, columnName, action);
      } else if (tableName != null) {
        sentryClient.grantTablePrivilege(StaticUserGroup.ADMIN1, roleName, serverName, dbName,
            tableName, action);
      } else if (dbName != null) {
        sentryClient.grantDatabasePrivilege(StaticUserGroup.ADMIN1, roleName, serverName,
            dbName, action);
      } else if (uriPath != null) {
        sentryClient.grantURIPrivilege(StaticUserGroup.ADMIN1, roleName, serverName, uriPath);
      } else if (serverName != null) {
        sentryClient.grantServerPrivilege(StaticUserGroup.ADMIN1, roleName, serverName, action);
      }
    }

  }

  private boolean usingSentryService() {
    return sentryClient != null;
  }
}
