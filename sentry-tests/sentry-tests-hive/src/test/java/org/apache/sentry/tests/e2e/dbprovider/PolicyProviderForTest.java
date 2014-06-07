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
package org.apache.sentry.tests.e2e.dbprovider;

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.provider.common.ProviderConstants.PRIVILEGE_PREFIX;
import static org.apache.sentry.tests.e2e.hive.StaticUserGroup.ADMIN1;
import static org.apache.sentry.tests.e2e.hive.StaticUserGroup.ADMINGROUP;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.policy.db.DBModelAuthorizables;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.tools.ant.util.StringUtils;
import org.mortbay.log.Log;

import com.google.common.collect.Sets;

public class PolicyProviderForTest extends PolicyFile {
  protected static final Set<String> ADMIN_GROUP_SET = Sets
      .newHashSet(ADMINGROUP);
  private static SentryPolicyServiceClient sentryClient;

  protected SentryPolicyServiceClient getSentryClient() {
    return sentryClient;
  }

  protected static void setSentryClient(
      SentryPolicyServiceClient newSentryClient) {
    sentryClient = newSentryClient;
  }

  public static void clearSentryClient() {
    sentryClient = null;
  }

  public static PolicyProviderForTest setAdminOnServer1(String admin)
      throws Exception {
    PolicyProviderForTest policyFile = new PolicyProviderForTest();
    policyFile.addRolesToGroup(admin, "admin_role")
        .addPermissionsToRole("admin_role", "server=server1");
    return policyFile;
  }

  public void write(File file) throws Exception {
    super.write(file);
    if (!usingSentryService()) {
      return;
    }

    // remove existing metadata
    for (TSentryRole tRole : sentryClient.listRoles(ADMIN1)) {
      sentryClient.dropRole(ADMIN1, tRole.getRoleName());
    }

    // create roles and add privileges
    for (Entry<String, Collection<String>> roleEntry : rolesToPermissions
        .asMap().entrySet()) {
      sentryClient.createRole(ADMIN1, roleEntry.getKey());
      for (String privilege : roleEntry.getValue()) {
        addPrivilege(roleEntry.getKey(), privilege);
      }
    }

    // grant roles to groups
    for (Entry<String, Collection<String>> groupEntry : groupsToRoles.asMap()
        .entrySet()) {
      for (String roleNames : groupEntry.getValue()) {
        for (String roleName : roleNames.split(",")) {
          try {
            sentryClient
                .grantRoleToGroup(ADMIN1, groupEntry.getKey(), roleName);
          } catch (SentryUserException e) {
            Log.warn("Error granting role " + roleName + " to group "
                + groupEntry.getKey());
          }
        }
      }
    }
  }

  private void addPrivilege(String roleName, String privileges) throws Exception {
    String serverName = null, dbName = null, tableName = null, uriPath = null;
    String action = AccessConstants.ALL;

    for(String section : AUTHORIZABLE_SPLITTER.split(privileges)) {
      // action is not an authorizeable
      if(!section.toLowerCase().startsWith(PRIVILEGE_PREFIX)) {
        DBModelAuthorizable dbAuthorizable = DBModelAuthorizables.from(section);
        if(dbAuthorizable == null) {
          throw new IOException("Unknow Auth type " + section);
        }

        if (AuthorizableType.Server.equals(dbAuthorizable.getAuthzType())) {
          serverName = dbAuthorizable.getName();
        } else if (AuthorizableType.Db.equals(dbAuthorizable.getAuthzType())) {
          dbName = dbAuthorizable.getName();
        } else if (AuthorizableType.Table.equals(dbAuthorizable.getAuthzType())) {
          tableName = dbAuthorizable.getName();
        } else if (AuthorizableType.URI.equals(dbAuthorizable.getAuthzType())) {
          uriPath = dbAuthorizable.getName();
        } else {
          throw new IOException("Unsupported auth type " + dbAuthorizable.getName()
              + " : " + dbAuthorizable.getTypeName());
        }
      } else {
        action = DBModelAction.valueOf(
            StringUtils.removePrefix(section, PRIVILEGE_PREFIX).toUpperCase())
            .toString();
      }
    }

    if (tableName != null) {
      sentryClient.grantTablePrivilege(ADMIN1, roleName, serverName, dbName,
          tableName, action);
    } else if (dbName != null) {
      sentryClient.grantDatabasePrivilege(ADMIN1, roleName, serverName, dbName);
    } else if (uriPath != null) {
      sentryClient.grantURIPrivilege(ADMIN1, roleName, serverName, uriPath);
    } else if (serverName != null) {
      sentryClient.grantServerPrivilege(ADMIN1, roleName, serverName);
      ;
    }
  }

  private boolean usingSentryService() {
    return sentryClient != null;
  }
}
