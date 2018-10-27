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
package org.apache.sentry.api.service.thrift;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.common.ApiConstants;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.common.Status;
import org.apache.sentry.api.common.ThriftConstants;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;
import org.mockito.Mockito;

// Test utils for the TestSentryPolicyStoreProcessor class
class SentryPolicyStoreProcessorTestUtils {
  // Constants variables
  private static final int NO_TIME = 0;
  private final Configuration conf;
  private final SentryPolicyStoreProcessor processor;
  private final SentryStoreInterface sentryStore;

  SentryPolicyStoreProcessorTestUtils(Configuration conf, SentryStoreInterface sentryStore) throws Exception {
    this.conf = conf;
    this.sentryStore = sentryStore;
    this.processor = new SentryPolicyStoreProcessor(
      ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME, conf, sentryStore);
  }

  PolicyStoreProcessorTestVerifier givenUser(String userName) {
    return new PolicyStoreProcessorTestVerifier(userName);
  }

  static TSentryPrivilege newPrivilegeOnDatabase(String action, String serverName, String dbName) {
    TSentryPrivilege privilege = new TSentryPrivilege(){{
      setPrivilegeScope(PrivilegeScope.DATABASE.toString());
      setServerName(serverName);
      setDbName(dbName);
      setCreateTime(NO_TIME);
      setAction(action);
    }};

    return privilege;
  }

  static TSentryPrivilege newPrivilegeOnDatabaseWithGrant(String action, String serverName, String dbName) {
    TSentryPrivilege privilege = new TSentryPrivilege(){{
      setPrivilegeScope(PrivilegeScope.DATABASE.toString());
      setServerName(serverName);
      setDbName(dbName);
      setCreateTime(NO_TIME);
      setAction(action);
      setGrantOption(TSentryGrantOption.TRUE);
    }};

    return privilege;
  }

  static TSentryPrivilege newPrivilegeOnTable(String action, String serverName, String dbName, String tableName) {
    TSentryPrivilege privilege = new TSentryPrivilege(){{
      setPrivilegeScope(PrivilegeScope.TABLE.toString());
      setServerName(serverName);
      setDbName(dbName);
      setTableName(tableName);
      setCreateTime(NO_TIME);
      setAction(action);
    }};

    return privilege;
  }

  static TSentryPrivilege newPrivilegeOnTableWithGrant(String action, String serverName, String dbName, String tableName) {
    TSentryPrivilege privilege = new TSentryPrivilege(){{
      setPrivilegeScope(PrivilegeScope.TABLE.toString());
      setServerName(serverName);
      setDbName(dbName);
      setTableName(tableName);
      setCreateTime(NO_TIME);
      setAction(action);
      setGrantOption(TSentryGrantOption.TRUE);
    }};

    return privilege;
  }

  static TSentryPrivilege newPrivilegeOnColumn(String action, String serverName, String dbName, String tableName, String column) {
    TSentryPrivilege privilege = new TSentryPrivilege(){{
      setPrivilegeScope(PrivilegeScope.COLUMN.toString());
      setServerName(serverName);
      setDbName(dbName);
      setTableName(tableName);
      setColumnName(column);
      setCreateTime(NO_TIME);
      setAction(action);
      setGrantOption(TSentryGrantOption.FALSE);
    }};

    return privilege;
  }

  class PolicyStoreProcessorTestVerifier {
    // Constant variables
    private final String REQUESTOR_USER;

    PolicyStoreProcessorTestVerifier(String requestorUser) {
      this.REQUESTOR_USER = requestorUser;
    }

    PolicyGrantPrivilegeVerifier grantPrivilegeToRole(TSentryPrivilege privilege, String roleName) {
      return new PolicyGrantPrivilegeVerifier(REQUESTOR_USER, privilege, roleName);
    }

    PolicyRevokePrivilegeVerifier revokePrivilegeFromRole(TSentryPrivilege privilege, String roleName) {
      return new PolicyRevokePrivilegeVerifier(REQUESTOR_USER, privilege, roleName);
    }
  }

  class PolicyRevokePrivilegeVerifier {
    // Constant variables
    private final String REQUESTOR_USER;
    private final TSentryPrivilege privilege;

    // Class variables
    private String roleName;

    PolicyRevokePrivilegeVerifier(String requestorUser, TSentryPrivilege privilege, String roleName) {
      this.REQUESTOR_USER = requestorUser;
      this.privilege = privilege;
      this.roleName = roleName;
    }

    PolicyRevokePrivilegeVerifier whenRequestStorePrivilegesReturn(Set<TSentryPrivilege> privileges)
      throws Exception {

      Set<String> groups = SentryPolicyStoreProcessor.getGroupsFromUserName(conf, REQUESTOR_USER);
      Mockito.when(sentryStore.listSentryPrivilegesByUsersAndGroups(
        Mockito.eq(groups),
        Mockito.eq(Collections.singleton(REQUESTOR_USER)),
        Mockito.eq(Mockito.any()),
        null)
      ).thenReturn(privileges);

      return this;
    }

    void verify(Status status) throws Exception {
      TAlterSentryRoleRevokePrivilegeRequest revokeRequest = new TAlterSentryRoleRevokePrivilegeRequest();
      revokeRequest.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
      revokeRequest.setRequestorUserName(REQUESTOR_USER);
      revokeRequest.setRoleName(roleName);
      revokeRequest.setPrivilege(privilege);

      TAlterSentryRoleRevokePrivilegeResponse response =
        processor.alter_sentry_role_revoke_privilege(revokeRequest);
      if (response.getStatus().getValue() == Status.OK.getCode()) {
        Mockito.verify(sentryStore).alterSentryRoleRevokePrivileges(revokeRequest.getRoleName(),
          revokeRequest.getPrivileges());
      } else {
        Mockito.verify(sentryStore, Mockito.times(0))
          .alterSentryRoleRevokePrivileges(Mockito.anyString(), Mockito.anySet());
      }

      assertEquals("Revoke " + privilege.getAction() + " response is not valid",
        status.getCode(), response.getStatus().getValue());

      Mockito.reset(sentryStore);
    }
  }

  class PolicyGrantPrivilegeVerifier {
    // Constant variables
    private final String REQUESTOR_USER;
    private final TSentryPrivilege privilege;

    // Class variables
    private String roleName;

    PolicyGrantPrivilegeVerifier(String requestorUser, TSentryPrivilege privilege, String roleName) {
      this.REQUESTOR_USER = requestorUser;
      this.privilege = privilege;
      this.roleName = roleName;
    }

    PolicyGrantPrivilegeVerifier whenRequestStorePrivilegesReturn(Set<TSentryPrivilege> privileges)
      throws Exception {

      Set<String> groups = SentryPolicyStoreProcessor.getGroupsFromUserName(conf, REQUESTOR_USER);
      Mockito.when(sentryStore.listSentryPrivilegesByUsersAndGroups(
          Mockito.eq(groups),
          Mockito.eq(Collections.singleton(REQUESTOR_USER)),
          Mockito.eq(Mockito.any()),
          null)
      ).thenReturn(privileges);

      return this;
    }

    void verify(Status status) throws Exception {
      TAlterSentryRoleGrantPrivilegeRequest grantRequest = new TAlterSentryRoleGrantPrivilegeRequest();
      grantRequest.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
      grantRequest.setRequestorUserName(REQUESTOR_USER);
      grantRequest.setRoleName(roleName);
      grantRequest.setPrivilege(privilege);

      TAlterSentryRoleGrantPrivilegeResponse response =
        processor.alter_sentry_role_grant_privilege(grantRequest);
      if (response.getStatus().getValue() == Status.OK.getCode()) {
        Mockito.verify(sentryStore).alterSentryRoleGrantPrivileges(grantRequest.getRoleName(),
          grantRequest.getPrivileges());
      } else {
        Mockito.verify(sentryStore, Mockito.times(0))
          .alterSentryRoleGrantPrivileges(Mockito.anyString(), Mockito.anySet());
      }

      assertEquals("Grant " + privilege.getAction() + " response is not valid",
        status.getCode(), response.getStatus().getValue());

      Mockito.reset(sentryStore);
    }
  }
}
