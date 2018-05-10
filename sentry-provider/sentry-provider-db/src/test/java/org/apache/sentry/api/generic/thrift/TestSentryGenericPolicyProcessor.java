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
package org.apache.sentry.api.generic.thrift;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.core.model.solr.Collection;
import org.apache.sentry.core.model.solr.Field;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryGrantDeniedException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.db.generic.service.persistent.PrivilegeObject;
import org.apache.sentry.provider.db.generic.service.persistent.SentryStoreLayer;
import org.apache.sentry.provider.db.generic.service.persistent.PrivilegeObject.Builder;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.core.common.utils.PolicyStoreConstants;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.sentry.api.common.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

public class TestSentryGenericPolicyProcessor extends org.junit.Assert {
  private static final String ADMIN_GROUP = "admin_group";
  private static final String ADMIN_USER = "admin_user";
  private static final String NOT_ADMIN_USER = "not_admin_user";
  private static final String NOT_ADMIN_GROUP = "not_admin_group";
  private static final String NO_GROUP_USER = "no_group_user";

  private SentryStoreLayer mockStore = Mockito.mock(SentryStoreLayer.class);
  private SentryGenericPolicyProcessor processor;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, MockGroupMapping.class.getName());
    processor =  new SentryGenericPolicyProcessor(conf, mockStore);
  }

  @Test
  public void testNotAdminOperation() throws Exception {
    String requestUser = NOT_ADMIN_USER;
    Status validateStatus = Status.ACCESS_DENIED;
    testOperation(requestUser, validateStatus);
  }

  private void testOperation(String requestUser, Status validateStatus) throws Exception {
    TCreateSentryRoleRequest createrequest = new TCreateSentryRoleRequest();
    createrequest.setRequestorUserName(requestUser);
    createrequest.setRoleName("r1");
    assertEquals(validateStatus, fromTSentryStatus(processor.create_sentry_role(createrequest).getStatus()));

    TDropSentryRoleRequest dropRequest = new TDropSentryRoleRequest();
    dropRequest.setRequestorUserName(requestUser);
    dropRequest.setRoleName("r1");
    assertEquals(validateStatus, fromTSentryStatus(processor.drop_sentry_role(dropRequest).getStatus()));

    TAlterSentryRoleAddGroupsRequest addRequest = new TAlterSentryRoleAddGroupsRequest();
    addRequest.setRequestorUserName(requestUser);
    addRequest.setRoleName("r1");
    addRequest.setGroups(Sets.newHashSet("g1"));
    assertEquals(validateStatus, fromTSentryStatus(processor.alter_sentry_role_add_groups(addRequest).getStatus()));

    TAlterSentryRoleDeleteGroupsRequest delRequest = new TAlterSentryRoleDeleteGroupsRequest();
    delRequest.setRequestorUserName(requestUser);
    delRequest.setRoleName("r1");
    delRequest.setGroups(Sets.newHashSet("g1"));
    assertEquals(validateStatus, fromTSentryStatus(processor.alter_sentry_role_delete_groups(delRequest).getStatus()));

    TDropPrivilegesRequest dropPrivRequest = new TDropPrivilegesRequest();
    dropPrivRequest.setRequestorUserName(requestUser);
    dropPrivRequest.setPrivilege(new TSentryPrivilege("test", "test", new ArrayList<TAuthorizable>(), "test"));
    assertEquals(validateStatus, fromTSentryStatus(processor.drop_sentry_privilege(dropPrivRequest).getStatus()));

    TRenamePrivilegesRequest renameRequest = new TRenamePrivilegesRequest();
    renameRequest.setRequestorUserName(requestUser);
    assertEquals(validateStatus, fromTSentryStatus(processor.rename_sentry_privilege(renameRequest).getStatus()));
  }

  private Status fromTSentryStatus(TSentryResponseStatus status) {
    return Status.fromCode(status.getValue());
  }

  @Test
  public void testAdminOperation() throws Exception {
    testOperation(ADMIN_USER, Status.OK);
  }

  @Test
  public void testGrantAndRevokePrivilege() throws Exception {
    setup();

    TSentryPrivilege tprivilege = new TSentryPrivilege("test", "test", new ArrayList<TAuthorizable>(), "test");
    tprivilege.setGrantOption(TSentryGrantOption.UNSET);

    TAlterSentryRoleGrantPrivilegeRequest grantRequest = new TAlterSentryRoleGrantPrivilegeRequest();
    grantRequest.setRequestorUserName(ADMIN_USER);
    grantRequest.setRoleName("r1");
    grantRequest.setPrivilege(tprivilege);
    assertEquals(Status.OK, fromTSentryStatus(processor.alter_sentry_role_grant_privilege(grantRequest).getStatus()));

    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = new TAlterSentryRoleRevokePrivilegeRequest();
    revokeRequest.setRequestorUserName(ADMIN_USER);
    revokeRequest.setRoleName("r1");
    revokeRequest.setPrivilege(tprivilege);
    assertEquals(Status.OK, fromTSentryStatus(processor.alter_sentry_role_revoke_privilege(revokeRequest).getStatus()));
  }

  @Test
  public void testOperationWithException() throws Exception {
    String roleName = anyString();
    Mockito.when(mockStore.createRole(anyString(), roleName, anyString()))
      .thenThrow(new SentryAlreadyExistsException("Role: " + roleName));

    roleName = anyString();
    Mockito.when(mockStore.dropRole(anyString(), roleName, anyString()))
      .thenThrow(new SentryNoSuchObjectException("Role: " + roleName ));

    roleName = anyString();
    Mockito.when(mockStore.alterRoleAddGroups(anyString(), roleName, anySetOf(String.class),anyString()))
      .thenThrow(new SentryNoSuchObjectException("Role: " + roleName));

    roleName = anyString();
    Mockito.when(mockStore.alterRoleDeleteGroups(anyString(), roleName, anySetOf(String.class), anyString()))
      .thenThrow(new SentryNoSuchObjectException("Role: " + roleName));

    roleName = anyString();
    Mockito.when(mockStore.alterRoleGrantPrivilege(anyString(), roleName, any(PrivilegeObject.class), anyString()))
    .thenThrow(new SentryGrantDeniedException("Role: " + roleName + " is not allowed to do grant"));

    roleName = anyString();
    Mockito.when(mockStore.alterRoleRevokePrivilege(anyString(), roleName, any(PrivilegeObject.class), anyString()))
    .thenThrow(new SentryGrantDeniedException("Role: " + roleName + " is not allowed to do grant"));

    Mockito.when(mockStore.dropPrivilege(anyString(), any(PrivilegeObject.class), anyString()))
    .thenThrow(new SentryInvalidInputException("Invalid input privilege object"));

    Mockito.when(mockStore.renamePrivilege(anyString(), anyString(), anyListOf(Authorizable.class),
        anyListOf(Authorizable.class), anyString()))
    .thenThrow(new RuntimeException("Unknown error"));

    setup();

    TCreateSentryRoleRequest createrequest = new TCreateSentryRoleRequest();
    createrequest.setRequestorUserName(ADMIN_USER);
    createrequest.setRoleName("r1");
    assertEquals(Status.ALREADY_EXISTS, fromTSentryStatus(processor.create_sentry_role(createrequest).getStatus()));

    TDropSentryRoleRequest dropRequest = new TDropSentryRoleRequest();
    dropRequest.setRequestorUserName(ADMIN_USER);
    dropRequest.setRoleName("r1");
    assertEquals(Status.NO_SUCH_OBJECT, fromTSentryStatus(processor.drop_sentry_role(dropRequest).getStatus()));

    TAlterSentryRoleAddGroupsRequest addRequest = new TAlterSentryRoleAddGroupsRequest();
    addRequest.setRequestorUserName(ADMIN_USER);
    addRequest.setRoleName("r1");
    addRequest.setGroups(Sets.newHashSet("g1"));
    assertEquals(Status.NO_SUCH_OBJECT, fromTSentryStatus(processor.alter_sentry_role_add_groups(addRequest).getStatus()));

    TAlterSentryRoleDeleteGroupsRequest delRequest = new TAlterSentryRoleDeleteGroupsRequest();
    delRequest.setRequestorUserName(ADMIN_USER);
    delRequest.setRoleName("r1");
    delRequest.setGroups(Sets.newHashSet("g1"));
    assertEquals(Status.NO_SUCH_OBJECT, fromTSentryStatus(processor.alter_sentry_role_delete_groups(delRequest).getStatus()));

    TDropPrivilegesRequest dropPrivRequest = new TDropPrivilegesRequest();
    dropPrivRequest.setRequestorUserName(ADMIN_USER);
    dropPrivRequest.setPrivilege(new TSentryPrivilege("test", "test", new ArrayList<TAuthorizable>(), "test"));
    assertEquals(Status.INVALID_INPUT, fromTSentryStatus(processor.drop_sentry_privilege(dropPrivRequest).getStatus()));

    TRenamePrivilegesRequest renameRequest = new TRenamePrivilegesRequest();
    renameRequest.setRequestorUserName(ADMIN_USER);
    assertEquals(Status.RUNTIME_ERROR, fromTSentryStatus(processor.rename_sentry_privilege(renameRequest).getStatus()));

    TSentryPrivilege tprivilege = new TSentryPrivilege("test", "test", new ArrayList<TAuthorizable>(), "test");
    tprivilege.setGrantOption(TSentryGrantOption.UNSET);

    TAlterSentryRoleGrantPrivilegeRequest grantRequest = new TAlterSentryRoleGrantPrivilegeRequest();
    grantRequest.setRequestorUserName(ADMIN_USER);
    grantRequest.setRoleName("r1");
    grantRequest.setPrivilege(tprivilege);
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.alter_sentry_role_grant_privilege(grantRequest).getStatus()));

    TAlterSentryRoleRevokePrivilegeRequest revokeRequest = new TAlterSentryRoleRevokePrivilegeRequest();
    revokeRequest.setRequestorUserName(ADMIN_USER);
    revokeRequest.setRoleName("r1");
    revokeRequest.setPrivilege(tprivilege);
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.alter_sentry_role_revoke_privilege(revokeRequest).getStatus()));
  }

  @Test
  public void testUserWithNoGroup() throws Exception {
    setup();

    TCreateSentryRoleRequest createrequest = new TCreateSentryRoleRequest();
    createrequest.setRequestorUserName(NO_GROUP_USER);
    createrequest.setRoleName("r1");
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.create_sentry_role(createrequest).getStatus()));

    TDropSentryRoleRequest dropRequest = new TDropSentryRoleRequest();
    dropRequest.setRequestorUserName(NO_GROUP_USER);
    dropRequest.setRoleName("r1");
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.drop_sentry_role(dropRequest).getStatus()));

    TAlterSentryRoleAddGroupsRequest addRequest = new TAlterSentryRoleAddGroupsRequest();
    addRequest.setRequestorUserName(NO_GROUP_USER);
    addRequest.setRoleName("r1");
    addRequest.setGroups(Sets.newHashSet("g1"));
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.alter_sentry_role_add_groups(addRequest).getStatus()));

    TAlterSentryRoleDeleteGroupsRequest delRequest = new TAlterSentryRoleDeleteGroupsRequest();
    delRequest.setRequestorUserName(NO_GROUP_USER);
    delRequest.setRoleName("r1");
    delRequest.setGroups(Sets.newHashSet("g1"));
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.alter_sentry_role_delete_groups(delRequest).getStatus()));

    TDropPrivilegesRequest dropPrivRequest = new TDropPrivilegesRequest();
    dropPrivRequest.setRequestorUserName(NO_GROUP_USER);
    dropPrivRequest.setPrivilege(new TSentryPrivilege("test", "test", new ArrayList<TAuthorizable>(), "test"));
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.drop_sentry_privilege(dropPrivRequest).getStatus()));

    TRenamePrivilegesRequest renameRequest = new TRenamePrivilegesRequest();
    renameRequest.setRequestorUserName(NO_GROUP_USER);
    assertEquals(Status.ACCESS_DENIED, fromTSentryStatus(processor.rename_sentry_privilege(renameRequest).getStatus()));

    // Can't test GrantPrivilege / RevokePrivilege since the authorization happens
    // in the persistence layer, which isn't setup in this test.
  }

  @Test
  public void testGetRolesAndPrivileges() throws Exception {
    String roleName = "r1";
    String groupName = "g1";
    PrivilegeObject queryPrivilege = new Builder()
                                   .setComponent("SOLR")
                                   .setAction(SolrConstants.QUERY)
                                   .setService("service1")
                                   .setAuthorizables(Arrays.asList(new Collection("c1"), new Field("f1")))
                                   .build();
    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
                                   .setAction(SolrConstants.UPDATE)
                                   .build();

    MSentryGMPrivilege mSentryGMPrivilege = new MSentryGMPrivilege("SOLR", "service1",
    Arrays.asList(new Collection("c1"), new Field("f1")),
    SolrConstants.QUERY, true);

    MSentryRole role = new MSentryRole("r1", 290);
    mSentryGMPrivilege.setRoles(Sets.newHashSet(role));

    Mockito.when(mockStore.getRolesByGroups(anyString(), anySetOf(String.class)))
    .thenReturn(Sets.newHashSet(roleName));

    Mockito.when(mockStore.getPrivilegesByProvider(anyString(), anyString(), anySetOf(String.class),
        anySetOf(String.class), anyListOf(Authorizable.class)))
    .thenReturn(Sets.newHashSet(queryPrivilege, updatePrivilege));

    Mockito.when(mockStore.getGroupsByRoles(anyString(), anySetOf(String.class)))
    .thenReturn(Sets.newHashSet(groupName));

    Mockito.when(mockStore.getPrivilegesByAuthorizable(anyString(), anyString(), anySetOf(String.class), anyListOf(Authorizable.class)))
    .thenReturn(Sets.newHashSet(mSentryGMPrivilege));

    Mockito.when(mockStore.getAllRoleNames())
    .thenReturn(Sets.newHashSet(roleName));

    TListSentryPrivilegesRequest request1 = new TListSentryPrivilegesRequest();
    request1.setRoleName(roleName);
    request1.setRequestorUserName(ADMIN_USER);
    TListSentryPrivilegesResponse response1 = processor.list_sentry_privileges_by_role(request1);
    assertEquals(Status.OK, fromTSentryStatus(response1.getStatus()));
    assertEquals(2, response1.getPrivileges().size());

    TListSentryRolesRequest request2 = new TListSentryRolesRequest();
    request2.setRequestorUserName(ADMIN_USER);
    request2.setGroupName(groupName);
    TListSentryRolesResponse response2 = processor.list_sentry_roles_by_group(request2);
    assertEquals(Status.OK, fromTSentryStatus(response2.getStatus()));
    assertEquals(1, response2.getRoles().size());

    TListSentryPrivilegesForProviderRequest request3 = new TListSentryPrivilegesForProviderRequest();
    request3.setGroups(Sets.newHashSet(groupName));
    request3.setRoleSet(new TSentryActiveRoleSet(true, null));
    TListSentryPrivilegesForProviderResponse response3 = processor.list_sentry_privileges_for_provider(request3);
    assertEquals(Status.OK, fromTSentryStatus(response3.getStatus()));
    assertEquals(2, response3.getPrivileges().size());

    // Optional parameters activeRoleSet and requested group name are both provided.
    TListSentryPrivilegesByAuthRequest request4 = new TListSentryPrivilegesByAuthRequest();
    request4.setGroups(Sets.newHashSet(groupName));
    request4.setRoleSet(new TSentryActiveRoleSet(true, null));
    request4.setRequestorUserName(ADMIN_USER);
    Set<String> authorizablesSet = Sets.newHashSet("Collection=c1->Field=f1");
    request4.setAuthorizablesSet(authorizablesSet);

    TListSentryPrivilegesByAuthResponse response4 = processor.list_sentry_privileges_by_authorizable(request4);
    assertEquals(Status.OK, fromTSentryStatus(response4.getStatus()));
    assertEquals(1, response4.getPrivilegesMapByAuth().size());

    // Optional parameters activeRoleSet and requested group name are both not provided.
    TListSentryPrivilegesByAuthRequest request5 = new TListSentryPrivilegesByAuthRequest();
    request5.setRequestorUserName("not_" + ADMIN_USER);
    authorizablesSet = Sets.newHashSet("Collection=c1->Field=f2");
    request5.setAuthorizablesSet(authorizablesSet);

    TListSentryPrivilegesByAuthResponse response5 = processor.list_sentry_privileges_by_authorizable(request5);
    assertEquals(Status.OK, fromTSentryStatus(response5.getStatus()));
    assertEquals(1, response5.getPrivilegesMapByAuth().size());
  }

  @Test(expected=SentrySiteConfigurationException.class)
  public void testConfigCannotCreateNotificationHandler() throws Exception {
    Configuration conf = new Configuration();
    conf.set(PolicyStoreConstants.SENTRY_GENERIC_POLICY_NOTIFICATION,"junk");
    SentryGenericPolicyProcessor.createHandlers(conf);
  }

  public static class MockGroupMapping implements GroupMappingService {
    public MockGroupMapping(Configuration conf, String resource) { //NOPMD
    }
    @Override
    public Set<String> getGroups(String user) {
      if (user.equalsIgnoreCase(ADMIN_USER)) {
        return Sets.newHashSet(ADMIN_GROUP);
      } else if (user.equalsIgnoreCase(NOT_ADMIN_USER)){
        return Sets.newHashSet(NOT_ADMIN_GROUP);
      } else {
        return Collections.emptySet();
      }
    }
  }

}
