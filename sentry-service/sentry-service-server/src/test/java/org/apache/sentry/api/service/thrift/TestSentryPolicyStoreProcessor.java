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

import static org.apache.sentry.service.common.ServiceConstants.ServerConfig.SENTRY_DB_POLICY_STORE_OWNER_AS_PRIVILEGE;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.sentry.api.common.ApiConstants;
import org.apache.sentry.api.common.Status;
import org.apache.sentry.api.common.ThriftConstants;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.db.service.persistent.CounterWait;
import org.apache.sentry.service.common.SentryOwnerPrivilegeType;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.common.ServiceConstants.SentryPrincipalType;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.FullUpdateInitializerState;
import org.apache.sentry.service.thrift.SentryStateBank;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryThriftAPIMismatchException;
import org.apache.sentry.core.common.utils.PolicyStoreConstants.PolicyStoreServerConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSentryPolicyStoreProcessor {

  private static final String SERVERNAME = "server1";
  private static final String DBNAME = "db1";
  private static final String TABLENAME = "table1";
  private static final String OWNER = "owner1";
  private Configuration conf;
  private static final SentryStore sentryStore = Mockito.mock(SentryStore.class);
  private static final CounterWait counterWait = Mockito.mock(CounterWait.class);
  private static final String ADMIN_GROUP = "admin_group";
  private static final String ADMIN_USER = "admin_user";
  private static final String NOT_ADMIN_USER = "not_admin_user";
  private static final String NOT_ADMIN_GROUP = "not_admin_group";

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

  @Before
  public void setup() throws Exception{
    conf = new Configuration(true);
    //Check behaviour when DB name is not set
    conf.set(SENTRY_DB_POLICY_STORE_OWNER_AS_PRIVILEGE, SentryOwnerPrivilegeType.ALL.toString());
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
            MockGroupMapping.class.getName());
    Mockito.when(sentryStore.getRoleCountGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getPrivilegeCountGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getGroupCountGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getHMSWaitersCountGauge()).thenReturn(new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return 0;
      }
    });
    Mockito.when(sentryStore.getLastNotificationIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });    Mockito.when(sentryStore.getLastPathsSnapshotIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getPermChangeIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getPathChangeIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });

    Mockito.doAnswer((invocation) -> {
      long id = (long) invocation.getArguments()[0];
      return id;
    }).when(counterWait).waitFor(Mockito.anyLong());

    Mockito.doAnswer((invocation) -> {
      return counterWait;
    }).when(sentryStore).getCounterWait();
  }

  @After
  public void reset () {
    Mockito.reset(sentryStore);
    Mockito.reset(counterWait);
  }

  @Test(expected=SentrySiteConfigurationException.class)
  public void testConfigNotNotificationHandler() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS, Object.class.getName());
    SentryPolicyStoreProcessor.createHandlers(conf);
  }
  @Test(expected=SentrySiteConfigurationException.class)
  public void testConfigCannotCreateNotificationHandler() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS,
        ExceptionInConstructorNotificationHandler.class.getName());
    SentryPolicyStoreProcessor.createHandlers(conf);
  }
  @Test(expected=SentrySiteConfigurationException.class)
  public void testConfigNotAClassNotificationHandler() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS, "junk");
    SentryPolicyStoreProcessor.createHandlers(conf);
  }
  @Test
  public void testConfigMultipleNotificationHandlers() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS,
        NoopNotificationHandler.class.getName() + "," +
            NoopNotificationHandler.class.getName() + " " +
            NoopNotificationHandler.class.getName());
    Assert.assertEquals(3, SentryPolicyStoreProcessor.createHandlers(conf).size());
  }
  public static class ExceptionInConstructorNotificationHandler extends NotificationHandler {
    public ExceptionInConstructorNotificationHandler(Configuration config) throws Exception {
      super(config);
      throw new Exception();
    }
  }
  public static class NoopNotificationHandler extends NotificationHandler {
    public NoopNotificationHandler(Configuration config) throws Exception {
      super(config);
    }
  }
  @Test(expected=SentryThriftAPIMismatchException.class)
  public void testSentryThriftAPIMismatch() throws Exception {
    SentryPolicyStoreProcessor.validateClientVersion(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT -1);
  }
  @Test
  public void testSentryThriftAPIMatchVersion() throws Exception {
    SentryPolicyStoreProcessor.validateClientVersion(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
  }

  @Test
  public void testConstructOwnerPrivilege() throws Exception {
    conf.set(SENTRY_DB_POLICY_STORE_OWNER_AS_PRIVILEGE, SentryOwnerPrivilegeType.NONE.toString());
    SentryPolicyStoreProcessor sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    TSentryPrivilege privilege = new TSentryPrivilege();
    TSentryAuthorizable authorizable = new TSentryAuthorizable("server1");
    authorizable.setDb("db1");
    authorizable.setTable("tb1");

    //Check the behaviour when owner privileges feature is not configured.
    assertNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));


    //Check behaviour when DB name is not set
    conf.set(SENTRY_DB_POLICY_STORE_OWNER_AS_PRIVILEGE, SentryOwnerPrivilegeType.ALL.toString());
    sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    authorizable = new TSentryAuthorizable("server1");
    authorizable.setTable("tb1");
    assertNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));

    //Check the behavior when DB name is set and table name is not set.
    authorizable = new TSentryAuthorizable("server1");
    authorizable.setDb("db1");
    privilege.setServerName("server1");
    privilege.setDbName("db1");
    privilege.setAction(AccessConstants.OWNER);
    privilege.setPrivilegeScope("DATABASE");
    Assert.assertNotNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));
    Assert.assertEquals(privilege, sentryServiceHandler.constructOwnerPrivilege(authorizable));

    //check the behaviour when both DB name and table name are set
    authorizable = new TSentryAuthorizable("server1");
    authorizable.setDb("db1");
    authorizable.setTable("tb1");
    privilege.setTableName("tb1");
    privilege.setPrivilegeScope("TABLE");
    Assert.assertNotNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));
    Assert.assertEquals(privilege, sentryServiceHandler.constructOwnerPrivilege(authorizable));

    //Check the behavior when grant option is configured.
    conf.set(SENTRY_DB_POLICY_STORE_OWNER_AS_PRIVILEGE, SentryOwnerPrivilegeType.ALL_WITH_GRANT.toString());
    sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    authorizable = new TSentryAuthorizable("server1");
    authorizable.setDb("db1");
    authorizable.setTable("tb1");
    privilege.setPrivilegeScope("TABLE");
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    Assert.assertNotNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));
    Assert.assertEquals(privilege, sentryServiceHandler.constructOwnerPrivilege(authorizable));
  }

  @Test
  public void testListPrivilegesByUserName() throws Exception {
    MockGroupMappingService.addUserGroupMapping("admin", Sets.newHashSet("admin"));

    Configuration conf = new Configuration();
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, "org.apache.sentry.api.service.thrift.MockGroupMappingService");
    conf.set(ServerConfig.ADMIN_GROUPS, "admin");

    SentryPolicyStoreProcessor policyStoreProcessor =
      new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
        conf, sentryStore);
    TListSentryPrivilegesResponse returnedResp;
    TListSentryPrivilegesResponse expectedResp;

    // Request privileges when user is null must throw an exception that principalName must not be null
    returnedResp = policyStoreProcessor.list_sentry_privileges_by_user(newPrivilegesRequest("admin", null, null));
    expectedResp = new TListSentryPrivilegesResponse();
    expectedResp.setStatus(Status.InvalidInput("principalName parameter must not be null",
      new SentryInvalidInputException("principalName parameter must not be null")));
    Assert.assertEquals(expectedResp.getStatus().getValue(), returnedResp.getStatus().getValue());

    // Prepare privileges for user1
    Set<TSentryPrivilege> user1Privileges = Sets.newHashSet(
      newSentryPrivilege("database", "db1", "t1", "*"),
      newSentryPrivilege("database", "db1", "t2", "*"));
    Mockito.when(sentryStore.getAllTSentryPrivilegesByUserName("user1")).thenReturn(user1Privileges);

    // Request privileges of a user as admin
    returnedResp = policyStoreProcessor.list_sentry_privileges_by_user(newPrivilegesRequest("admin", "user1", null));
    Assert.assertEquals(2, returnedResp.getPrivileges().size());
    Assert.assertEquals(Status.OK(),  returnedResp.getStatus());
    assertTrue("User should have ALL privileges in db1.t1",
      returnedResp.getPrivileges().contains(newSentryPrivilege("database", "db1", "t1", "*")));
    assertTrue("User should have ALL privileges in db1.t2",
      returnedResp.getPrivileges().contains(newSentryPrivilege("database", "db1", "t2", "*")));

    // Request privileges of a user as the same user
    returnedResp = policyStoreProcessor.list_sentry_privileges_by_user(newPrivilegesRequest("user1", "user1", null));
    Assert.assertEquals(2, returnedResp.getPrivileges().size());
    Assert.assertEquals(Status.OK(),  returnedResp.getStatus());
    assertTrue("User should have ALL privileges in db1.t1",
      returnedResp.getPrivileges().contains(newSentryPrivilege("database", "db1", "t1", "*")));
    assertTrue("User should have ALL privileges in db1.t2",
      returnedResp.getPrivileges().contains(newSentryPrivilege("database", "db1", "t2", "*")));

    // Request privileges of a user as an unauthorized user
    returnedResp = policyStoreProcessor.list_sentry_privileges_by_user(newPrivilegesRequest("user2", "user1", null));
    Assert.assertEquals(Status.ACCESS_DENIED.getCode(), returnedResp.getStatus().getValue());
    assertNull(returnedResp.getPrivileges());

    // Request privileges of a user on a specified authorizable as admin
    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setServer("server1");
    authorizable.setDb("db1");
    authorizable.setTable("t1");

    user1Privileges = Sets.newHashSet(
      newSentryPrivilege("database", "db1", "t1", "*"));
    Mockito.when(sentryStore.getTSentryPrivileges(SentryPrincipalType.USER,Sets.newHashSet("user1"), authorizable)).thenReturn(user1Privileges);

    returnedResp = policyStoreProcessor.list_sentry_privileges_by_user(newPrivilegesRequest("user1", "user1", authorizable));
    Assert.assertEquals(1, returnedResp.getPrivileges().size());
    Assert.assertEquals(Status.OK(),  returnedResp.getStatus());
    assertTrue("User should have ALL privileges in db1.t1",
      returnedResp.getPrivileges().contains(newSentryPrivilege("database", "db1", "t1", "*")));
  }

  private TListSentryPrivilegesRequest newPrivilegesRequest(String requestorUser, String principalName, TSentryAuthorizable authorizable) {
    TListSentryPrivilegesRequest request = new TListSentryPrivilegesRequest();
    request.setRequestorUserName(requestorUser);
    request.setPrincipalName(principalName);
    request.setAuthorizableHierarchy(authorizable);
    return request;
  }

  private static TSentryPrivilege newSentryPrivilege(String scope, String dbname, String tablename, String action) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(scope);
    privilege.setServerName(SERVERNAME);
    privilege.setDbName(dbname);
    privilege.setTableName(tablename);
    privilege.setAction(action);
    return privilege;
  }

@Test
  public void testCreateTableEventProcessing() throws Exception {
    SentryPolicyStoreProcessor sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setDb(DBNAME);
    authorizable.setTable(TABLENAME);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();
    notification.setId(1L);
    notification.setOwnerType(TSentryPrincipalType.ROLE);
    notification.setOwnerName(OWNER);
    notification.setAuthorizable(authorizable);
    notification.setEventType(EventMessage.EventType.CREATE_TABLE.toString());

    sentryServiceHandler.sentry_notify_hms_event(notification);

     TSentryPrivilege ownerPrivilege = sentryServiceHandler.constructOwnerPrivilege(authorizable);
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).alterSentryGrantOwnerPrivilege(OWNER, SentryPrincipalType.ROLE, ownerPrivilege, null);

  // Verify that owner privilege is granted when owner belongs to sentry admin group.
  notification.setOwnerType(TSentryPrincipalType.USER);
  notification.setOwnerName(ADMIN_USER);
  sentryServiceHandler.sentry_notify_hms_event(notification);
  Mockito.verify(
          sentryStore, Mockito.times(1)).alterSentryGrantOwnerPrivilege(ADMIN_USER, SentryPrincipalType.USER,
          ownerPrivilege, null);
  notification.setOwnerName(OWNER);
  notification.setOwnerType(TSentryPrincipalType.USER);
  sentryServiceHandler.sentry_notify_hms_event(notification);

  // Verify Sentry Store is invoked to grant privilege.
  Mockito.verify(
          sentryStore, Mockito.times(1)
  ).alterSentryGrantOwnerPrivilege(OWNER, SentryPrincipalType.USER, ownerPrivilege, null);
  }


  @Test
  public void testCreateDatabaseEventProcessing() throws Exception {

    SentryPolicyStoreProcessor sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setDb(DBNAME);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();
    notification.setId(1L);
    notification.setOwnerType(TSentryPrincipalType.ROLE);
    notification.setOwnerName(OWNER);
    notification.setAuthorizable(authorizable);
    notification.setEventType(EventType.CREATE_DATABASE.toString());

    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    TSentryPrivilege ownerPrivilege = sentryServiceHandler.constructOwnerPrivilege(authorizable);
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).alterSentryGrantOwnerPrivilege(OWNER, SentryPrincipalType.ROLE, ownerPrivilege, null);

    notification.setOwnerType(TSentryPrincipalType.USER);
    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).alterSentryGrantOwnerPrivilege(OWNER, SentryPrincipalType.USER, ownerPrivilege, null);

  //  Mockito.reset(sentryStore);
    // Verify that owner privilege is granted when owner belongs to sentry admin group.
    notification.setOwnerType(TSentryPrincipalType.USER);
    notification.setOwnerName(ADMIN_USER);
    sentryServiceHandler.sentry_notify_hms_event(notification);
    Mockito.verify(
        sentryStore, Mockito.times(1)).alterSentryGrantOwnerPrivilege(ADMIN_USER, SentryPrincipalType.USER,
        ownerPrivilege, null);
  }

  @Test
  public void testNotificationSync() throws Exception {

    SentryPolicyStoreProcessor sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setDb(DBNAME);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();
    notification.setId(1L);
    notification.setOwnerType(TSentryPrincipalType.ROLE);
    notification.setOwnerName(OWNER);
    notification.setAuthorizable(authorizable);
    notification.setEventType(EventType.CREATE_DATABASE.toString());

    sentryServiceHandler.sentry_notify_hms_event(notification);

    // Verify that synchronization is attempted
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).getCounterWait();

    Mockito.verify(counterWait, Mockito.times(1)).waitFor(1L);

    SentryStateBank.enableState(FullUpdateInitializerState.COMPONENT,
        FullUpdateInitializerState.FULL_SNAPSHOT_INPROGRESS);

    sentryServiceHandler.sentry_notify_hms_event(notification);

    // Verify that synchronization is not attempted because
    // full snapshot is in progress
    Mockito.reset(sentryStore);
    Mockito.reset(counterWait);
    Mockito.verify(
            sentryStore, Mockito.times(0)
    ).getCounterWait();
    Mockito.verify(counterWait, Mockito.times(0)).waitFor(1L);

  }

  @Test
  public void testAlterTableEventProcessing() throws Exception {

    conf.set(SENTRY_DB_POLICY_STORE_OWNER_AS_PRIVILEGE, SentryOwnerPrivilegeType.ALL.toString());

    SentryPolicyStoreProcessor sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setDb(DBNAME);
    authorizable.setTable(TABLENAME);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();
    notification.setId(1L);
    notification.setAuthorizable(authorizable);
    notification.setEventType(EventType.ALTER_TABLE.toString());


    // Verify that owner privilege is granted when owner belongs to sentry admin group.
    notification.setOwnerType(TSentryPrincipalType.USER);
    notification.setOwnerName(ADMIN_USER);
    sentryServiceHandler.sentry_notify_hms_event(notification);
    // Verify Sentry Store API to update the privilege is not invoked when ownership is transferred to
    // user belonging to admin group
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).updateOwnerPrivilege(Mockito.eq(authorizable), Mockito.eq(ADMIN_USER), Mockito.eq(SentryPrincipalType.USER),
            Mockito.anyList());

    notification.setOwnerType(TSentryPrincipalType.ROLE);
    notification.setOwnerName(OWNER);
    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).updateOwnerPrivilege(Mockito.eq(authorizable), Mockito.eq(OWNER), Mockito.eq(SentryPrincipalType.ROLE),
    Mockito.anyList());


    notification.setOwnerType(TSentryPrincipalType.USER);
    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).updateOwnerPrivilege(Mockito.eq(authorizable), Mockito.eq(OWNER), Mockito.eq(SentryPrincipalType.ROLE),
    Mockito.anyList());
  }

  @Test
  public void testListRolesPrivileges() throws Exception {
    MockGroupMappingService.addUserGroupMapping("admin", Sets.newHashSet("admin"));

    Configuration conf = new Configuration();
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, MockGroupMappingService.class.getName());
    conf.set(ServerConfig.ADMIN_GROUPS, "admin");

    SentryPolicyStoreProcessor policyStoreProcessor =
      new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
        conf, sentryStore);

    TSentryPrivilegesResponse returnedResp;

    TSentryPrivilegesRequest request = new TSentryPrivilegesRequest();
    request.setRequestorUserName("user1");

    // Request privileges when requestorUser is not an admin returns an access denied exception
    returnedResp = policyStoreProcessor.list_roles_privileges(request);
    Assert.assertEquals(Status.ACCESS_DENIED.getCode(), returnedResp.getStatus().getValue());

    request.setRequestorUserName("admin");

    // Request privileges when no roles are created yet returns an empty map object
    Mockito.when(sentryStore.getAllRolesPrivileges()).thenReturn(
      Collections.emptyMap());
    returnedResp = policyStoreProcessor.list_roles_privileges(request);
    Assert.assertEquals(Status.OK.getCode(),  returnedResp.getStatus().getValue());
    Assert.assertEquals(0, returnedResp.getPrivilegesMap().size());

    // Request privileges when roles exist returns a map of the form [roleName, set<privileges>]
    ImmutableMap<String, Set<TSentryPrivilege>> rolesPrivileges = ImmutableMap.of(
      "role1", Sets.newHashSet(
        newSentryPrivilege("TABLE", "db1", "tbl1", "ALL"),
        newSentryPrivilege("DATABASE", "db1", "", "INSERT")),
      "role2", Sets.newHashSet(
        newSentryPrivilege("SERVER", "", "", "ALL")),
      "role3", Sets.newHashSet()
    );

    Mockito.when(sentryStore.getAllRolesPrivileges()).thenReturn(rolesPrivileges);
    returnedResp = policyStoreProcessor.list_roles_privileges(request);
    Assert.assertEquals(Status.OK(),  returnedResp.getStatus());
    Assert.assertEquals(3, returnedResp.getPrivilegesMap().size());
    Assert.assertEquals(2, returnedResp.getPrivilegesMap().get("role1").size());
    Assert.assertEquals(1, returnedResp.getPrivilegesMap().get("role2").size());
    Assert.assertEquals(0, returnedResp.getPrivilegesMap().get("role3").size());
  }

  @Test
  public void testListUsersPrivileges() throws Exception {
    MockGroupMappingService.addUserGroupMapping("admin", Sets.newHashSet("admin"));

    Configuration conf = new Configuration();
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, MockGroupMappingService.class.getName());
    conf.set(ServerConfig.ADMIN_GROUPS, "admin");

    SentryPolicyStoreProcessor policyStoreProcessor =
      new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
        conf, sentryStore);

    TSentryPrivilegesResponse returnedResp;

    TSentryPrivilegesRequest request = new TSentryPrivilegesRequest();
    request.setRequestorUserName("user1");

    // Request privileges when requestorUser is not an admin returns an access denied exception
    returnedResp = policyStoreProcessor.list_users_privileges(request);
    Assert.assertEquals(Status.ACCESS_DENIED.getCode(), returnedResp.getStatus().getValue());

    request.setRequestorUserName("admin");

    // Request privileges when no roles are created yet returns an empty map object
    Mockito.when(sentryStore.getAllUsersPrivileges()).thenReturn(
      Collections.emptyMap());
    returnedResp = policyStoreProcessor.list_users_privileges(request);
    Assert.assertEquals(Status.OK.getCode(),  returnedResp.getStatus().getValue());
    Assert.assertEquals(0, returnedResp.getPrivilegesMap().size());

    // Request privileges when roles exist returns a map of the form [userName, set<privileges>]
    ImmutableMap<String, Set<TSentryPrivilege>> usersPrivileges = ImmutableMap.of(
      "user1", Sets.newHashSet(
        newSentryPrivilege("TABLE", "db1", "tbl1", "ALL"),
        newSentryPrivilege("DATABASE", "db1", "", "INSERT")),
      "user2", Sets.newHashSet(
        newSentryPrivilege("SERVER", "", "", "ALL")),
      "user3", Sets.newHashSet()
    );

    Mockito.when(sentryStore.getAllUsersPrivileges()).thenReturn(usersPrivileges);
    returnedResp = policyStoreProcessor.list_users_privileges(request);
    Assert.assertEquals(Status.OK(),  returnedResp.getStatus());
    Assert.assertEquals(3, returnedResp.getPrivilegesMap().size());
    Assert.assertEquals(2, returnedResp.getPrivilegesMap().get("user1").size());
    Assert.assertEquals(1, returnedResp.getPrivilegesMap().get("user2").size());
    Assert.assertEquals(0, returnedResp.getPrivilegesMap().get("user3").size());
  }

  @Test
  public void testGrantNotPermittedPrivilegesThrowsException() throws TException {
    MockGroupMappingService.addUserGroupMapping("admin", Sets.newHashSet("admin"));

    Configuration conf = new Configuration();
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, MockGroupMappingService.class.getName());
    conf.set(ServerConfig.ADMIN_GROUPS, "admin");
    conf.set(ServerConfig.SENTRY_DB_EXPLICIT_GRANTS_PERMITTED, "ALL,SELECT,INSERT,CREATE");

    // Initialize the SentryPolicyStoreProcessor with the permitted grants
    SentryPolicyStoreProcessor policyStoreProcessor = null;
    try {
      policyStoreProcessor = new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
          conf, sentryStore);
    } catch (Exception e) {
      Assert.fail("SentryPolicyStoreProcessor constructor should not throw an exception.");
    }

    TAlterSentryRoleGrantPrivilegeResponse response = null;
    TAlterSentryRoleGrantPrivilegeRequest request =
      new TAlterSentryRoleGrantPrivilegeRequest(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT, "admin", "role1");

    // Attempt to grant the ALTER privilege
    request.setPrivileges(Sets.newHashSet(newSentryPrivilege("SERVER", "", "", "ALTER")));
    response = policyStoreProcessor.alter_sentry_role_grant_privilege(request);
    Assert.assertEquals("Grant ALTER should not be permitted.",
      Status.ACCESS_DENIED.getCode(), response.getStatus().getValue());

    // Attempt to grant the SELECT privilege
    request.setPrivileges(Sets.newHashSet(newSentryPrivilege("SERVER", "", "", "SELECT")));
    response = policyStoreProcessor.alter_sentry_role_grant_privilege(request);
    Assert.assertEquals("Grant SELECT should be permitted.",
      Status.OK.getCode(), response.getStatus().getValue());

    // Attempt to grant the ALTER,SELECT privilege
    request.setPrivileges(Sets.newHashSet(
      newSentryPrivilege("SERVER", "", "", "ALTER"),
      newSentryPrivilege("SERVER", "", "", "SELECT")
    ));

    response = policyStoreProcessor.alter_sentry_role_grant_privilege(request);
    Assert.assertEquals("Grant ALTER should not be permitted.",
      Status.ACCESS_DENIED.getCode(), response.getStatus().getValue());
    assertTrue("ALTER privileges should not be permitted",
      response.getStatus().getMessage().contains("ALTER"));
    Assert.assertFalse("SELECT privileges should be permitted",
      response.getStatus().getMessage().contains("SELECT"));
  }
}
