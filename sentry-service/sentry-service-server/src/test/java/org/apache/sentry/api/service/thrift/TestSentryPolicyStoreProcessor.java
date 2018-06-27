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
import org.apache.sentry.provider.db.service.persistent.CounterWait;
import org.apache.sentry.service.common.ServiceConstants;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.common.ServiceConstants.SentryEntityType;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryThriftAPIMismatchException;
import org.apache.sentry.core.common.utils.PolicyStoreConstants.PolicyStoreServerConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSentryPolicyStoreProcessor {

  private static final String DBNAME = "db1";
  private static final String TABLENAME = "table1";
  private static final String OWNER = "owner1";
  private Configuration conf;
  private static final SentryStore sentryStore = Mockito.mock(SentryStore.class);
  private static final CounterWait counterWait = Mockito.mock(CounterWait.class);
  @Before
  public void setup() throws Exception{
    conf = new Configuration(false);
    //Check behaviour when DB name is not set
    conf.setBoolean(ServiceConstants.ServerConfig.SENTRY_ENABLE_OWNER_PRIVILEGES, true);

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
    conf.setBoolean(ServiceConstants.ServerConfig.SENTRY_ENABLE_OWNER_PRIVILEGES, false);
    SentryPolicyStoreProcessor sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    TSentryPrivilege privilege = new TSentryPrivilege();
    TSentryAuthorizable authorizable = new TSentryAuthorizable("");
    authorizable.setDb("db1");
    authorizable.setTable("tb1");

    //Check the behaviour when owner privileges feature is not configured.
    assertNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));


    //Check behaviour when DB name is not set
    conf.setBoolean(ServiceConstants.ServerConfig.SENTRY_ENABLE_OWNER_PRIVILEGES, true);
    sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    authorizable = new TSentryAuthorizable("");
    authorizable.setTable("tb1");
    assertNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));

    //Check the behavior when DB name is set and table name is not set.
    authorizable = new TSentryAuthorizable("");
    authorizable.setDb("db1");
    privilege.setDbName("db1");
    privilege.setAction(AccessConstants.OWNER);
    privilege.setPrivilegeScope("DATABASE");
    Assert.assertNotNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));
    Assert.assertEquals(privilege, sentryServiceHandler.constructOwnerPrivilege(authorizable));

    //check the behaviour when both DB name and table name are set
    authorizable = new TSentryAuthorizable("");
    authorizable.setDb("db1");
    authorizable.setTable("tb1");
    privilege.setTableName("tb1");
    privilege.setPrivilegeScope("TABLE");
    Assert.assertNotNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));
    Assert.assertEquals(privilege, sentryServiceHandler.constructOwnerPrivilege(authorizable));

    //Check the behavior when grant option is configured.
    conf.setBoolean(ServiceConstants.ServerConfig.SENTRY_OWNER_PRIVILEGE_WITH_GRANT,
            true);
    sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    authorizable = new TSentryAuthorizable("");
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

    // Request privileges when user is null must throw an exception that entityName must not be null
    returnedResp = policyStoreProcessor.list_sentry_privileges_by_user(newPrivilegesRequest("admin", null, null));
    expectedResp = new TListSentryPrivilegesResponse();
    expectedResp.setStatus(Status.InvalidInput("entityName parameter must not be null",
      new SentryInvalidInputException("entityName parameter must not be null")));
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
    Mockito.when(sentryStore.getTSentryPrivileges(SentryEntityType.USER,Sets.newHashSet("user1"), authorizable)).thenReturn(user1Privileges);

    returnedResp = policyStoreProcessor.list_sentry_privileges_by_user(newPrivilegesRequest("user1", "user1", authorizable));
    Assert.assertEquals(1, returnedResp.getPrivileges().size());
    Assert.assertEquals(Status.OK(),  returnedResp.getStatus());
    assertTrue("User should have ALL privileges in db1.t1",
      returnedResp.getPrivileges().contains(newSentryPrivilege("database", "db1", "t1", "*")));
  }

  private TListSentryPrivilegesRequest newPrivilegesRequest(String requestorUser, String entityName, TSentryAuthorizable authorizable) {
    TListSentryPrivilegesRequest request = new TListSentryPrivilegesRequest();
    request.setRequestorUserName(requestorUser);
    request.setEntityName(entityName);
    request.setAuthorizableHierarchy(authorizable);
    return request;
  }

  private static TSentryPrivilege newSentryPrivilege(String scope, String dbname, String tablename, String action) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(scope);
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
    notification.setOwnerType(TSentryObjectOwnerType.ROLE);
    notification.setOwnerName(OWNER);
    notification.setAuthorizable(authorizable);
    notification.setEventType(EventMessage.EventType.CREATE_TABLE.toString());

    sentryServiceHandler.sentry_notify_hms_event(notification);

     TSentryPrivilege ownerPrivilege = sentryServiceHandler.constructOwnerPrivilege(authorizable);
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).alterSentryGrantOwnerPrivilege(OWNER, SentryEntityType.ROLE, ownerPrivilege, null);

    notification.setOwnerType(TSentryObjectOwnerType.USER);
    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
  Mockito.verify(
          sentryStore, Mockito.times(1)
  ).alterSentryGrantOwnerPrivilege(OWNER, SentryEntityType.USER, ownerPrivilege, null);
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
    notification.setOwnerType(TSentryObjectOwnerType.ROLE);
    notification.setOwnerName(OWNER);
    notification.setAuthorizable(authorizable);
    notification.setEventType(EventType.CREATE_DATABASE.toString());

    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    TSentryPrivilege ownerPrivilege = sentryServiceHandler.constructOwnerPrivilege(authorizable);
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).alterSentryGrantOwnerPrivilege(OWNER, SentryEntityType.ROLE, ownerPrivilege, null);

    notification.setOwnerType(TSentryObjectOwnerType.USER);
    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).alterSentryGrantOwnerPrivilege(OWNER, SentryEntityType.USER, ownerPrivilege, null);
  }

  @Test
  public void testAlterTableEventProcessing() throws Exception {

    SentryPolicyStoreProcessor sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setDb(DBNAME);
    authorizable.setTable(TABLENAME);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();
    notification.setId(1L);
    notification.setOwnerType(TSentryObjectOwnerType.ROLE);
    notification.setOwnerName(OWNER);
    notification.setAuthorizable(authorizable);
    notification.setEventType(EventType.ALTER_TABLE.toString());

    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).updateOwnerPrivilege(Mockito.eq(authorizable), Mockito.eq(OWNER), Mockito.eq(SentryEntityType.ROLE),
    Mockito.anyList());


    notification.setOwnerType(TSentryObjectOwnerType.USER);
    sentryServiceHandler.sentry_notify_hms_event(notification);

    //Verify Sentry Store is invoked to grant privilege.
    Mockito.verify(
            sentryStore, Mockito.times(1)
    ).updateOwnerPrivilege(Mockito.eq(authorizable), Mockito.eq(OWNER), Mockito.eq(SentryEntityType.ROLE),
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
}
