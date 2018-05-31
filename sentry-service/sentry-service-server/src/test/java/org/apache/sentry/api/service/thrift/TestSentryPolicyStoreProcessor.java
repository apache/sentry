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
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.sentry.api.common.ApiConstants;
import org.apache.sentry.api.common.Status;
import org.apache.sentry.api.common.ThriftConstants;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.model.db.AccessConstants;
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

  private Configuration conf;
  private static final SentryStore sentryStore = Mockito.mock(SentryStore.class);
  @Before
  public void setup() {
    conf = new Configuration(false);

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
    Assert.assertNotNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));
    Assert.assertEquals(privilege, sentryServiceHandler.constructOwnerPrivilege(authorizable));

    //check the behaviour when both DB name and table name are set
    authorizable = new TSentryAuthorizable("");
    authorizable.setDb("db1");
    authorizable.setTable("tb1");
    privilege.setTableName("tb1");
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
}
