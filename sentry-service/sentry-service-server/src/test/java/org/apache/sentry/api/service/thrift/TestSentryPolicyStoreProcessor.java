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

import com.codahale.metrics.Gauge;
import org.apache.sentry.api.common.ApiConstants;
import org.apache.sentry.api.common.ThriftConstants;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.service.common.ServiceConstants;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
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
    Assert.assertNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));


    //Check behaviour when DB name is not set
    conf.setBoolean(ServiceConstants.ServerConfig.SENTRY_ENABLE_OWNER_PRIVILEGES, true);
    sentryServiceHandler =
            new SentryPolicyStoreProcessor(ApiConstants.SentryPolicyServiceConstants.SENTRY_POLICY_SERVICE_NAME,
                    conf, sentryStore);
    authorizable = new TSentryAuthorizable("");
    authorizable.setTable("tb1");
    Assert.assertNull(sentryServiceHandler.constructOwnerPrivilege(authorizable));

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
}
