/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package org.apache.sentry.service.thrift;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.UserProvider;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * This class tests the notification metadata cache in HiveNotificationFetcher.
 */
public class TestHiveNotificationFetcherCache {
  private static Configuration conf = null;
  private static File dataDir;
  private static File policyFilePath;
  private static String[] adminGroups = {"adminGroup1"};
  private static char[] passwd = new char[]{'1', '2', '3'};
  private static SentryStore store;
  private static HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
  private static HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
  private static List<NotificationEvent> unFilteredEvents = new ArrayList<NotificationEvent>();


  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration(false);
    final String ourUrl = UserProvider.SCHEME_NAME + ":///";
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    // enable HDFS sync, so perm and path changes will be saved into DB
    conf.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    conf.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");

    // THis should be a UserGroupInformation provider
    CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);


    // The user credentials are stored as a static variable by UserGrouoInformation provider.
    // We need to only set the password the first time, an attempt to set it for the second
    // time fails with an exception.
    if (provider.getCredentialEntry(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_PASS) == null) {
      provider.createCredentialEntry(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_PASS, passwd);
      provider.flush();
    }

    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf.set(ServiceConstants.ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_URL,
            "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
    conf.set(ServiceConstants.ServerConfig.SENTRY_STORE_JDBC_PASS, "dummy");
    conf.setStrings(ServiceConstants.ServerConfig.ADMIN_GROUPS, adminGroups);
    conf.set(ServiceConstants.ServerConfig.SENTRY_STORE_GROUP_MAPPING,
            ServiceConstants.ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    policyFilePath = new File(dataDir, "local_policy_file.ini");
    conf.set(ServiceConstants.ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
            policyFilePath.getPath());

    // These tests do not need to retry transactions, so setting to 1 to reduce testing time
    conf.setInt(ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY, 1);

    // SentryStore should be initialized only once. The tables created by the test cases will
    // be cleaned up during the @After method.
     store = new SentryStore(conf);

      boolean hdfsSyncEnabled = SentryServiceUtil.isHDFSSyncEnabled(conf);
      store.setPersistUpdateDeltas(hdfsSyncEnabled);
    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    Mockito.when(hmsClient.getNextNotification(Mockito.eq(0L), Mockito.eq(Integer.MAX_VALUE),
            (IMetaStoreClient.NotificationFilter) Mockito.isNull())).thenAnswer(new Answer<NotificationEventResponse>() {
      @Override
      public NotificationEventResponse answer(InvocationOnMock invocation)
              throws Throwable {
        NotificationEventResponse response = new NotificationEventResponse();
        for (NotificationEvent event : unFilteredEvents) {
          response.addToEvents(event);
        }

        return response;
      }
    });

    // This mock will also test that the NotificationFilter works as expected
    Mockito.when(hmsClient.getNextNotification(Mockito.eq(0L), Mockito.eq(Integer.MAX_VALUE),
            (IMetaStoreClient.NotificationFilter) Mockito.notNull())).thenAnswer(new Answer<NotificationEventResponse>() {
      @Override
      public NotificationEventResponse answer(InvocationOnMock invocation)
              throws Throwable {
        IMetaStoreClient.NotificationFilter filter = (IMetaStoreClient.NotificationFilter) invocation.getArguments()[2];
        NotificationEventResponse response = new NotificationEventResponse();
        for (NotificationEvent event : unFilteredEvents) {
          if (filter.accept(event)) {
            response.addToEvents(event);
          }
        }

        return response;
      }
    });
  }

  @After
  public void after() {
    store.clearAllTables();
    unFilteredEvents.clear();
  }

  /**
   * This test makes sure that there is no additional load on the database because of additional fetches done by
   * notification fetcher by making sure that there are no cache miss when there are no gaps and out-of-sequence
   * notifications.
   * @throws Exception
   */
  @Test
  public void testWithNoGapsAndOutOfSequenceNotifications() throws Exception {

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(store, hmsConnection, new Configuration())) {
      List<NotificationEvent> filteredEvents;

      for (int count = 1; count <= 150; count++) {
        unFilteredEvents.add(new NotificationEvent(count, 0, "CREATE_DATABASE", ""));
      }

      filteredEvents = fetcher.fetchNotifications(0);
      assertEquals(150, filteredEvents.size());
      assertEquals(1, filteredEvents.get(0).getEventId());
      assertEquals("CREATE_DATABASE", filteredEvents.get(0).getEventType());

      filteredEvents = fetcher.fetchNotifications(150);
      assertEquals(0, filteredEvents.size());
      assertEquals(0, fetcher.getNotificationCacheMissCount());
    }
  }

  /**
   * This test makes sure that there is no additional load on the database because of additional fetches done by
   * notification fetcher by making sure that there are no cache miss even when there are gaps and out-of-sequence
   * notifications.
   * @throws Exception
   */
  @Test
  public void testWithGapsAndOutOfSequenceNotifications() throws Exception {
    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(store, hmsConnection, new Configuration())) {
      List<NotificationEvent> filteredEvents = new ArrayList<NotificationEvent>();
      int count = 1;
      for (int fetchCount = 1; fetchCount <= 10; fetchCount++) {
        for (; count <= (fetchCount * 10); count++) {
          if (!(count % 3 == 0 || count % 7 == 0) || (count %10 == 0)) {
            unFilteredEvents.add(new NotificationEvent(count, 0, "CREATE_DATABASE", ""));
          }
        }
        filteredEvents = fetcher.fetchNotifications(count - 1);
      }

      assertTrue("Invalid notification count", (filteredEvents.size() < 100));
      assertEquals(0, fetcher.getNotificationCacheMissCount());

      for (count = 1; count <= 100; count++) {
        if (count % 3 == 0 || count % 7 == 0) {
          unFilteredEvents.add(new NotificationEvent(count, 0, "CREATE_DATABASE", ""));
        }
      }
      fetcher.fetchNotifications(count - 1);
      assertEquals(0, fetcher.getNotificationCacheMissCount());
    }
  }
}
