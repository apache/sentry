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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.sentry.hdfs.UniquePathsUpdate;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestHiveNotificationFetcher {
  @Test
  public void testGetEmptyNotificationsWhenHmsReturnsANullResponse() throws Exception {
    SentryStore store = Mockito.mock(SentryStore.class);
    HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);

    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(store, hmsConnection)) {
      List<NotificationEvent> events;

      Mockito.when(hmsClient.getNextNotification(0, Integer.MAX_VALUE, null))
          .thenReturn(null);

      events = fetcher.fetchNotifications(0);
      assertTrue(events.isEmpty());
    }
  }

  @Test
  public void testGetEmptyNotificationsWhenHmsReturnsEmptyEvents() throws Exception {
    SentryStore store = Mockito.mock(SentryStore.class);
    HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);

    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(store, hmsConnection)) {
      List<NotificationEvent> events;

      Mockito.when(hmsClient.getNextNotification(0, Integer.MAX_VALUE, null))
          .thenReturn(new NotificationEventResponse(Collections.<NotificationEvent>emptyList()));

      events = fetcher.fetchNotifications(0);
      assertTrue(events.isEmpty());
    }
  }

  @Test
  public void testGetAllNotificationsReturnedByHms() throws Exception {
    SentryStore store = Mockito.mock(SentryStore.class);
    HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);

    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(store, hmsConnection)) {
      List<NotificationEvent> events;

      Mockito.when(hmsClient.getNextNotification(0, Integer.MAX_VALUE, null))
          .thenReturn(new NotificationEventResponse(
              Arrays.<NotificationEvent>asList(
                  new NotificationEvent(1L, 0, "CREATE_DATABASE", ""),
                  new NotificationEvent(2L, 0, "CREATE_TABLE", "")
              )
          ));

      events = fetcher.fetchNotifications(0);
      assertEquals(2, events.size());
      assertEquals(1, events.get(0).getEventId());
      assertEquals("CREATE_DATABASE", events.get(0).getEventType());
      assertEquals(2, events.get(1).getEventId());
      assertEquals("CREATE_TABLE", events.get(1).getEventType());
    }
  }

  @Test
  public void testGetDuplicatedEventsAndFilterEventsAlreadySeen() throws Exception {
    final SentryStore store = Mockito.mock(SentryStore.class);
    HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);

    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(store, hmsConnection)) {
      List<NotificationEvent> events;

      /*
       * Requesting an ID > 0 will request all notifications from 0 again but filter those
       * already seen notifications with ID = 1
       */

      // This mock will also test that the NotificationFilter works as expected
      Mockito.when(hmsClient.getNextNotification(Mockito.eq(0L), Mockito.eq(Integer.MAX_VALUE),
          (NotificationFilter) Mockito.notNull())).thenAnswer(new Answer<NotificationEventResponse>() {
            @Override
            public NotificationEventResponse answer(InvocationOnMock invocation)
                throws Throwable {
              NotificationFilter filter = (NotificationFilter) invocation.getArguments()[2];
              NotificationEventResponse response = new NotificationEventResponse();

              List<NotificationEvent> events = Arrays.<NotificationEvent>asList(
                  new NotificationEvent(1L, 0, "CREATE_DATABASE", ""),
                  new NotificationEvent(1L, 0, "CREATE_TABLE", ""),
                  new NotificationEvent(2L, 0, "ALTER_TABLE", "")
              );

              for (NotificationEvent event : events) {
                String hash = UniquePathsUpdate.sha1(event);
                
                // We simulate that CREATE_DATABASE is already processed
                if (event.getEventType().equals("CREATE_DATABASE")) {
                  Mockito.when(store.isNotificationProcessed(Mockito.eq(hash))).thenReturn(true);
                } else {
                  Mockito.when(store.isNotificationProcessed(Mockito.eq(hash))).thenReturn(false);
                }

                if (filter.accept(event)) {
                  response.addToEvents(event);
                }
              }

              return response;
            }
          });

      events = fetcher.fetchNotifications(1);
      assertEquals(2, events.size());
      assertEquals(1, events.get(0).getEventId());
      assertEquals("CREATE_TABLE", events.get(0).getEventType());
      assertEquals(2, events.get(1).getEventId());
      assertEquals("ALTER_TABLE", events.get(1).getEventType());
    }
  }
}
