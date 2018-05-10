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
package org.apache.sentry.binding.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Testing class that tests and verifies the sync sentry notifications are called correctly.
 */
public class TestSentrySyncHMSNotificationsPostEventListener {
  private static final boolean FAILED_STATUS = false;
  private static final boolean SUCCESSFUL_STATUS = true;
  private static final boolean EVENT_ID_SET = true;
  private static final boolean EVENT_ID_UNSET = false;

  private SentrySyncHMSNotificationsPostEventListener eventListener;
  private SentryPolicyServiceClient mockSentryClient;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException, MetaException, SentryUserException {
    String sentryConfFile = tempFolder.newFile().getAbsolutePath();

    HiveConf hiveConf = new HiveConf(TestSentrySyncHMSNotificationsPostEventListener.class);
    hiveConf.set(HiveAuthzConf.HIVE_SENTRY_CONF_URL, "file://" + sentryConfFile);

    // Instead of generating an empty sentry-site.xml, we just write the same info from HiveConf.
    // The SentrySyncHMSNotificationsPostEventListener won't use any information from it after all.
    hiveConf.writeXml(new FileOutputStream(sentryConfFile));

    eventListener = new SentrySyncHMSNotificationsPostEventListener(hiveConf);

    mockSentryClient = Mockito.mock(SentryPolicyServiceClient.class);

    // For some reason I cannot use a Mockito.spy() on the eventListener and just mock the
    // getSentryServiceClient() to return the mock. When the TestURI runs before this
    // test, then a mock exception is thrown saying a I have an unfinished stubbing method.
    // This was the best approach I could take for now.
    eventListener.setSentryServiceClient(mockSentryClient);
  }

  @Test
  public void testFailedEventsDoNotSyncNotifications() throws MetaException, SentryUserException {
    callAllEventsThatSynchronize(FAILED_STATUS, EVENT_ID_UNSET);
    Mockito.verifyZeroInteractions(mockSentryClient);
  }

  @Test
  public void testEventsWithoutAnEventIdDoNotSyncNotifications() throws MetaException {
    callAllEventsThatSynchronize(SUCCESSFUL_STATUS, EVENT_ID_UNSET);
    Mockito.verifyZeroInteractions(mockSentryClient);
  }

  @Test
  public void testSuccessfulEventsWithAnEventIdSyncNotifications() throws Exception {
    long latestEventId = callAllEventsThatSynchronize(SUCCESSFUL_STATUS, EVENT_ID_SET);

    for (int i=1; i<=latestEventId; i++) {
      Mockito.verify(
          mockSentryClient, Mockito.times(1)
      ).syncNotifications(i);
    }

    Mockito.verify(
        mockSentryClient, Mockito.times((int)latestEventId)
    ).close();

    Mockito.verifyNoMoreInteractions(mockSentryClient);
  }

  @Test
  public void testSyncNotificationsWithNewLatestProcessedIdMayAvoidSyncingCalls() throws Exception {
    Mockito.doAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        Long id = (Long)invocation.getArguments()[0];
        return id + 1;
      }
    }).when(mockSentryClient).syncNotifications(Mockito.anyLong());

    long latestEventId = callAllEventsThatSynchronize(SUCCESSFUL_STATUS, EVENT_ID_SET);

    for (int i=1; i<=latestEventId; i+=2) {
      Mockito.verify(
          mockSentryClient, Mockito.times(1)
      ).syncNotifications(i);
    }

    Mockito.verify(
        mockSentryClient, Mockito.times((int)latestEventId / 2)
    ).close();

    Mockito.verifyNoMoreInteractions(mockSentryClient);
  }

  private long callAllEventsThatSynchronize(boolean status, boolean eventIdSet) throws MetaException {
    long eventId = 0;

    CreateDatabaseEvent createDatabaseEvent = new CreateDatabaseEvent(null, status , null);
    setEventId(eventIdSet, createDatabaseEvent, ++eventId);
    eventListener.onCreateDatabase(createDatabaseEvent);

    DropDatabaseEvent dropDatabaseEvent = new DropDatabaseEvent(null, status , null);
    setEventId(eventIdSet, dropDatabaseEvent, ++eventId);
    eventListener.onDropDatabase(dropDatabaseEvent);

    CreateTableEvent createTableEvent = new CreateTableEvent(null, status , null);
    setEventId(eventIdSet, createTableEvent, ++eventId);
    eventListener.onCreateTable(createTableEvent);

    DropTableEvent dropTableEvent = new DropTableEvent(null, status , false, null);
    setEventId(eventIdSet, dropTableEvent, ++eventId);
    eventListener.onDropTable(dropTableEvent);

    return eventId;
  }

  private void setEventId(boolean eventIdSet, ListenerEvent eventListener, long eventId) {
    if (eventIdSet) {
      eventListener.putParameter(
          MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME, String.valueOf(eventId));
    }
  }
}
