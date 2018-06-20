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
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.sentry.api.service.thrift.TSentryAuthorizable;
import org.apache.sentry.api.service.thrift.TSentryHmsEventNotification;
import org.apache.sentry.api.service.thrift.TSentryObjectOwnerType;
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
  private static final String DBNAME = "db1";
  private static final String TABLENAME = "table1";
  private static final String TABLENAME_NEW = "table_new";
  private static final String OWNER = "owner1";
  private static final String OWNER_NEW = "owner_new";


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
  public void testSuccessfulEventsWithAnEventIdSyncNotifications() throws Exception {
    int eventId = 1;

    callAllEventsThatSynchronize(EventType.CREATE_DATABASE, SUCCESSFUL_STATUS, eventId++);
    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();
    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId - 1);
    notification.setEventType(EventType.CREATE_DATABASE.toString());
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);

    callAllEventsThatSynchronize(EventType.DROP_DATABASE, SUCCESSFUL_STATUS, eventId++);
    notification.setId(eventId - 1);
    notification.setEventType(EventType.DROP_DATABASE.toString());
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);

    callAllEventsThatSynchronize(EventType.CREATE_TABLE, SUCCESSFUL_STATUS, eventId++);
    notification.setId(eventId - 1);
    notification.setEventType(EventType.CREATE_TABLE.toString());
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);

    long latestEventId = callAllEventsThatSynchronize(EventType.DROP_TABLE, SUCCESSFUL_STATUS, eventId++);
    notification.setId(eventId - 1);
    notification.setEventType(EventType.DROP_TABLE.toString());
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);


    Mockito.verify(
            mockSentryClient, Mockito.times((int) latestEventId)
    ).close();

    Mockito.verifyNoMoreInteractions(mockSentryClient);
  }

  @Test
  public void testSyncNotificationsWithNewLatestProcessedIdMayAvoidSyncingCalls() throws Exception {
    Mockito.doAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        TSentryHmsEventNotification notification = (TSentryHmsEventNotification) invocation.getArguments()[0];
        return notification.getId() + 1;
      }
    }).when(mockSentryClient).notifyHmsNotification(Mockito.anyObject());

    long latestEventId = callAllEventsThatSynchronize(SUCCESSFUL_STATUS, EVENT_ID_SET);
    verifyInvocations();

    Mockito.verify(
            mockSentryClient, Mockito.times((int) latestEventId)
    ).close();
  }

  @Test
  public void notificationOnTableCreate() throws Exception {
    int eventId = 1;
    Table tb = new Table();
    tb.setDbName(DBNAME);
    tb.setTableName(TABLENAME);
    tb.setOwner(OWNER);
    CreateTableEvent createTableEvent = new CreateTableEvent(tb, true, null);
    setEventId(EVENT_ID_SET, createTableEvent, eventId);
    eventListener.onCreateTable(createTableEvent);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();

    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId);
    notification.setEventType(EventMessage.EventType.CREATE_TABLE.toString());
    notification.setOwnerName(OWNER);
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    notification.getAuthorizable().setDb(DBNAME);
    notification.getAuthorizable().setTable(TABLENAME);
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
  }

  @Test
  public void notificationOnTableDrop() throws Exception {
    int eventId = 1;
    Table tb = new Table();
    tb.setDbName(DBNAME);
    tb.setTableName(TABLENAME);
    tb.setOwner(OWNER);
    DropTableEvent dropTableEvent = new DropTableEvent(tb, true, true, null);
    setEventId(EVENT_ID_SET, dropTableEvent, eventId);
    eventListener.onDropTable(dropTableEvent);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();

    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId);
    notification.setEventType(EventMessage.EventType.DROP_TABLE.toString());
    notification.setOwnerName(OWNER);
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    notification.getAuthorizable().setDb(DBNAME);
    notification.getAuthorizable().setTable(TABLENAME);
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
  }

  @Test
  public void notificationOnDatabaseCreate() throws Exception {
    int eventId = 1;
    Database db = new Database();
    db.setName(DBNAME);
    db.setOwnerName(OWNER);
    db.setOwnerType(PrincipalType.USER);
    CreateDatabaseEvent createDatabaseEvent = new CreateDatabaseEvent(db, true, null);
    setEventId(EVENT_ID_SET, createDatabaseEvent, eventId);
    eventListener.onCreateDatabase(createDatabaseEvent);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();

    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId);
    notification.setEventType(EventMessage.EventType.CREATE_DATABASE.toString());
    notification.setOwnerName(OWNER);
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    notification.getAuthorizable().setDb(DBNAME);
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
  }

  @Test
  public void notificationOnDatabaseDrop() throws Exception {
    int eventId = 1;
    Database db = new Database();
    db.setName(DBNAME);
    db.setOwnerName(OWNER);
    db.setOwnerType(PrincipalType.USER);
    DropDatabaseEvent dropDatabaseEvent = new DropDatabaseEvent(db, true, null);
    setEventId(EVENT_ID_SET, dropDatabaseEvent, eventId);
    eventListener.onDropDatabase(dropDatabaseEvent);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();

    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId);
    notification.setEventType(EventType.DROP_DATABASE.toString());
    notification.setOwnerName(OWNER);
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    notification.getAuthorizable().setDb(DBNAME);
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
  }


  @Test
  public void notificationOnAlterTableOwnerChange() throws Exception {
    int eventId = 1;
    Table old_tb = new Table();
    old_tb.setDbName(DBNAME);
    old_tb.setTableName(TABLENAME);
    old_tb.setOwner(OWNER);

    Table new_tb = new Table();
    new_tb.setDbName(DBNAME);
    new_tb.setTableName(TABLENAME);
    new_tb.setOwner(OWNER_NEW);

    AlterTableEvent alterTableEvent = new AlterTableEvent(old_tb, new_tb, true, null);
    setEventId(EVENT_ID_SET, alterTableEvent, eventId);
    eventListener.onAlterTable(alterTableEvent);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();

    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId);
    notification.setEventType(EventType.ALTER_TABLE.toString());
    notification.setOwnerName(OWNER_NEW);
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    notification.getAuthorizable().setDb(DBNAME);
    notification.getAuthorizable().setTable(TABLENAME);

    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
  }

  @Test
  public void notificationOnAlterTableRename() throws Exception {
    int eventId = 1;
    Table old_tb = new Table();
    old_tb.setDbName(DBNAME);
    old_tb.setTableName(TABLENAME);
    old_tb.setOwner(OWNER);

    Table new_tb = new Table();
    new_tb.setDbName(DBNAME);
    new_tb.setTableName(TABLENAME_NEW);
    new_tb.setOwner(OWNER);

    AlterTableEvent alterTableEvent = new AlterTableEvent(old_tb, new_tb, true, null);
    setEventId(EVENT_ID_SET, alterTableEvent, eventId);
    eventListener.onAlterTable(alterTableEvent);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();

    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId);
    notification.setEventType(EventType.ALTER_TABLE.toString());
    notification.getAuthorizable().setDb(DBNAME);
    notification.getAuthorizable().setTable(TABLENAME_NEW);

    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
  }

  @Test
  public void notificationOnAlterTableNoRenameAndOwnerChange() throws Exception {
    int eventId = 1;
    Table old_tb = new Table();
    old_tb.setDbName(DBNAME);
    old_tb.setTableName(TABLENAME);
    old_tb.setOwner(OWNER);

    Table new_tb = new Table();
    new_tb.setDbName(DBNAME);
    new_tb.setTableName(TABLENAME);
    new_tb.setOwner(OWNER);

    AlterTableEvent alterTableEvent = new AlterTableEvent(old_tb, new_tb, true, null);
    setEventId(EVENT_ID_SET, alterTableEvent, eventId);
    eventListener.onAlterTable(alterTableEvent);

    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();

    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(eventId);
    notification.setEventType(EventType.ALTER_TABLE.toString());
    notification.getAuthorizable().setDb(DBNAME);
    notification.getAuthorizable().setTable(TABLENAME);

    Mockito.verify(
            mockSentryClient, Mockito.times(0)
    ).notifyHmsNotification(Mockito.anyObject());
  }

  private long callAllEventsThatSynchronize(boolean status, boolean eventIdSet) throws MetaException {
    long eventId = 0;

    CreateDatabaseEvent createDatabaseEvent = new CreateDatabaseEvent(null, status, null);
    setEventId(eventIdSet, createDatabaseEvent, ++eventId);
    eventListener.onCreateDatabase(createDatabaseEvent);

    DropDatabaseEvent dropDatabaseEvent = new DropDatabaseEvent(null, status, null);
    setEventId(eventIdSet, dropDatabaseEvent, ++eventId);
    eventListener.onDropDatabase(dropDatabaseEvent);

    CreateTableEvent createTableEvent = new CreateTableEvent(null, status, null);
    setEventId(eventIdSet, createTableEvent, ++eventId);
    eventListener.onCreateTable(createTableEvent);

    DropTableEvent dropTableEvent = new DropTableEvent(null, status, false, null);
    setEventId(eventIdSet, dropTableEvent, ++eventId);
    eventListener.onDropTable(dropTableEvent);

    return eventId;
  }

  private long callAllEventsThatSynchronize(EventType event, boolean status, long eventId) throws MetaException {
    switch (event) {
      case CREATE_DATABASE:
        CreateDatabaseEvent createDatabaseEvent = new CreateDatabaseEvent(null, status, null);
        setEventId(true, createDatabaseEvent, eventId);
        eventListener.onCreateDatabase(createDatabaseEvent);
        break;
      case DROP_DATABASE:
        DropDatabaseEvent dropDatabaseEvent = new DropDatabaseEvent(null, status, null);
        setEventId(true, dropDatabaseEvent, eventId);
        eventListener.onDropDatabase(dropDatabaseEvent);
        break;
      case CREATE_TABLE:

        CreateTableEvent createTableEvent = new CreateTableEvent(null, status, null);
        setEventId(true, createTableEvent, eventId);
        eventListener.onCreateTable(createTableEvent);
        break;
      case DROP_TABLE:
        DropTableEvent dropTableEvent = new DropTableEvent(null, status, false, null);
        setEventId(true, dropTableEvent, eventId);
        eventListener.onDropTable(dropTableEvent);
        break;
      default:
        return eventId;
    }
    return eventId;
  }

  private void verifyInvocations() throws Exception {
    TSentryHmsEventNotification notification = new TSentryHmsEventNotification();
    int i = 1;
    notification.setAuthorizable(new TSentryAuthorizable());

    notification.setId(i);
    notification.setEventType(EventMessage.EventType.CREATE_DATABASE.toString());
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
    i += 2;


    notification.setId(i);
    notification.setEventType(EventMessage.EventType.CREATE_TABLE.toString());
    notification.setOwnerType(TSentryObjectOwnerType.USER);
    Mockito.verify(
            mockSentryClient, Mockito.times(1)
    ).notifyHmsNotification(notification);
  }

  private void setEventId(boolean eventIdSet, ListenerEvent eventListener, long eventId) {
    if (eventIdSet) {
      eventListener.putParameter(
              MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME, String.valueOf(eventId));
    }
  }

}
