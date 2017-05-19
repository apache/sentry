/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.service.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestHMSFollower {
  SentryJSONMessageFactory messageFactory = new SentryJSONMessageFactory();
  SentryStore sentryStore = Mockito.mock(SentryStore.class);
  final static String hiveInstance = "server2";

  @Test
  public void testCreateDatabase() throws Exception {
    String dbName = "db1";

    // Create notification events
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.CREATE_DATABASE.toString(),
        messageFactory.buildCreateDatabaseMessage(new Database(dbName, null, "hdfs:///db1", null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");

    verify(sentryStore, times(0)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable));
  }

  @Test
  public void testDropDatabase() throws Exception {
    String dbName = "db1";

    // Create notification events
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.DROP_DATABASE.toString(),
        messageFactory.buildDropDatabaseMessage(new Database(dbName, null, "hdfs:///db1", null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");

    verify(sentryStore, times(1)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable)) ;
  }
  @Test
  public void testCreateTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    verify(sentryStore, times(0)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable));
  }
  @Test
  public void testDropTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.DROP_TABLE.toString(),
        messageFactory.buildDropTableMessage(new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    verify(sentryStore, times(1)).dropPrivilege(authorizable, HMSFollower.onDropSentryPrivilege(authorizable));
  }
  @Test
  public void testRenameTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    String newDbName = "db1";
    String newTableName = "table2";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0, HCatEventMessage.EventType.ALTER_TABLE.toString(),
        messageFactory.buildAlterTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
            new Table(newTableName, newDbName, null, 0, 0, 0, sd, null, null, null, null, null)).toString());
    notificationEvent.setDbName(newDbName);
    notificationEvent.setTableName(newTableName);
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, hiveInstance);
    hmsFollower.processNotificationEvents(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);

    verify(sentryStore, times(1)).renamePrivilege(authorizable, newAuthorizable, HMSFollower.onRenameSentryPrivilege(authorizable, newAuthorizable));
  }
}
