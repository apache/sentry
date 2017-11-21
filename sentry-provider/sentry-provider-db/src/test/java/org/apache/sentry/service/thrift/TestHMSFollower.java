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

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.HCatEventMessage.EventType;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory;
import org.apache.sentry.core.common.utils.PubSub;
import org.apache.sentry.hdfs.UniquePathsUpdate;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import static org.apache.sentry.hdfs.ServiceConstants.ServerConfig.SENTRY_SERVICE_FULL_UPDATE_PUBSUB;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.login.LoginException;

public class TestHMSFollower {

  private final static String hiveInstance = "server2";
  private final static Configuration configuration = new Configuration();
  private final SentryJSONMessageFactory messageFactory = new SentryJSONMessageFactory();
  private final SentryStore sentryStore = Mockito.mock(SentryStore.class);
  private static HiveSimpleConnectionFactory hiveConnectionFactory;

  private final static HiveConnectionFactory hmsConnectionMock
      = Mockito.mock(HiveConnectionFactory.class);
  private final static HiveMetaStoreClient hmsClientMock
      = Mockito.mock(HiveMetaStoreClient.class);

  @BeforeClass
  public static void setup() throws IOException, LoginException {
    hiveConnectionFactory = new HiveSimpleConnectionFactory(configuration, new HiveConf());
    hiveConnectionFactory.init();
    configuration.set("sentry.hive.sync.create", "true");
    configuration.set(SENTRY_SERVICE_FULL_UPDATE_PUBSUB, "true");

    // enable HDFS sync, so perm and path changes will be saved into DB
    configuration.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    configuration.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");
  }

  @Before
  public void setupMocks() throws Exception {
    reset(hmsConnectionMock, hmsClientMock);
    when(hmsConnectionMock.connect()).thenReturn(new HMSClient(hmsClientMock));
  }

  @Test
  public void testPersistAFullSnapshotWhenNoSnapshotAreProcessedYet() throws Exception {
    /*
     * TEST CASE
     *
     * Simulates (by using mocks) that Sentry has not processed any notifications, so this
     * should trigger a new full HMS snapshot request with the eventId = 1
     */

    final long SENTRY_PROCESSED_EVENT_ID = SentryStore.EMPTY_NOTIFICATION_ID;
    final long HMS_PROCESSED_EVENT_ID = 1L;

    // Mock that returns a full snapshot
    Map<String, Collection<String>> snapshotObjects = new HashMap<>();
    snapshotObjects.put("db", Sets.newHashSet("/db"));
    snapshotObjects.put("db.table", Sets.newHashSet("/db/table"));
    PathsImage fullSnapshot = new PathsImage(snapshotObjects, HMS_PROCESSED_EVENT_ID, 1);

    // Mock that returns the current HMS notification ID
    when(hmsClientMock.getCurrentNotificationEventId())
        .thenReturn(new CurrentNotificationEventId(fullSnapshot.getId()));

    SentryHMSClient sentryHmsClient = Mockito.mock(SentryHMSClient.class);
    when(sentryHmsClient.getFullSnapshot()).thenReturn(fullSnapshot);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hmsConnectionMock, hiveInstance);
    hmsFollower.setSentryHmsClient(sentryHmsClient);

    // 1st run should get a full snapshot because AuthzPathsMapping is empty
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(SENTRY_PROCESSED_EVENT_ID);
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(true);
    when(sentryStore.isHmsNotificationEmpty()).thenReturn(true);
    hmsFollower.run();
    verify(sentryStore, times(1)).persistFullPathsImage(
        fullSnapshot.getPathImage(), fullSnapshot.getId());
    // Saving notificationID is in the same transaction of saving full snapshot
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(fullSnapshot.getId());

    reset(sentryStore);

    // 2nd run should not get a snapshot because is already processed
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(fullSnapshot.getId());
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(0)).persistFullPathsImage(Mockito.anyMap(), Mockito.anyLong());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
  }

  @Test
  public void testPersistAFullSnapshotWhenFullSnapshotTrigger() throws Exception {
    /*
     * TEST CASE
     *
     * Simulates (by using mocks) the following:
     *
     * HMS client always returns the paths image with the eventId == 1.
     *
     * On the 1st run:  Sentry has not processed any notifications, so this
     * should trigger a new full HMS snapshot request with the eventId = 1
     *
     * On the 2nd run: Sentry store returns the latest eventId == 1,
     * which matches the eventId returned by HMS client. Because of the match,
     * no full update is triggered.
     *
     * On the 3d run: before the run, full update flag in HMSFollower is set via
     * publish-subscribe mechanism.
     * Sentry store still returns the latest eventId == 1,
     * which matches the eventId returned by HMS client. Because of the match,
     * no full update should be triggered. However, because of the trigger set,
     * a new full HMS snapshot will be triggered.
     *
     * On the 4th run: Sentry store returns the latest eventId == 1,
     * which matches the eventId returned by HMS client. Because of the match,
     * no full update is triggered. This is to check that forced trigger set
     * for run 3 only works once.
     *
     */

    final long SENTRY_PROCESSED_EVENT_ID = SentryStore.EMPTY_NOTIFICATION_ID;
    final long HMS_PROCESSED_EVENT_ID = 1L;

    // Mock that returns a full snapshot
    Map<String, Collection<String>> snapshotObjects = new HashMap<>();
    snapshotObjects.put("db", Sets.newHashSet("/db"));
    snapshotObjects.put("db.table", Sets.newHashSet("/db/table"));
    PathsImage fullSnapshot = new PathsImage(snapshotObjects, HMS_PROCESSED_EVENT_ID, 1);

    // Mock that returns the current HMS notification ID
    when(hmsClientMock.getCurrentNotificationEventId())
        .thenReturn(new CurrentNotificationEventId(fullSnapshot.getId()));

    SentryHMSClient sentryHmsClient = Mockito.mock(SentryHMSClient.class);
    when(sentryHmsClient.getFullSnapshot()).thenReturn(fullSnapshot);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hmsConnectionMock, hiveInstance);
    hmsFollower.setSentryHmsClient(sentryHmsClient);

    // 1st run should get a full snapshot because AuthzPathsMapping is empty
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(SENTRY_PROCESSED_EVENT_ID);
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(true);
    when(sentryStore.isHmsNotificationEmpty()).thenReturn(true);
    hmsFollower.run();
    verify(sentryStore, times(1)).persistFullPathsImage(
        fullSnapshot.getPathImage(), fullSnapshot.getId());
    // Saving notificationID is in the same transaction of saving full snapshot
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(fullSnapshot.getId());

    reset(sentryStore);

    // 2nd run should not get a snapshot because is already processed
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(fullSnapshot.getId());
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(0)).persistFullPathsImage(Mockito.anyMap(), Mockito.anyLong());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());

    reset(sentryStore);

    // 3d run should not get a snapshot because is already processed,
    // but because of full update trigger it will, as in the first run
    PubSub.getInstance().publish(PubSub.Topic.HDFS_SYNC_HMS, "message");

    when(sentryStore.getLastProcessedNotificationID()).thenReturn(fullSnapshot.getId());
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(1)).persistFullPathsImage(
        fullSnapshot.getPathImage(), fullSnapshot.getId());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(fullSnapshot.getId());

    reset(sentryStore);

    // 4th run should not get a snapshot because is already processed and publish-subscribe
    // trigger is only supposed to work once. This is exactly as 2nd run.
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(fullSnapshot.getId());
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(0)).persistFullPathsImage(Mockito.anyMap(), Mockito.anyLong());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());

  }

  @Test
  public void testPersistAFullSnapshotWhenAuthzsnapshotIsEmptyAndHDFSSyncIsEnabled() throws Exception {
    /*
     * TEST CASE
     *
     * Simulates (by using mocks) the following:
     *
     * Disable HDFSSync before triggering a full snapshot
     *
     * HMS client always returns the paths image with the eventId == 1.
     *
     * On the 1st run:  Sentry notification table is empty, so this
     * should trigger a new full HMS snapshot request with the eventId = 1
     * but it should not persist it, in stead only set last
     * last processed notification Id. This will prevent a
     * unless until notifications are out of sync or hdfs sync is enabled
     *
     * On the 2nd run: Just enable hdfs sync and a full snapshot should be triggered
     * because MAuthzPathsSnapshotId table is empty
     *
     */

    configuration.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "");
    configuration.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "");

    final long SENTRY_PROCESSED_EVENT_ID = SentryStore.EMPTY_NOTIFICATION_ID;
    final long HMS_PROCESSED_EVENT_ID = 1L;

    // Mock that returns a full snapshot
    Map<String, Collection<String>> snapshotObjects = new HashMap<>();
    snapshotObjects.put("db", Sets.newHashSet("/db"));
    snapshotObjects.put("db.table", Sets.newHashSet("/db/table"));
    PathsImage fullSnapshot = new PathsImage(snapshotObjects, HMS_PROCESSED_EVENT_ID, 1);

    SentryHMSClient sentryHmsClient = Mockito.mock(SentryHMSClient.class);
    when(sentryHmsClient.getFullSnapshot()).thenReturn(fullSnapshot);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hmsConnectionMock, hiveInstance);
    hmsFollower.setSentryHmsClient(sentryHmsClient);

    // 1st run should get a full snapshot because hms notificaions is empty
    // but it should never be persisted because HDFS sync is disabled
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(SENTRY_PROCESSED_EVENT_ID);
    when(sentryStore.isHmsNotificationEmpty()).thenReturn(true);
    hmsFollower.run();
    verify(sentryStore, times(0)).persistFullPathsImage(
        fullSnapshot.getPathImage(), fullSnapshot.getId());
    // Since hdfs sync is disabled we would set last processed notifications
    // and since we did trigger createFullSnapshot() method we won't process any notifications
    verify(sentryStore, times(1)).setLastProcessedNotificationID(fullSnapshot.getId());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(fullSnapshot.getId());

    reset(sentryStore);

    //Re-enable HDFS Sync and simply start the HMS follower thread, full snap shot
    // should be triggered because MAuthzPathsSnapshotId table is empty
    configuration.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    configuration.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");

    //Create a new hmsFollower instance since configuration is changing
    hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hmsConnectionMock, hiveInstance);
    hmsFollower.setSentryHmsClient(sentryHmsClient);


    //Set last processed notification Id to match the full new value 1L
    final long LATEST_EVENT_ID = 1L;
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(LATEST_EVENT_ID);
    //Mock that sets isHmsNotificationEmpty to false
    when(sentryStore.isHmsNotificationEmpty()).thenReturn(false);
    // Mock that sets the current HMS notification ID. Set it to match
    // last processed notification Id so that doesn't trigger a full snapshot
    when(hmsClientMock.getCurrentNotificationEventId())
        .thenReturn(new CurrentNotificationEventId(LATEST_EVENT_ID));
    //Mock that sets getting next notification eve
    when(hmsClientMock.getNextNotification(Mockito.eq(HMS_PROCESSED_EVENT_ID - 1), Mockito.eq(Integer.MAX_VALUE),
        (NotificationFilter) Mockito.notNull()))
        .thenReturn(new NotificationEventResponse(
            Arrays.<NotificationEvent>asList(
                new NotificationEvent(LATEST_EVENT_ID, 0, "", "")
            )
        ));
    //Mock that sets isAuthzPathsSnapshotEmpty to true so trigger this particular test
    when(sentryStore.isAuthzPathsSnapshotEmpty()).thenReturn(true);

    hmsFollower.run();
    verify(sentryStore, times(1)).persistFullPathsImage(
        fullSnapshot.getPathImage(), fullSnapshot.getId());
    verify(sentryStore, times(0)).setLastProcessedNotificationID(fullSnapshot.getId());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(fullSnapshot.getId());

    reset(sentryStore);
  }

  @Test
  public void testPersistAFullSnapshotWhenLastHmsNotificationIsLowerThanLastProcessed()
      throws Exception {
    /*
     * TEST CASE
     *
     * Simulates (by using mocks) that Sentry already processed (and persisted) a notification
     * with Id = 5, but the latest notification processed by the HMS is eventId = 1. So, an
     * out-of-sync issue is happening on Sentry and HMS. This out-of-sync issue should trigger
     * a new full HMS snapshot request with the same eventId = 1;
     */

    final long SENTRY_PROCESSED_EVENT_ID = 5L;
    final long HMS_PROCESSED_EVENT_ID = 1L;

    // Mock that returns a full snapshot
    Map<String, Collection<String>> snapshotObjects = new HashMap<>();
    snapshotObjects.put("db", Sets.newHashSet("/db"));
    snapshotObjects.put("db.table", Sets.newHashSet("/db/table"));
    PathsImage fullSnapshot = new PathsImage(snapshotObjects, HMS_PROCESSED_EVENT_ID, 1);

    // Mock that returns the current HMS notification ID
    when(hmsClientMock.getCurrentNotificationEventId())
        .thenReturn(new CurrentNotificationEventId(fullSnapshot.getId()));

    SentryHMSClient sentryHmsClient = Mockito.mock(SentryHMSClient.class);
    when(sentryHmsClient.getFullSnapshot()).thenReturn(fullSnapshot);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hmsConnectionMock, hiveInstance);
    hmsFollower.setSentryHmsClient(sentryHmsClient);

    // 1st run should get a full snapshot
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(SENTRY_PROCESSED_EVENT_ID);
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(1)).persistFullPathsImage(Mockito.anyMap(), Mockito.anyLong());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());

    reset(sentryStore);

    // 2nd run should not get a snapshot because is already processed
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(HMS_PROCESSED_EVENT_ID);
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(0)).persistFullPathsImage(Mockito.anyMap(), Mockito.anyLong());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
  }

  @Test
  public void testPersistAFullSnapshotWhenNextExpectedEventIsNotAvailable() throws Exception {
    /*
     * TEST CASE
     *
     * Simulates (by using mocks) that Sentry already processed (and persisted) a notification
     * with Id = 1, and the latest notification processed by the HMS is eventId = 5. So, new
     * notifications should be fetched.
     *
     * The number of expected notifications should be 4, but we simulate that we fetch only one
     * notification with eventId = 5 causing an out-of-sync because the expected notificatoin
     * should be 2. This out-of-sync should trigger a new full HMS snapshot request with the
     * same eventId = 5.
     */

    final long SENTRY_PROCESSED_EVENT_ID = 1L;
    final long HMS_PROCESSED_EVENT_ID = 5L;

    // Mock that returns a full snapshot
    Map<String, Collection<String>> snapshotObjects = new HashMap<>();
    snapshotObjects.put("db", Sets.newHashSet("/db"));
    snapshotObjects.put("db.table", Sets.newHashSet("/db/table"));
    PathsImage fullSnapshot = new PathsImage(snapshotObjects, HMS_PROCESSED_EVENT_ID, 1);

    // Mock that returns the current HMS notification ID
    when(hmsClientMock.getCurrentNotificationEventId())
        .thenReturn(new CurrentNotificationEventId(fullSnapshot.getId()));

    SentryHMSClient sentryHmsClient = Mockito.mock(SentryHMSClient.class);
    when(sentryHmsClient.getFullSnapshot()).thenReturn(fullSnapshot);

    when(hmsClientMock.getNextNotification(Mockito.eq(SENTRY_PROCESSED_EVENT_ID - 1), Mockito.eq(Integer.MAX_VALUE),
        (NotificationFilter) Mockito.notNull()))
        .thenReturn(new NotificationEventResponse(
            Arrays.<NotificationEvent>asList(
                new NotificationEvent(fullSnapshot.getId(), 0, "", "")
            )
        ));

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hmsConnectionMock, hiveInstance);
    hmsFollower.setSentryHmsClient(sentryHmsClient);

    // 1st run should get a full snapshot
    when(sentryStore.getLastProcessedNotificationID())
        .thenReturn(SENTRY_PROCESSED_EVENT_ID);
    when(sentryStore.isHmsNotificationEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(1)).persistFullPathsImage(Mockito.anyMap(), Mockito.anyLong());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());

    reset(sentryStore);

    // 2nd run should not get a snapshot because is already processed
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(HMS_PROCESSED_EVENT_ID);
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    hmsFollower.run();
    verify(sentryStore, times(0)).persistFullPathsImage(Mockito.anyMap(), Mockito.anyLong());
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
  }

  /**
   * Test that HMSFollower uses the input authentication server name when it is not null
   */
  @Test
  public void testInputConfigurationGetInputAuthServerName() {
    Configuration sentryConfiguration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(sentryConfiguration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    String authServerName = hmsFollower.getAuthServerName();

    Assert.assertEquals(true, authServerName.equals(hiveInstance));
  }

  /**
   * Test that HMSFollower uses the default authentication server name when its constructor input
   * value is null and the configuration does not configure AUTHZ_SERVER_NAME nor
   * AUTHZ_SERVER_NAME_DEPRECATED
   */
  @Test
  public void testNoConfigurationGetDefaultAuthServerName() {
    Configuration sentryConfiguration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(sentryConfiguration, sentryStore, null,
        hiveConnectionFactory, null);
    String authServerName = hmsFollower.getAuthServerName();

    Assert.assertEquals(true, authServerName.equals(AuthzConfVars.AUTHZ_SERVER_NAME_DEPRECATED.getDefault()));
  }

  /**
   * Test that HMSFollower uses the configured authentication server name when its constructor input
   * value is null and the configuration contains configuration for AUTHZ_SERVER_NAME
   */
  @Test
  public void testNewNameConfigurationGetAuthServerName() {
    String serverName = "newServer";
    Configuration sentryConfiguration = new Configuration();
    sentryConfiguration.set(HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), serverName);
    HMSFollower hmsFollower = new HMSFollower(sentryConfiguration, sentryStore, null,
        hiveConnectionFactory, null);
    String authServerName = hmsFollower.getAuthServerName();

    Assert.assertEquals(true, authServerName.equals(serverName));
  }

  /**
   * Test that HMSFollower uses the configured deprecated authentication server name when its constructor input
   * value is null and the configuration contains configuration for AUTHZ_SERVER_NAME_DEPRECATED
   */
  @Test
  public void testOldNameConfigurationGetAuthServerName() {
    String serverName = "oldServer";
    Configuration sentryConfiguration = new Configuration();
    sentryConfiguration.set(AuthzConfVars.AUTHZ_SERVER_NAME_DEPRECATED.getVar(), serverName);
    HMSFollower hmsFollower = new HMSFollower(sentryConfiguration, sentryStore, null,
        hiveConnectionFactory, null);
    String authServerName = hmsFollower.getAuthServerName();

    Assert.assertEquals(true, authServerName.equals(serverName));
  }

  /**
   * Constructs create database event and makes sure that appropriate sentry store API's
   * are invoke when the event is processed by hms follower.
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatabase() throws Exception {
    String dbName = "db1";

    // Create notification events
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.CREATE_DATABASE.toString(),
        messageFactory.buildCreateDatabaseMessage(new Database(dbName, null, "hdfs:///db1", null))
            .toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    hmsFollower.processNotifications(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");

    verify(sentryStore, times(1))
        .dropPrivilege(authorizable, NotificationProcessor.getPermUpdatableOnDrop(authorizable));
  }

  /**
   * Constructs drop database event and makes sure that appropriate sentry store API's
   * are invoke when the event is processed by hms follower.
   *
   * @throws Exception
   */
  @Test
  public void testDropDatabase() throws Exception {
    String dbName = "db1";

    // Create notification events
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.DROP_DATABASE.toString(),
        messageFactory.buildDropDatabaseMessage(new Database(dbName, null, "hdfs:///db1", null))
            .toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    hmsFollower.processNotifications(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");

    verify(sentryStore, times(1))
        .dropPrivilege(authorizable, NotificationProcessor.getPermUpdatableOnDrop(authorizable));
  }

  /**
   * Constructs create table event and makes sure that appropriate sentry store API's
   * are invoke when the event is processed by hms follower.
   *
   * @throws Exception
   */
  @Test
  public void testCreateTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null))
            .toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    hmsFollower.processNotifications(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    verify(sentryStore, times(1))
        .dropPrivilege(authorizable, NotificationProcessor.getPermUpdatableOnDrop(authorizable));
  }

  /**
   * Constructs drop table event and makes sure that appropriate sentry store API's
   * are invoke when the event is processed by hms follower.
   *
   * @throws Exception
   */
  @Test
  public void testDropTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.DROP_TABLE.toString(),
        messageFactory.buildDropTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null))
            .toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    hmsFollower.processNotifications(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb("db1");
    authorizable.setTable(tableName);

    verify(sentryStore, times(1))
        .dropPrivilege(authorizable, NotificationProcessor.getPermUpdatableOnDrop(authorizable));
  }

  /**
   * Constructs rename table event and makes sure that appropriate sentry store API's
   * are invoke when the event is processed by hms follower.
   *
   * @throws Exception
   */
  @Test
  public void testRenameTable() throws Exception {
    String dbName = "db1";
    String tableName = "table1";

    String newDbName = "db1";
    String newTableName = "table2";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.ALTER_TABLE.toString(),
        messageFactory.buildAlterTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
            new Table(newTableName, newDbName, null, 0, 0, 0, sd, null, null, null, null, null))
            .toString());
    notificationEvent.setDbName(newDbName);
    notificationEvent.setTableName(newTableName);
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    hmsFollower.processNotifications(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);

    verify(sentryStore, times(1)).renamePrivilege(authorizable, newAuthorizable,
        NotificationProcessor.getPermUpdatableOnRename(authorizable, newAuthorizable));
  }


  @Ignore
  /**
   * Constructs a bunch of events and passed to processor of hms follower. One of those is alter
   * partition event with out actually changing anything(invalid event). Idea is to make sure that
   * hms follower calls appropriate sentry store API's for the events processed by hms follower
   * after processing the invalid alter partition event.
   *
   * @throws Exception
   */
  @Test
  public void testAlterPartitionWithInvalidEvent() throws Exception {
    String dbName = "db1";
    String tableName1 = "table1";
    String tableName2 = "table2";
    long inputEventId = 1;
    List<NotificationEvent> events = new ArrayList<>();
    NotificationEvent notificationEvent;
    List<FieldSchema> partCols;
    StorageDescriptor sd;
    Mockito.doNothing().when(sentryStore).persistLastProcessedNotificationID(Mockito.anyLong());
    //noinspection unchecked
    Mockito.doNothing().when(sentryStore).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table1");
    partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName1, dbName, null, 0, 0, 0, sd, partCols, null, null, null,
        null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(table).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();

    // Create a partition
    List<Partition> partitions = new ArrayList<>();
    StorageDescriptor invalidSd = new StorageDescriptor();
    invalidSd.setLocation(null);
    Partition partition = new Partition(Collections.singletonList("today"), dbName, tableName1,
        0, 0, sd, null);
    partitions.add(partition);
    notificationEvent = new NotificationEvent(inputEventId, 0, EventType.ADD_PARTITION.toString(),
       messageFactory.buildAddPartitionMessage(table, partitions).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    //Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle ADD_PARTITION notification
    // and persistLastProcessedNotificationID was not invoked.
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();

    // Create a alter notification with out actually changing anything.
    // This is an invalid event and should be processed by sentry store.
    // Event Id should be explicitly persisted using persistLastProcessedNotificationID
    notificationEvent = new NotificationEvent(inputEventId, 0, EventType.ALTER_PARTITION.toString(),
        messageFactory.buildAlterPartitionMessage(table, partition, partition).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that persistLastProcessedNotificationID is invoked explicitly.
    verify(sentryStore, times(1)).persistLastProcessedNotificationID(inputEventId - 1);
    reset(sentryStore);
    events.clear();

    // Create a alter notification with some actual change.
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://user/hive/warehouse/db1.db/table1");
    Partition updatedPartition = new Partition(partition);
    updatedPartition.setSd(sd);
    notificationEvent = new NotificationEvent(inputEventId, 0, EventType.ALTER_PARTITION.toString(),
      messageFactory.buildAlterPartitionMessage(table, partition, updatedPartition).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that updateAuthzPathsMapping was invoked once to handle ALTER_PARTITION
    // notification and persistLastProcessedNotificationID was not invoked.
    verify(sentryStore, times(1)).updateAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyString(), Mockito.anyString(), Mockito.any(UniquePathsUpdate.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(inputEventId - 1);
    reset(sentryStore);
    events.clear();

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table2");
    partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table1 = new Table(tableName2, dbName, null, 0, 0, 0, sd, partCols, null, null, null,
        null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(table1).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName2);
    events.add(notificationEvent);
    // Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
  }

  /**
   * Constructs a bunch of events and passed to processor of hms follower. One of those is alter
   * table event with out actually changing anything(invalid event). Idea is to make sure that
   * hms follower calls appropriate sentry store API's for the events processed by hms follower
   * after processing the invalid alter table event.
   *
   * @throws Exception
   */
  @Test
  public void testAlterTableWithInvalidEvent() throws Exception {
    String dbName = "db1";
    String tableName1 = "table1";
    String tableName2 = "table2";
    long inputEventId = 1;
    List<NotificationEvent> events = new ArrayList<>();
    NotificationEvent notificationEvent;
    List<FieldSchema> partCols;
    StorageDescriptor sd;
    Mockito.doNothing().when(sentryStore).persistLastProcessedNotificationID(Mockito.anyLong());
    //noinspection unchecked
    Mockito.doNothing().when(sentryStore).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));

    Configuration configuration = new Configuration();
    // enable HDFS sync, so perm and path changes will be saved into DB
    configuration.set(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory");
    configuration.set(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "org.apache.sentry.hdfs.SentryPlugin");

    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table1");
    partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName1, dbName, null, 0, 0, 0, sd, partCols, null, null, null,
        null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(table).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();

    // Create alter table notification with out actually changing anything.
    // This notification should not be processed by sentry server
    // Notification should be persisted explicitly
    notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.ALTER_TABLE.toString(),
        messageFactory.buildAlterTableMessage(
            new Table(tableName1, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
            new Table(tableName1, dbName, null, 0, 0, 0, sd, null, null, null, null, null))
            .toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName1);
    events = new ArrayList<>();
    events.add(notificationEvent);
    inputEventId += 1;
    // Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that renameAuthzObj and deleteAuthzPathsMapping were  not invoked
    // to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID is explicitly invoked
    verify(sentryStore, times(0)).renameAuthzObj(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(UniquePathsUpdate.class));
    //noinspection unchecked
    verify(sentryStore, times(0)).deleteAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));
    verify(sentryStore, times(1)).persistLastProcessedNotificationID(Mockito.anyLong());
    reset(sentryStore);
    events.clear();

    // Create a table
    sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table2");
    partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table1 = new Table(tableName2, dbName, null, 0, 0, 0, sd, partCols, null, null, null,
        null);
    notificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(table1).toString());
    notificationEvent.setDbName(dbName);
    notificationEvent.setTableName(tableName2);
    events.add(notificationEvent);
    // Process the notification
    hmsFollower.processNotifications(events);
    // Make sure that addAuthzPathsMapping was invoked once to handle CREATE_TABLE notification
    // and persistLastProcessedNotificationID was not invoked.
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(),
        Mockito.anyCollection(), Mockito.any(UniquePathsUpdate.class));
    verify(sentryStore, times(0)).persistLastProcessedNotificationID(Mockito.anyLong());
  }

  /**
   * Constructs a two events and passed to processor of hms follower. First one being create table
   * event with location information(Invalid Event). Idea is to make sure that hms follower calls
   * appropriate sentry store API's for the event processed by hms follower after processing the
   * invalid create table event.
   *
   * @throws Exception
   */
  @Test
  public void testCreateTableAfterInvalidEvent() throws Exception {
    String dbName = "db1";
    String tableName = "table1";
    long inputEventId = 1;

    Mockito.doNothing().when(sentryStore).persistLastProcessedNotificationID(Mockito.anyLong());
    //noinspection unchecked
    Mockito.doNothing().when(sentryStore)
        .addAuthzPathsMapping(Mockito.anyString(), Mockito.anyCollection(),
            Mockito.any(UniquePathsUpdate.class));

    // Create invalid notification event. The location of the storage descriptor is null, which is invalid for creating table
    StorageDescriptor invalidSd = new StorageDescriptor();
    invalidSd.setLocation(null);
    NotificationEvent invalidNotificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, invalidSd, null, null, null, null, null))
            .toString());

    // Create valid notification event
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs://db1.db/table1");
    inputEventId += 1;
    NotificationEvent notificationEvent = new NotificationEvent(inputEventId, 0,
        HCatEventMessage.EventType.CREATE_TABLE.toString(),
        messageFactory.buildCreateTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null))
            .toString());
    List<NotificationEvent> events = new ArrayList<>();
    events.add(invalidNotificationEvent);
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    hmsFollower.processNotifications(events);

    // invalid event updates notification ID directly
    verify(sentryStore, times(1)).persistLastProcessedNotificationID(inputEventId - 1);

    // next valid event update path, which updates notification ID
    //noinspection unchecked
    verify(sentryStore, times(1)).addAuthzPathsMapping(Mockito.anyString(), Mockito.anyCollection(),
        Mockito.any(UniquePathsUpdate.class));
  }

  @Test
  public void testNoHdfsNoPersistAFullSnapshot() throws Exception {
    /*
     * TEST CASE
     *
     * Simulates (by using mocks) that Sentry has not processed any notifications, so this
     * should trigger a new full HMS snapshot request with the eventId = 1
     */

    final long SENTRY_PROCESSED_EVENT_ID = SentryStore.EMPTY_NOTIFICATION_ID;
    final long HMS_PROCESSED_EVENT_ID = 1L;

    // Mock that returns a full snapshot
    Map<String, Collection<String>> snapshotObjects = new HashMap<>();
    snapshotObjects.put("db", Sets.newHashSet("/db"));
    snapshotObjects.put("db.table", Sets.newHashSet("/db/table"));
    PathsImage fullSnapshot = new PathsImage(snapshotObjects, HMS_PROCESSED_EVENT_ID, 1);

    // Mock that returns the current HMS notification ID
    when(hmsClientMock.getCurrentNotificationEventId())
        .thenReturn(new CurrentNotificationEventId(fullSnapshot.getId()));

    SentryHMSClient sentryHmsClient = Mockito.mock(SentryHMSClient.class);
    when(sentryHmsClient.getFullSnapshot()).thenReturn(fullSnapshot);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hmsConnectionMock, hiveInstance);
    hmsFollower.setSentryHmsClient(sentryHmsClient);

    // 1st run should get a full snapshot because AuthzPathsMapping is empty
    when(sentryStore.getLastProcessedNotificationID()).thenReturn(SENTRY_PROCESSED_EVENT_ID);
    when(sentryStore.isAuthzPathsMappingEmpty()).thenReturn(false);
    when(sentryStore.isHmsNotificationEmpty()).thenReturn(true);
    hmsFollower.run();
    verify(sentryStore, times(0)).persistFullPathsImage(fullSnapshot.getPathImage(), fullSnapshot.getId());
    verify(sentryStore, times(1)).setLastProcessedNotificationID(fullSnapshot.getId());
    verify(sentryStore, times(1)).isHmsNotificationEmpty();
    verify(sentryStore, times(0)).isAuthzPathsMappingEmpty();
  }

  @Test
  public void testNoHdfsSyncAlterTableNotPersisted() throws Exception {
    String dbName = "db1";
    String tableName = "table1";
    String newDbName = "db1";
    String newTableName = "table2";

    // Create notification events
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs:///db1.db/table1");
    NotificationEvent notificationEvent = new NotificationEvent(1, 0,
        HCatEventMessage.EventType.ALTER_TABLE.toString(),
        messageFactory.buildAlterTableMessage(
            new Table(tableName, dbName, null, 0, 0, 0, sd, null, null, null, null, null),
            new Table(newTableName, newDbName, null, 0, 0, 0, sd, null, null, null, null, null))
            .toString());
    notificationEvent.setDbName(newDbName);
    notificationEvent.setTableName(newTableName);
    List<NotificationEvent> events = new ArrayList<>();
    events.add(notificationEvent);

    Configuration configuration = new Configuration();
    HMSFollower hmsFollower = new HMSFollower(configuration, sentryStore, null,
        hiveConnectionFactory, hiveInstance);
    hmsFollower.processNotifications(events);

    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setServer(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);

    verify(sentryStore, times(0)).renamePrivilege(authorizable, newAuthorizable,
        NotificationProcessor.getPermUpdatableOnRename(authorizable, newAuthorizable));
  }
}
