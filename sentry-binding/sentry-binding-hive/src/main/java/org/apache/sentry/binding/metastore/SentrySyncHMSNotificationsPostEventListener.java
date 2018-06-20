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

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This HMS post-event listener is used only to synchronize with HMS notifications on the Sentry server
 * whenever a DDL event happens on the Hive metastore.
 */
public class SentrySyncHMSNotificationsPostEventListener extends MetaStoreEventListener {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentrySyncHMSNotificationsPostEventListener.class);

  private final HiveAuthzConf authzConf;

  /*
   * Latest processed ID by the Sentry server. May only increase.
   *
   * This listener will track the latest event ID processed by the Sentry server so that it avoids calling
   * the sync request in case a late thread attempts to synchronize again an already processed ID.
   *
   * The variable is shared across threads, so the AtomicLong variable guarantees that is increased
   * monotonically.
   */
  private final AtomicLong latestProcessedId = new AtomicLong(0);

  /*
   * A client used for testing purposes only. I
   *
   * It may be set by unit-tests as a mock object and used to verify that the client methods
   * were called correctly (see TestSentrySyncHMSNotificationsPostEventListener)
   */
  private SentryPolicyServiceClient serviceClient;

  public SentrySyncHMSNotificationsPostEventListener(Configuration config) {
    super(config);

    if (!(config instanceof HiveConf)) {
      String error = "Could not initialize Plugin - Configuration is not an instanceof HiveConf";
      LOGGER.error(error);
      throw new RuntimeException(error);
    }

    authzConf = HiveAuthzConf.getAuthzConf((HiveConf) config);
  }

  /**
   * Notify sentry server when new table is created
   *
   * @param tableEvent Create table Event
   * @throws MetaException
   */
  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    // Failure event, Need not be notified.
    if (failedEvent(tableEvent, EventType.CREATE_TABLE)) {
      return;
    }
    SentryHmsEvent event = new SentryHmsEvent(tableEvent);
    notifyHmsEvent(event);
  }

  /**
   * Notify sentry server when table is dropped
   *
   * @param tableEvent Drop table event
   * @throws MetaException
   */
  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    // Failure event, Need not be notified.
    if (failedEvent(tableEvent, EventType.DROP_TABLE)) {
      return;
    }
    SentryHmsEvent event = new SentryHmsEvent(tableEvent);
    notifyHmsEvent(event);
  }

  /**
   * Notify sentry server when when table is altered.
   * Owner information is updated in the request only when there is owner change.
   * Sentry is not notified when neither rename happened nor owner is changed
   *
   * @param tableEvent Alter table event
   * @throws MetaException When both the owner change and rename is seen.
   */
  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    if (tableEvent == null) {
      return;
    }

    Table oldTable = tableEvent.getOldTable();
    Table newTable = tableEvent.getNewTable();

    if (oldTable == null) {
      return;
    }

    if (newTable == null) {
      return;
    }
    // Failure event, Need not be notified.
    if (failedEvent(tableEvent, EventType.ALTER_TABLE)) {
      return;
    }

    if(StringUtils.equals(oldTable.getOwner(), newTable.getOwner()) &&
        StringUtils.equalsIgnoreCase(oldTable.getDbName(), newTable.getDbName()) &&
        StringUtils.equalsIgnoreCase(oldTable.getTableName(), newTable.getTableName())) {
      // nothing to notify, neither rename happened nor owner is changed
      return;
    }

    SentryHmsEvent event = new SentryHmsEvent(tableEvent);
    notifyHmsEvent(event);
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    // no-op
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    // no-op
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    // no-op
  }

  /**
   * Notify sentry server when new database is created
   *
   * @param dbEvent Create database event
   * @throws MetaException
   */
  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    // Failure event, Need not be notified.
    if (failedEvent(dbEvent, EventType.CREATE_DATABASE)) {
      return;
    }
    SentryHmsEvent event = new SentryHmsEvent(dbEvent);
    notifyHmsEvent(event);
  }

  /**
   * Notify sentry server when database is dropped
   *
   * @param dbEvent Drop database event.
   * @throws MetaException
   */
  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    // Failure event, Need not be notified.
    if (failedEvent(dbEvent, EventType.DROP_DATABASE)) {
      return;
    }
    SentryHmsEvent event = new SentryHmsEvent(dbEvent);
    notifyHmsEvent(event);
  }

  /**
   * Notifies sentry server about the HMS Event and related metadata.
   *
   * @param event Sentry HMS event.
   */
  private void notifyHmsEvent(SentryHmsEvent event ) {
    /* If the HMS is running in an active transaction, then we do not want to sync with Sentry
     * because the desired eventId is not available for Sentry yet, and Sentry may block the HMS
     * forever or until a read time-out happens.
     * */
    if(event.isMetastoreTransactionActive()) {
      return;
    }

    if (!shouldSyncEvent(event)) {
      event.setEventId(0L);
    }

    try (SentryPolicyServiceClient sentryClient = this.getSentryServiceClient()) {
      LOGGER.debug("Notifying sentry about Notification for {} (id: {})", event.getEventType(),
              event.getEventId());
      long sentryLatestProcessedId = sentryClient.notifyHmsNotification(event.getHmsEventNotification());
      LOGGER.debug("Finished Notifying sentry about Notification for {} (id: {})", event.getEventType(),
             event.getEventId());
      LOGGER.debug("Latest processed event ID returned by the Sentry server: {}", sentryLatestProcessedId);
      updateProcessedId(sentryLatestProcessedId);
    } catch (Exception e) {
      // This error is only logged. There is no need to throw an error to Hive because HMS sync is called
      // after the notification is already generated by Hive (as post-event).
      LOGGER.error("Encountered failure while notifying notification for {} (id: {})",
              event.getEventType(), event.getEventId(), e);
    }
  }

  /**
   * Updates the latest processed ID, if and only if eventId is bigger. This keeps the contract that
   * {@link #latestProcessedId} may only increase.
   *
   * @param eventId The value to be set on the {@link #latestProcessedId}
   */
  private void updateProcessedId(long eventId) {
    long oldVal = latestProcessedId.get();
    if (eventId > oldVal) {
      // It is fine for the compareAndSet to fail
      latestProcessedId.compareAndSet(oldVal, eventId);
    }
  }

  /**
   * Sets the sentry client object (for testing purposes only)
   * <p>
   * It may be set by unit-tests as a mock object and used to verify that the client methods
   * were called correctly (see TestSentrySyncHMSNotificationsPostEventListener).
   */
  @VisibleForTesting
  void setSentryServiceClient(SentryPolicyServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  private SentryPolicyServiceClient getSentryServiceClient() throws MetaException {
    // Return the sentry client in case was set by the unit tests.
    if (serviceClient != null) {
      return serviceClient;
    }

    try {
      return SentryServiceClientFactory.create(authzConf);
    } catch (Exception e) {
      throw new MetaException("Failed to connect to Sentry service " + e.getMessage());
    }
  }

  private boolean failedEvent(ListenerEvent event, EventType eventType) {
    if (!event.getStatus()) {
      LOGGER.debug("Skip HMS synchronization request with the Sentry server for {} " +
              "{} since the operation failed. \n", eventType.toString(), event);
      return true;
    }

    return false;
  }

  /**
   * Performs checks to make sure if the event should be synced.
   *
   * @param event SentryHmsEvent
   * @return False: if Event should not be synced, True otherwise.
   */
  private boolean shouldSyncEvent(SentryHmsEvent event) {

    // Sync need not be performed, Event id is not updated in the event.
    if(event.getEventId() < 0) {
      return false;
    }

    // This check is only for performance reasons to avoid calling the sync thrift call if the Sentry
    // server already processed the requested eventId.
    if (event.getEventId() <= latestProcessedId.get()) {
      return false;
    }
    return true;
  }
}
