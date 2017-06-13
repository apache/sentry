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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hive.hcatalog.listener.MetaStoreEventListenerConstants;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This HMS post-event listener is used only to synchronize with HMS notifications on the Sentry server
 * whenever a DDL event happens on the Hive metastore.
 */
public class SentrySyncHMSNotificationsPostEventListener extends MetaStoreEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentrySyncHMSNotificationsPostEventListener.class);

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

  public SentrySyncHMSNotificationsPostEventListener(Configuration config) {
    super(config);

    if (!(config instanceof HiveConf)) {
      String error = "Could not initialize Plugin - Configuration is not an instanceof HiveConf";
      LOGGER.error(error);
      throw new RuntimeException(error);
    }

    authzConf = HiveAuthzConf.getAuthzConf((HiveConf)config);
  }

  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    syncNotificationEvents(tableEvent, "onCreateTable");
  }

  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    syncNotificationEvents(tableEvent, "onDropTable");
  }

  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    syncNotificationEvents(tableEvent, "onAlterTable");
  }

  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    syncNotificationEvents(partitionEvent, "onAddPartition");
  }

  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    syncNotificationEvents(partitionEvent, "onDropPartition");
  }

  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    syncNotificationEvents(partitionEvent, "onAlterPartition");
  }

  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    syncNotificationEvents(dbEvent, "onCreateDatabase");
  }

  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    syncNotificationEvents(dbEvent, "onDropDatabase");
  }

  /**
   * It requests the Sentry server the synchronization of recent notification events.
   *
   * After the sync call, the latest processed ID will be stored for future reference to avoid
   * syncing an ID that was already processed.
   *
   * @param event An event that contains a DB_NOTIFICATION_EVENT_ID_KEY_NAME value to request.
   * @throws MetaException In case an error happens when getting the SentryPolicyServiceClient.
   */
  private void syncNotificationEvents(ListenerEvent event, String eventName) throws MetaException {
    // Do not sync notifications if the event has failed.
    if (failedEvent(event, eventName)) {
      return;
    }

    Map<String, String> eventParameters = event.getParameters();
    if (!eventParameters.containsKey(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)) {
      return;
    }

    long eventId =
        Long.parseLong(eventParameters.get(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME));

    // This check is only for performance reasons to avoid calling the sync thrift call if the Sentry server
    // already processed the requested eventId.
    if (eventId <= latestProcessedId.get()) {
      return;
    }

    try(SentryPolicyServiceClient sentryClient = this.getSentryServiceClient()) {
      LOGGER.debug("Starting Sentry/HMS notifications sync for " + eventName + " (id: " + eventId + ")");
      long latestProcessedId = sentryClient.syncNotifications(eventId);
      LOGGER.debug("Finished Sentry/HMS notifications sync for " + eventName + " (id: " + eventId + ")");
      LOGGER.debug("Latest processed event ID returned by the Sentry server: " + latestProcessedId);

      updateProcessedId(latestProcessedId);
    } catch (SentryUserException e) {
      // This error is only logged. There is no need to throw an error to Hive because HMS sync is called
      // after the notification is already generated by Hive (as post-event).
      LOGGER.error("Failed to sync requested HMS notifications up to the event ID: " + eventId, e);
    }  catch (MetaException e) {
      throw e;
    } catch (Exception e) {
      throw new MetaException("Failed to connect to Sentry service "
              + e.getMessage());
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

  @VisibleForTesting
  SentryPolicyServiceClient getSentryServiceClient()
      throws MetaException {
    try {
      return SentryServiceClientFactory.create(authzConf);
    } catch (Exception e) {
      throw new MetaException("Failed to connect to Sentry service "
          + e.getMessage());
    }
  }

  public long getLastProcessedNotificationID() {
    return latestProcessedId.get();
  }

  private boolean failedEvent(ListenerEvent event, String eventName) {
    if (!event.getStatus()) {
      LOGGER.debug("Skip HMS synchronization request with the Sentry server for " + eventName + " event," +
          " since the operation failed. \n");
      return true;
    }

    return false;
  }
}
