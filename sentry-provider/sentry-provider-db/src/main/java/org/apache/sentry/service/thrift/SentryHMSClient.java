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

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.SentryMetrics;

import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for <Code>HiveMetaStoreClient</Code>
 *
 * <p>Abstracts communication with HMS and exposes APi's to connect/disconnect to HMS and to
 * request HMS snapshots and also for new notifications.
 */
class SentryHMSClient implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHMSClient.class);
  private final Configuration conf;
  private HiveMetaStoreClient client = null;
  private HiveConnectionFactory hiveConnectionFactory;

  private static final String SNAPSHOT = "snapshot";
  /** Measures time to get full snapshot. */
  private final Timer updateTimer = SentryMetrics.getInstance()
      .getTimer(name(FullUpdateInitializer.class, SNAPSHOT));
  /** Number of times update failed. */
  private final Counter failedSnapshotsCount = SentryMetrics.getInstance()
      .getCounter(name(FullUpdateInitializer.class, "failed"));

  SentryHMSClient(Configuration conf, HiveConnectionFactory hiveConnectionFactory) {
    this.conf = conf;
    this.hiveConnectionFactory = hiveConnectionFactory;
  }

  /**
   * Used only for testing purposes.
   *x
   * @param client HiveMetaStoreClient to be initialized
   */
  @VisibleForTesting
  void setClient(HiveMetaStoreClient client) {
    this.client = client;
  }

  /**
   * Used to know if the client is connected to HMS
   *
   * @return true if the client is connected to HMS false, if client is not connected.
   */
  boolean isConnected() {
    return client != null;
  }

  /**
   * Connects to HMS by creating HiveMetaStoreClient.
   *
   * @throws IOException          if could not establish connection
   * @throws InterruptedException if connection was interrupted
   * @throws MetaException        if other errors happened
   */
  void connect()
      throws IOException, InterruptedException, MetaException {
    if (client != null) {
      return;
    }
    client = hiveConnectionFactory.connect().getClient();
  }

  /**
   * Disconnects the HMS client.
   */
  public void disconnect() throws Exception {
    try {
      if (client != null) {
        LOGGER.info("Closing the HMS client connection");
        client.close();
      }
    } catch (Exception e) {
      LOGGER.error("failed to close Hive Connection Factory", e);
    } finally {
      client = null;
    }
  }

  /**
   * Closes the HMS client.
   *
   * <p>This is similar to disconnect. As this class implements AutoClosable, close should be
   * implemented.
   */
  public void close() throws Exception {
    disconnect();
  }

  /**
   * Creates HMS full snapshot.
   *
   * @return Full path snapshot and the last notification id on success
   */
  PathsImage getFullSnapshot() {
    try {
      if (client == null) {
        LOGGER.error("Client is not connected to HMS");
        return new PathsImage(Collections.<String, Set<String>>emptyMap(),
            SentryStore.EMPTY_NOTIFICATION_ID, SentryStore.EMPTY_PATHS_SNAPSHOT_ID);
      }

      CurrentNotificationEventId eventIdBefore = client.getCurrentNotificationEventId();
      Map<String, Set<String>> pathsFullSnapshot = fetchFullUpdate();
      if (pathsFullSnapshot.isEmpty()) {
        return new PathsImage(pathsFullSnapshot, SentryStore.EMPTY_NOTIFICATION_ID,
            SentryStore.EMPTY_PATHS_SNAPSHOT_ID);
      }

      CurrentNotificationEventId eventIdAfter = client.getCurrentNotificationEventId();
      LOGGER.info("NotificationID, Before Snapshot: {}, After Snapshot {}",
          eventIdBefore.getEventId(), eventIdAfter.getEventId());
      // To ensure point-in-time snapshot consistency, need to make sure
      // there were no HMS updates while retrieving the snapshot. If there are updates, snapshot
      // is discarded. New attempt will be made after 500 milliseconds when
      // HMSFollower runs again.
      if (!eventIdBefore.equals(eventIdAfter)) {
        LOGGER.error("Snapshot discarded, updates to HMS data while shapshot is being taken."
            + "ID Before: {}. ID After: {}", eventIdBefore.getEventId(), eventIdAfter.getEventId());
        return new PathsImage(Collections.<String, Set<String>>emptyMap(),
            SentryStore.EMPTY_NOTIFICATION_ID, SentryStore.EMPTY_PATHS_SNAPSHOT_ID);
      }

      LOGGER.info("Successfully fetched hive full snapshot, Current NotificationID: {}.",
          eventIdAfter);
      // As eventIDAfter is the last event that was processed, eventIDAfter is used to update
      // lastProcessedNotificationID instead of getting it from persistent store.
      return new PathsImage(pathsFullSnapshot, eventIdAfter.getEventId(),
          SentryStore.EMPTY_PATHS_SNAPSHOT_ID);
    } catch (TException failure) {
      LOGGER.error("Failed to communicate to HMS");
      return new PathsImage(Collections.<String, Set<String>>emptyMap(),
          SentryStore.EMPTY_NOTIFICATION_ID, SentryStore.EMPTY_PATHS_SNAPSHOT_ID);
    }
  }

  /**
   * Retrieve a Hive full snapshot from HMS.
   *
   * @return HMS snapshot. Snapshot consists of a mapping from auth object name to the set of paths
   *     corresponding to that name.
   */
  private Map<String, Set<String>> fetchFullUpdate() {
    LOGGER.info("Request full HMS snapshot");
    try (FullUpdateInitializer updateInitializer =
             new FullUpdateInitializer(hiveConnectionFactory, conf);
         Context context = updateTimer.time()) {
      Map<String, Set<String>> pathsUpdate = updateInitializer.getFullHMSSnapshot();
      LOGGER.info("Obtained full HMS snapshot");
      return pathsUpdate;
    } catch (Exception ignored) {
      failedSnapshotsCount.inc();
      LOGGER.error("Snapshot created failed ", ignored);
      return Collections.emptyMap();
    }
  }

  /**
   * Returns all HMS notifications with ID greater than the specified one
   *
   * @param notificationId ID of the last notification that was processed.
   * @return Collection of new events to be synced
   */
  Collection<NotificationEvent> getNotifications(long notificationId) throws Exception {
    if (client == null) {
      LOGGER.error("Client is not connected to HMS");
      return Collections.emptyList();
    }

    LOGGER.debug("Checking for notifications beyond {}", notificationId);

    // A bug HIVE-15761 (fixed on Hive 2.4.0) should allow requesting notifications using
    // an unprocessed notification ID without causing an exception. For now, we just
    // leave this workaround and log debug messages.
    CurrentNotificationEventId eventId = client.getCurrentNotificationEventId();
    LOGGER.debug("ID of Last HMS notifications is: {}", eventId.getEventId());
    if (eventId != null && eventId.getEventId() < notificationId) {
      LOGGER.debug("Last notification of HMS is smaller than what sentry processed, Something is"
          + "wrong. Sentry will request a full Snapshot");
      return Collections.emptyList();
    }

    if (eventId != null && eventId.getEventId() == notificationId) {
      return Collections.emptyList();
    }

    NotificationEventResponse response =
        client.getNextNotification(notificationId, Integer.MAX_VALUE, null);
    if (response != null && response.isSetEvents()) {
      LOGGER.debug("Last Id processed:{}. Received collection of notifications, Size:{}",
          notificationId, response.getEvents().size());
      return response.getEvents();
    }

    return Collections.emptyList();
  }

  /**
   * @return the latest notification Id logged by the HMS
   * @throws Exception when an error occurs when talking to the HMS client
   */
  public long getCurrentNotificationId() throws Exception {
    if (client == null) {
      LOGGER.error("Client is not connected to HMS");
      return SentryStore.EMPTY_NOTIFICATION_ID;
    }

    CurrentNotificationEventId eventId = client.getCurrentNotificationEventId();
    if (eventId.isSetEventId()) {
      return eventId.getEventId();
    }

    return SentryStore.EMPTY_NOTIFICATION_ID;
  }
}
