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

import com.google.common.annotations.VisibleForTesting;

import java.net.SocketException;

import java.util.Collection;
import javax.jdo.JDODataStoreException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HMSFollower is the thread which follows the Hive MetaStore state changes from Sentry.
 * It gets the full update and notification logs from HMS and applies it to
 * update permissions stored in Sentry using SentryStore and also update the &lt obj,path &gt state
 * stored for HDFS-Sentry sync.
 */
public class HMSFollower implements Runnable, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HMSFollower.class);
  private static boolean connectedToHms = false;
  private final SentryHMSClient client;
  private final Configuration authzConf;
  private final SentryStore sentryStore;
  private final NotificationProcessor notificationProcessor;

  private final LeaderStatusMonitor leaderMonitor;

  /**
   * Configuring Hms Follower thread.
   *
   * @param conf sentry configuration
   * @param store sentry store
   * @param leaderMonitor singleton instance of LeaderStatusMonitor
   */
  HMSFollower(Configuration conf, SentryStore store, LeaderStatusMonitor leaderMonitor,
              HiveSimpleConnectionFactory hiveConnectionFactory) {
    this(conf, store, leaderMonitor, hiveConnectionFactory, null);
  }

  @VisibleForTesting
  /**
   * Constructor should be used only for testing purposes.
   *
   * @param conf sentry configuration
   * @param store sentry store
   * @param leaderMonitor
   * @param authServerName Server that sentry is Authorizing
   */
  HMSFollower(Configuration conf, SentryStore store, LeaderStatusMonitor leaderMonitor,
              HiveSimpleConnectionFactory hiveConnectionFactory, String authServerName) {
    LOGGER.info("HMSFollower is being initialized");
    authzConf = conf;
    this.leaderMonitor = leaderMonitor;
    sentryStore = store;
   if (authServerName == null) {
     HiveConf hiveConf = new HiveConf();
     authServerName = hiveConf.get(HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME.getVar());
   }
    notificationProcessor = new NotificationProcessor(sentryStore, authServerName, authzConf);
    client = new SentryHMSClient(authzConf, hiveConnectionFactory);
  }

  @VisibleForTesting
  public static boolean isConnectedToHms() {
    return connectedToHms;
  }

  @Override
  public void close() {
    if (client != null) {
      // Close any outstanding connections to HMS
      try {
        client.disconnect();
      } catch (Exception failure) {
        LOGGER.error("Failed to close the Sentry Hms Client", failure);
      }
    }
  }

  @Override
  public void run() {
    long lastProcessedNotificationId;
    try {
      // Initializing lastProcessedNotificationId based on the latest persisted notification ID.
      lastProcessedNotificationId = sentryStore.getLastProcessedNotificationID();
    } catch (Exception e) {
      LOGGER.error("Failed to get the last processed notification id from sentry store, "
          + "Skipping the processing", e);
      return;
    }
    // Wake any clients connected to this service waiting for HMS already processed notifications.
    wakeUpWaitingClientsForSync(lastProcessedNotificationId);
    // Only the leader should listen to HMS updates
    if ((leaderMonitor != null) && !leaderMonitor.isLeader()) {
      // Close any outstanding connections to HMS
      close();
      return;
    }
    syncupWithHms(lastProcessedNotificationId);
  }

  /**
   * Processes new Hive Metastore notifications.
   *
   * <p>If no notifications are processed yet, then it
   * does a full initial snapshot of the Hive Metastore followed by new notifications updates that
   * could have happened after it.
   *
   * <p>Clients connections waiting for an event notification will be
   * woken up afterwards.
   */
  private void syncupWithHms(long notificationId) {
    try {
      client.connect();
      connectedToHms = true;
    } catch (Throwable e) {
      LOGGER.error("HMSFollower cannot connect to HMS!!", e);
      return;
    }

    try {
      long lastProcessedNotificationId = notificationId;
      // Create a full HMS snapshot if there is none
      // Decision of taking full snapshot is based on AuthzPathsMapping information persisted
      // in the sentry persistent store. If AuthzPathsMapping is empty, snapshot is needed.
      if (sentryStore.isAuthzPathsMappingEmpty()) {
        lastProcessedNotificationId = createFullSnapshot();
        if (lastProcessedNotificationId == SentryStore.EMPTY_NOTIFICATION_ID) {
          return;
        }
      }
      // Get the new notification from HMS and process them.
      processNotifications(client.getNotifications(lastProcessedNotificationId));
    } catch (TException e) {
      // If the underlying exception is around socket exception,
      // it is better to retry connection to HMS
      if (e.getCause() instanceof SocketException) {
        LOGGER.error("Encountered Socket Exception during fetching Notification entries,"
            + " will attempt to reconnect to HMS after configured interval", e);
        close();
      } else {
        LOGGER.error("ThriftException occurred communicating with HMS", e);
      }
    } catch (Throwable t) {
      // catching errors to prevent the executor to halt.
      LOGGER.error("Exception in HMSFollower! Caused by: " + t.getMessage(),
          t);
    }
  }

  /**
   * Request for full snapshot and persists it if there is no snapshot available in the
   * sentry store. Also, wakes-up any waiting clients.
   *
   * @return ID of last notification processed.
   * @throws Exception if there are failures
   */
  private long createFullSnapshot() throws Exception {
    LOGGER.debug("Attempting to take full HMS snapshot");
    PathsImage snapshotInfo = client.getFullSnapshot();
    if (snapshotInfo.getPathImage().isEmpty()) {
      return snapshotInfo.getId();
    }
    try {
      LOGGER.debug("Persisting HMS path full snapshot");
      sentryStore.persistFullPathsImage(snapshotInfo.getPathImage());
      sentryStore.persistLastProcessedNotificationID(snapshotInfo.getId());
    } catch (Exception failure) {
      LOGGER.error("Received exception while persisting HMS path full snapshot ");
      throw failure;
    }
    // Wake up any HMS waiters that could have been put on hold before getting the
    // eventIDBefore value.
    wakeUpWaitingClientsForSync(snapshotInfo.getId());
    // HMSFollower connected to HMS and it finished full snapshot if that was required
    // Log this message only once
    LOGGER.info("Sentry HMS support is ready");
    return snapshotInfo.getId();
  }

  /**
   * Process the collection of notifications and wake up any waiting clients.
   * Also, persists the notification ID regardless of processing result.
   *
   * @param events list of event to be processed
   * @throws Exception if the complete notification list is not processed because of JDO Exception
   */
  void processNotifications(Collection<NotificationEvent> events) throws Exception {
    boolean isNotificationProcessed;
    if (events.isEmpty()) {
      return;
    }
    for (NotificationEvent event : events) {
      isNotificationProcessed = false;
      try {
        // Only the leader should process the notifications
        if ((leaderMonitor != null) && !leaderMonitor.isLeader()) {
          return;
        }
        isNotificationProcessed = notificationProcessor.processNotificationEvent(event);
      } catch (Exception e) {
        if (e.getCause() instanceof JDODataStoreException) {
          LOGGER.info("Received JDO Storage Exception, Could be because of processing "
              + "duplicate notification");
          if (event.getEventId() <= sentryStore.getLastProcessedNotificationID()) {
            // Rest of the notifications need not be processed.
            LOGGER.error("Received event with Id: {} which is smaller then the ID "
                + "persisted in store", event.getEventId());
            break;
          }
        } else {
          LOGGER.error("Processing the notification with ID:{} failed with exception {}",
              event.getEventId(), e);
        }
      }
      if (!isNotificationProcessed) {
        try {
          // Update the notification ID in the persistent store even when the notification is
          // not processed as the content in in the notification is not valid.
          // Continue processing the next notification.
          LOGGER.debug("Explicitly Persisting Notification ID:{}", event.getEventId());
          sentryStore.persistLastProcessedNotificationID(event.getEventId());
        } catch (Exception failure) {
          LOGGER.error("Received exception while persisting the notification ID "
              + event.getEventId());
          throw failure;
        }
      }
      // Wake up any HMS waiters that are waiting for this ID.
      wakeUpWaitingClientsForSync(event.getEventId());
    }
  }

  /**
   * Wakes up HMS waiters waiting for a specific event notification.
   *
   * @param eventId Id of a notification
   */
  private void wakeUpWaitingClientsForSync(long eventId) {
    CounterWait counterWait = sentryStore.getCounterWait();

    // Wake up any HMS waiters that are waiting for this ID.
    // counterWait should never be null, but tests mock SentryStore and a mocked one
    // doesn't have it.
    if (counterWait != null) {
      counterWait.update(eventId);
    }
  }
}
