/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import org.apache.sentry.core.common.utils.PubSub;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;

import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME_DEPRECATED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jdo.JDODataStoreException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.thrift.TException;
import org.apache.sentry.service.thrift.SentryHMSClient;
import org.apache.sentry.service.thrift.HiveConnectionFactory;
import org.apache.sentry.service.thrift.HiveNotificationFetcher;
import org.apache.sentry.api.common.SentryServiceUtil;
import org.apache.sentry.service.thrift.SentryStateBank;
import org.apache.sentry.service.thrift.SentryServiceState;
import org.apache.sentry.service.thrift.HMSFollowerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HMSFollower is the thread which follows the Hive MetaStore state changes from Sentry.
 * It gets the full update and notification logs from HMS and applies it to
 * update permissions stored in Sentry using SentryStore and also update the &lt obj,path &gt state
 * stored for HDFS-Sentry sync.
 */
public class HMSFollower implements Runnable, AutoCloseable, PubSub.Subscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(HMSFollower.class);
  private static final String FULL_UPDATE_TRIGGER = "FULL UPDATE TRIGGER: ";
  private static boolean connectedToHms = false;

  private SentryHMSClient client;
  private final Configuration authzConf;
  private final SentryStoreInterface sentryStore;
  private final NotificationProcessor notificationProcessor;
  private boolean readyToServe;
  private final HiveNotificationFetcher notificationFetcher;
  private final boolean hdfsSyncEnabled;
  private final AtomicBoolean fullUpdateHMS = new AtomicBoolean(false);

  private final LeaderStatusMonitor leaderMonitor;

  /**
   * Determine how deep should sentry look for newer notifications
   * Default value is -1 which means it gets till the max
   */
  private int sentryHMSFetchSize;
  /**
   * Current generation of HMS snapshots. HMSFollower is single-threaded, so no need
   * to protect against concurrent modification.
   */
  private long hmsImageId = SentryConstants.EMPTY_PATHS_SNAPSHOT_ID;

  /**
   * Configuring Hms Follower thread.
   *
   * @param conf sentry configuration
   * @param store sentry store
   * @param leaderMonitor singleton instance of LeaderStatusMonitor
   */
  public HMSFollower(Configuration conf, SentryStoreInterface store, LeaderStatusMonitor leaderMonitor,
              HiveConnectionFactory hiveConnectionFactory) {
    this(conf, store, leaderMonitor, hiveConnectionFactory, null);
  }

  /**
   * Constructor should be used only for testing purposes.
   *
   * @param conf sentry configuration
   * @param store sentry store
   * @param leaderMonitor
   * @param authServerName Server that sentry is Authorizing
   */
  @VisibleForTesting
  public HMSFollower(Configuration conf, SentryStoreInterface store, LeaderStatusMonitor leaderMonitor,
              HiveConnectionFactory hiveConnectionFactory, String authServerName) {
    LOGGER.info("HMSFollower is being initialized");
    readyToServe = false;
    authzConf = conf;
    this.leaderMonitor = leaderMonitor;
    sentryStore = store;

    if (authServerName == null) {
      authServerName = conf.get(AUTHZ_SERVER_NAME.getVar(),
        conf.get(AUTHZ_SERVER_NAME_DEPRECATED.getVar(), AUTHZ_SERVER_NAME_DEPRECATED.getDefault()));
    }

    notificationProcessor = new NotificationProcessor(sentryStore, authServerName, authzConf);
    client = new SentryHMSClient(authzConf, hiveConnectionFactory);
    hdfsSyncEnabled = SentryServiceUtil.isHDFSSyncEnabledNoCache(authzConf); // no cache to test different settings for hdfs sync
    notificationFetcher = new HiveNotificationFetcher(sentryStore, hiveConnectionFactory);

    // subscribe to full update notification
    if (conf.getBoolean(ServerConfig.SENTRY_SERVICE_FULL_UPDATE_PUBSUB, false)) {
      LOGGER.info(FULL_UPDATE_TRIGGER + "subscribing to topic " + PubSub.Topic.HDFS_SYNC_HMS.getName());
      PubSub.getInstance().subscribe(PubSub.Topic.HDFS_SYNC_HMS, this);
    }

    sentryHMSFetchSize = conf.getInt(ServerConfig.SENTRY_HMS_FETCH_SIZE, ServerConfig.SENTRY_HMS_FETCH_SIZE_DEFAULT);
    if(sentryHMSFetchSize < 0) {
      LOGGER.info("Sentry will fetch from HMS max depth");
    } else {
      LOGGER.info("Sentry will fetch from HMS with depth of {}", sentryHMSFetchSize);
    }

    if(!hdfsSyncEnabled) {
      try {
        // Clear all the HMS metadata learned so far and learn it fresh when the feature
        // is enabled back.
        store.clearHmsPathInformation();
      } catch (Exception ex) {
        LOGGER.error("Failed to clear HMS path info", ex);
        LOGGER.error("Please manually clear data from SENTRY_PATH_CHANGE/AUTHZ_PATH/AUTHZ_PATHS_MAPPING tables." +
                "If not, HDFS ACL's will be inconsistent when HDFS sync feature is enabled back.");
      }
    }
  }

  @VisibleForTesting
  public static boolean isConnectedToHms() {
    return connectedToHms;
  }

  @VisibleForTesting
  void setSentryHmsClient(SentryHMSClient client) {
    this.client = client;
  }

  @Override
  public void close() {
    if (client != null) {
      // Close any outstanding connections to HMS
      try {
        client.disconnect();
        SentryStateBank.disableState(HMSFollowerState.COMPONENT,HMSFollowerState.CONNECTED);
      } catch (Exception failure) {
        LOGGER.error("Failed to close the Sentry Hms Client", failure);
      }
    }

    notificationFetcher.close();
  }

  @Override
  public void run() {
    SentryStateBank.enableState(HMSFollowerState.COMPONENT,HMSFollowerState.STARTED);
    long lastProcessedNotificationId;
    try {
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
      if (!isLeader()) {
        // Close any outstanding connections to HMS
        close();
        return;
      }
      syncupWithHms(lastProcessedNotificationId);
    } finally {
      SentryStateBank.disableState(HMSFollowerState.COMPONENT,HMSFollowerState.STARTED);
    }
  }

  private boolean isLeader() {
    return (leaderMonitor == null) || leaderMonitor.isLeader();
  }

  @VisibleForTesting
  String getAuthServerName() {
    return notificationProcessor.getAuthServerName();
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
   void syncupWithHms(long notificationId) {
    try {
      client.connect();
      connectedToHms = true;
      SentryStateBank.enableState(HMSFollowerState.COMPONENT,HMSFollowerState.CONNECTED);
    } catch (Throwable e) {
      LOGGER.error("HMSFollower cannot connect to HMS!!", e);
      return;
    }

    try {
      if (hdfsSyncEnabled) {
        // Before getting notifications, checking if a full HMS snapshot is required.
        if (isFullSnapshotRequired(notificationId)) {
          createFullSnapshot();
          return;
        }
      } else if (isSentryOutOfSync(notificationId)) {
        // Out-of-sync, fetching all the notifications
        // in HMS NOTIFICATION_LOG.
        sentryStore.setLastProcessedNotificationID(0L);
        notificationId = 0L;
      }

      Collection<NotificationEvent> notifications = null;

      if(sentryHMSFetchSize < 0 ) {
        notifications = notificationFetcher.fetchNotifications(notificationId);
      } else {
        notifications = notificationFetcher.fetchNotifications(notificationId, sentryHMSFetchSize);
      }

      // After getting notifications, check if HMS did some clean-up and notifications
      // are out-of-sync with Sentry.
      if (hdfsSyncEnabled &&
              areNotificationsOutOfSync(notifications, notificationId)) {
        // Out-of-sync, taking a HMS full snapshot.
        createFullSnapshot();
        return;
      }

      if (!readyToServe) {
        // Allow users and/or applications who look into the Sentry console output to see
        // when Sentry is ready to serve.
        System.out.println("Sentry HMS support is ready");
        readyToServe = true;
      }

      // Continue with processing new notifications if no snapshots are done.
      processNotifications(notifications);
    } catch (TException e) {
      LOGGER.error("An error occurred while fetching HMS notifications: ", e);
      close();
    } catch (Throwable t) {
      // catching errors to prevent the executor to halt.
      LOGGER.error("Exception in HMSFollower! Caused by: " + t.getMessage(), t);

      close();
    }
  }

  /**
   * Checks if a new full HMS snapshot request is needed by checking if:
   * <ul>
   *   <li>Sentry HMS Notification table is EMPTY</li>
   *   <li>HDFSSync is enabled and Sentry Authz Snapshot table is EMPTY</li>
   *   <li>The current notification Id on the HMS is less than the
   *   latest processed by Sentry.</li>
   *   <li>Full Snapshot Signal is detected</li>
   * </ul>
   *
   * @param latestSentryNotificationId The notification Id to check against the HMS
   * @return True if a full snapshot is required; False otherwise.
   * @throws Exception If an error occurs while checking the SentryStore or the HMS client.
   */
  private boolean isFullSnapshotRequired(long latestSentryNotificationId) throws Exception {
    if (sentryStore.isHmsNotificationEmpty()) {
      String logMessage = String.format("Sentry Store has no HMS Notifications. Create Full HMS Snapshot. "
          + "latest sentry notification Id = %d", latestSentryNotificationId);
      LOGGER.debug(logMessage);
      System.out.println(SentryServiceUtil.getCurrentTimeStampWithMessage(logMessage));
      return true;
    }

    // Once HDFS sync is enabled, and if MAuthzPathsMapping
    // table is still empty, we need to request a full snapshot
    if (sentryStore.isAuthzPathsSnapshotEmpty()) {
      String logMessage = String.format("HDFSSync is enabled and MAuthzPathsMapping table is empty. Need to request a full snapshot", latestSentryNotificationId);
      LOGGER.debug(logMessage);
      System.out.println(SentryServiceUtil.getCurrentTimeStampWithMessage(logMessage));
      return true;
    }

    if(isSentryOutOfSync(latestSentryNotificationId)) {
      return true;
    }

    // check if forced full update is required, reset update flag to false
    // to only do it once per forced full update request.
    if (fullUpdateHMS.compareAndSet(true, false)) {
      String logMessage = FULL_UPDATE_TRIGGER + "initiating full HMS snapshot request";
      LOGGER.info(logMessage);
      System.out.println(SentryServiceUtil.getCurrentTimeStampWithMessage(logMessage));
      return true;
    }
    return false;
  }

  /**
   * Checks the last notification processed by sentry and the current event-id of
   * HMS to see if sentry is out of sync.
   *
   * @param latestSentryNotificationId The notification Id to check against the HMS
   * @return True, sentry is out-of-sync, False otherwise
   * @throws Exception If an error occurs while fetching the current notification from HMS
   */
  private boolean isSentryOutOfSync(long latestSentryNotificationId) throws Exception {
    long currentHmsNotificationId = notificationFetcher.getCurrentNotificationId();
    if (currentHmsNotificationId < latestSentryNotificationId) {
      LOGGER.info("The current notification ID on HMS = {} is less than the latest processed Sentry "
                      + "notification ID = {}. Sentry, Out-of-sync",
              currentHmsNotificationId, latestSentryNotificationId);
      return true;
    }
    return false;
  }

  /**
   * Checks if the HMS and Sentry processed notifications are out-of-sync.
   * This could happen because the HMS did some clean-up of old notifications
   * and Sentry was not requesting notifications during that time.
   *
   * @param events All new notifications to check for an out-of-sync.
   * @param latestProcessedId The latest notification processed by Sentry to check against the
   *        list of notifications events.
   * @return True if an out-of-sync is found; False otherwise.
   */
  private boolean areNotificationsOutOfSync(Collection<NotificationEvent> events,
      long latestProcessedId) {
    if (events.isEmpty()) {
      return false;
    }

    /*
     * If the sequence of notifications has a gap, then an out-of-sync might
     * have happened due to the following issue:
     *
     * - HDFS sync was disabled or Sentry was shutdown for a time period longer than
     * the HMS notification clean-up thread causing old notifications to be deleted.
     *
     * HMS notifications may contain both gaps in the sequence and duplicates
     * (the same ID repeated more then once for different events).
     *
     * To accept duplicates (see NotificationFetcher for more info), then a gap is found
     * if the 1st notification received is higher than the current ID processed + 1.
     * i.e.
     *   1st ID = 3, latest ID = 3 (duplicate found but no gap detected)
     *   1st ID = 4, latest ID = 3 (consecutive ID found but no gap detected)
     *   1st ID = 5, latest ID = 3 (a gap is detected)
     */

    List<NotificationEvent> eventList = (List<NotificationEvent>) events;
    long firstNotificationId = eventList.get(0).getEventId();

    if (firstNotificationId > (latestProcessedId + 1)) {
      String logMessage = String.format("First HMS event notification Id = %d is greater than latest Sentry processed"
          + "notification Id = %d + 1. Need to request a full HMS snapshot.", firstNotificationId, latestProcessedId);
      LOGGER.info(logMessage);
      System.out.println(SentryServiceUtil.getCurrentTimeStampWithMessage(logMessage));
      return true;
    }

    return false;
  }

  /**
   * Request for full snapshot and persists it if there is no snapshot available in the sentry
   * store. Also, wakes-up any waiting clients.
   *
   * @return ID of last notification processed.
   * @throws Exception if there are failures
   */
  private long createFullSnapshot() throws Exception {
    LOGGER.debug("Attempting to take full HMS snapshot");
    Preconditions.checkState(!SentryStateBank.isEnabled(SentryServiceState.COMPONENT,
        SentryServiceState.FULL_UPDATE_RUNNING),
        "HMSFollower shown loading full snapshot when it should not be.");
    try {
      // Set that the full update is running
      SentryStateBank
          .enableState(SentryServiceState.COMPONENT, SentryServiceState.FULL_UPDATE_RUNNING);

      PathsImage snapshotInfo = client.getFullSnapshot();
      if (snapshotInfo.getPathImage().isEmpty()) {
        LOGGER.debug("Received empty path image from HMS while taking a full snapshot");
        return snapshotInfo.getId();
      }

      // Check we're still the leader before persisting the new snapshot
      if (!isLeader()) {
        LOGGER.info("Not persisting full snapshot since not a leader");
        return SentryConstants.EMPTY_NOTIFICATION_ID;
      }
      try {
        if (hdfsSyncEnabled) {
          String logMessage = String.format("Persisting full snapshot for notification Id = %d", snapshotInfo.getId());
          LOGGER.info(logMessage);
          System.out.println(SentryServiceUtil.getCurrentTimeStampWithMessage(logMessage));
          sentryStore.persistFullPathsImage(snapshotInfo.getPathImage(), snapshotInfo.getId());
        } else {
          // We need to persist latest notificationID for next poll
          LOGGER.info("HDFSSync is disabled. Not Persisting full snapshot, "
              + "but only setting last processed notification Id = {}", snapshotInfo.getId());
          sentryStore.setLastProcessedNotificationID(snapshotInfo.getId());
        }
      } catch (Exception failure) {
        LOGGER.error("Received exception while persisting HMS path full snapshot ");
        throw failure;
      }
      // Wake up any HMS waiters that could have been put on hold before getting the
      // eventIDBefore value.
      wakeUpWaitingClientsForSync(snapshotInfo.getId());
      // HMSFollower connected to HMS and it finished full snapshot if that was required
      // Log this message only once
      String logMessage = String.format("Create full snapshot process is complete: snapshot Id %d", snapshotInfo.getId());
      LOGGER.info(logMessage);
      System.out.println(SentryServiceUtil.getCurrentTimeStampWithMessage(logMessage));
      return snapshotInfo.getId();
    } catch(Exception failure) {
      LOGGER.error("Received exception while creating HMS path full snapshot ");
      throw failure;
    } finally {
      SentryStateBank
          .disableState(SentryServiceState.COMPONENT, SentryServiceState.FULL_UPDATE_RUNNING);
    }
  }

  /**
   * Process the collection of notifications and wake up any waiting clients.
   * Also, persists the notification ID regardless of processing result.
   *
   * @param events list of event to be processed
   * @throws Exception if the complete notification list is not processed because of JDO Exception
   */
  public void processNotifications(Collection<NotificationEvent> events) throws Exception {
    boolean isNotificationProcessed;
    if (events.isEmpty()) {
      return;
    }

    for (NotificationEvent event : events) {
      isNotificationProcessed = false;
      try {
        // Only the leader should process the notifications
        if (!isLeader()) {
          LOGGER.debug("Not processing notifications since not a leader");
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
          LOGGER.debug("Explicitly Persisting Notification ID = {} ", event.getEventId());
          sentryStore.persistLastProcessedNotificationID(event.getEventId());
        } catch (Exception failure) {
          LOGGER.error("Received exception while persisting the notification ID = {}", event.getEventId());
          throw failure;
        }
      }
      // Wake up any HMS waiters that are waiting for this ID.
      wakeUpWaitingClientsForSync(event.getEventId());
    }
  }

  /**
   * Wakes up HMS waiters waiting for a specific event notification.<p>
   *
   * Verify that HMS image id didn't change since the last time we looked.
   * If id did, it is possible that notifications jumped backward, so reset
   * the counter to the current value.
   *
   * @param eventId Id of a notification
   */
  private void wakeUpWaitingClientsForSync(long eventId) {
    CounterWait counterWait = sentryStore.getCounterWait();

    LOGGER.debug("wakeUpWaitingClientsForSync: eventId = {}, hmsImageId = {}", eventId, hmsImageId);
    // Wake up any HMS waiters that are waiting for this ID.
    // counterWait should never be null, but tests mock SentryStore and a mocked one
    // doesn't have it.
    if (counterWait == null) {
      return;
    }

    long lastHMSSnapshotId = hmsImageId;
    try {
      // Read actual HMS image ID
      lastHMSSnapshotId = sentryStore.getLastProcessedImageID();
      LOGGER.debug("wakeUpWaitingClientsForSync: lastHMSSnapshotId = {}", lastHMSSnapshotId);
    } catch (Exception e) {
      counterWait.update(eventId);
      LOGGER.error("Failed to get the last processed HMS image id from sentry store");
      return;
    }

    // Reset the counter if the persisted image ID is greater than current image ID
    if (lastHMSSnapshotId > hmsImageId) {
      counterWait.reset(eventId);
      hmsImageId = lastHMSSnapshotId;
      LOGGER.debug("wakeUpWaitingClientsForSync: reset counterWait with eventId = {}, new hmsImageId = {}", eventId, hmsImageId);
    }

    LOGGER.debug("wakeUpWaitingClientsForSync: update counterWait with eventId = {}, hmsImageId = {}", eventId, hmsImageId);
    counterWait.update(eventId);
  }

  /**
   * PubSub.Subscriber callback API
   */
  @Override
  public void onMessage(PubSub.Topic topic, String message) {
    Preconditions.checkArgument(topic == PubSub.Topic.HDFS_SYNC_HMS, "Unexpected topic %s instead of %s", topic, PubSub.Topic.HDFS_SYNC_HMS);
    LOGGER.info(FULL_UPDATE_TRIGGER + "Received [{}, {}] notification", topic, message);
    fullUpdateHMS.set(true);
  }
}
