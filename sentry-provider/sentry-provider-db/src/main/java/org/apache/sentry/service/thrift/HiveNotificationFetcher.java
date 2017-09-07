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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.sentry.hdfs.UniquePathsUpdate;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class used to fetch Hive MetaStore notifications.
 */
public final class HiveNotificationFetcher implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveNotificationFetcher.class);

  private final SentryStore sentryStore;
  private final HiveConnectionFactory hmsConnectionFactory;
  private HiveMetaStoreClient hmsClient;

  /* The following cache and last filtered ID help us to avoid making less calls to the DB */
  private long lastIdFiltered = 0;
  private Set<String> cache = new HashSet<>();

  HiveNotificationFetcher(SentryStore sentryStore, HiveConnectionFactory hmsConnectionFactory) {
    this.sentryStore = sentryStore;
    this.hmsConnectionFactory = hmsConnectionFactory;
  }

  /**
   * Fetch new HMS notifications appeared since a specified event ID. The returned list may
   * include notifications with the same specified ID if they were not seen by Sentry.
   *
   * @param lastEventId The event ID to use to request notifications.
   * @return A list of newer notifications unseen by Sentry.
   * @throws Exception If an error occurs on the HMS communication.
   */
  List<NotificationEvent> fetchNotifications(long lastEventId) throws Exception {
    return fetchNotifications(lastEventId, Integer.MAX_VALUE);
  }

  /**
   * Fetch new HMS notifications appeared since a specified event ID. The returned list may
   * include notifications with the same specified ID if they were not seen by Sentry.
   *
   * @param lastEventId The event ID to use to request notifications.
   * @param maxEvents The maximum number of events to fetch.
   * @return A list of newer notifications unseen by Sentry.
   * @throws Exception If an error occurs on the HMS communication.
   */
  List<NotificationEvent> fetchNotifications(long lastEventId, int maxEvents) throws Exception {
    NotificationFilter filter = null;

    /*
     * HMS may bring duplicated events that were committed later than the previous request. To bring
     * those newer duplicated events, we request new notifications from the last seen ID - 1.
     *
     * A current problem is that we could miss duplicates committed much more later, but because
     * HMS does not guarantee the order of those, then it is safer to avoid processing them.
     *
     * TODO: We can avoid doing this once HIVE-16886 is fixed.
     */
    if (lastEventId > 0) {
      filter = createNotificationFilterFor(lastEventId);
      lastEventId--;
    }

    LOGGER.debug("Requesting HMS notifications since ID = {}", lastEventId);

    NotificationEventResponse response;
    try {
      response = getHmsClient().getNextNotification(lastEventId, maxEvents, filter);
    } catch (Exception e) {
      close();
      throw e;
    }

    if (response != null && response.isSetEvents()) {
      LOGGER.debug("Fetched {} new HMS notification(s)", response.getEventsSize());
      return response.getEvents();
    }

    return Collections.emptyList();
  }

  /**
   * Returns a HMS notification filter for a specific notification ID. HMS notifications may
   * have duplicated IDs, so the filter uses a SHA-1 hash to check for a unique notification.
   *
   * @param id the notification ID to filter
   * @return the HMS notification filter
   */
  private NotificationFilter createNotificationFilterFor(final long id) {
    /*
     * A SHA-1 hex value that keeps unique notifications processed is persisted on the Sentry DB.
     * To keep unnecessary calls to the DB, we use a cache that keeps seen hashes of the
     * specified ID. If a new filter ID is used, then we clean up the cache.
     */

    if (lastIdFiltered != id) {
      lastIdFiltered = id;
      cache.clear();
    }

    return new NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent notificationEvent) {
        if (notificationEvent.getEventId() == id) {
          String hash = UniquePathsUpdate.sha1(notificationEvent);

          try {
            if (cache.contains(hash) || sentryStore.isNotificationProcessed(hash)) {
              cache.add(hash);

              LOGGER.debug("Ignoring HMS notification already processed: ID = {}", id);
              return false;
            }
          } catch (Exception e) {
            LOGGER.error("An error occurred while checking if notification {} is already "
                + "processed: {}", id, e.getMessage());

            // We cannot throw an exception on this filter, so we return false assuming this
            // notification is already processed
            return false;
          }
        }

        return true;
      }
    };
  }

  /**
   * Gets the HMS client connection object.
   * If will create a new connection if no connection object exists.
   *
   * @return The HMS client used to communication with the Hive MetaStore.
   * @throws Exception If it cannot connect to the HMS service.
   */
  private HiveMetaStoreClient getHmsClient() throws Exception {
    if (hmsClient == null) {
      try {
        hmsClient = hmsConnectionFactory.connect().getClient();
      } catch (Exception e) {
        LOGGER.error("Fail to connect to the HMS service: {}", e.getMessage());
        throw e;
      }
    }

    return hmsClient;
  }

  /**
   * @return the latest notification Id logged by the HMS
   * @throws Exception when an error occurs when talking to the HMS client
   */
  long getCurrentNotificationId() throws Exception {
    CurrentNotificationEventId eventId;
    try {
      eventId = getHmsClient().getCurrentNotificationEventId();
    } catch (Exception e) {
      close();
      throw e;
    }

    if (eventId != null && eventId.isSetEventId()) {
      return eventId.getEventId();
    }

    return SentryStore.EMPTY_NOTIFICATION_ID;
  }

  /* AutoCloseable implementations */

  @Override
  public void close() {
    try {
      if (hmsClient != null) {
        hmsClient.close();
      }

      cache.clear();
    } finally {
      hmsClient = null;
    }
  }
}
