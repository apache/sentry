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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;
import java.util.HashSet;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.sentry.hdfs.UniquePathsUpdate;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.core.common.exception.SentryOutOfSyncException;
import org.apache.sentry.provider.db.service.thrift.SentryMetrics;
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
  /*
   This value helps HiveNotificationFetcher to decide the notification-id to use while fetching the notifications from
   HMS to handle out-of-sequence notifications.
   */
  private final int refetchCount;

  /*
   In each iteration, HMSFollower will try to fetch notification which has the Max(event-id)
   among the notifications it already fetched. This flag is used to check if that notification
   is fetched in subsequent request. It's initialized to false in every iteration if sentry has fetched notifications
   in any one the previous fetches.
   */
  private boolean foundLastProcessedNotification = false;
  /*
   Idea of this cache is to store the notification event id and hash to avoid database lookup as there will be a lot
   of notifications that are fetched again and again to handle out-of-sync notifications. Least recently used entries
   in this cache are evicted automatically once the map is full.
  */
  private final LRUMap cache;

  /*
  Cache is designed to hold all the events with event-id's in the range of MAX(event-id) (that sentry processed) and
   MAX(event-id) - (notification fetch count) from the configuration. Theoretically, all of these event-id's could have
   duplicates. Cache size should be sufficient to hold all of them. This factor is used to calculate the size of cache.
   */
  public static final int CACHE_BUFFER_FACTOR = 10;

/*
  This value is used to build a cache. Cache anticipates below duplicates and pre-allocated memory.
 */
  private static final int DUPLICATE_COUNT = 3;

  // Counter for failed transactions
  private static final Counter notificationCacheMissCount =
          SentryMetrics.getInstance().
                  getCounter(name(HiveNotificationFetcher.class,
                          "Cache Miss"));

  public HiveNotificationFetcher(SentryStore sentryStore, HiveConnectionFactory hmsConnectionFactory,
                                 Configuration conf) {
    refetchCount = conf.getInt(ServiceConstants.ServerConfig.SENTRY_HMS_NOTIFICATION_REFETCH_COUNT,
            ServiceConstants.ServerConfig.SENTRY_HMS_NOTIFICATION_REFETCH_COUNT_DEFAULT);
    this.sentryStore = sentryStore;
    this.hmsConnectionFactory = hmsConnectionFactory;
    /*
     Size of map is dependent on SENTRY_HMS_NOTIFICATION_REFETCH_COUNT and some additional buffer.
    */
    this.cache = new LRUMap(refetchCount + (refetchCount / CACHE_BUFFER_FACTOR));
  }

  /**
   * Fetch new HMS notifications appeared since a specified event ID. The returned list may
   * include notifications with the same specified ID if they were not seen by Sentry.
   *
   * @param lastEventId The event ID to use to request notifications.
   * @return A list of newer notifications unseen by Sentry.
   * @throws Exception If an error occurs on the HMS communication.
   */
  public List<NotificationEvent> fetchNotifications(long lastEventId) throws Exception {
    return fetchNotifications(lastEventId, Integer.MAX_VALUE);
  }

  /**
   * Update cache with notification hash and event-id
   * @param event Notification event
   */
  public void updateCache(NotificationEvent event) {
    HashSet<String> eventSet;
    String hash = UniquePathsUpdate.sha1(event);

    if(Strings.isNullOrEmpty(hash)) {
      LOGGER.error("Hash provided is either null/empty, Cache not updated");
    }
    eventSet = (HashSet<String>)cache.get(event.getEventId());
    if(eventSet != null) {
      eventSet.add(hash);
    } else {
      eventSet = new HashSet<>(DUPLICATE_COUNT, 1);
      eventSet.add(hash);
      cache.put(event.getEventId(), eventSet);
    }
  }

  /**
   * Find if the Notification is found in Cache.
   * @param eventId notification event id
   * @param hash notification hash
   * @return True, if the hash is found in Cache
   *         False, otherwise
   */
  @VisibleForTesting
  boolean isFoundInCache(long eventId, String hash) {
    HashSet<String> eventSet;
    eventSet = (HashSet<String>)cache.get(eventId);

    if(eventSet == null) {
      return false;
    } else {
      return eventSet.contains(hash);
    }
  }

  /**
   * Find if the Notification event-id is found in Cache.
   * @param eventId notification event id.
   * @return True, if the event-id is found in Cache
   *         False, otherwise
   */
  boolean isFoundInCache(long eventId) {
    return cache.get(eventId) != null;
  }

  /**
   * Get the Cache size
   * @return size of the Cache
   */
  @VisibleForTesting
  int getCacheSize() {
    return cache.size();
  }

  /**
   * Fetch new HMS notifications appeared since a specified event ID. The returned list may
   * include notifications with the same specified ID if they were not seen by Sentry.
   * In each iteration HiveNotificationFetcher will get the Max(event-id) that sentry has processed and tries to go back
   * by configured number of event-ids and re-fetches them. Idea is that Max(event-id) should be fetched in subsequent
   * fetch. If not, sentry should consider create full snapshot.This could happen for several scenario's.
   * <ul>
   *   <li>HMS is been restored from a snapshot taken in the past</li>
   *   <li>Sentry did not fetch notifications from for a while. One use case is HDFS Sync being disabled</li>
   *   <li>HMS is intentionally reset</li>
   *   <li>NOTIFICATION_LOG table is cleared</li>
   * </ul>
   *
   * @param lastEventId The event ID to use to request notifications.
   * @param maxEvents The maximum number of events to fetch.
   * @return A list of newer notifications unseen by Sentry.
   * @throws  SentryOutOfSyncException If event with event-id equals to Max(event-id) processed by sentry is not received
   *   in HMS response.
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

    // Value of lastEventId has to be retained for logging purposes, making a copy.
    long minFetchId = lastEventId;
    if (minFetchId > 0) {
      filter = createNotificationFilterFor(minFetchId);
      minFetchId = (minFetchId > refetchCount) ? (minFetchId - refetchCount) : 0;
      foundLastProcessedNotification = false;
    } else {
      foundLastProcessedNotification = true;
    }

    LOGGER.debug("Requesting HMS notifications since ID = {} Max(Event-id) processed: {}", minFetchId, lastEventId);

    NotificationEventResponse response;
    try {
      /**
       * Logic below triggers full-snapshots in below situation.
       * 1. When HMS response with notification events does not have the event with Max(Event-id) processed by sentry
       *     and the event-id of the first event in the response is not equal to Max(Event-id)+1 processed by sentry
       * Logic below will not trigger full-snapshots in below situations.
       * 1. When the response doesn't have ay notification events. This will be seen in situations where there no changes
       * to HMS data for a white and all the notification events are evicted from the NOTIFICATION_LOG table because of
       * TTL expiration.
       * 2. When HMS response with notification events does have the event with Max(Event-id) processed by sentry.
       */
      response = getHmsClient().getNextNotification(minFetchId, maxEvents, filter);
      if ((response != null) && (response.getEventsSize() > 0) && !foundLastProcessedNotification &&
              (response.getEvents().get(0).getEventId() != (lastEventId + 1))) {
        LOGGER.error("Max event-id processed by sentry is " + lastEventId + " but the " +
                "Id of the first event received from HMS in subsequent fetch is " +
                response.getEvents().get(0).getEventId() + ", Requesting for Full snapshot");
        //Full snapshot should be requested.
        throw new SentryOutOfSyncException("Notification Log doesn't have the " +
                "last notification processed by sentry");
      }
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

    return new NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent notificationEvent) {
        LOGGER.debug("Applying filter created for event-id {} on Event with ID:{}", id, notificationEvent.getEventId());
        if (notificationEvent.getEventId() <= id) {
          String hash = UniquePathsUpdate.sha1(notificationEvent);
          try {
            // This check makes sure that the last notification processed by
            // HMSFollower is present in NOTIFICATION_LOG table. If it is not found
            // it can be assumed that there were events that were cleaned up before
            // Sentry could fetch them. When this happens sentry should take full snapshot again.
            if ((notificationEvent.getEventId() == id) && !foundLastProcessedNotification) {
              foundLastProcessedNotification = true;
            }
            if (isFoundInCache(notificationEvent.getEventId(), hash) == true) {
              LOGGER.debug("Ignoring HMS notification already processed: ID = {}", notificationEvent.getEventId());
              return false;
            } else if (sentryStore.isNotificationProcessed(hash)) {
              notificationCacheMissCount.inc();
              LOGGER.debug("Ignoring HMS notification already processed: ID = {} - cache miss", notificationEvent.getEventId());
              return false;
            }
          } catch (Exception e) {
            LOGGER.error("An error occurred while checking if notification {} is already "
                    + "processed: {}", notificationEvent.getEventId(), e);

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
  public long getCurrentNotificationId() throws Exception {
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

    } finally {
      hmsClient = null;
    }
  }

  /**
   * Gets notification cache miss count.
   * @return notification cache miss count.
   */
  public long getNotificationCacheMissCount() {
    return notificationCacheMissCount.getCount();
  }
}
