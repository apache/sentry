/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.service.thrift;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.exception.SentryInvalidHMSEventException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.hdfs.PermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.sentry.binding.metastore.messaging.json.*;

import javax.jdo.JDODataStoreException;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE;
import static org.apache.sentry.hdfs.Updateable.Update;

/**
 * HMSFollower is the thread which follows the Hive MetaStore state changes from Sentry.
 * It gets the full update and notification logs from HMS and applies it to
 * update permissions stored in Sentry using SentryStore and also update the &lt obj,path &gt state
 * stored for HDFS-Sentry sync.
 */
@SuppressWarnings("PMD")
public class HMSFollower implements Runnable, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HMSFollower.class);
  private HiveSimpleConnectionFactory hiveConnectionFactory;
  // Track the latest eventId of the event that has been logged. So we don't log the same message
  private long lastLoggedEventId = SentryStore.EMPTY_CHANGE_ID;
  private static boolean connectedToHMS = false;
  private HMSClient client;
  private final Configuration authzConf;
  private final SentryStore sentryStore;
  private String hiveInstance;

  private boolean needLogHMSSupportReady = true;
  private final LeaderStatusMonitor leaderMonitor;

  HMSFollower(Configuration conf, SentryStore store, LeaderStatusMonitor leaderMonitor,
              HiveSimpleConnectionFactory hiveConnectionFactory) {
    authzConf = conf;
    this.leaderMonitor = leaderMonitor;
    sentryStore = store;
    this.hiveConnectionFactory = hiveConnectionFactory;
  }

  @VisibleForTesting
  HMSFollower(Configuration conf, SentryStore sentryStore, String hiveInstance)
      throws IOException, LoginException {
    this(conf, sentryStore, null, null);
    this.hiveInstance = hiveInstance;
    hiveConnectionFactory = new HiveSimpleConnectionFactory(conf, new HiveConf());
    hiveConnectionFactory.init();
  }

  @VisibleForTesting
  public static boolean isConnectedToHMS() {
    return connectedToHMS;
  }

  @Override
  public void close() {
    // Close any outstanding connections to HMS
    closeHMSConnection();
    try {
      hiveConnectionFactory.close();
    } catch (Exception e) {
      LOGGER.error("failed to close Hive Connection Factory", e);
    }
  }

  /**
   * Returns HMS Client if successful, returns null if HMS is not ready yet to take connections
   * Throws @LoginException if Kerberos context creation failed using Sentry's kerberos credentials
   * Throws @MetaException if there was a problem on creating an HMSClient
   */
  private HiveMetaStoreClient getMetaStoreClient()
    throws IOException, InterruptedException, MetaException {
    if (client == null) {
      client = hiveConnectionFactory.connect();
      connectedToHMS = true;
    }
    return client.getClient();
  }

  @Override
  public void run() {
    Long lastProcessedNotificationID;
    try {
      // Initializing lastProcessedNotificationID based on the latest persisted notification ID.
      lastProcessedNotificationID = sentryStore.getLastProcessedNotificationID();
    } catch (Exception e) {
      LOGGER.error("Failed to get the last processed notification id from sentry store, " +
        "Skipping the processing", e);
      return;
    }
    // Wake any clients connected to this service waiting for HMS already processed notifications.
    wakeUpWaitingClientsForSync(lastProcessedNotificationID);
    // Only the leader should listen to HMS updates
    if ((leaderMonitor != null) && !leaderMonitor.isLeader()) {
      // Close any outstanding connections to HMS
      closeHMSConnection();
      return;
    }
    processHiveMetastoreUpdates();
  }

  /**
   * Wakes up HMS waiters waiting for a specific event notification.
   *
   * @param eventID
   */
  private void wakeUpWaitingClientsForSync(long eventID) {
    CounterWait counterWait = sentryStore.getCounterWait();

    // Wake up any HMS waiters that are waiting for this ID.
    // counterWait should never be null, but tests mock SentryStore and a mocked one
    // doesn't have it.
    if (counterWait != null) {
      counterWait.update(eventID);
    }
  }

  /**
   * Processes new Hive Metastore notifications.
   *
   * If no notifications are processed yet, then it does a full initial snapshot of the Hive Metastore
   * followed by new notifications updates that could have happened after it.
   *
   * Clients connections waiting for an event notification will be woken up afterwards.
   */
  private void processHiveMetastoreUpdates() {
    try {
      // Decision of taking full snapshot is based on AuthzPathsMapping information persisted
      // in the sentry persistent store. If AuthzPathsMapping is empty, shapshot is needed.
      Long lastProcessedNotificationID;
      if (sentryStore.isAuthzPathsMappingEmpty()) {
        // TODO: expose time used for full update in the metrics

        // To ensure point-in-time snapshot consistency, need to make sure
        // there were no HMS updates while retrieving the snapshot.
        // In detail the logic is:
        //
        // 1. Read current HMS notification ID_initial
        // 2. Read HMS metadata state
        // 3. Read current notification ID_new
        // 4. If ID_initial != ID_new then the attempts for retrieving full HMS snapshot
        // will be dropped. A new attempts will be made after 500 milliseconds when
        // HMSFollower run again.

        CurrentNotificationEventId eventIDBefore = getMetaStoreClient().getCurrentNotificationEventId();
        LOGGER.info("Before fetching hive full snapshot, Current NotificationID = {}", eventIDBefore);

        Map<String, Set<String>> pathsFullSnapshot = fetchFullUpdate();
        if(pathsFullSnapshot.isEmpty()) {
          LOGGER.info("Hive full snapshot is Empty. Perhaps, HMS does not have any data");
          return;
        }

        CurrentNotificationEventId eventIDAfter = getMetaStoreClient().getCurrentNotificationEventId();
        LOGGER.info("After fetching hive full snapshot, Current NotificationID = {}", eventIDAfter);

        if (!eventIDBefore.equals(eventIDAfter)) {
          LOGGER.error("Fail to get a point-in-time hive full snapshot. Current ID = {}",
            eventIDAfter);
          return;
        }

        LOGGER.info("Successfully fetched hive full snapshot, Current NotificationID = {}",
          eventIDAfter);
        // As eventIDAfter is the last event that was processed, eventIDAfter is used to update
        // lastProcessedNotificationID instead of getting it from persistent store.
        lastProcessedNotificationID = eventIDAfter.getEventId();
        sentryStore.persistFullPathsImage(pathsFullSnapshot);
        sentryStore.persistLastProcessedNotificationID(eventIDAfter.getEventId());
        // Wake up any HMS waiters that could have been put on hold before getting the eventIDBefore value.
        wakeUpWaitingClientsForSync(lastProcessedNotificationID);
      } else {
        // Every time HMSFollower is scheduled to run, value should be updates based
        // on the value stored in database.
        lastProcessedNotificationID = sentryStore.getLastProcessedNotificationID();
      }

      // HMSFollower connected to HMS and it finished full snapshot if that was required
      // Log this message only once
      if (needLogHMSSupportReady && connectedToHMS) {
        LOGGER.info("Sentry HMS support is ready");
        needLogHMSSupportReady = false;
      }

      // HIVE-15761: Currently getNextNotification API may return an empty
      // NotificationEventResponse causing TProtocolException.
      // Workaround: Only processes the notification events newer than the last updated one.
      CurrentNotificationEventId eventId = getMetaStoreClient().getCurrentNotificationEventId();
      LOGGER.debug("Last Notification in HMS {} lastProcessedNotificationID is {}",
        eventId.getEventId(), lastProcessedNotificationID);
      if (eventId.getEventId() > lastProcessedNotificationID) {
        NotificationEventResponse response =
          getMetaStoreClient().getNextNotification(lastProcessedNotificationID, Integer.MAX_VALUE, null);
        if (response.isSetEvents()) {
          if (!response.getEvents().isEmpty()) {
            if (lastProcessedNotificationID != lastLoggedEventId) {
              // Only log when there are updates and the notification ID has changed.
              LOGGER.debug("lastProcessedNotificationID = {}. Processing {} events",
                      lastProcessedNotificationID, response.getEvents().size());
              lastLoggedEventId = lastProcessedNotificationID;
            }

            processNotificationEvents(response.getEvents());
          }
        }
      }
    } catch (TException e) {
      // If the underlying exception is around socket exception, it is better to retry connection to HMS
      if (e.getCause() instanceof SocketException) {
        LOGGER.error("Encountered Socket Exception during fetching Notification entries, will reconnect to HMS", e);
        client.invalidate();
        closeHMSConnection();
      } else {
        LOGGER.error("ThriftException occured fetching Notification entries, will try", e);
      }
    } catch (SentryInvalidInputException | SentryInvalidHMSEventException e) {
      LOGGER.error("Encounter SentryInvalidInputException|SentryInvalidHMSEventException " +
        "while processing notification log", e);
    } catch (Throwable t) {
      // catching errors to prevent the executor to halt.
      LOGGER.error("Caught unexpected exception in HMSFollower! Caused by: " + t.getMessage(),
        t.getCause());
      t.printStackTrace();
    }
  }

  /**
   * Function to close HMS connection and any associated kerberos context (if applicable)
   */
  private void closeHMSConnection() {
    try {
      if (client != null) {
        LOGGER.info("Closing the HMS client connection");
        client.close();
        connectedToHMS = false;
      }
    } finally {
      client = null;
    }
  }

  /**
   * Retrieve a Hive full snapshot from HMS.
   *
   * @return HMS snapshot. Snapshot consists of a mapping from auth object name
   * to the set of paths corresponding to that name.
   * @throws InterruptedException
   * @throws TException
   * @throws ExecutionException
   */
  private Map<String, Set<String>> fetchFullUpdate()
    throws TException, ExecutionException {
    LOGGER.info("Request full HMS snapshot");
    try (FullUpdateInitializer updateInitializer =
                 new FullUpdateInitializer(hiveConnectionFactory, authzConf)) {
      Map<String, Set<String>> pathsUpdate = updateInitializer.getFullHMSSnapshot();
      LOGGER.info("Obtained full HMS snapshot");
      return pathsUpdate;
    } catch (Exception ignored) {
      // Caller will retry later
      return Collections.emptyMap();
    }
  }

  private boolean syncWithPolicyStore(HiveAuthzConf.AuthzConfVars syncConfVar) {
    return "true"
        .equalsIgnoreCase((authzConf.get(syncConfVar.getVar(), syncConfVar.getDefault())));
  }

  /**
   * Throws SentryInvalidHMSEventException if Notification event contains insufficient information
   */
  void processNotificationEvents(List<NotificationEvent> events) throws Exception {
    SentryJSONMessageDeserializer deserializer = new SentryJSONMessageDeserializer();

    boolean isNotificationProcessingSkipped = false;
    for (NotificationEvent event : events) {
      String dbName;
      String tableName;
      String oldLocation;
      String newLocation;
      String location;
      List<String> locations;
      NotificationProcessor notificationProcessor = new NotificationProcessor(sentryStore, LOGGER);
      try {
        LOGGER.debug("Processing notification with id {} and type {}", event.getEventId(),
          event.getEventType());
        switch (HCatEventMessage.EventType.valueOf(event.getEventType())) {
          case CREATE_DATABASE:
            SentryJSONCreateDatabaseMessage message =
              deserializer.getCreateDatabaseMessage(event.getMessage());
            dbName = message.getDB();
            location = message.getLocation();
            if ((dbName == null) || (location == null)) {
              isNotificationProcessingSkipped = true;
              LOGGER.error(String.format("Create database event " +
                  "has incomplete information. dbName = %s location = %s",
                StringUtils.defaultIfBlank(dbName, "null"),
                StringUtils.defaultIfBlank(location, "null")));
              break;
            }
            if (syncWithPolicyStore(AUTHZ_SYNC_CREATE_WITH_POLICY_STORE)) {
              dropSentryDbPrivileges(dbName, event);
            }
            notificationProcessor.processCreateDatabase(dbName, location, event.getEventId());
            break;
          case DROP_DATABASE:
            SentryJSONDropDatabaseMessage dropDatabaseMessage =
              deserializer.getDropDatabaseMessage(event.getMessage());
            dbName = dropDatabaseMessage.getDB();
            location = dropDatabaseMessage.getLocation();
            if (dbName == null) {
              isNotificationProcessingSkipped = true;
              LOGGER.error("Drop database event has incomplete information: dbName = null");
              break;
            }
            if (syncWithPolicyStore(AUTHZ_SYNC_DROP_WITH_POLICY_STORE)) {
              dropSentryDbPrivileges(dbName, event);
            }
            notificationProcessor.processDropDatabase(dbName, location, event.getEventId());
            break;
          case CREATE_TABLE:
            SentryJSONCreateTableMessage createTableMessage = deserializer.getCreateTableMessage(event.getMessage());
            dbName = createTableMessage.getDB();
            tableName = createTableMessage.getTable();
            location = createTableMessage.getLocation();
            if ((dbName == null) || (tableName == null) || (location == null)) {
              isNotificationProcessingSkipped = true;
              LOGGER.error(String.format("Create table event " + "has incomplete information."
                  + " dbName = %s, tableName = %s, location = %s",
                StringUtils.defaultIfBlank(dbName, "null"),
                StringUtils.defaultIfBlank(tableName, "null"),
                StringUtils.defaultIfBlank(location, "null")));
              break;
            }
            if (syncWithPolicyStore(AUTHZ_SYNC_CREATE_WITH_POLICY_STORE)) {
              dropSentryTablePrivileges(dbName, tableName, event);
            }
            notificationProcessor.processCreateTable(dbName, tableName, location, event.getEventId());
            break;
          case DROP_TABLE:
            SentryJSONDropTableMessage dropTableMessage = deserializer.getDropTableMessage(event.getMessage());
            dbName = dropTableMessage.getDB();
            tableName = dropTableMessage.getTable();
            if ((dbName == null) || (tableName == null)) {
              isNotificationProcessingSkipped = true;
              LOGGER.error(String.format("Drop table event " +
                  "has incomplete information. dbName = %s, tableName = %s",
                StringUtils.defaultIfBlank(dbName, "null"),
                StringUtils.defaultIfBlank(tableName, "null")));
              break;
            }
            if (syncWithPolicyStore(AUTHZ_SYNC_DROP_WITH_POLICY_STORE)) {
              dropSentryTablePrivileges(dbName, tableName, event);
            }
            notificationProcessor.processDropTable(dbName, tableName, event.getEventId());
            break;
          case ALTER_TABLE:
            SentryJSONAlterTableMessage alterTableMessage = deserializer.getAlterTableMessage(event.getMessage());

            String oldDbName = alterTableMessage.getDB();
            String oldTableName = alterTableMessage.getTable();
            String newDbName = event.getDbName();
            String newTableName = event.getTableName();
            oldLocation = alterTableMessage.getOldLocation();
            newLocation = alterTableMessage.getNewLocation();

            if ((oldDbName == null) ||
              (oldTableName == null) ||
              (newDbName == null) ||
              (newTableName == null) ||
              (oldLocation == null) ||
              (newLocation == null)) {
              isNotificationProcessingSkipped = true;
              LOGGER.error(String.format("Alter table event " +
                  "has incomplete information. oldDbName = %s, oldTableName = %s, oldLocation = %s, " +
                  "newDbName = %s, newTableName = %s, newLocation = %s",
                StringUtils.defaultIfBlank(oldDbName, "null"),
                StringUtils.defaultIfBlank(oldTableName, "null"),
                StringUtils.defaultIfBlank(oldLocation, "null"),
                StringUtils.defaultIfBlank(newDbName, "null"),
                StringUtils.defaultIfBlank(newTableName, "null"),
                StringUtils.defaultIfBlank(newLocation, "null")));
              break;
            } else if ((oldDbName.equalsIgnoreCase(newDbName)) &&
              (oldTableName.equalsIgnoreCase(newTableName)) &&
              (oldLocation.equalsIgnoreCase(newLocation))) {
              isNotificationProcessingSkipped = true;
              LOGGER.info(String.format("Alter table notification ignored as neither name nor " +
                "location has changed: oldAuthzObj = %s, oldLocation = %s, newAuthzObj = %s, " +
                "newLocation = %s", oldDbName + "." + oldTableName , oldLocation,
                newDbName + "." + newTableName, newLocation));
              break;
            }

            if (!newDbName.equalsIgnoreCase(oldDbName) || !oldTableName.equalsIgnoreCase(newTableName)) {
              // Name has changed
              try {
                renamePrivileges(oldDbName, oldTableName, newDbName, newTableName);
              } catch (SentryNoSuchObjectException e) {
                LOGGER.info("Rename Sentry privilege ignored as there are no privileges on the table: %s.%s",
                  oldDbName, oldTableName);
              } catch (Exception e) {
                isNotificationProcessingSkipped = true;
                LOGGER.info("Could not process Alter table event. Event: " + event.toString(), e);
                break;
              }
            }
            notificationProcessor.processAlterTable(oldDbName, newDbName, oldTableName,
              newTableName, oldLocation, newLocation, event.getEventId());
            break;
          case ADD_PARTITION:
            SentryJSONAddPartitionMessage addPartitionMessage =
              deserializer.getAddPartitionMessage(event.getMessage());
            dbName = addPartitionMessage.getDB();
            tableName = addPartitionMessage.getTable();
            locations = addPartitionMessage.getLocations();
            if ((dbName == null) || (tableName == null) || (locations == null)) {
              isNotificationProcessingSkipped = true;
              LOGGER.error(String.format("Create table event has incomplete information. " +
                  "dbName = %s, tableName = %s, locations = %s",
                StringUtils.defaultIfBlank(dbName, "null"),
                StringUtils.defaultIfBlank(tableName, "null"),
                locations != null ? locations.toString() : "null"));
              break;
            }
            notificationProcessor.processAddPartition(dbName, tableName, locations, event.getEventId());
            break;
          case DROP_PARTITION:
            SentryJSONDropPartitionMessage dropPartitionMessage =
              deserializer.getDropPartitionMessage(event.getMessage());
            dbName = dropPartitionMessage.getDB();
            tableName = dropPartitionMessage.getTable();
            locations = dropPartitionMessage.getLocations();
            if ((dbName == null) || (tableName == null) || (locations == null)) {
              isNotificationProcessingSkipped = true;
              LOGGER.error(String.format("Drop partition event " +
                  "has incomplete information. dbName = %s, tableName = %s, location = %s",
                StringUtils.defaultIfBlank(dbName, "null"),
                StringUtils.defaultIfBlank(tableName, "null"),
                locations != null ? locations.toString() : "null"));
              break;
            }
            notificationProcessor.processDropPartition(dbName, tableName, locations, event.getEventId());

            break;
          case ALTER_PARTITION:
            SentryJSONAlterPartitionMessage alterPartitionMessage =
              deserializer.getAlterPartitionMessage(event.getMessage());
            dbName = alterPartitionMessage.getDB();
            tableName = alterPartitionMessage.getTable();
            oldLocation = alterPartitionMessage.getOldLocation();
            newLocation = alterPartitionMessage.getNewLocation();

            if ((dbName == null) ||
              (tableName == null) ||
              (oldLocation == null) ||
              (newLocation == null)) {
              isNotificationProcessingSkipped = true;
              LOGGER.error(String.format("Alter partition event " +
                  "has incomplete information. dbName = %s, tableName = %s, " +
                  "oldLocation = %s, newLocation = %s",
                StringUtils.defaultIfBlank(dbName, "null"),
                StringUtils.defaultIfBlank(tableName, "null"),
                StringUtils.defaultIfBlank(oldLocation, "null"),
                StringUtils.defaultIfBlank(newLocation, "null")));
              break;
            } else if (oldLocation.equalsIgnoreCase(newLocation)) {
              isNotificationProcessingSkipped = true;
              LOGGER.info(String.format("Alter partition notification ignored as" +
                "location has not changed: AuthzObj = %s, Location = %s", dbName + "." +
                "." + tableName, oldLocation));
              break;
            }

            notificationProcessor.processAlterPartition(dbName, tableName, oldLocation,
              newLocation, event.getEventId());
            break;
          case INSERT:
            // TODO DO we need to do anything here?
            break;
        }
      } catch (Exception e) {
        if (e.getCause() instanceof JDODataStoreException) {
          LOGGER.info("Received JDO Storage Exception, Could be because of processing " +
            "duplicate notification");
          if (event.getEventId() <= sentryStore.getLastProcessedNotificationID()) {
            // Rest of the notifications need not be processed.
            throw e;
          }
        }
        sentryStore.persistLastProcessedNotificationID(event.getEventId());
      }
      if (isNotificationProcessingSkipped) {
        // Update the notification ID in the persistent store even when the notification is
        // not processed as the content in in the notification is not valid.
        // Continue processing the next notification.
        sentryStore.persistLastProcessedNotificationID(event.getEventId());
        isNotificationProcessingSkipped = false;
      }
      // Wake up any HMS waiters that are waiting for this ID.
      wakeUpWaitingClientsForSync(event.getEventId());
    }
  }

  private void dropSentryDbPrivileges(String dbName, NotificationEvent event) throws Exception {
    try {
      TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
      authorizable.setDb(dbName);
      sentryStore.dropPrivilege(authorizable, onDropSentryPrivilege(authorizable));
    } catch (SentryNoSuchObjectException e) {
      LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the database: %s", dbName);
    } catch (Exception e) {
      throw new SentryInvalidInputException("Could not process Drop database event." +
        "Event: " + event.toString(), e);
    }
  }

  private void dropSentryTablePrivileges(String dbName, String tableName, NotificationEvent event) throws Exception {
    try {
      TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
      authorizable.setDb(dbName);
      authorizable.setTable(tableName);
      sentryStore.dropPrivilege(authorizable, onDropSentryPrivilege(authorizable));
    } catch (SentryNoSuchObjectException e) {
      LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the table: %s.%s", dbName, tableName);
    } catch (Exception e) {
      throw new SentryInvalidInputException("Could not process Drop table event. Event: " + event.toString(), e);
    }
  }

  private void renamePrivileges(String oldDbName, String oldTableName, String newDbName, String newTableName) throws
    Exception {
    TSentryAuthorizable oldAuthorizable = new TSentryAuthorizable(hiveInstance);
    oldAuthorizable.setDb(oldDbName);
    oldAuthorizable.setTable(oldTableName);
    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);
    Update update =
      onRenameSentryPrivilege(oldAuthorizable, newAuthorizable);
    sentryStore.renamePrivilege(oldAuthorizable, newAuthorizable, update);
  }

  @VisibleForTesting
  static Update onDropSentryPrivilege(TSentryAuthorizable authorizable) {
    PermissionsUpdate update = new PermissionsUpdate(SentryStore.INIT_CHANGE_ID, false);
    String authzObj = getAuthzObj(authorizable);
    update.addPrivilegeUpdate(authzObj).putToDelPrivileges(PermissionsUpdate.ALL_ROLES, PermissionsUpdate.ALL_ROLES);
    return update;
  }

  @VisibleForTesting
  static Update onRenameSentryPrivilege(TSentryAuthorizable oldAuthorizable,
                                        TSentryAuthorizable newAuthorizable)
    throws SentryPolicyStorePlugin.SentryPluginException {
    String oldAuthz = getAuthzObj(oldAuthorizable);
    String newAuthz = getAuthzObj(newAuthorizable);
    PermissionsUpdate update = new PermissionsUpdate(SentryStore.INIT_CHANGE_ID, false);
    TPrivilegeChanges privUpdate = update.addPrivilegeUpdate(PermissionsUpdate.RENAME_PRIVS);
    privUpdate.putToAddPrivileges(newAuthz, newAuthz);
    privUpdate.putToDelPrivileges(oldAuthz, oldAuthz);
    return update;
  }

  public static String getAuthzObj(TSentryAuthorizable authzble) {
    String authzObj = null;
    if (!SentryStore.isNULL(authzble.getDb())) {
      String dbName = authzble.getDb();
      String tblName = authzble.getTable();
      if (SentryStore.isNULL(tblName)) {
        authzObj = dbName;
      } else {
        authzObj = dbName + "." + tblName;
      }
    }
    return authzObj == null ? null : authzObj.toLowerCase();
  }
}
