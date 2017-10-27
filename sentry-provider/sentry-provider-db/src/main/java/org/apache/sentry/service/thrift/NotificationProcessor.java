/*
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

package org.apache.sentry.service.thrift;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hive.hcatalog.messaging.HCatEventMessage.EventType;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAddPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAlterPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAlterTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONCreateDatabaseMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONCreateTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropDatabaseMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageDeserializer;
import org.apache.sentry.core.common.exception.SentryInvalidHMSEventException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.PermissionsUpdate;
import org.apache.sentry.hdfs.SentryMalformedPathException;
import org.apache.sentry.hdfs.UniquePathsUpdate;
import org.apache.sentry.hdfs.Updateable.Update;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.SentryMetrics;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE;



/**
 * NotificationProcessor processes various notification events generated from
 * the Hive MetaStore state change, and applies these changes to the complete
 * HMS Paths snapshot or delta update stored in Sentry using SentryStore.
 *
 * <p>NotificationProcessor should not skip processing notification events for any reason.
 * If some notification events are to be skipped, appropriate logic should be added in
 * HMSFollower before invoking NotificationProcessor.
 */
final class NotificationProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationProcessor.class);
  private final SentryStore sentryStore;
  private final SentryJSONMessageDeserializer deserializer;
  private final String authServerName;
  // These variables can be updated even after object is instantiated, for testing purposes.
  private boolean syncStoreOnCreate = false;
  private boolean syncStoreOnDrop = false;
  private final boolean hdfsSyncEnabled;

  /**
   * Configuring notification processor.
   *
   * @param sentryStore sentry backend store
   * @param authServerName Server that sentry is authorizing
   * @param conf sentry configuration
   */
  NotificationProcessor(SentryStore sentryStore, String authServerName,
      Configuration conf) {
    this.sentryStore = sentryStore;
    deserializer = new SentryJSONMessageDeserializer();
    this.authServerName = authServerName;
    syncStoreOnCreate = Boolean
        .parseBoolean(conf.get(AUTHZ_SYNC_CREATE_WITH_POLICY_STORE.getVar(),
            AUTHZ_SYNC_CREATE_WITH_POLICY_STORE.getDefault()));
    syncStoreOnDrop = Boolean.parseBoolean(conf.get(AUTHZ_SYNC_DROP_WITH_POLICY_STORE.getVar(),
        AUTHZ_SYNC_DROP_WITH_POLICY_STORE.getDefault()));
    hdfsSyncEnabled = SentryServiceUtil.isHDFSSyncEnabled(conf);
  }

  /**
   * Split path into components on the "/" character.
   * The path should not start with "/".
   * This is consumed by Thrift interface, so the return result should be
   * {@code List<String>}
   *
   * @param path input oath e.g. {@code foo/bar}
   * @return list of components, e.g. [foo, bar]
   */
  private static List<String> splitPath(String path) {
    return Lists.newArrayList(PathUtils.splitPath(path));
  }

  /**
   * Constructs permission update to be persisted for drop event that can be persisted
   * from thrift object.
   *
   * @param authorizable thrift object that is dropped.
   * @return update to be persisted
   * @throws SentryInvalidInputException if the required fields are set in argument provided
   */
  @VisibleForTesting
  static Update getPermUpdatableOnDrop(TSentryAuthorizable authorizable)
      throws SentryInvalidInputException {
    PermissionsUpdate update = new PermissionsUpdate(SentryStore.INIT_CHANGE_ID, false);
    String authzObj = SentryServiceUtil.getAuthzObj(authorizable);
    update.addPrivilegeUpdate(authzObj)
        .putToDelPrivileges(PermissionsUpdate.ALL_ROLES, PermissionsUpdate.ALL_ROLES);
    return update;
  }

  @VisibleForTesting
  String getAuthServerName() {
    return authServerName;
  }

  /**
   * Constructs permission update to be persisted for rename event that can be persisted from thrift
   * object.
   *
   * @param oldAuthorizable old thrift object
   * @param newAuthorizable new thrift object
   * @return update to be persisted
   * @throws SentryInvalidInputException if the required fields are set in arguments provided
   */
  @VisibleForTesting
  static Update getPermUpdatableOnRename(TSentryAuthorizable oldAuthorizable,
      TSentryAuthorizable newAuthorizable)
      throws SentryInvalidInputException {
    String oldAuthz = SentryServiceUtil.getAuthzObj(oldAuthorizable);
    String newAuthz = SentryServiceUtil.getAuthzObj(newAuthorizable);
    PermissionsUpdate update = new PermissionsUpdate(SentryStore.INIT_CHANGE_ID, false);
    TPrivilegeChanges privUpdate = update.addPrivilegeUpdate(PermissionsUpdate.RENAME_PRIVS);
    privUpdate.putToAddPrivileges(newAuthz, newAuthz);
    privUpdate.putToDelPrivileges(oldAuthz, oldAuthz);
    return update;
  }

  /**
   * This function is only used for testing purposes.
   *
   * @param value to be set
   */
  @SuppressWarnings("SameParameterValue")
  @VisibleForTesting
  void setSyncStoreOnCreate(boolean value) {
    syncStoreOnCreate = value;
  }

  /**
   * This function is only used for testing purposes.
   *
   * @param value to be set
   */
  @SuppressWarnings("SameParameterValue")
  @VisibleForTesting
  void setSyncStoreOnDrop(boolean value) {
    syncStoreOnDrop = value;
  }

  /**
   * Processes the event and persist to sentry store.
   *
   * @param event to be processed
   * @return true, if the event is persisted to sentry store. false, if the event is not persisted.
   * @throws Exception if there is an error processing the event.
   */
  boolean processNotificationEvent(NotificationEvent event) throws Exception {
    LOGGER
        .debug("Processing event with id:{} and Type:{}", event.getEventId(), event.getEventType());

    // Expose time used for each request time as a metric.
    // We use lower-case version of the event name.
    EventType eventType = EventType.valueOf(event.getEventType());
    Timer timer = SentryMetrics
        .getInstance()
        .getTimer(name(HMSFollower.class, eventType.toString().toLowerCase()));

    try (Context ignored = timer.time()) {
      switch (eventType) {
        case CREATE_DATABASE:
          return processCreateDatabase(event);
        case DROP_DATABASE:
          return processDropDatabase(event);
        case CREATE_TABLE:
          return processCreateTable(event);
        case DROP_TABLE:
          return processDropTable(event);
        case ALTER_TABLE:
          return processAlterTable(event);
        case ADD_PARTITION:
          return processAddPartition(event);
        case DROP_PARTITION:
          return processDropPartition(event);
        case ALTER_PARTITION:
          return processAlterPartition(event);
        default:
          LOGGER.error("Notification with ID:{} has invalid event type: {}", event.getEventId(),
              event.getEventType());
          return false;
      }
    }
  }

  /**
   * Processes "create database" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processCreateDatabase(NotificationEvent event) throws Exception {
    SentryJSONCreateDatabaseMessage message =
        deserializer.getCreateDatabaseMessage(event.getMessage());
    String dbName = message.getDB();
    String location = message.getLocation();
    if ((dbName == null) || (location == null)) {
      LOGGER.error("Create database event "
              + "has incomplete information. dbName: {} location: {}",
          StringUtils.defaultIfBlank(dbName, "null"),
          StringUtils.defaultIfBlank(location, "null"));
      return false;
    }

    if (syncStoreOnCreate) {
      dropSentryDbPrivileges(dbName, event);
    }

    if (hdfsSyncEnabled) {
      List<String> locations = Collections.singletonList(location);
      addPaths(dbName, locations, event);

      return true;
    }

    return false;
  }

  /**
   * Processes "drop database" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processDropDatabase(NotificationEvent event) throws Exception {
    SentryJSONDropDatabaseMessage dropDatabaseMessage =
        deserializer.getDropDatabaseMessage(event.getMessage());
    String dbName = dropDatabaseMessage.getDB();
    String location = dropDatabaseMessage.getLocation();
    if (dbName == null) {
      LOGGER.error("Drop database event has incomplete information: dbName = null");
      return false;
    }
    if (syncStoreOnDrop) {
      dropSentryDbPrivileges(dbName, event);
    }

    if (hdfsSyncEnabled) {
      List<String> locations = Collections.singletonList(location);
      removePaths(dbName, locations, event);
      return true;
    }
    return false;
  }

  /**
   * Processes "create table" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processCreateTable(NotificationEvent event)
      throws Exception {
    SentryJSONCreateTableMessage createTableMessage = deserializer
        .getCreateTableMessage(event.getMessage());
    String dbName = createTableMessage.getDB();
    String tableName = createTableMessage.getTable();
    String location = createTableMessage.getLocation();
    if ((dbName == null) || (tableName == null) || (location == null)) {
      LOGGER.error(String.format("Create table event " + "has incomplete information."
              + " dbName = %s, tableName = %s, location = %s",
          StringUtils.defaultIfBlank(dbName, "null"),
          StringUtils.defaultIfBlank(tableName, "null"),
          StringUtils.defaultIfBlank(location, "null")));
      return false;
    }
    if (syncStoreOnCreate) {
      dropSentryTablePrivileges(dbName, tableName, event);
    }

    if (hdfsSyncEnabled) {
      String authzObj = SentryServiceUtil.getAuthzObj(dbName, tableName);
      List<String> locations = Collections.singletonList(location);
      addPaths(authzObj, locations, event);
      return true;
    }

    return false;
  }

  /**
   * Processes "drop table" notification event. It drops all partitions belongs to
   * the table as well. And applies its corresponding snapshot change as well
   * as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processDropTable(NotificationEvent event) throws Exception {
    SentryJSONDropTableMessage dropTableMessage = deserializer
        .getDropTableMessage(event.getMessage());
    String dbName = dropTableMessage.getDB();
    String tableName = dropTableMessage.getTable();
    if ((dbName == null) || (tableName == null)) {
      LOGGER.error("Drop table event "
          + "has incomplete information. dbName: {}, tableName: {}",
          StringUtils.defaultIfBlank(dbName, "null"),
          StringUtils.defaultIfBlank(tableName, "null"));
      return false;
    }
    if (syncStoreOnDrop) {
      dropSentryTablePrivileges(dbName, tableName, event);
    }

    if (hdfsSyncEnabled) {
      String authzObj = SentryServiceUtil.getAuthzObj(dbName, tableName);
      removeAllPaths(authzObj, event);
      return true;
    }

    return false;
  }

  /**
   * Processes "alter table" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processAlterTable(NotificationEvent event) throws Exception {

    if (!hdfsSyncEnabled) {
      return false;
    }

    SentryJSONAlterTableMessage alterTableMessage =
        deserializer.getAlterTableMessage(event.getMessage());
    String oldDbName = alterTableMessage.getDB();
    String oldTableName = alterTableMessage.getTable();
    String newDbName = event.getDbName();
    String newTableName = event.getTableName();
    String oldLocation = alterTableMessage.getOldLocation();
    String newLocation = alterTableMessage.getNewLocation();

    if ((oldDbName == null)
        || (oldTableName == null)
        || (newDbName == null)
        || (newTableName == null)
        || (oldLocation == null)
        || (newLocation == null)) {
      LOGGER.error(String.format("Alter table event "
              + "has incomplete information. oldDbName = %s, oldTableName = %s, oldLocation = %s, "
              + "newDbName = %s, newTableName = %s, newLocation = %s",
          StringUtils.defaultIfBlank(oldDbName, "null"),
          StringUtils.defaultIfBlank(oldTableName, "null"),
          StringUtils.defaultIfBlank(oldLocation, "null"),
          StringUtils.defaultIfBlank(newDbName, "null"),
          StringUtils.defaultIfBlank(newTableName, "null"),
          StringUtils.defaultIfBlank(newLocation, "null")));
      return false;
    }

    if ((oldDbName.equals(newDbName))
        && (oldTableName.equals(newTableName))
        && (oldLocation.equals(newLocation))) {
      LOGGER.error(String.format("Alter table notification ignored as neither name nor "
              + "location has changed: oldAuthzObj = %s, oldLocation = %s, newAuthzObj = %s, "
              + "newLocation = %s", oldDbName + "." + oldTableName, oldLocation,
          newDbName + "." + newTableName, newLocation));
      return false;
    }

    if (!newDbName.equalsIgnoreCase(oldDbName) || !oldTableName.equalsIgnoreCase(newTableName)) {
      // Name has changed
      try {
        renamePrivileges(oldDbName, oldTableName, newDbName, newTableName);
      } catch (SentryNoSuchObjectException e) {
        LOGGER.info("Rename Sentry privilege ignored as there are no privileges on the table:"
            + " {}.{}", oldDbName, oldTableName);
      } catch (Exception e) {
        LOGGER.info("Could not process Alter table event. Event: {}", event.toString(), e);
        return false;
      }
    }
    String oldAuthzObj = oldDbName + "." + oldTableName;
    String newAuthzObj = newDbName + "." + newTableName;
    renameAuthzPath(oldAuthzObj, newAuthzObj, oldLocation, newLocation, event);
    return true;
  }

  /**
   * Processes "add partition" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processAddPartition(NotificationEvent event)
      throws Exception {
    if (!hdfsSyncEnabled) {
      return false;
    }

    SentryJSONAddPartitionMessage addPartitionMessage =
        deserializer.getAddPartitionMessage(event.getMessage());
    String dbName = addPartitionMessage.getDB();
    String tableName = addPartitionMessage.getTable();
    List<String> locations = addPartitionMessage.getLocations();
    if ((dbName == null) || (tableName == null) || (locations == null)) {
      LOGGER.error(String.format("Create table event has incomplete information. "
              + "dbName = %s, tableName = %s, locations = %s",
          StringUtils.defaultIfBlank(dbName, "null"),
          StringUtils.defaultIfBlank(tableName, "null"),
          locations != null ? locations.toString() : "null"));
      return false;
    }
    String authzObj = SentryServiceUtil.getAuthzObj(dbName, tableName);
    addPaths(authzObj, locations, event);
    return true;
  }

  /**
   * Processes "drop partition" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processDropPartition(NotificationEvent event)
      throws Exception {
    if (!hdfsSyncEnabled) {
      return false;
    }

    SentryJSONDropPartitionMessage dropPartitionMessage =
        deserializer.getDropPartitionMessage(event.getMessage());
    String dbName = dropPartitionMessage.getDB();
    String tableName = dropPartitionMessage.getTable();
    List<String> locations = dropPartitionMessage.getLocations();
    if ((dbName == null) || (tableName == null) || (locations == null)) {
      LOGGER.error(String.format("Drop partition event "
              + "has incomplete information. dbName = %s, tableName = %s, location = %s",
          StringUtils.defaultIfBlank(dbName, "null"),
          StringUtils.defaultIfBlank(tableName, "null"),
          locations != null ? locations.toString() : "null"));
      return false;
    }
    String authzObj = SentryServiceUtil.getAuthzObj(dbName, tableName);
    removePaths(authzObj, locations, event);
    return true;
  }

  /**
   * Processes "alter partition" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param event notification event to be processed.
   * @throws Exception if encounters errors while persisting the path change
   */
  private boolean processAlterPartition(NotificationEvent event) throws Exception {
    if (!hdfsSyncEnabled) {
      return false;
    }

    SentryJSONAlterPartitionMessage alterPartitionMessage =
        deserializer.getAlterPartitionMessage(event.getMessage());
    String dbName = alterPartitionMessage.getDB();
    String tableName = alterPartitionMessage.getTable();
    String oldLocation = alterPartitionMessage.getOldLocation();
    String newLocation = alterPartitionMessage.getNewLocation();

    if ((dbName == null)
        || (tableName == null)
        || (oldLocation == null)
        || (newLocation == null)) {
      LOGGER.error(String.format("Alter partition event "
              + "has incomplete information. dbName = %s, tableName = %s, "
              + "oldLocation = %s, newLocation = %s",
          StringUtils.defaultIfBlank(dbName, "null"),
          StringUtils.defaultIfBlank(tableName, "null"),
          StringUtils.defaultIfBlank(oldLocation, "null"),
          StringUtils.defaultIfBlank(newLocation, "null")));
      return false;
    }

    if (oldLocation.equals(newLocation)) {
      LOGGER.debug(String.format("Alter partition notification ignored as"
          + "location has not changed: AuthzObj = %s, Location = %s", dbName + "."
          + "." + tableName, oldLocation));
      return false;
    }

    String oldAuthzObj = dbName + "." + tableName;
    renameAuthzPath(oldAuthzObj, oldAuthzObj, oldLocation, newLocation, event);
    return true;
  }

  /**
   * Adds an authzObj along with a set of paths into the authzObj -> [Paths] mapping
   * as well as persist the corresponding delta path change to Sentry DB.
   *
   * @param authzObj the given authzObj
   * @param locations a set of paths need to be added
   * @param event the NotificationEvent object from where authzObj and locations were obtained
   */
  private void addPaths(String authzObj, Collection<String> locations, NotificationEvent event)
      throws Exception {
    // AuthzObj is case insensitive
    authzObj = authzObj.toLowerCase();

    UniquePathsUpdate update = new UniquePathsUpdate(event, false);
    Collection<String> paths = new HashSet<>(locations.size());
    // addPath and persist into Sentry DB.
    // Skip update if encounter malformed path.
    for (String location : locations) {
      String pathTree = getPath(location);
      if (pathTree == null) {
        LOGGER.debug("HMS Path Update ["
            + "OP : addPath, "
            + "authzObj : " + authzObj + ", "
            + "path : " + location + "] - nothing to add" + ", "
            + "notification event ID: " + event.getEventId() + "]");
      } else {
        LOGGER.debug("HMS Path Update ["
            + "OP : addPath, " + "authzObj : "
            + authzObj + ", "
            + "path : " + location + ", "
            + "notification event ID: " + event.getEventId() + "]");
        update.newPathChange(authzObj).addToAddPaths(splitPath(pathTree));
        paths.add(pathTree);
      }
    }
    sentryStore.addAuthzPathsMapping(authzObj, paths, update);
  }

  /**
   * Removes a set of paths map to a given authzObj from the authzObj -> [Paths] mapping
   * as well as persist the corresponding delta path change to Sentry DB.
   *
   * @param authzObj the given authzObj
   * @param locations a set of paths need to be removed
   * @param event the NotificationEvent object from where authzObj and locations were obtained
   */
  private void removePaths(String authzObj, Collection<String> locations, NotificationEvent event)
      throws Exception {
    // AuthzObj is case insensitive
    authzObj = authzObj.toLowerCase();

    UniquePathsUpdate update = new UniquePathsUpdate(event, false);
    Collection<String> paths = new HashSet<>(locations.size());
    for (String location : locations) {
      String pathTree = getPath(location);
      if (pathTree == null) {
        LOGGER.debug("HMS Path Update ["
            + "OP : removePath, "
            + "authzObj : " + authzObj + ", "
            + "path : " + location + "] - nothing to remove" + ", "
            + "notification event ID: " + event.getEventId() + "]");
      } else {
        LOGGER.debug("HMS Path Update ["
            + "OP : removePath, "
            + "authzObj : " + authzObj + ", "
            + "path : " + location + ", "
            + "notification event ID: " + event.getEventId() + "]");
        update.newPathChange(authzObj).addToDelPaths(splitPath(pathTree));
        paths.add(pathTree);
      }
    }
    sentryStore.deleteAuthzPathsMapping(authzObj, paths, update);
  }

  /**
   * Removes a given authzObj and all paths belongs to it from the
   * authzObj -> [Paths] mapping as well as persist the corresponding
   * delta path change to Sentry DB.
   *
   * @param authzObj the given authzObj to be deleted
   * @param event the NotificationEvent object from where authzObj and locations were obtained
   */
  private void removeAllPaths(String authzObj, NotificationEvent event)
      throws Exception {
    // AuthzObj is case insensitive
    authzObj = authzObj.toLowerCase();

    LOGGER.debug("HMS Path Update ["
        + "OP : removeAllPaths, "
        + "authzObj : " + authzObj + ", "
        + "notification event ID: " + event.getEventId() + "]");
    UniquePathsUpdate update = new UniquePathsUpdate(event, false);
    update.newPathChange(authzObj).addToDelPaths(
        Lists.newArrayList(PathsUpdate.ALL_PATHS));
    sentryStore.deleteAllAuthzPathsMapping(authzObj, update);
  }

  /**
   * Renames a given authzObj and alter the paths belongs to it from the
   * authzObj -> [Paths] mapping as well as persist the corresponding
   * delta path change to Sentry DB.
   *
   * @param oldAuthzObj the existing authzObj
   * @param newAuthzObj the new name to be changed to
   * @param oldLocation a existing path of the given authzObj
   * @param newLocation a new path to be changed to
   * @param event the NotificationEvent object from where authzObj and locations were obtained
   */
  private void renameAuthzPath(String oldAuthzObj, String newAuthzObj, String oldLocation,
      String newLocation, NotificationEvent event) throws Exception {
    // AuthzObj is case insensitive
    oldAuthzObj = oldAuthzObj.toLowerCase();
    newAuthzObj = newAuthzObj.toLowerCase();
    String oldPathTree = getPath(oldLocation);
    String newPathTree = getPath(newLocation);

    LOGGER.debug("HMS Path Update ["
        + "OP : renameAuthzObject, "
        + "oldAuthzObj : " + oldAuthzObj + ", "
        + "newAuthzObj : " + newAuthzObj + ", "
        + "oldLocation : " + oldLocation + ", "
        + "newLocation : " + newLocation + ", "
        + "notification event ID: " + event.getEventId() + "]");

    // In the case of HiveObj name has changed
    if (!oldAuthzObj.equalsIgnoreCase(newAuthzObj)) {
      // Skip update if encounter malformed path for both oldLocation and newLocation.
      if ((oldPathTree != null) && (newPathTree != null)) {
        UniquePathsUpdate update = new UniquePathsUpdate(event, false);
        update.newPathChange(oldAuthzObj).addToDelPaths(splitPath(oldPathTree));
        update.newPathChange(newAuthzObj).addToAddPaths(splitPath(newPathTree));
        if (oldLocation.equals(newLocation)) {
          //Only name has changed
          // - Alter table rename for an external table
          sentryStore.renameAuthzObj(oldAuthzObj, newAuthzObj, update);
        } else {
          // Both name and location has changed
          // - Alter table rename for managed table
          sentryStore.renameAuthzPathsMapping(oldAuthzObj, newAuthzObj, oldPathTree,
              newPathTree, update);
        }
      } else {
        updateAuthzPathsMapping(oldAuthzObj, oldPathTree, newAuthzObj, newPathTree, event);
      }
    } else if (!oldLocation.equals(newLocation)) {
      // Only Location has changed, e.g. Alter table set location
      if ((oldPathTree != null) && (newPathTree != null)) {
        UniquePathsUpdate update = new UniquePathsUpdate(event, false);
        update.newPathChange(oldAuthzObj).addToDelPaths(splitPath(oldPathTree));
        update.newPathChange(oldAuthzObj).addToAddPaths(splitPath(newPathTree));
        sentryStore.updateAuthzPathsMapping(oldAuthzObj, oldPathTree,
            newPathTree, update);
      } else {
        updateAuthzPathsMapping(oldAuthzObj, oldPathTree, newAuthzObj, newPathTree,event);
      }
    } else {
      LOGGER.error("Update Notification for Auhorizable object {}, with no change, skipping",
          oldAuthzObj);
      throw new SentryInvalidHMSEventException("Update Notification for Authorizable object"
          + "with no change");
    }
  }

  private void updateAuthzPathsMapping(String oldAuthzObj, String oldPathTree,
      String newAuthzObj, String newPathTree, NotificationEvent event) throws Exception {
    if (oldPathTree != null) {
      UniquePathsUpdate update = new UniquePathsUpdate(event, false);
      update.newPathChange(oldAuthzObj).addToDelPaths(splitPath(oldPathTree));
      sentryStore.deleteAuthzPathsMapping(oldAuthzObj,
          Collections.singleton(oldPathTree),
          update);
    } else if (newPathTree != null) {
      UniquePathsUpdate update = new UniquePathsUpdate(event, false);
      update.newPathChange(newAuthzObj).addToAddPaths(splitPath(newPathTree));
      sentryStore.addAuthzPathsMapping(newAuthzObj,
          Collections.singleton(newPathTree),
          update);
    }

  }

  /**
   * Get path tree from a given path. It return null if encounters
   * SentryMalformedPathException which indicates a malformed path.
   *
   * @param path a path
   * @return the path tree given a path.
   */
  private String getPath(String path) {
    try {
      return PathsUpdate.parsePath(path);
    } catch (SentryMalformedPathException e) {
      LOGGER.error("Unexpected path while parsing {}", path, e);
    }
    return null;
  }

  private void dropSentryDbPrivileges(String dbName, NotificationEvent event) {
    try {
      TSentryAuthorizable authorizable = new TSentryAuthorizable(authServerName);
      authorizable.setDb(dbName);
      sentryStore.dropPrivilege(authorizable, getPermUpdatableOnDrop(authorizable));
    } catch (SentryNoSuchObjectException e) {
      LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the database: {}",
          dbName);
    } catch (Exception e) {
      LOGGER.error("Could not process Drop database event." + "Event: " + event.toString(), e);
    }
  }

  private void dropSentryTablePrivileges(String dbName, String tableName,
      NotificationEvent event) {
    try {
      TSentryAuthorizable authorizable = new TSentryAuthorizable(authServerName);
      authorizable.setDb(dbName);
      authorizable.setTable(tableName);
      sentryStore.dropPrivilege(authorizable, getPermUpdatableOnDrop(authorizable));
    } catch (SentryNoSuchObjectException e) {
      LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the table: {}.{}",
          dbName, tableName);
    } catch (Exception e) {
      LOGGER.error("Could not process Drop table event. Event: " + event.toString(), e);
    }
  }

  private void renamePrivileges(String oldDbName, String oldTableName, String newDbName,
      String newTableName) throws
      Exception {
    TSentryAuthorizable oldAuthorizable = new TSentryAuthorizable(authServerName);
    oldAuthorizable.setDb(oldDbName);
    oldAuthorizable.setTable(oldTableName);
    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(authServerName);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);
    Update update =
        getPermUpdatableOnRename(oldAuthorizable, newAuthorizable);
    sentryStore.renamePrivilege(oldAuthorizable, newAuthorizable, update);
  }
}
