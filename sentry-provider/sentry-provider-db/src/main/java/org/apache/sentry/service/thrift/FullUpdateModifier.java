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

package org.apache.sentry.service.thrift;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.MessageDeserializer;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAddPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAlterPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONAlterTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONCreateDatabaseMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONCreateTableMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropDatabaseMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropPartitionMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONDropTableMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Apply newer events to the full update.
 *
 * <p>The process of obtaining ful snapshot from HMS is not atomic.
 * While we read information from HMS it may change - some new objects can be created,
 * or some can be removed or modified. This class is used to reconsile changes to
 * the full snapshot.
 */
final class FullUpdateModifier {
  private static final Logger LOGGER = LoggerFactory.getLogger(FullUpdateModifier.class);

  // Prevent creation of class instances
  private FullUpdateModifier() {
  }

  /**
   * Take a full snapshot and apply an MS event to it.
   *
   * <p>We pass serializer as a parameter to simplify testing.
   *
   * @param image Full snapshot
   * @param event HMS notificatin event
   * @param deserializer Message deserializer -
   *                     should produce Sentry JSON serializer type messages.
   */
  // NOTE: we pass deserializer here instead of using built-in one to simplify testing.
  // Tests use mock serializers and thus we do not have to construct proper events.
  static void applyEvent(Map<String, Collection<String>> image, NotificationEvent event,
                         MessageDeserializer deserializer) {
    HCatEventMessage.EventType eventType =
            HCatEventMessage.EventType.valueOf(event.getEventType());

    switch (eventType) {
      case CREATE_DATABASE:
        createDatabase(image, event, deserializer);
        break;
      case DROP_DATABASE:
        dropDatabase(image, event, deserializer);
        break;
      case CREATE_TABLE:
        createTable(image, event, deserializer);
        break;
      case DROP_TABLE:
        dropTable(image, event, deserializer);
        break;
      case ALTER_TABLE:
        alterTable(image, event, deserializer);
        break;
      case ADD_PARTITION:
        addPartition(image, event, deserializer);
        break;
      case DROP_PARTITION:
        dropPartition(image, event, deserializer);
        break;
      case ALTER_PARTITION:
        alterPartition(image, event, deserializer);
        break;
      default:
        LOGGER.error("Notification with ID:{} has invalid event type: {}", event.getEventId(),
                event.getEventType());
        break;
    }
  }

  /**
   * Add mapping from the new database name to location {dbname: {location}}.
   */
  private static void createDatabase(Map<String, Collection<String>> image, NotificationEvent event,
                                     MessageDeserializer deserializer) {
    SentryJSONCreateDatabaseMessage message =
            (SentryJSONCreateDatabaseMessage) deserializer
                    .getCreateDatabaseMessage(event.getMessage());

    String dbName = message.getDB();
    if ((dbName == null) || dbName.isEmpty()) {
      LOGGER.error("Create database event is missing database name");
      return;
    }
    dbName = dbName.toLowerCase();

    String location = message.getLocation();
    if ((location == null) || location.isEmpty()) {
      LOGGER.error("Create database event is missing database location");
      return;
    }

    String path = FullUpdateInitializer.pathFromURI(location);
    if (path == null) {
      return;
    }

    // Add new database if it doesn't exist yet
    if (!image.containsKey(dbName)) {
      LOGGER.debug("create database {} with location {}", dbName, location);
      image.put(dbName.intern(), Collections.singleton(path));
    } else {
      // Sanity check the information and print warnings if database exists but
      // with a different location
      Set<String> oldLocations = (Set<String>)image.get(dbName);
      LOGGER.debug("database {} already exists, ignored", dbName);
      if (!oldLocations.contains(location)) {
        LOGGER.warn("database {} exists but location is different from {}", dbName, location);
      }
    }
  }

  /**
   * Remove a mapping from database name and remove all mappings which look like dbName.tableName
   * where dbName matches database name.
   */
  private static void dropDatabase(Map<String, Collection<String>> image, NotificationEvent event,
                                   MessageDeserializer deserializer) {
    SentryJSONDropDatabaseMessage message =
            (SentryJSONDropDatabaseMessage) deserializer.getDropDatabaseMessage(event.getMessage());

    String dbName = message.getDB();
    if ((dbName == null) || dbName.isEmpty()) {
      LOGGER.error("Drop database event is missing database name");
      return;
    }
    dbName = dbName.toLowerCase();
    String location = message.getLocation();
    if ((location == null) || location.isEmpty()) {
      LOGGER.error("Drop database event is missing database location");
      return;
    }

    String path = FullUpdateInitializer.pathFromURI(location);
    if (path == null) {
      return;
    }

    // If the database is alreday deleted, we have nothing to do
    Set<String> locations = (Set<String>)image.get(dbName);
    if (locations == null) {
      LOGGER.debug("database {} is already deleted", dbName);
      return;
    }

    if (!locations.contains(path)) {
      LOGGER.warn("Database {} location does not match {}", dbName, path);
      return;
    }

    LOGGER.debug("drop database {} with location {}", dbName, location);

    // Drop information about the database
    image.remove(dbName);

    String dbPrefix = dbName + ".";

    // Remove all objects for this database
    for (Iterator<Map.Entry<String, Collection<String>>> it = image.entrySet().iterator();
         it.hasNext(); ) {
      Map.Entry<String, Collection<String>> entry = it.next();
      String key = entry.getKey();
      if (key.startsWith(dbPrefix)) {
        LOGGER.debug("Removing {}", key);
        it.remove();
      }
    }
  }

  /**
   * Add mapping for dbName.tableName.
   */
  private static void createTable(Map<String, Collection<String>> image, NotificationEvent event,
                                  MessageDeserializer deserializer) {
    SentryJSONCreateTableMessage message = (SentryJSONCreateTableMessage) deserializer
            .getCreateTableMessage(event.getMessage());

    String dbName = message.getDB();
    if ((dbName == null) || dbName.isEmpty()) {
      LOGGER.error("Create table event is missing database name");
      return;
    }
    String tableName = message.getTable();
    if ((tableName == null) || tableName.isEmpty()) {
      LOGGER.error("Create table event is missing table name");
      return;
    }

    String location = message.getLocation();
    if ((location == null) || location.isEmpty()) {
      LOGGER.error("Create table event is missing table location");
      return;
    }

    String path = FullUpdateInitializer.pathFromURI(location);
    if (path == null) {
      return;
    }

    String authName = dbName.toLowerCase() + "." + tableName.toLowerCase();
    // Add new table if it doesn't exist yet
    if (!image.containsKey(authName)) {
      LOGGER.debug("create table {} with location {}", authName, location);
      Set<String> locations = new HashSet<>(1);
      locations.add(path);
      image.put(authName.intern(), locations);
    } else {
      // Sanity check the information and print warnings if table exists but
      // with a different location
      Set<String> oldLocations = (Set<String>)image.get(authName);
      LOGGER.debug("Table {} already exists, ignored", authName);
      if (!oldLocations.contains(location)) {
        LOGGER.warn("Table {} exists but location is different from {}", authName, location);
      }
    }
  }

  /**
   * Drop mapping from dbName.tableName
   */
  private static void dropTable(Map<String, Collection<String>> image, NotificationEvent event,
                                MessageDeserializer deserializer) {
    SentryJSONDropTableMessage message = (SentryJSONDropTableMessage) deserializer
            .getDropTableMessage(event.getMessage());

    String dbName = message.getDB();
    if ((dbName == null) || dbName.isEmpty()) {
      LOGGER.error("Drop table event is missing database name");
      return;
    }
    String tableName = message.getTable();
    if ((tableName == null) || tableName.isEmpty()) {
      LOGGER.error("Drop table event is missing table name");
      return;
    }

    String location = message.getLocation();
    if ((location == null) || location.isEmpty()) {
      LOGGER.error("Drop table event is missing table location");
      return;
    }

    String path = FullUpdateInitializer.pathFromURI(location);
    if (path == null) {
      return;
    }

    String authName = dbName.toLowerCase() + "." + tableName.toLowerCase();
    Set<String> locations = (Set<String>)image.get(authName);
    if (locations != null && locations.contains(path)) {
      LOGGER.debug("Removing {}", authName);
      image.remove(authName);
    } else {
      LOGGER.warn("can't find matching table {} with location {}", authName, location);
    }
  }

  /**
   * ALTER TABLE is a complicated function that can alter multiple things.
   *
   * <p>We take care iof the following cases:
   * <ul>
   *   <li>Change database name. This is the most complicated one.
   *   We need to change the actual database name and change all mappings
   *   that look like "dbName.tableName" to the new dbName</li>
   *  <li>Change table name</li>
   *  <li>Change location</li>
   * </ul>
   *
   */
  private static void alterTable(Map<String, Collection<String>> image, NotificationEvent event,
                                 MessageDeserializer deserializer) {
    SentryJSONAlterTableMessage message =
            (SentryJSONAlterTableMessage) deserializer.getAlterTableMessage(event.getMessage());
    String prevDbName = message.getDB();
    if ((prevDbName == null) || prevDbName.isEmpty()) {
      LOGGER.error("Alter table event is missing old database name");
      return;
    }
    prevDbName = prevDbName.toLowerCase();
    String prevTableName = message.getTable();
    if ((prevTableName == null) || prevTableName.isEmpty()) {
      LOGGER.error("Alter table event is missing old table name");
      return;
    }
    prevTableName = prevTableName.toLowerCase();

    String newDbName = event.getDbName();
    if ((newDbName == null) || newDbName.isEmpty()) {
      LOGGER.error("Alter table event is missing new database name");
      return;
    }
    newDbName = newDbName.toLowerCase();

    String newTableName = event.getTableName();
    if ((newTableName == null) || newTableName.isEmpty()) {
      LOGGER.error("Alter table event is missing new table name");
      return;
    }
    newTableName = newTableName.toLowerCase();

    String prevLocation = message.getOldLocation();
    if ((prevLocation == null) || prevLocation.isEmpty()) {
      LOGGER.error("Alter table event is missing old location");
      return;
    }
    String prevPath = FullUpdateInitializer.pathFromURI(prevLocation);
    if (prevPath == null) {
      return;
    }

    String newLocation = message.getNewLocation();
    if ((newLocation == null) || newLocation.isEmpty()) {
      LOGGER.error("Alter table event is missing new location");
      return;
    }
    String newPath = FullUpdateInitializer.pathFromURI(newLocation);
    if (newPath == null) {
      return;
    }

    String prevAuthName = prevDbName + "." + prevTableName;
    String newAuthName = newDbName + "." + newTableName;

    if (!prevDbName.equals(newDbName)) {
      // Database name change
      LOGGER.debug("Changing database name: {} -> {}", prevDbName, newDbName);
      Set<String> locations = (Set<String>)image.get(prevDbName);
      if (locations != null) {
        // Rename database if it is not renamed yet
        if (!image.containsKey(newDbName)) {
          image.put(newDbName, locations);
          image.remove(prevDbName);
          // Walk through all tables and rename DB part of the AUTH name
          // AUTH name is "dbName.TableName" so we need to replace dbName with the new name
          String prevDbPrefix = prevDbName + ".";
          String newDbPrefix = newDbName + ".";
          renamePrefixKeys(image, prevDbPrefix, newDbPrefix);
        } else {
          LOGGER.warn("database {} rename: found existing database {}", prevDbName, newDbName);
        }
      } else {
        LOGGER.debug("database {} not found", prevDbName);
      }
    }

    if (!prevAuthName.equals(newAuthName)) {
      // Either the database name or table name changed, rename objects
      Set<String> locations = (Set<String>)image.get(prevAuthName);
      if (locations != null) {
        // Rename if it is not renamed yet
        if (!image.containsKey(newAuthName)) {
          LOGGER.debug("rename {} -> {}", prevAuthName, newAuthName);
          image.put(newAuthName, locations);
          image.remove(prevAuthName);
        } else {
          LOGGER.warn("auth {} rename: found existing object {}", prevAuthName, newAuthName);
        }
      } else {
        LOGGER.debug("auth {} not found", prevAuthName);
      }
    }

    if (!prevPath.equals(newPath)) {
      LOGGER.debug("Location change: {} -> {}", prevPath, newPath);
      // Location change
      Set<String> locations = (Set<String>) image.get(newAuthName);
      if (locations != null && locations.contains(prevPath) && !locations.contains(newPath)) {
        locations.remove(prevPath);
        locations.add(newPath);
      } else {
        LOGGER.warn("can not process location change for {}", newAuthName);
        LOGGER.warn("old locatio = {}, new location = {}", prevPath, newPath);
      }
    }
  }

  /**
   * Add partition just adds a new location to the existing table.
   */
  private static void addPartition(Map<String, Collection<String>> image, NotificationEvent event,
                                   MessageDeserializer deserializer) {
    SentryJSONAddPartitionMessage message =
            (SentryJSONAddPartitionMessage) deserializer.getAddPartitionMessage(event.getMessage());

    String dbName = message.getDB();
    if ((dbName == null) || dbName.isEmpty()) {
      LOGGER.error("Add partition event is missing database name");
      return;
    }
    String tableName = message.getTable();
    if ((tableName == null) || tableName.isEmpty()) {
      LOGGER.error("Add partition event for {} is missing table name", dbName);
      return;
    }

    String authName = dbName.toLowerCase() + "." + tableName.toLowerCase();

    List<String> locations = message.getLocations();
    if (locations == null || locations.isEmpty()) {
      LOGGER.error("Add partition event for {} is missing partition locations", authName);
      return;
    }

    Set<String> oldLocations = (Set<String>) image.get(authName);
    if (oldLocations == null) {
      LOGGER.warn("Add partition for {}: missing table locations",authName);
      return;
    }

    // Add each partition
    for (String location: locations) {
      String path = FullUpdateInitializer.pathFromURI(location);
      if (path != null) {
        LOGGER.debug("Adding partition {}:{}", authName, path);
        oldLocations.add(path);
      }
    }
  }

  /**
   * Drop partition removes location from the existing table.
   */
  private static void dropPartition(Map<String, Collection<String>> image, NotificationEvent event,
                                    MessageDeserializer deserializer) {
    SentryJSONDropPartitionMessage message =
            (SentryJSONDropPartitionMessage) deserializer
                    .getDropPartitionMessage(event.getMessage());
    String dbName = message.getDB();
    if ((dbName == null) || dbName.isEmpty()) {
      LOGGER.error("Drop partition event is missing database name");
      return;
    }
    String tableName = message.getTable();
    if ((tableName == null) || tableName.isEmpty()) {
      LOGGER.error("Drop partition event for {} is missing table name", dbName);
      return;
    }

    String authName = dbName.toLowerCase() + "." + tableName.toLowerCase();

    List<String> locations = message.getLocations();
    if (locations == null || locations.isEmpty()) {
      LOGGER.error("Drop partition event for {} is missing partition locations", authName);
      return;
    }

    Set<String> oldLocations = (Set<String>) image.get(authName);
    if (oldLocations == null) {
      LOGGER.warn("Add partition for {}: missing table locations",authName);
      return;
    }

    // Drop each partition
    for (String location: locations) {
      String path = FullUpdateInitializer.pathFromURI(location);
      if (path != null) {
        oldLocations.remove(path);
      }
    }
  }

  private static void alterPartition(Map<String, Collection<String>> image, NotificationEvent event,
                                     MessageDeserializer deserializer) {
    SentryJSONAlterPartitionMessage message =
            (SentryJSONAlterPartitionMessage) deserializer
                    .getAlterPartitionMessage(event.getMessage());

    String dbName = message.getDB();
    if ((dbName == null) || dbName.isEmpty()) {
      LOGGER.error("Alter partition event is missing database name");
      return;
    }
    String tableName = message.getTable();
    if ((tableName == null) || tableName.isEmpty()) {
      LOGGER.error("Alter partition event for {} is missing table name", dbName);
      return;
    }

    String authName = dbName.toLowerCase() + "." + tableName.toLowerCase();

    String prevLocation = message.getOldLocation();
    if (prevLocation == null || prevLocation.isEmpty()) {
      LOGGER.error("Alter partition event for {} is missing old location", authName);
    }

    String prevPath = FullUpdateInitializer.pathFromURI(prevLocation);
    if (prevPath == null) {
      return;
    }

    String newLocation = message.getNewLocation();
    if (newLocation == null || newLocation.isEmpty()) {
      LOGGER.error("Alter partition event for {} is missing new location", authName);
    }

    String newPath = FullUpdateInitializer.pathFromURI(newLocation);
    if (newPath == null) {
      return;
    }

    if (prevPath.equals(newPath)) {
      LOGGER.warn("Alter partition event for {} has the same old and new path {}",
              authName, prevPath);
      return;
    }

    Set<String> locations = (Set<String>) image.get(authName);
    if (locations == null) {
      LOGGER.warn("Missing partition locations for {}", authName);
      return;
    }

    // Rename partition
    if (locations.remove(prevPath)) {
      LOGGER.debug("Renaming {} to {}", prevPath, newPath);
      locations.add(newPath);
    }
  }

  /**
   * Walk through the map and rename all instances of oldKey to newKey.
   */
  @VisibleForTesting
  protected static void renamePrefixKeys(Map<String, Collection<String>> image,
                                         String oldKey, String newKey) {
    // The trick is that we can't just iterate through the map, remove old values and
    // insert new values. While we can remove old values with iterators,
    // we can't insert new ones while we walk. So we collect the keys to be added in
    // a new map and merge them in the end.
    Map<String, Set<String>> replacement = new HashMap<>();

    for (Iterator<Map.Entry<String, Collection<String>>> it = image.entrySet().iterator();
         it.hasNext(); ) {
      Map.Entry<String, Collection<String>> entry = it.next();
      String key = entry.getKey();
      if (key.startsWith(oldKey)) {
        String updatedKey = key.replaceAll("^" + oldKey + "(.*)", newKey + "$1");
        if (!image.containsKey(updatedKey)) {
          LOGGER.debug("Rename {} to {}", key, updatedKey);
          replacement.put(updatedKey, (Set<String>) entry.getValue());
          it.remove();
        } else {
          LOGGER.warn("skipping key {} - already present", updatedKey);
        }
      }
    }

    mergeMaps(image, replacement);
  }

  /**
   * Merge replacement values into the original map but only if they are not
   * already there.
   *
   * @param m1 source map
   * @param m2 map with replacement values
   */
  private static void mergeMaps(Map<String, Collection<String>> m1, Map<String, Set<String>> m2) {
    // Merge replacement values into the original map but only if they are not
    // already there
    for (Map.Entry<String, Set<String>> entry : m2.entrySet()) {
      if (!m1.containsKey(entry.getKey())) {
        m1.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
