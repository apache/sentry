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

import com.google.common.collect.Lists;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.SentryMalformedPathException;
import org.apache.sentry.core.common.exception.SentryInvalidHMSEventException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * NotificationProcessor processes various notification events generated from
 * the Hive MetaStore state change, and applies these changes on the complete
 * HMS Paths snapshot or delta update stored in Sentry using SentryStore.
 * <p>
 * NotificationProcessor should not skip processing notification events for any reason.
 * If some notification events are to be skipped, appropriate logic should be added in
 * HMSFollower before invoking NotificationProcessor.
 */
class NotificationProcessor {

  private final Logger LOGGER;
  private final SentryStore sentryStore;

  NotificationProcessor(SentryStore sentryStore, Logger LOGGER) {
    this.LOGGER = LOGGER;
    this.sentryStore = sentryStore;
  }

  /**
   * Processes "create database" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param dbName database name
   * @param location database location
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processCreateDatabase(String dbName, String location, long seqNum) throws Exception {
    List<String> locations = Collections.singletonList(location);
    addPaths(dbName, locations, seqNum);
  }

  /**
   * Processes "drop database" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param dbName database name
   * @param location database location
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processDropDatabase(String dbName, String location, long seqNum) throws Exception {
    List<String> locations = Collections.singletonList(location);
    removePaths(dbName, locations, seqNum);
  }

  /**
   * Processes "create table" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param dbName database name
   * @param tableName table name
   * @param location table location
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processCreateTable(String dbName, String tableName, String location, long seqNum)
        throws Exception {
    String authzObj = dbName + "." + tableName;
    List<String> locations = Collections.singletonList(location);
    addPaths(authzObj, locations, seqNum);
  }

  /**
   * Processes "drop table" notification event. It drops all partitions belongs to
   * the table as well. And applies its corresponding snapshot change as well
   * as delta path update into Sentry DB.
   *
   * @param dbName database name
   * @param tableName table name
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processDropTable(String dbName, String tableName, long seqNum) throws Exception {
    String authzObj = dbName + "." + tableName;
    removeAllPaths(authzObj, seqNum);
  }

  /**
   * Processes "alter table" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param oldDbName old database name
   * @param newDbName new database name
   * @param oldTableName old table name
   * @param newTableName new table name
   * @param oldLocation old table location
   * @param newLocation new table location
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processAlterTable(String oldDbName, String newDbName, String oldTableName,
          String newTableName, String oldLocation, String newLocation, long seqNum)
              throws Exception {
    String oldAuthzObj = oldDbName + "." + oldTableName;
    String newAuthzObj = newDbName + "." + newTableName;
    renameAuthzPath(oldAuthzObj, newAuthzObj, oldLocation, newLocation, seqNum);
  }

  /**
   * Processes "add partition" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param dbName database name
   * @param tableName table name
   * @param locations partition locations
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processAddPartition(String dbName, String tableName,
                           Collection<String> locations, long seqNum)
        throws Exception {
    String authzObj = dbName + "." + tableName;
    addPaths(authzObj, locations, seqNum);
  }

  /**
   * Processes "drop partition" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param dbName database name
   * @param tableName table name
   * @param locations partition locations
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processDropPartition(String dbName, String tableName,
                            Collection<String> locations, long seqNum)
        throws Exception {
    String authzObj = dbName + "." + tableName;
    removePaths(authzObj, locations, seqNum);
  }

  /**
   * Processes "alter partition" notification event, and applies its corresponding
   * snapshot change as well as delta path update into Sentry DB.
   *
   * @param dbName database name
   * @param tableName table name
   * @param oldLocation old partition location
   * @param newLocation new partition location
   * @param seqNum notification event ID
   * @throws Exception if encounters errors while persisting the path change
   */
  void processAlterPartition(String dbName, String tableName, String oldLocation,
        String newLocation, long seqNum) throws Exception {
    String oldAuthzObj = dbName + "." + tableName;
    renameAuthzPath(oldAuthzObj, oldAuthzObj, oldLocation, newLocation, seqNum);
  }

  /**
   * Adds an authzObj along with a set of paths into the authzObj -> [Paths] mapping
   * as well as persist the corresponding delta path change to Sentry DB.
   *
   * @param authzObj the given authzObj
   * @param locations a set of paths need to be added
   * @param seqNum notification event ID
   * @throws Exception
   */
  private void addPaths(String authzObj, Collection<String> locations, long seqNum)
        throws Exception {
    // AuthzObj is case insensitive
    authzObj = authzObj.toLowerCase();

    PathsUpdate update = new PathsUpdate(seqNum, false);
    Collection<String> paths = new HashSet<>(locations.size());
    // addPath and persist into Sentry DB.
    // Skip update if encounter malformed path.
    for (String location : locations) {
      String pathTree = getPath(location);
      if (pathTree == null) {
        LOGGER.debug("#### HMS Path Update ["
            + "OP : addPath, "
            + "authzObj : " + authzObj + ", "
            + "path : " + location + "] - nothing to add" + ", "
            + "notification event ID: " + seqNum + "]");
      } else {
        LOGGER.debug("#### HMS Path Update ["
            + "OP : addPath, " + "authzObj : "
            + authzObj + ", "
            + "path : " + location + ", "
            + "notification event ID: " + seqNum + "]");
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
   * @param seqNum notification event ID
   * @throws Exception
   */
  private void removePaths(String authzObj, Collection<String> locations, long seqNum)
        throws Exception {
    // AuthzObj is case insensitive
    authzObj = authzObj.toLowerCase();

    PathsUpdate update = new PathsUpdate(seqNum, false);
    Collection<String> paths = new HashSet<>(locations.size());
    for (String location : locations) {
      String pathTree = getPath(location);
      if (pathTree == null) {
        LOGGER.debug("#### HMS Path Update ["
            + "OP : removePath, "
            + "authzObj : " + authzObj + ", "
            + "path : " + location + "] - nothing to remove" + ", "
            + "notification event ID: " + seqNum + "]");
      } else {
        LOGGER.debug("#### HMS Path Update ["
            + "OP : removePath, "
            + "authzObj : " + authzObj + ", "
            + "path : " + location + ", "
            + "notification event ID: " + seqNum + "]");
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
   * @param seqNum notification event ID
   * @throws Exception
   */
  private void removeAllPaths(String authzObj, long seqNum)
        throws Exception {
    // AuthzObj is case insensitive
    authzObj = authzObj.toLowerCase();

    LOGGER.debug("#### HMS Path Update ["
        + "OP : removeAllPaths, "
        + "authzObj : " + authzObj + ", "
        + "notification event ID: " + seqNum + "]");
    PathsUpdate update = new PathsUpdate(seqNum, false);
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
   * @param seqNum
   * @throws Exception
   */
  private void renameAuthzPath(String oldAuthzObj, String newAuthzObj, String oldLocation,
          String newLocation, long seqNum) throws Exception {
    // AuthzObj is case insensitive
    oldAuthzObj = oldAuthzObj.toLowerCase();
    newAuthzObj = newAuthzObj.toLowerCase();
    String oldPathTree = getPath(oldLocation);
    String newPathTree = getPath(newLocation);

    LOGGER.debug("#### HMS Path Update ["
        + "OP : renameAuthzObject, "
        + "oldAuthzObj : " + oldAuthzObj + ", "
        + "newAuthzObj : " + newAuthzObj   + ", "
        + "oldLocation : " + oldLocation + ", "
        + "newLocation : " + newLocation + ", "
        + "notification event ID: " + seqNum + "]");

    // In the case of HiveObj name has changed
    if (!oldAuthzObj.equalsIgnoreCase(newAuthzObj)) {
      // Skip update if encounter malformed path for both oldLocation and newLocation.
      if ((oldPathTree != null) && (newPathTree != null)) {
        PathsUpdate update = new PathsUpdate(seqNum, false);
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
      } else if (oldPathTree != null) {
        PathsUpdate update = new PathsUpdate(seqNum, false);
        update.newPathChange(oldAuthzObj).addToDelPaths(splitPath(oldPathTree));
        sentryStore.deleteAuthzPathsMapping(oldAuthzObj,
            Collections.singleton(oldPathTree),
            update);
      } else if (newPathTree != null) {
        PathsUpdate update = new PathsUpdate(seqNum, false);
        update.newPathChange(newAuthzObj).addToAddPaths(splitPath(newPathTree));
        sentryStore.addAuthzPathsMapping(newAuthzObj,
            Collections.singleton(newPathTree),
            update);
      }
    } else if (!oldLocation.equals(newLocation)) {
      // Only Location has changed, e.g. Alter table set location
      if ((oldPathTree != null) && (newPathTree != null)) {
        PathsUpdate update = new PathsUpdate(seqNum, false);
        update.newPathChange(oldAuthzObj).addToDelPaths(splitPath(oldPathTree));
        update.newPathChange(oldAuthzObj).addToAddPaths(splitPath(newPathTree));
        sentryStore.updateAuthzPathsMapping(oldAuthzObj, oldPathTree,
            newPathTree, update);
      } else if (oldPathTree != null) {
        PathsUpdate update = new PathsUpdate(seqNum, false);
        update.newPathChange(oldAuthzObj).addToDelPaths(splitPath(oldPathTree));
        sentryStore.deleteAuthzPathsMapping(oldAuthzObj,
              Collections.singleton(oldPathTree),
              update);
      } else if (newPathTree != null) {
        PathsUpdate update = new PathsUpdate(seqNum, false);
        update.newPathChange(oldAuthzObj).addToAddPaths(splitPath(newPathTree));
        sentryStore.addAuthzPathsMapping(oldAuthzObj,
              Collections.singleton(newPathTree),
              update);
      }
    } else {
      LOGGER.error("Update Notification for Auhorizable object {}, with no change, skipping",
        oldAuthzObj);
      throw new SentryInvalidHMSEventException("Update Notification for Authorizable object" +
        "with no change");
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

  /**
   * Split path into components on the "/" character.
   * The path should not start with "/".
   * This is consumed by Thrift interface, so the return result should be
   * {@code List<String>}
   * @param path input oath e.g. {@code foo/bar}
   * @return list of commponents, e.g. [foo, bar]
   */
  private List<String> splitPath(String path) {
    return (Lists.newArrayList(path.split("/")));
  }
}
