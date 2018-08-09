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
package org.apache.sentry.binding.metastore;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
// import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent; TODO: enable one HIVE-18031 is available
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.sentry.api.service.thrift.TSentryAuthorizable;
import org.apache.sentry.api.service.thrift.TSentryPrincipalType;

import java.util.Map;

/**
 * Sentry HMS Event holds the information from the HMS Event that sentry is interested in
 */
class SentryHmsEvent {
  // eventId is used to synchronize the event. If synchronization is not needed it is initialized to 0.
  private long eventId;
  private final EventType eventType;
  private String ownerName;
  private TSentryPrincipalType ownerType;
  private TSentryAuthorizable authorizable;
  private final Boolean isMetastoreTransactionActive;

  private static final Map<PrincipalType, TSentryPrincipalType> mapOwnerType = ImmutableMap.of(
          PrincipalType.ROLE, TSentryPrincipalType.ROLE,
          PrincipalType.USER, TSentryPrincipalType.USER
  );

  /**
   * Construct SentryHmsEvent from ListenerEvent and event Type
   *
   * event and transaction information is initialized.
   * @param event ListenerEvent
   * @param type EventType
   */
  private SentryHmsEvent(ListenerEvent event, EventType type) {
    String isActive = event.getParameters().getOrDefault(
            MetaStoreEventListenerConstants.HIVE_METASTORE_TRANSACTION_ACTIVE, null);
    isMetastoreTransactionActive = (isActive != null) && Boolean.valueOf(isActive);
    eventId = getEventId(event);
    eventType = type;
  }

  /**
   * Construct SentryHmsEvent from CreateTableEvent
   *
   * event, transaction, owner and authorizable info is initialized from event.
   * @param inServerName name of the server associated with the event
   * @param event CreateTableEvent
   */
  public SentryHmsEvent(String inServerName, CreateTableEvent event) {
    this(event, EventType.CREATE_TABLE);
    setOwnerInfo(event.getTable());
    setAuthorizable(inServerName, event.getTable());
  }

  /**
   * Construct SentryHmsEvent from DropTableEvent
   *
   * event, transaction, owner and authorizable info is initialized from event.
   * @param inServerName name of the server associated with the event
   * @param event DropTableEvent
   */
  public SentryHmsEvent(String inServerName, DropTableEvent event) {
    this(event, EventType.DROP_TABLE);
    setOwnerInfo(event.getTable());
    setAuthorizable(inServerName, event.getTable());
  }

  /**
   * Construct SentryHmsEvent from AlterTableEvent
   *
   * event, transaction, owner and authorizable info is initialized from event.
   * @param inServerName name of the server associated with the event
   * @param event AlterTableEvent
   */
  public SentryHmsEvent(String inServerName, AlterTableEvent event) {
    this(event, EventType.ALTER_TABLE);
    if(!StringUtils.equals(event.getOldTable().getOwner(), event.getNewTable().getOwner())) {
      // Owner Changed. We don't set owner info for other cases of alter table.
      // In this way, sentry server only updates owner privilege when object is created, dropped or
      // owner is updated
      setOwnerInfo(event.getNewTable());
    }
    setAuthorizable(inServerName, event.getNewTable());
  }

  /**
   * Construct SentryHmsEvent from AlterDatabaseEvent
   *
   * event, transaction, owner and authorizable info is initialized from event.
   * @param inServerName name of the server associated with the event
   * @param event AlterDatabaseEvent
   */
  /* TODO: Enable once HIVE-18031 is available
  public SentryHmsEvent(String inServerName, AlterDatabaseEvent event) {
    this(event, EventType.ALTER_DATABASE);

    if (!StringUtils.equals(event.getOldDatabase().getOwnerName(), event.getNewDatabase().getOwnerName())) {
      // Owner Changed. We don't set owner info for other cases of alter database.
      // In this way, sentry server only updates owner privilege when object is created, dropped or
      // owner is updated
      setOwnerInfo(event.getNewDatabase());
    }
    setAuthorizable(inServerName, event.getNewDatabase());
  }
  */

  /**
   * Construct SentryHmsEvent from CreateDatabaseEvent
   *
   * event, transaction, owner and authorizable info is initialized from event.
   * @param inServerName name of the server associated with the event
   * @param event CreateDatabaseEvent
   */
  public SentryHmsEvent(String inServerName, CreateDatabaseEvent event) {
    this(event, EventType.CREATE_DATABASE);
    setOwnerInfo(event.getDatabase());
    setAuthorizable(inServerName, event.getDatabase());
  }

  /**
   * Construct SentryHmsEvent from DropDatabaseEvent
   *
   * event, transaction, owner and authorizable info is initialized from event.
   * @param inServerName name of the server associated with the event
   * @param event DropDatabaseEvent
   */
  public SentryHmsEvent(String inServerName, DropDatabaseEvent event) {
    this(event, EventType.DROP_DATABASE);
    setOwnerInfo(event.getDatabase());
    setAuthorizable(inServerName, event.getDatabase());
  }

  public EventType getEventType() {
    return eventType;
  }

  public long getEventId() {
    return eventId;
  }

  public TSentryPrincipalType getOwnerType() {
    return ownerType;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public TSentryAuthorizable getAuthorizable() {
    return authorizable;
  }

  private void setOwnerInfo(Table table) {
    ownerName = (table != null) ? table.getOwner() : null;
    // Hive 2.3.2 currently support owner type. Assuming user as the type for now.
    // TODO once sentry dependency is changed to a hive version that suppots user type for table this
    // hard coding should be rempved.
    ownerType = TSentryPrincipalType.USER;
  }

  private void setOwnerInfo(Database database) {
    ownerName = (database != null) ? database.getOwnerName() : null;
    ownerType = (database != null) ?
            getTSentryHmsObjectOwnerType(database.getOwnerType()) : null;
  }

  private void setAuthorizable(String serverName, Table table) {
    if (authorizable == null) {
      authorizable = new TSentryAuthorizable(serverName);
    }
    authorizable.setDb((table != null) ? table.getDbName() : null);
    authorizable.setTable((table != null) ? table.getTableName() : null);
  }

  private void setAuthorizable(String serverName, Database database) {
    if (authorizable == null) {
      authorizable = new TSentryAuthorizable(serverName);
    }
    authorizable.setDb((database != null) ? database.getName() : null);
  }

  /**
   * Updates the event_id
   * @param eventId event id
   */
  public void setEventId(long eventId) {
    this.eventId = eventId;
  }

  /**
   * Converts Principle to Owner Type defined by sentry.
   *
   * @param principalType Hive Principle Type
   * @return TSentryPrincipalType if the input is valid else null
   */
  private TSentryPrincipalType getTSentryHmsObjectOwnerType(PrincipalType principalType) {
    return mapOwnerType.get(principalType);
  }

  /**
   * Gets event-id from Event
   *
   * @param event HMS Event
   * @return returns the eventId extracted from Event OR -1 if the eventId is not found the event provided.
   */
  private long getEventId(ListenerEvent event) {
    return Long.parseLong(event.getParameters().getOrDefault(
       MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME, "-1"));
  }

  /**
   * @return True if the HMS is calling this notification in an active transaction; False otherwise
   */
  public Boolean isMetastoreTransactionActive() {
    return isMetastoreTransactionActive;
  }
}
