/**
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
package org.apache.sentry.binding.metastore;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory;
import org.apache.sentry.provider.db.SentryMetastoreListenerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.builder.ToStringBuilder;
/*
A HMS listener class which should ideally go into the transaction which persists the Hive metadata.
This class writes all DDL events to the NotificationLog through rawstore.addNotificationEvent(event)
This class is very similar to DbNotificationListener, except:
1. It uses a custom SentryJSONMessageFactory which adds additional information to the message part of the event
 to avoid another round trip from the clients
2. It handles the cases where actual operation has failed, and hence skips writing to the notification log.
3. Has additional validations to make sure event has the required information.

This can be replaced with DbNotificationListener in future and sentry's message factory can be plugged in if:
- HIVE-14011 is fixed: Make MessageFactory truly pluggable
- 2 and 3 above are handled in DbNotificationListener
*/

public class SentryMetastorePostEventListenerNotificationLog extends MetaStoreEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryMetastoreListenerPlugin.class);
  private RawStore rs;
  private HiveConf hiveConf;
  SentryJSONMessageFactory messageFactory;

  private static SentryMetastorePostEventListenerNotificationLog.CleanerThread cleaner = null;

  //Same as DbNotificationListener to make the transition back easy
  private synchronized void init(HiveConf conf) {
    try {
      this.rs = RawStoreProxy.getProxy(conf, conf, conf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL), 999999);
    } catch (MetaException var3) {
      LOGGER.error("Unable to connect to raw store, notifications will not be tracked", var3);
      this.rs = null;
    }

    if(cleaner == null && this.rs != null) {
      cleaner = new SentryMetastorePostEventListenerNotificationLog.CleanerThread(conf, this.rs);
      cleaner.start();
    }
  }

  public SentryMetastorePostEventListenerNotificationLog(Configuration config) {
    super(config);
    // The code in MetastoreUtils.getMetaStoreListeners() that calls this looks for a constructor
    // with a Configuration parameter, so we have to declare config as Configuration.  But it
    // actually passes a HiveConf, which we need.  So we'll do this ugly down cast.
    if (!(config instanceof HiveConf)) {
      String error = "Could not initialize Plugin - Configuration is not an instanceof HiveConf";
      LOGGER.error(error);
      throw new RuntimeException(error);
    }
    hiveConf = (HiveConf)config;
    messageFactory = new SentryJSONMessageFactory();
    init(hiveConf);
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent)
          throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Create database event failed");
      return;
    }

    String location = dbEvent.getDatabase().getLocationUri();
    if (Strings.isNullOrEmpty(location)) {
      throw new SentryMalformedEventException("CreateDatabaseEvent has invalid location", dbEvent);
    }
    String dbName = dbEvent.getDatabase().getName();
    if (Strings.isNullOrEmpty(dbName)) {
      throw new SentryMalformedEventException("CreateDatabaseEvent has invalid dbName", dbEvent);
    }

    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_CREATE_DATABASE_EVENT,
            messageFactory.buildCreateDatabaseMessage(dbEvent.getDatabase()).toString());
    event.setDbName(dbName);
    this.enqueue(event);
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Drop database event failed");
      return;
    }

    String dbName = dbEvent.getDatabase().getName();
    if (dbName == null || dbName.isEmpty()) {
      throw new SentryMalformedEventException("DropDatabaseEvent has invalid dbName", dbEvent);
    }

    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_DROP_DATABASE_EVENT,
            messageFactory.buildDropDatabaseMessage(dbEvent.getDatabase()).toString());
    event.setDbName(dbName);
    this.enqueue(event);
  }

  @Override
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Create table event failed");
      return;
    }

    String dbName = tableEvent.getTable().getDbName();
    if (dbName == null || dbName.isEmpty()) {
      throw new SentryMalformedEventException("CreateTableEvent has invalid dbName", tableEvent);
    }
    String tableName = tableEvent.getTable().getTableName();
    if (tableName == null || tableName.isEmpty()) {
      throw new SentryMalformedEventException("CreateTableEvent has invalid tableName", tableEvent);
    }
    // Create table event should also contain a location.
    // But, Create view also generates a Create table event, but it does not have a location.
    // Create view is identified by the tableType. But turns out tableType is not set in some cases.
    // We assume that tableType is set for all create views.
    //TODO: Location can be null/empty, handle that in HMSFollower
    String tableType = tableEvent.getTable().getTableType();
    if(!(tableType != null && tableType.equals(TableType.VIRTUAL_VIEW.name()))) {
        if (tableType == null) {
        LOGGER.warn("TableType is null, assuming it is not TableType.VIRTUAL_VIEW: tableEvent", tableEvent);
      }
      String location = tableEvent.getTable().getSd().getLocation();
      if (location == null || location.isEmpty()) {
        throw new SentryMalformedEventException("CreateTableEvent has invalid location", tableEvent);
      }
    }
    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_CREATE_TABLE_EVENT,
            messageFactory.buildCreateTableMessage(tableEvent.getTable()).toString());
    event.setDbName(dbName);
    event.setTableName(tableName);
    this.enqueue(event);
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Drop table event failed");
      return;
    }

    String dbName = tableEvent.getTable().getDbName();
    if (dbName == null || dbName.isEmpty()) {
      throw new SentryMalformedEventException("DropTableEvent has invalid dbName", tableEvent);
    }
    String tableName = tableEvent.getTable().getTableName();
    if (tableName == null || tableName.isEmpty()) {
      throw new SentryMalformedEventException("DropTableEvent has invalid tableName", tableEvent);
    }

    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_DROP_TABLE_EVENT,
            messageFactory.buildDropTableMessage(tableEvent.getTable()).toString());
    event.setDbName(dbName);
    event.setTableName(tableName);
    this.enqueue(event);
  }

  @Override
  public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Alter table event failed");
      return;
    }

    String dbName = tableEvent.getNewTable().getDbName();
    if (dbName == null || dbName.isEmpty()) {
      throw new SentryMalformedEventException("AlterTableEvent's newTable has invalid dbName", tableEvent);
    }
    String tableName = tableEvent.getNewTable().getTableName();
    if (tableName == null || tableName.isEmpty()) {
      throw new SentryMalformedEventException("AlterTableEvent's newTable has invalid tableName", tableEvent);
    }
    dbName = tableEvent.getOldTable().getDbName();
    if (dbName == null || dbName.isEmpty()) {
      throw new SentryMalformedEventException("AlterTableEvent's oldTable has invalid dbName", tableEvent);
    }
    tableName = tableEvent.getOldTable().getTableName();
    if (tableName == null || tableName.isEmpty()) {
      throw new SentryMalformedEventException("AlterTableEvent's oldTable has invalid tableName", tableEvent);
    }
    //Alter view also generates an alter table event, but it does not have a location
    //TODO: Handle this case in Sentry
    if(!tableEvent.getOldTable().getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
      String location = tableEvent.getNewTable().getSd().getLocation();
      if (location == null || location.isEmpty()) {
        throw new SentryMalformedEventException("AlterTableEvent's newTable has invalid location", tableEvent);
      }
      location = tableEvent.getOldTable().getSd().getLocation();
      if (location == null || location.isEmpty()) {
        throw new SentryMalformedEventException("AlterTableEvent's oldTable has invalid location", tableEvent);
      }
    }

    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_ALTER_TABLE_EVENT,
            messageFactory.buildAlterTableMessage(tableEvent.getOldTable(), tableEvent.getNewTable()).toString());
    event.setDbName(tableEvent.getNewTable().getDbName());
    event.setTableName(tableEvent.getNewTable().getTableName());
    this.enqueue(event);
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent)
          throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Alter partition event failed");
      return;
    }

    String dbName = partitionEvent.getNewPartition().getDbName();
    if (dbName == null || dbName.isEmpty()) {
      throw new SentryMalformedEventException("AlterPartitionEvent's newPartition has invalid dbName", partitionEvent);
    }
    String tableName = partitionEvent.getNewPartition().getTableName();
    if (tableName == null || tableName.isEmpty()) {
      throw new SentryMalformedEventException("AlterPartitionEvent's newPartition has invalid tableName", partitionEvent);
    }

    //TODO: Need more validations, but it is tricky as there are many variations and validations change for each one
    // Alter partition Location
    // Alter partition property
    // Any more?

    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_ALTER_PARTITION_EVENT,
            messageFactory.buildAlterPartitionMessage(partitionEvent.getTable(), partitionEvent.getOldPartition(),
            partitionEvent.getNewPartition()).toString());

    event.setDbName(partitionEvent.getNewPartition().getDbName());
    event.setTableName(partitionEvent.getNewPartition().getTableName());
    this.enqueue(event);
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
          throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Add partition event failed");
      return;
    }

    String dbName = partitionEvent.getTable().getDbName();
    if (dbName == null || dbName.isEmpty()) {
      throw new SentryMalformedEventException("AddPartitionEvent has invalid dbName", partitionEvent);
    }
    String tableName = partitionEvent.getTable().getTableName();
    if (tableName == null || tableName.isEmpty()) {
      throw new SentryMalformedEventException("AddPartitionEvent's newPartition has invalid tableName", partitionEvent);
    }

    //TODO: Need more validations?

    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_ADD_PARTITION_EVENT,
            messageFactory.buildAddPartitionMessage(partitionEvent.getTable(), partitionEvent.getPartitions()).toString());

    event.setDbName(partitionEvent.getTable().getDbName());
    event.setTableName(partitionEvent.getTable().getTableName());
    this.enqueue(event);
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)
          throws MetaException {

    // do not write to Notification log if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.info("Skipping writing to NotificationLog as the Drop partition event failed");
      return;
    }

    NotificationEvent event = new NotificationEvent(0L, now(), HCatConstants.HCAT_DROP_PARTITION_EVENT,
            messageFactory.buildDropPartitionMessage(partitionEvent.getTable(), partitionEvent.getPartition()).toString());
    //TODO: Why is this asymmetric with add partitions(s)?
    // Seems like adding multiple partitions generate a single event
    // where as single partition drop generated an event?

    event.setDbName(partitionEvent.getTable().getDbName());
    event.setTableName(partitionEvent.getTable().getTableName());
    this.enqueue(event);
  }

  private int now() {
    long millis = System.currentTimeMillis();
    millis /= 1000;
    if (millis > Integer.MAX_VALUE) {
      LOGGER.warn("We've passed max int value in seconds since the epoch, " +
          "all notification times will be the same!");
      return Integer.MAX_VALUE;
    }
    return (int)millis;
  }

  //Same as DbNotificationListener to make the transition back easy
  private void enqueue(NotificationEvent event) {
    if(this.rs != null) {
      this.rs.addNotificationEvent(event);
    } else {
      LOGGER.warn("Dropping event " + event + " since notification is not running.");
    }
  }

  //Same as DbNotificationListener to make the transition back easy
  private static class CleanerThread extends Thread {
    private RawStore rs;
    private int ttl;

    CleanerThread(HiveConf conf, RawStore rs) {
      super("CleanerThread");
      this.rs = rs;
      this.setTimeToLive(conf.getTimeVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL, TimeUnit.SECONDS));
      this.setDaemon(true);
    }

    public void run() {
      while(true) {
        this.rs.cleanNotificationEvents(this.ttl);

        try {
          Thread.sleep(60000L);
        } catch (InterruptedException var2) {
          LOGGER.info("Cleaner thread sleep interupted", var2);
        }
      }
    }

    public void setTimeToLive(long configTtl) {
      if(configTtl > 2147483647L) {
        this.ttl = 2147483647;
      } else {
        this.ttl = (int)configTtl;
      }

    }
  }
  private class SentryMalformedEventException extends MetaException {
    SentryMalformedEventException(String msg, Object event) {
      //toString is not implemented in Event classes,
      // hence using reflection to print the details of the Event object.
      super(msg + "Event: " + ToStringBuilder.reflectionToString(event));
    }
  }
}