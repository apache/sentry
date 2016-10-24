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
package org.apache.sentry.service.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.exception.*;
import org.apache.sentry.hdfs.UpdateableAuthzPaths;
import org.apache.sentry.hdfs.FullUpdateInitializer;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.sentry.binding.metastore.messaging.json.*;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE;

/*
HMSFollower is the thread which follows the Hive MetaStore state changes from Sentry.
It gets the full update and notification logs from HMS and applies it to
update permissions stored in Sentry using SentryStore and also update the <obj,path> state
stored for HDFS- Sentry sync.
 */
@SuppressWarnings("PMD")
public class HMSFollower implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HMSFollower.class);

  private long currentEventID;
  private static boolean connectedToHMS = false;
  private HiveMetaStoreClient client;
  private SentryKerberosContext kerberosContext;
  private Configuration authzConf;
  private boolean kerberos;
  private SentryStore sentryStore;
  private String hiveInstance;
  final static int maxRetriesForLogin = 3;
  final static int maxRetriesForConnection = 3;

  private volatile UpdateableAuthzPaths authzPaths;
  private boolean needHiveSnapshot = true;

  HMSFollower(Configuration conf) throws SentryNoSuchObjectException,
      SentryAccessDeniedException, SentrySiteConfigurationException, IOException { //TODO: Handle any possible exceptions or throw specific exceptions
    LOGGER.info("HMSFollower is being initialized");
    authzConf = conf;
    sentryStore = new SentryStore(authzConf);
    //TODO: Initialize currentEventID from Sentry db
    currentEventID = 0;
  }

  @VisibleForTesting
  HMSFollower(Configuration conf, SentryStore sentryStore, String hiveInstance) {
    this.authzConf = conf;
    this.sentryStore = sentryStore;
    this.hiveInstance = hiveInstance;
  }

  @VisibleForTesting
  public static boolean isConnectedToHMS() {
    return connectedToHMS;
  }

  /*
  Returns HMS Client if successful, returns null if HMS is not ready yet to take connections
  Throws @LoginException if Kerberos context creation failed using Sentry's kerberos credentials
  Throws @MetaException if there was a problem on creating an HMSClient
   */
  private HiveMetaStoreClient getMetaStoreClient(Configuration conf)
      throws LoginException, MetaException {
    if(client != null) {
      return client;
    }
    // Seems like HMS client creation although seems successful,
    // it actually connects to an invalid HMS instance.
    // So it seems like it is necessary to wait until we make sure metastore config is properly loaded.
    boolean loadedHiveConf = HiveConf.isLoadMetastoreConfig();
    if(!loadedHiveConf) {
      return null;
    }
    final HiveConf hiveConf = new HiveConf();
    hiveInstance = hiveConf.get(HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME.getVar());

    String principal, keytab;

    //TODO: Is this the right(standard) way to create a HMS client? HiveMetastoreClientFactoryImpl?
    //TODO: Check if HMS is using kerberos instead of relying on Sentry conf
    //TODO: Handle TGT renewals
    kerberos = ServiceConstants.ServerConfig.SECURITY_MODE_KERBEROS.equalsIgnoreCase(
        conf.get(ServiceConstants.ServerConfig.SECURITY_MODE, ServiceConstants.ServerConfig.SECURITY_MODE_KERBEROS).trim());
    if (kerberos) {
      LOGGER.info("Making a kerberos connection to HMS");
      //TODO: Is this needed? Use Hadoop libraries to translate the _HOST placeholder with actual hostname
      //Validate principal
      principal = Preconditions.checkNotNull(ServiceConstants.ServerConfig.PRINCIPAL,
          ServiceConstants.ServerConfig.PRINCIPAL + " is required");
      LOGGER.info("Using kerberos principal: " + principal);
      final String[] principalParts = SaslRpcServer.splitKerberosName(principal);
      Preconditions.checkArgument(principalParts.length == 3,
          "Kerberos principal should have 3 parts: " + principal);

      keytab = Preconditions.checkNotNull(conf.get(ServiceConstants.ServerConfig.KEY_TAB),
          ServiceConstants.ServerConfig.KEY_TAB + " is required");
      File keytabFile = new File(keytab);
      Preconditions.checkState(keytabFile.isFile() && keytabFile.canRead(),
          "Keytab " + keytab + " does not exist or is not readable.");
      boolean establishedKerberosContext = false;
      int attempt = 1;
      while(establishedKerberosContext) {
        try {
          kerberosContext = new SentryKerberosContext(principal, keytab, true);
          establishedKerberosContext = true;
          LOGGER.info("Established kerberos context, will now connect to HMS");
        } catch (LoginException e) {
          //Kerberos login failed
          if( attempt > maxRetriesForLogin ) {
            throw e;
          }
          attempt++;
        }
      }
      boolean establishedConnection = false;
      attempt = 1;
      while(establishedConnection) {
        try {
          client = Subject.doAs(kerberosContext.getSubject(), new PrivilegedExceptionAction<HiveMetaStoreClient>() {
            @Override
            public HiveMetaStoreClient run() throws Exception {
              return new HiveMetaStoreClient(hiveConf);
            }
          });
          LOGGER.info("Secure connection established with HMS");
        } catch (PrivilegedActionException e) {
          if( attempt > maxRetriesForConnection ) {
            //We should just retry as it is possible that HMS is not ready yet to receive requests
            //TODO: How do we differentiate between kerberos problem versus HMS not being up?
            LOGGER.error("Cannot connect to HMS", e);
          }
          attempt++;
        }
      }
    } else {
      //This is only for testing purposes. Sentry strongly recommends strong authentication
      client = new HiveMetaStoreClient(hiveConf);
      LOGGER.info("Non secure connection established with HMS");
    }
    return client;
  }

  public void run() {
    if (client == null) {
      try {
        client = getMetaStoreClient(authzConf);
        if (client == null) {
          //TODO: Do we want to throw an exception after a certain timeout?
          return;
        } else {
          connectedToHMS = true;
          LOGGER.info("HMSFollower of Sentry successfully connected to HMS");
        }
      } catch (Exception e) {
        LOGGER.error("HMSFollower cannot connect to HMS!!");
        return;
      }
    }

    try {
      if (isNeedHiveSnapshot()) {
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

        CurrentNotificationEventId eventIDBefore = null;
        CurrentNotificationEventId eventIDAfter = null;

        try {
          eventIDBefore = client.getCurrentNotificationEventId();
          LOGGER.info(String.format("Before fetching hive full snapshot, Current NotificationID = %s.",
              eventIDBefore));
          fetchFullUpdate();
          eventIDAfter = client.getCurrentNotificationEventId();
          LOGGER.info(String.format("After fetching hive full snapshot, Current NotificationID = %s.",
              eventIDAfter));
        } catch (Exception ex) {
          LOGGER.error("#### Encountered failure during fetching one hive full snapshot !! Current NotificationID = " +
              eventIDAfter.toString(), ex);
          return;
        }

        if (!eventIDBefore.equals(eventIDAfter)) {
          LOGGER.error("#### Fail to get a point-in-time hive full snapshot !! Current NotificationID = " +
              eventIDAfter.toString());
          return;
        }

        LOGGER.info(String.format("Successfully fetched hive full snapshot, Current NotificationID = %s.",
            eventIDAfter));
        needHiveSnapshot = false;
        currentEventID = eventIDAfter.getEventId();
      }

      NotificationEventResponse response = client.getNextNotification(currentEventID, Integer.MAX_VALUE, null);
      if (response.isSetEvents()) {
        LOGGER.info(String.format("CurrentEventID = %s. Processing %s events",
            currentEventID, response.getEvents().size()));
        processNotificationEvents(response.getEvents());
      }
    } catch (TException e) {
      LOGGER.error("ThriftException occured fetching Notification entries, will try");
      e.printStackTrace();
    } catch (SentryInvalidInputException|SentryInvalidHMSEventException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve HMS full snapshot.
   */
  private void fetchFullUpdate() throws Exception {
    FullUpdateInitializer updateInitializer = null;

    try {
      updateInitializer = new FullUpdateInitializer(client, authzConf);
      HMSFollower.this.authzPaths = updateInitializer.createInitialUpdate();
      // TODO: notify HDFS plugin
      LOGGER.info("#### Hive full update initialization complete !!");
    } finally {
      if (updateInitializer != null) {
        try {
          updateInitializer.close();
        } catch (Exception e) {
          LOGGER.info("#### Exception while closing updateInitializer !!", e);
        }
      }
    }
  }

  private boolean isNeedHiveSnapshot() {
    // An indicator that in request of a full hive update.

    // TODO: Will need to get Hive snapshot if the Notification ID
    // we are requesting has been rolled over in the NotificationLog
    // table of Hive
    return needHiveSnapshot;
  }

  private boolean syncWithPolicyStore(HiveAuthzConf.AuthzConfVars syncConfVar) {
    return "true"
        .equalsIgnoreCase((authzConf.get(syncConfVar.getVar(), "true")));
  }

  /*
  Throws SentryInvalidHMSEventException if Notification event contains insufficient information
   */

  void processNotificationEvents(List<NotificationEvent> events) throws
      SentryInvalidHMSEventException, SentryInvalidInputException {
    SentryJSONMessageDeserializer deserializer = new SentryJSONMessageDeserializer();

    for (NotificationEvent event : events) {
      String dbName, tableName, oldLocation, newLocation, location;
      switch (HCatEventMessage.EventType.valueOf(event.getEventType())) {
        case CREATE_DATABASE:
          SentryJSONCreateDatabaseMessage message = deserializer.getCreateDatabaseMessage(event.getMessage());
          dbName = message.getDB();

          location = message.getLocation();
          if (dbName == null || location == null) {
            throw new SentryInvalidHMSEventException(String.format("Create database event has incomplete information. " +
                "dbName = %s location = %s", dbName, location));
          }
          if (syncWithPolicyStore(AUTHZ_SYNC_CREATE_WITH_POLICY_STORE)) {
            try {
              dropSentryDbPrivileges(dbName);
            } catch (SentryNoSuchObjectException e) {
                LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the database: %s", dbName);
            } catch (SentryInvalidInputException e) {
              throw new SentryInvalidInputException("Could not process Create database event. Event: " + event.toString(), e);
            }
          }
          //TODO: HDFSPlugin.addPath(dbName, location)
          break;
        case DROP_DATABASE:
          SentryJSONDropDatabaseMessage dropDatabaseMessage = deserializer.getDropDatabaseMessage(event.getMessage());
          dbName = dropDatabaseMessage.getDB();
          if (dbName == null) {
            throw new SentryInvalidHMSEventException(String.format("Drop database event has incomplete information. " +
                "dbName = %s", dbName));
          }
          if (syncWithPolicyStore(AUTHZ_SYNC_DROP_WITH_POLICY_STORE)) {
            try {
              dropSentryDbPrivileges(dbName);
            } catch (SentryNoSuchObjectException e) {
              LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the database: %s", dbName);
            } catch (SentryInvalidInputException e) {
              throw new SentryInvalidInputException("Could not process Drop database event. Event: " + event.toString(), e);
            }
          }
          //TODO: HDFSPlugin.deletePath(dbName, location)
          break;
        case CREATE_TABLE:
          SentryJSONCreateTableMessage createTableMessage = deserializer.getCreateTableMessage(event.getMessage());
          dbName = createTableMessage.getDB();
          tableName = createTableMessage.getTable();
          location = createTableMessage.getLocation();
          if (dbName == null || tableName == null || location == null) {
            throw new SentryInvalidHMSEventException(String.format("Create table event has incomplete information. " +
                "dbName = %s, tableName = %s, location = %s", dbName, tableName, location));
          }
          if (syncWithPolicyStore(AUTHZ_SYNC_CREATE_WITH_POLICY_STORE)) {
            try {
              dropSentryTablePrivileges(dbName, tableName);
            } catch (SentryNoSuchObjectException e) {
              LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the table: %s.%s", dbName, tableName);
            } catch (SentryInvalidInputException e) {
              throw new SentryInvalidInputException("Could not process Create table event. Event: " + event.toString(), e);
            }
          }
          //TODO: HDFSPlugin.deletePath(dbName, location)
          break;
        case DROP_TABLE:
          SentryJSONDropTableMessage dropTableMessage = deserializer.getDropTableMessage(event.getMessage());
          dbName = dropTableMessage.getDB();
          tableName = dropTableMessage.getTable();
          if (dbName == null || tableName == null) {
            throw new SentryInvalidHMSEventException(String.format("Drop table event has incomplete information. " +
                "dbName = %s, tableName = %s", dbName, tableName));
          }
          if (syncWithPolicyStore(AUTHZ_SYNC_DROP_WITH_POLICY_STORE)) {
            try{
              dropSentryTablePrivileges(dbName, tableName);
            } catch (SentryNoSuchObjectException e) {
              LOGGER.info("Drop Sentry privilege ignored as there are no privileges on the table: %s.%s", dbName, tableName);
            } catch (SentryInvalidInputException e) {
              throw new SentryInvalidInputException("Could not process Drop table event. Event: " + event.toString(), e);
            }
          }
          //TODO: HDFSPlugin.deletePath(dbName, location)
          break;
        case ALTER_TABLE:
          SentryJSONAlterTableMessage alterTableMessage = deserializer.getAlterTableMessage(event.getMessage());

          String oldDbName = alterTableMessage.getDB();
          String oldTableName = alterTableMessage.getTable();
          String newDbName = event.getDbName();
          String newTableName = event.getTableName();
          oldLocation = alterTableMessage.getOldLocation();
          newLocation = alterTableMessage.getLocation();

          if (oldDbName == null || oldTableName == null || newDbName == null || newTableName == null ||
              oldLocation == null || newLocation == null) {
            throw new SentryInvalidHMSEventException(String.format("Alter table event has incomplete information. " +
                "oldDbName = %s, oldTableName = %s, oldLocation = %s, newDbName = %s, newTableName = %s, newLocation = %s",
                oldDbName, oldTableName, oldLocation, newDbName, newTableName, newLocation));
          }

          if(!newDbName.equalsIgnoreCase(oldDbName) || !oldTableName.equalsIgnoreCase(newTableName)) { // Name has changed
            if(!oldLocation.equals(newLocation)) { // Location has changed

              //Name and path has changed
              // - Alter table rename for managed table
              //TODO: Handle HDFS plugin

            } else {
              //Only name has changed
              // - Alter table rename for an external table
              //TODO: Handle HDFS plugin

            }
            try {
              renamePrivileges(oldDbName, oldTableName, newDbName, newTableName);
            } catch (SentryNoSuchObjectException e) {
              LOGGER.info("Rename Sentry privilege ignored as there are no privileges on the table: %s.%s", oldDbName, oldTableName);
            } catch (SentryInvalidInputException e) {
              throw new SentryInvalidInputException("Could not process Alter table event. Event: " + event.toString(), e);
            }
          } else if(!oldLocation.equals(newLocation)) { // Only Location has changed{
            //- Alter table set location
            //TODO: Handle HDFS plugin
          } else {
            LOGGER.info(String.format("Alter table notification ignored as neither name nor location has changed: " +
                "oldDbName = %s, oldTableName = %s, oldLocation = %s, newDbName = %s, newTableName = %s, newLocation = %s",
            oldDbName, oldTableName, oldLocation, newDbName, newTableName, newLocation));
          }
          //TODO: Write test cases for all these cases
          break;
        case ADD_PARTITION:
        case DROP_PARTITION:
        case ALTER_PARTITION:
          //TODO: Handle HDFS plugin
          break;
      }
    currentEventID = event.getEventId();
    }
  }

  private void dropSentryDbPrivileges(String dbName) throws SentryNoSuchObjectException, SentryInvalidInputException {
    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setDb(dbName);
    sentryStore.dropPrivilege(authorizable);
  }
  private void dropSentryTablePrivileges(String dbName, String tableName) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    TSentryAuthorizable authorizable = new TSentryAuthorizable(hiveInstance);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);
    sentryStore.dropPrivilege(authorizable);
  }
  private void renamePrivileges(String oldDbName, String oldTableName, String newDbName, String newTableName) throws
      SentryNoSuchObjectException, SentryInvalidInputException {
    TSentryAuthorizable oldAuthorizable = new TSentryAuthorizable(hiveInstance);
    oldAuthorizable.setDb(oldDbName);
    oldAuthorizable.setTable(oldTableName);
    TSentryAuthorizable newAuthorizable = new TSentryAuthorizable(hiveInstance);
    newAuthorizable.setDb(newDbName);
    newAuthorizable.setTable(newTableName);
    sentryStore.renamePrivilege(oldAuthorizable, newAuthorizable);
  }
}
