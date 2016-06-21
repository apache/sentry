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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.db.SentryMetastoreListenerPlugin;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ConfUtilties;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryMetastorePostEventListener extends MetaStoreEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryMetastoreListenerPlugin.class);
  private final HiveAuthzConf authzConf;
  private final Server server;

  private List<SentryMetastoreListenerPlugin> sentryPlugins = new ArrayList<SentryMetastoreListenerPlugin>();

  public SentryMetastorePostEventListener(Configuration config) {
    super(config);

    authzConf = HiveAuthzConf.getAuthzConf((HiveConf)config);
    server = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
    Iterable<String> pluginClasses = ConfUtilties.CLASS_SPLITTER
        .split(config.get(ServerConfig.SENTRY_METASTORE_PLUGINS,
            ServerConfig.SENTRY_METASTORE_PLUGINS_DEFAULT).trim());

    try {
      for (String pluginClassStr : pluginClasses) {
        Class<?> clazz = config.getClassByName(pluginClassStr);
        if (!SentryMetastoreListenerPlugin.class.isAssignableFrom(clazz)) {
          throw new IllegalArgumentException("Class ["
              + pluginClassStr + "] is not a "
              + SentryMetastoreListenerPlugin.class.getName());
        }
        SentryMetastoreListenerPlugin plugin = (SentryMetastoreListenerPlugin) clazz
            .getConstructor(Configuration.class, Configuration.class)
            .newInstance(config, authzConf);
        sentryPlugins.add(plugin);
      }
    } catch (Exception e) {
      LOGGER.error("Could not initialize Plugin !!", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {

    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip sync paths/privileges with Sentry server for onCreateTable event," +
        " since the operation failed. \n");
      return;
    }

    if (tableEvent.getTable().getSd().getLocation() != null) {
      String authzObj = tableEvent.getTable().getDbName() + "."
          + tableEvent.getTable().getTableName();
      String path = tableEvent.getTable().getSd().getLocation();
      for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
        plugin.addPath(authzObj, path);
      }
    }

    // drop the privileges on the given table, in case if anything was left
    // behind during the drop
    if (!syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE)) {
      return;
    }

    dropSentryTablePrivilege(tableEvent.getTable().getDbName(),
        tableEvent.getTable().getTableName());
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {

    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip syncing paths/privileges with Sentry server for onDropTable event," +
        " since the operation failed. \n");
      return;
    }

    if (tableEvent.getTable().getSd().getLocation() != null) {
      String authzObj = tableEvent.getTable().getDbName() + "."
          + tableEvent.getTable().getTableName();
      for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
        plugin.removeAllPaths(authzObj, null);
      }
    }
    // drop the privileges on the given table
    if (!syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE)) {
      return;
    }

    if (!tableEvent.getStatus()) {
      return;
    }

    dropSentryTablePrivilege(tableEvent.getTable().getDbName(),
        tableEvent.getTable().getTableName());
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent)
      throws MetaException {

    // don't sync paths/privileges if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.debug("Skip syncing paths/privileges with Sentry server for onCreateDatabase event," +
        " since the operation failed. \n");
      return;
    }

    if (dbEvent.getDatabase().getLocationUri() != null) {
      String authzObj = dbEvent.getDatabase().getName();
      String path = dbEvent.getDatabase().getLocationUri();
      for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
        plugin.addPath(authzObj, path);
      }
    }
    // drop the privileges on the database, in case anything left behind during
    // last drop db
    if (!syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE)) {
      return;
    }

    dropSentryDbPrivileges(dbEvent.getDatabase().getName());
  }

  /**
   * Drop the privileges on the database. Note that child tables will be
   * dropped individually by client, so we just need to handle the removing
   * the db privileges. The table drop should cleanup the table privileges.
   */
  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {

    // don't sync paths/privileges if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.debug("Skip syncing paths/privileges with Sentry server for onDropDatabase event," +
        " since the operation failed. \n");
      return;
    }

    String authzObj = dbEvent.getDatabase().getName();
    for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
      List<String> tNames = dbEvent.getHandler().get_all_tables(authzObj);
      plugin.removeAllPaths(authzObj, tNames);
    }
    if (!syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE)) {
      return;
    }

    dropSentryDbPrivileges(dbEvent.getDatabase().getName());
  }

  /**
   * Adjust the privileges when table is renamed
   */
  @Override
  public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
    String oldTableName = null, newTableName = null;

    // don't sync privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip syncing privileges with Sentry server for onAlterTable event," +
        " since the operation failed. \n");
      return;
    }

    if (tableEvent.getOldTable() != null) {
      oldTableName = tableEvent.getOldTable().getTableName();
    }

    if (tableEvent.getNewTable() != null) {
      newTableName = tableEvent.getNewTable().getTableName();
    }

    renameSentryTablePrivilege(tableEvent.getOldTable().getDbName(),
        oldTableName, tableEvent.getOldTable().getSd().getLocation(),
        tableEvent.getNewTable().getDbName(), newTableName,
        tableEvent.getNewTable().getSd().getLocation());
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent)
      throws MetaException {

    // don't sync privileges if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.debug("Skip syncing privileges with Sentry server for onAlterPartition event," +
        " since the operation failed. \n");
      return;
    }

    String oldLoc = null, newLoc = null;
    if (partitionEvent.getOldPartition() != null) {
      oldLoc = partitionEvent.getOldPartition().getSd().getLocation();
    }
    if (partitionEvent.getNewPartition() != null) {
      newLoc = partitionEvent.getNewPartition().getSd().getLocation();
    }

    if ((oldLoc != null) && (newLoc != null) && (!oldLoc.equals(newLoc))) {
      String authzObj =
          partitionEvent.getOldPartition().getDbName() + "."
              + partitionEvent.getOldPartition().getTableName();
      for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
        plugin.renameAuthzObject(authzObj, oldLoc,
            authzObj, newLoc);
      }
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
      throws MetaException {

    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.debug("Skip syncing path with Sentry server for onAddPartition event," +
        " since the operation failed. \n");
      return;
    }

    for (Partition part : partitionEvent.getPartitions()) {
      if ((part.getSd() != null) && (part.getSd().getLocation() != null)) {
        String authzObj = part.getDbName() + "." + part.getTableName();
        String path = part.getSd().getLocation();
        for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
          plugin.addPath(authzObj, path);
        }
      }
    }
    super.onAddPartition(partitionEvent);
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)
      throws MetaException {

    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.debug("Skip syncing path with Sentry server for onDropPartition event," +
        " since the operation failed. \n");
      return;
    }

    String authzObj = partitionEvent.getTable().getDbName() + "."
        + partitionEvent.getTable().getTableName();
    String path = partitionEvent.getPartition().getSd().getLocation();
    for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
      plugin.removePath(authzObj, path);
    }
    super.onDropPartition(partitionEvent);
  }

  private SentryPolicyServiceClient getSentryServiceClient()
      throws MetaException {
    try {
      return SentryServiceClientFactory.create(authzConf);
    } catch (Exception e) {
      throw new MetaException("Failed to connect to Sentry service "
          + e.getMessage());
    }
  }

  private void dropSentryDbPrivileges(String dbName) throws MetaException {
    List<Authorizable> authorizableTable = new ArrayList<Authorizable>();
    authorizableTable.add(server);
    authorizableTable.add(new Database(dbName));
    try {
      dropSentryPrivileges(authorizableTable);
    } catch (SentryUserException e) {
      throw new MetaException("Failed to remove Sentry policies for drop DB "
          + dbName + " Error: " + e.getMessage());
    } catch (IOException e) {
      throw new MetaException("Failed to find local user " + e.getMessage());
    }

  }

  private void dropSentryTablePrivilege(String dbName, String tabName)
      throws MetaException {
    List<Authorizable> authorizableTable = new ArrayList<Authorizable>();
    authorizableTable.add(server);
    authorizableTable.add(new Database(dbName));
    authorizableTable.add(new Table(tabName));

    try {
      dropSentryPrivileges(authorizableTable);
    } catch (SentryUserException e) {
      throw new MetaException(
          "Failed to remove Sentry policies for drop table " + dbName + "."
              + tabName + " Error: " + e.getMessage());
    } catch (IOException e) {
      throw new MetaException("Failed to find local user " + e.getMessage());
    }

  }
  private void dropSentryPrivileges(
      List<? extends Authorizable> authorizableTable)
      throws SentryUserException, IOException, MetaException {
    String requestorUserName = UserGroupInformation.getCurrentUser()
        .getShortUserName();
    SentryPolicyServiceClient sentryClient = getSentryServiceClient();
    sentryClient.dropPrivileges(requestorUserName, authorizableTable);

    // Close the connection after dropping privileges is done.
    sentryClient.close();
  }

  private void renameSentryTablePrivilege(String oldDbName, String oldTabName,
      String oldPath, String newDbName, String newTabName, String newPath)
      throws MetaException {
    List<Authorizable> oldAuthorizableTable = new ArrayList<Authorizable>();
    oldAuthorizableTable.add(server);
    oldAuthorizableTable.add(new Database(oldDbName));
    oldAuthorizableTable.add(new Table(oldTabName));

    List<Authorizable> newAuthorizableTable = new ArrayList<Authorizable>();
    newAuthorizableTable.add(server);
    newAuthorizableTable.add(new Database(newDbName));
    newAuthorizableTable.add(new Table(newTabName));

    if (!oldTabName.equalsIgnoreCase(newTabName)
        && syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_ALTER_WITH_POLICY_STORE)) {

      SentryPolicyServiceClient sentryClient = getSentryServiceClient();

      try {
        String requestorUserName = UserGroupInformation.getCurrentUser()
            .getShortUserName();
        sentryClient.renamePrivileges(requestorUserName, oldAuthorizableTable, newAuthorizableTable);
      } catch (SentryUserException e) {
        throw new MetaException(
            "Failed to remove Sentry policies for rename table " + oldDbName
            + "." + oldTabName + "to " + newDbName + "." + newTabName
            + " Error: " + e.getMessage());
      } catch (IOException e) {
        throw new MetaException("Failed to find local user " + e.getMessage());
      } finally {

        // Close the connection after renaming privileges is done.
        sentryClient.close();
      }
    }
    // The HDFS plugin needs to know if it's a path change (set location)
    for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
      if(!oldPath.equals(newPath)) {
        plugin.renameAuthzObject(oldDbName + "." + oldTabName, oldPath,
                newDbName + "." + newTabName, newPath);
      }
    }
  }

  private boolean syncWithPolicyStore(AuthzConfVars syncConfVar) {
    return "true"
        .equalsIgnoreCase((authzConf.get(syncConfVar.getVar(), "true")));
  }

}
