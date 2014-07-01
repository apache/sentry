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
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.HiveAuthzBindingHook;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;

public class SentryMetastorePostEventListener extends MetaStoreEventListener {
  private final SentryServiceClientFactory sentryClientFactory;
  private final HiveAuthzConf authzConf;
  private final Server server;

  public SentryMetastorePostEventListener(Configuration config) {
    super(config);
    sentryClientFactory = new SentryServiceClientFactory();

    authzConf = HiveAuthzConf.getAuthzConf(new HiveConf());
    server = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
  }

  @Override
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
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
    // drop the privileges on the given table
    if (!syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE)) {
      return;
    }
    dropSentryTablePrivilege(tableEvent.getTable().getDbName(),
        tableEvent.getTable().getTableName());
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent)
      throws MetaException {
    // drop the privileges on the database, incase anything left behind during
    // last drop db
    if (!syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE)) {
      return;
    }
    dropSentryDbPrivileges(dbEvent.getDatabase().getName());
  }

  /**
   * Drop the privileges on the database // note that child tables will be
   * dropped individually by client, so we // just need to handle the removing
   * the db privileges. The table drop // should cleanup the table privileges
   */
  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
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
    if (!syncWithPolicyStore(AuthzConfVars.AUTHZ_SYNC_ALTER_WITH_POLICY_STORE)) {
      return;
    }
    if (tableEvent.getOldTable() != null) {
      oldTableName = tableEvent.getOldTable().getTableName();
    }
    if (tableEvent.getNewTable() != null) {
      newTableName = tableEvent.getNewTable().getTableName();
    }
    if (!oldTableName.equalsIgnoreCase(newTableName)) {
      renameSentryTablePrivilege(tableEvent.getOldTable().getDbName(),
          oldTableName, tableEvent.getNewTable().getDbName(), newTableName);
    }
  }

  private SentryPolicyServiceClient getSentryServiceClient()
      throws MetaException {
    try {
      return sentryClientFactory.create(authzConf);
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
  }

  private void renameSentryTablePrivilege(String oldDbName, String oldTabName,
      String newDbName, String newTabName)
      throws MetaException {
    List<Authorizable> oldAuthorizableTable = new ArrayList<Authorizable>();
    oldAuthorizableTable.add(server);
    oldAuthorizableTable.add(new Database(oldDbName));
    oldAuthorizableTable.add(new Table(oldTabName));

    List<Authorizable> newAuthorizableTable = new ArrayList<Authorizable>();
    newAuthorizableTable.add(server);
    newAuthorizableTable.add(new Database(newDbName));
    newAuthorizableTable.add(new Table(newTabName));

    try {
      String requestorUserName = UserGroupInformation.getCurrentUser()
          .getShortUserName();
      SentryPolicyServiceClient sentryClient = getSentryServiceClient();
      sentryClient.renamePrivileges(requestorUserName, oldAuthorizableTable, newAuthorizableTable);
    } catch (SentryUserException e) {
      throw new MetaException(
          "Failed to remove Sentry policies for rename table " + oldDbName
              + "." + oldTabName + "to " + newDbName + "." + newTabName
              + " Error: " + e.getMessage());
    } catch (IOException e) {
      throw new MetaException("Failed to find local user " + e.getMessage());
    }
  }

  private boolean syncWithPolicyStore(AuthzConfVars syncConfVar) {
    return "true"
        .equalsIgnoreCase((authzConf.get(syncConfVar.getVar(), "true")));
  }
}
