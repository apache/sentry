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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Sentry binding for Hive Metastore. The binding is integrated into Metastore
 * via the pre-event listener which are fired prior to executing the metadata
 * action. This point we are only authorizing metadata writes since the listners
 * are not fired from read events. Each action builds a input and output
 * hierarchy as per the objects used in the given operations. This is then
 * passed down to the hive binding which handles the authorization. This ensures
 * that we follow the same privilege model and policies.
 */
public class MetastoreAuthzBinding extends MetaStorePreEventListener {

  /**
   * Build the set of object hierarchies ie fully qualified db model objects
   */
  private static class HierarcyBuilder {
    private List<List<DBModelAuthorizable>> authHierarchy;

    public HierarcyBuilder() {
      authHierarchy = new ArrayList<List<DBModelAuthorizable>>();
    }

    public HierarcyBuilder addServerToOutput(Server server) {
      List<DBModelAuthorizable> serverHierarchy = new ArrayList<DBModelAuthorizable>();
      serverHierarchy.add(server);
      authHierarchy.add(serverHierarchy);
      return this;
    }

    public HierarcyBuilder addDbToOutput(Server server, String dbName) {
      List<DBModelAuthorizable> dbHierarchy = new ArrayList<DBModelAuthorizable>();
      dbHierarchy.add(server);
      dbHierarchy.add(new Database(dbName));
      authHierarchy.add(dbHierarchy);
      return this;
    }

    public HierarcyBuilder addUriToOutput(Server server, String uriPath) {
      List<DBModelAuthorizable> uriHierarchy = new ArrayList<DBModelAuthorizable>();
      uriHierarchy.add(server);
      uriHierarchy.add(new AccessURI(uriPath));
      authHierarchy.add(uriHierarchy);
      return this;
    }

    public HierarcyBuilder addTableToOutput(Server server, String dbName,
        String tableName) {
      List<DBModelAuthorizable> tableHierarchy = new ArrayList<DBModelAuthorizable>();
      tableHierarchy.add(server);
      tableHierarchy.add(new Database(dbName));
      tableHierarchy.add(new Table(tableName));
      authHierarchy.add(tableHierarchy);
      return this;
    }

    public List<List<DBModelAuthorizable>> build() {
      return authHierarchy;
    }
  }

  private HiveAuthzConf authzConf;
  private final Server authServer;
  private final HiveConf hiveConf;
  private final ImmutableSet<String> serviceUsers;
  private HiveAuthzBinding hiveAuthzBinding;

  public MetastoreAuthzBinding(Configuration config) throws Exception {
    super(config);
    String hiveAuthzConf = config.get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
    if (hiveAuthzConf == null
        || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
      throw new IllegalArgumentException("Configuration key "
          + HiveAuthzConf.HIVE_SENTRY_CONF_URL + " value '" + hiveAuthzConf
          + "' is invalid.");
    }
    try {
      authzConf = new HiveAuthzConf(new URL(hiveAuthzConf));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Configuration key "
          + HiveAuthzConf.HIVE_SENTRY_CONF_URL + " specifies a malformed URL '"
          + hiveAuthzConf + "'", e);
    }
    hiveConf = new HiveConf(config, this.getClass());
    this.authServer = new Server(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME
        .getVar()));
    serviceUsers = ImmutableSet.copyOf(toTrimedLower(Sets.newHashSet(authzConf
        .getStrings(AuthzConfVars.AUTHZ_METASTORE_SERVICE_USERS.getVar(),
            new String[] { "" }))));
  }

  /**
   * Main listener callback which is the entry point for Sentry
   */
  @Override
  public void onEvent(PreEventContext context) throws MetaException,
      NoSuchObjectException, InvalidOperationException {

    switch (context.getEventType()) {
    case CREATE_TABLE:
      authorizeCreateTable((PreCreateTableEvent) context);
      break;
    case DROP_TABLE:
      authorizeDropTable((PreDropTableEvent) context);
      break;
    case ALTER_TABLE:
      authorizeAlterTable((PreAlterTableEvent) context);
      break;
    case ADD_PARTITION:
      authorizeAddPartition((PreAddPartitionEvent) context);
      break;
    case DROP_PARTITION:
      authorizeDropPartition((PreDropPartitionEvent) context);
      break;
    case ALTER_PARTITION:
      authorizeAlterPartition((PreAlterPartitionEvent) context);
      break;
    case CREATE_DATABASE:
      authorizeCreateDatabase((PreCreateDatabaseEvent) context);
      break;
    case DROP_DATABASE:
      authorizeDropDatabase((PreDropDatabaseEvent) context);
      break;
    case LOAD_PARTITION_DONE:
      // noop for now
      break;
    default:
      break;
    }
  }

  private void authorizeCreateDatabase(PreCreateDatabaseEvent context)
      throws InvalidOperationException, MetaException {
    authorizeMetastoreAccess(HiveOperation.CREATEDATABASE,
        new HierarcyBuilder().build(),
        new HierarcyBuilder().addServerToOutput(getAuthServer()).build());
  }

  private void authorizeDropDatabase(PreDropDatabaseEvent context)
      throws InvalidOperationException, MetaException {
    authorizeMetastoreAccess(HiveOperation.DROPDATABASE,
        new HierarcyBuilder().build(),
        new HierarcyBuilder().addServerToOutput(getAuthServer()).build());
  }

  private void authorizeCreateTable(PreCreateTableEvent context)
      throws InvalidOperationException, MetaException {
    HierarcyBuilder inputBuilder = new HierarcyBuilder();
    if (!StringUtils.isEmpty(context.getTable().getSd().getLocation())) {
      inputBuilder.addUriToOutput(getAuthServer(), context.getTable().getSd()
          .getLocation());
    }
    authorizeMetastoreAccess(HiveOperation.CREATETABLE, inputBuilder.build(),
        new HierarcyBuilder().addDbToOutput(
            getAuthServer(), context.getTable().getDbName()).build());
  }

  private void authorizeDropTable(PreDropTableEvent context)
      throws InvalidOperationException, MetaException {
    authorizeMetastoreAccess(
        HiveOperation.DROPTABLE,
        new HierarcyBuilder().build(),
        new HierarcyBuilder().addDbToOutput(getAuthServer(),
            context.getTable().getDbName()).build());
  }

  private void authorizeAlterTable(PreAlterTableEvent context)
      throws InvalidOperationException, MetaException {

    HierarcyBuilder inputBuilder = new HierarcyBuilder();
    // if the operation requires location change, then add URI privilege check
    if (context.getOldTable().getSd().getLocation()
        .compareTo(context.getNewTable().getSd().getLocation()) != 0) {
      inputBuilder.addUriToOutput(getAuthServer(), context.getNewTable()
          .getSd().getLocation());
    }
    authorizeMetastoreAccess(
        HiveOperation.ALTERTABLE_ADDCOLS, inputBuilder.build(),
        new HierarcyBuilder().addDbToOutput(getAuthServer(),
            context.getOldTable().getDbName()).build());
  }

  private void authorizeAddPartition(PreAddPartitionEvent context)
      throws InvalidOperationException, MetaException, NoSuchObjectException {
    HierarcyBuilder inputBuilder = new HierarcyBuilder();

    // check if we need to validate URI permissions when storage location is
    // non-default, ie something not under the parent table
    String partitionLocation = context.getPartition().getSd().getLocation();
    if (!StringUtils.isEmpty(partitionLocation)) {
      String tableLocation = context
          .getHandler()
          .get_table(context.getPartition().getDbName(),
              context.getPartition().getTableName()).getSd().getLocation();
      if (!partitionLocation.startsWith(tableLocation)) {
        inputBuilder.addUriToOutput(getAuthServer(), context.getPartition()
          .getSd().getLocation());
      }
    }
    authorizeMetastoreAccess(HiveOperation.ALTERTABLE_ADDPARTS,
        inputBuilder.build(),
        new HierarcyBuilder().addDbToOutput(getAuthServer(),
            context.getPartition().getDbName()).build());
  }

  private void authorizeDropPartition(PreDropPartitionEvent context)
      throws InvalidOperationException, MetaException {
    authorizeMetastoreAccess(
        HiveOperation.ALTERTABLE_DROPPARTS,
        new HierarcyBuilder().build(),
        new HierarcyBuilder().addDbToOutput(getAuthServer(),
            context.getPartition().getDbName()).build());
  }

  private void authorizeAlterPartition(PreAlterPartitionEvent context)
      throws InvalidOperationException, MetaException {
    authorizeMetastoreAccess(
        HiveOperation.ALTERPARTITION_LOCATION,
        new HierarcyBuilder().build(),
        new HierarcyBuilder()
            .addDbToOutput(getAuthServer(),
            context.getNewPartition().getDbName()).build());
  }

  private InvalidOperationException invalidOperationException(Exception e) {
    InvalidOperationException ex = new InvalidOperationException(e.getMessage());
    ex.initCause(e.getCause());
    return ex;
  }

  /**
   * Assemble the required privileges and requested privileges. Validate using
   * Hive bind auth provider
   * @param hiveOp
   * @param inputHierarchy
   * @param outputHierarchy
   * @throws InvalidOperationException
   */
  private void authorizeMetastoreAccess(HiveOperation hiveOp,
      List<List<DBModelAuthorizable>> inputHierarchy,
      List<List<DBModelAuthorizable>> outputHierarchy)
      throws InvalidOperationException {
    try {
      HiveAuthzBinding hiveAuthzBinding = getHiveAuthzBinding();
      String userName = ShimLoader.getHadoopShims().getUGIForConf(hiveConf)
          .getShortUserName();
      if (needsAuthorization(userName)) {
        hiveAuthzBinding.authorize(hiveOp, HiveAuthzPrivilegesMap
            .getHiveAuthzPrivileges(hiveOp), new Subject(userName),
        inputHierarchy, outputHierarchy);
      }
    } catch (AuthorizationException e1) {
      throw invalidOperationException(e1);
    } catch (LoginException e1) {
      throw invalidOperationException(e1);
    } catch (IOException e1) {
      throw invalidOperationException(e1);
    } catch (Exception e) {
      throw invalidOperationException(e);
    }

  }

  public Server getAuthServer() {
    return authServer;
  }

  private boolean needsAuthorization(String userName) {
    return !serviceUsers.contains(userName);
  }

  private static Set<String> toTrimedLower(Set<String> s) {
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }

  private HiveAuthzBinding getHiveAuthzBinding() throws Exception {
    if (hiveAuthzBinding == null) {
      hiveAuthzBinding = new HiveAuthzBinding(hiveConf, authzConf);
    }
    return hiveAuthzBinding;
  }

}
