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

package org.apache.sentry.hdfs;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.UpdateForwarder.ExternalImageRetriever;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SentryPlugin implements SentryPolicyStorePlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryPlugin.class);

  public static volatile SentryPlugin instance;

  static class PermImageRetriever implements ExternalImageRetriever<PermissionsUpdate> {

    private final SentryStore sentryStore;

    public PermImageRetriever(SentryStore sentryStore) {
      this.sentryStore = sentryStore;
    }

    @Override
    public PermissionsUpdate retrieveFullImage(long currSeqNum) {
      Map<String, HashMap<String, String>> privilegeImage = sentryStore.retrieveFullPrivilegeImage();
      Map<String, LinkedList<String>> roleImage = sentryStore.retrieveFullRoleImage();
      
      TPermissionsUpdate tPermUpdate = new TPermissionsUpdate(true, currSeqNum,
          new HashMap<String, TPrivilegeChanges>(),
          new HashMap<String, TRoleChanges>());
      for (Map.Entry<String, HashMap<String, String>> privEnt : privilegeImage.entrySet()) {
        String authzObj = privEnt.getKey();
        HashMap<String,String> privs = privEnt.getValue();
        tPermUpdate.putToPrivilegeChanges(authzObj, new TPrivilegeChanges(
            authzObj, privs, new HashMap<String, String>()));
      }
      for (Map.Entry<String, LinkedList<String>> privEnt : roleImage.entrySet()) {
        String role = privEnt.getKey();
        LinkedList<String> groups = privEnt.getValue();
        tPermUpdate.putToRoleChanges(role, new TRoleChanges(role, groups, new LinkedList<String>()));
      }
      PermissionsUpdate permissionsUpdate = new PermissionsUpdate(tPermUpdate);
      permissionsUpdate.setSeqNum(currSeqNum);
      return permissionsUpdate;
    }
    
  }

  private UpdateForwarder<PathsUpdate> pathsUpdater;
  private UpdateForwarder<PermissionsUpdate> permsUpdater;
  private final AtomicLong permSeqNum = new AtomicLong(5);

  @Override
  public void initialize(Configuration conf, SentryStore sentryStore) throws SentryPluginException {
    HiveConf hiveConf = new HiveConf(conf, Configuration.class);
    final MetastoreClient hmsClient = new ExtendedMetastoreClient(hiveConf);
    final String[] pathPrefixes = conf
        .getStrings(ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES,
            ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES_DEFAULT);
    pathsUpdater = new UpdateForwarder<PathsUpdate>(new UpdateableAuthzPaths(
        pathPrefixes), createHMSImageRetriever(pathPrefixes, hmsClient), 100);
    PermImageRetriever permImageRetriever = new PermImageRetriever(sentryStore);
    permsUpdater = new UpdateForwarder<PermissionsUpdate>(
        new UpdateablePermissions(permImageRetriever), permImageRetriever, 100);
    instance = this;
  }

  public List<PathsUpdate> getAllPathsUpdatesFrom(long pathSeqNum) {
    return pathsUpdater.getAllUpdatesFrom(pathSeqNum);
  }

  public List<PermissionsUpdate> getAllPermsUpdatesFrom(long permSeqNum) {
    return permsUpdater.getAllUpdatesFrom(permSeqNum);
  }

  public void handlePathUpdateNotification(PathsUpdate update) {
    pathsUpdater.handleUpdateNotification(update);
    LOGGER.info("Recieved Authz Path update [" + update.getSeqNum() + "]..");
  }

  private ExternalImageRetriever<PathsUpdate> createHMSImageRetriever(
      final String[] pathPrefixes, final MetastoreClient hmsClient) {
    return new ExternalImageRetriever<PathsUpdate>() {
      @Override
      public PathsUpdate retrieveFullImage(long currSeqNum) {
        PathsUpdate tempUpdate = new PathsUpdate(currSeqNum, false);
        List<Database> allDatabases = hmsClient.getAllDatabases();
        for (Database db : allDatabases) {
          tempUpdate.newPathChange(db.getName()).addToAddPaths(
              PathsUpdate.cleanPath(db.getLocationUri()));
          List<Table> allTables = hmsClient.getAllTablesOfDatabase(db);
          for (Table tbl : allTables) {
            TPathChanges tblPathChange = tempUpdate.newPathChange(tbl
                .getDbName() + "." + tbl.getTableName());
            List<Partition> tblParts = hmsClient.listAllPartitions(db, tbl);
            tblPathChange.addToAddPaths(PathsUpdate.cleanPath(tbl.getSd()
                    .getLocation() == null ? db.getLocationUri() : tbl
                    .getSd().getLocation()));
            for (Partition part : tblParts) {
              tblPathChange.addToAddPaths(PathsUpdate.cleanPath(part.getSd()
                  .getLocation()));
            }
          }
        }
        UpdateableAuthzPaths tmpAuthzPaths = new UpdateableAuthzPaths(
            pathPrefixes);
        tmpAuthzPaths.updatePartial(Lists.newArrayList(tempUpdate),
            new ReentrantReadWriteLock());
        PathsUpdate retUpdate = new PathsUpdate(currSeqNum, true);
        retUpdate.getThriftObject().setPathsDump(
            tmpAuthzPaths.getPathsDump().createPathsDump());
        return retUpdate;
      }
    };
  }

  @Override
  public void onAlterSentryRoleAddGroups(
      TAlterSentryRoleAddGroupsRequest request) throws SentryPluginException {
    PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
    TRoleChanges rUpdate = update.addRoleUpdate(request.getRoleName());
    for (TSentryGroup group : request.getGroups()) {
      rUpdate.addToAddGroups(group.getGroupName());
    }
    permsUpdater.handleUpdateNotification(update);
    LOGGER.info("Authz Perm preUpdate [" + update.getSeqNum() + ", " + request.getRoleName() + "]..");
  }

  @Override
  public void onAlterSentryRoleDeleteGroups(
      TAlterSentryRoleDeleteGroupsRequest request)
      throws SentryPluginException {
    PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
    TRoleChanges rUpdate = update.addRoleUpdate(request.getRoleName());
    for (TSentryGroup group : request.getGroups()) {
      rUpdate.addToDelGroups(group.getGroupName());
    }
    permsUpdater.handleUpdateNotification(update);
    LOGGER.info("Authz Perm preUpdate [" + update.getSeqNum() + ", " + request.getRoleName() + "]..");
  }

  @Override
  public void onAlterSentryRoleGrantPrivilege(
      TAlterSentryRoleGrantPrivilegeRequest request)
      throws SentryPluginException {
    String authzObj = getAuthzObj(request.getPrivilege());
    if (authzObj != null) {
      PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
      update.addPrivilegeUpdate(authzObj).putToAddPrivileges(
          request.getRoleName(), request.getPrivilege().getAction().toUpperCase());
      permsUpdater.handleUpdateNotification(update);
      LOGGER.info("Authz Perm preUpdate [" + update.getSeqNum() + "]..");
    }
  }

  @Override
  public void onAlterSentryRoleRevokePrivilege(
      TAlterSentryRoleRevokePrivilegeRequest request)
      throws SentryPluginException {
    String authzObj = getAuthzObj(request.getPrivilege());
    if (authzObj != null) {
      PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
      update.addPrivilegeUpdate(authzObj).putToDelPrivileges(
          request.getRoleName(), request.getPrivilege().getAction().toUpperCase());
      permsUpdater.handleUpdateNotification(update);
      LOGGER.info("Authz Perm preUpdate [" + update.getSeqNum() + ", " + authzObj + "]..");
    }
  }

  @Override
  public void onDropSentryRole(TDropSentryRoleRequest request)
      throws SentryPluginException {
    PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
    update.addPrivilegeUpdate(PermissionsUpdate.ALL_AUTHZ_OBJ).putToDelPrivileges(
        request.getRoleName(), PermissionsUpdate.ALL_AUTHZ_OBJ);
    update.addRoleUpdate(request.getRoleName()).addToDelGroups(PermissionsUpdate.ALL_GROUPS);
    permsUpdater.handleUpdateNotification(update);
    LOGGER.info("Authz Perm preUpdate [" + update.getSeqNum() + ", " + request.getRoleName() + "]..");
  }

  private String getAuthzObj(TSentryPrivilege privilege) {
    String authzObj = null;
    if (!SentryStore.isNULL(privilege.getDbName())) {
      String dbName = privilege.getDbName();
      String tblName = privilege.getTableName();
      if (tblName == null) {
        authzObj = dbName;
      } else {
        authzObj = dbName + "." + tblName;
      }
    }
    return authzObj;
  }

}
