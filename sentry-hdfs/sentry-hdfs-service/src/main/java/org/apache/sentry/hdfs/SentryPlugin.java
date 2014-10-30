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

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.hdfs.UpdateForwarder.ExternalImageRetriever;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin.SentryPluginException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TDropPrivilegesRequest;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TRenamePrivilegesRequest;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

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
  private PermImageRetriever permImageRetriever;

  long getLastSeenHMSPathSeqNum() {
    return pathsUpdater.getLastSeen();
  }

  @Override
  public void initialize(Configuration conf, SentryStore sentryStore) throws SentryPluginException {
    final String[] pathPrefixes = conf
        .getStrings(ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES,
            ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES_DEFAULT);
    final int initUpdateRetryDelayMs =
        conf.getInt(ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS,
            ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT);
    pathsUpdater = new UpdateForwarder<PathsUpdate>(new UpdateableAuthzPaths(
        pathPrefixes), null, 100, initUpdateRetryDelayMs);
    permImageRetriever = new PermImageRetriever(sentryStore);
    permsUpdater = new UpdateForwarder<PermissionsUpdate>(
        new UpdateablePermissions(permImageRetriever), permImageRetriever,
        100, initUpdateRetryDelayMs);
    LOGGER.info("Sentry HDFS plugin initialized !!");
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
    LOGGER.debug("Recieved Authz Path update [" + update.getSeqNum() + "]..");
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
    LOGGER.debug("Authz Perm preUpdate [" + update.getSeqNum() + ", " + request.getRoleName() + "]..");
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
    LOGGER.debug("Authz Perm preUpdate [" + update.getSeqNum() + ", " + request.getRoleName() + "]..");
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
      LOGGER.debug("Authz Perm preUpdate [" + update.getSeqNum() + "]..");
    }
  }

  @Override
  public void onRenameSentryPrivilege(TRenamePrivilegesRequest request)
      throws SentryPluginException {
    String oldAuthz = getAuthzObj(request.getOldAuthorizable());
    String newAuthz = getAuthzObj(request.getNewAuthorizable());
    PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
    TPrivilegeChanges privUpdate = update.addPrivilegeUpdate(PermissionsUpdate.RENAME_PRIVS);
    privUpdate.putToAddPrivileges(newAuthz, newAuthz);
    privUpdate.putToDelPrivileges(oldAuthz, oldAuthz);
    permsUpdater.handleUpdateNotification(update);
    LOGGER.debug("Authz Perm preUpdate [" + update.getSeqNum() + ", " + newAuthz + ", " + oldAuthz + "]..");
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
      LOGGER.debug("Authz Perm preUpdate [" + update.getSeqNum() + ", " + authzObj + "]..");
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
    LOGGER.debug("Authz Perm preUpdate [" + update.getSeqNum() + ", " + request.getRoleName() + "]..");
  }

  @Override
  public void onDropSentryPrivilege(TDropPrivilegesRequest request)
      throws SentryPluginException {
    PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
    String authzObj = getAuthzObj(request.getAuthorizable());
    update.addPrivilegeUpdate(authzObj).putToDelPrivileges(
        PermissionsUpdate.ALL_ROLES, PermissionsUpdate.ALL_ROLES);
    permsUpdater.handleUpdateNotification(update);
    LOGGER.debug("Authz Perm preUpdate [" + update.getSeqNum() + ", " + authzObj + "]..");
  }

  private String getAuthzObj(TSentryPrivilege privilege) {
    String authzObj = null;
    if (!SentryStore.isNULL(privilege.getDbName())) {
      String dbName = privilege.getDbName();
      String tblName = privilege.getTableName();
      if (SentryStore.isNULL(tblName)) {
        authzObj = dbName;
      } else {
        authzObj = dbName + "." + tblName;
      }
    }
    return authzObj;
  }

  private String getAuthzObj(TSentryAuthorizable authzble) {
    String authzObj = null;
    if (!SentryStore.isNULL(authzble.getDb())) {
      String dbName = authzble.getDb();
      String tblName = authzble.getTable();
      if (SentryStore.isNULL(tblName)) {
        authzObj = dbName;
      } else {
        authzObj = dbName + "." + tblName;
      }
    }
    return authzObj;
  }
}
