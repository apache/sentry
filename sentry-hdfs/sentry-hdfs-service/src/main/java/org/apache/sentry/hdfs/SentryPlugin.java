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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.utils.SigUtils;
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

  /**
   * SentryPlugin facilitates HDFS synchronization between HMS and NameNode.
   * <p>
   * Normally, synchronization happens via partial (incremental) updates:
   * <ol>
   * <li>
   * Whenever updates happen on HMS, they are immediately pushed to Sentry.
   * Commonly, it's a single update per remote call.
   * <li>
   * The NameNode periodically asks Sentry for updates. Sentry may return zero
   * or more updates previously received from HMS.
   * </ol>
   * <p>
   * Each individual update is assigned a corresponding sequence number. Those
   * numbers serve to detect the out-of-sync situations between HMS and Sentry and
   * between Sentry and NameNode. Detecting out-of-sync situation triggers full
   * update between the components that are out-of-sync.
   * <p>
   * SentryPlugin also implements signal-triggered mechanism of full path
   * updates from HMS to Sentry and from Sentry to NameNode, to address
   * mission-critical out-of-sync situations that may be encountered in the field.
   * Those out-of-sync situations may not be detectable via the exsiting sequence
   * numbers mechanism (most likely due to the implementation bugs).
   * <p>
   * To facilitate signal-triggered full update from HMS to Sentry and from Sentry
   * to the NameNode, the following 3 boolean variables are defined:
   * fullUpdateHMS, fullUpdateHMSWait, and fullUpdateNN.
   * <ol>
   * <li>
   * The purpose of fullUpdateHMS is to ensure that Sentry asks HMS for full
   * update, and does so only once per signal.
   * <li>
   * The purpose of fullUpdateNN is to ensure that Sentry sends full update
   * to NameNode, and does so only once per signal.
   * <li>
   * The purpose of fullUpdateHMSWait is to ensure that NN update only happens
   * after HMS update.
   * </ol>
   * The details:
   * <ol>
   * <li>
   * Upon receiving a signal, fullUpdateHMS, fullUpdateHMSWait, and fullUpdateNN
   * are all set to true.
   * <li>
   * On the next call to getLastSeenHMSPathSeqNum() from HMS, Sentry checks if
   * fullUpdateHMS == true. If yes, it returns invalid (zero) sequence number
   * to HMS, so HMS would push full update by calling handlePathUpdateNotification()
   * next time. fullUpdateHMS is immediately reset to false, to only trigger one
   * full update request to HMS per signal.
   * <li>
   * When HMS calls handlePathUpdateNotification(), Sentry checks if the update
   * is a full image. If it is, fullUpdateHMSWait is set to false.
   * <li>
   * When NameNode calls getAllPathsUpdatesFrom() asking for partial update,
   * Sentry checks if both fullUpdateNN == true and fullUpdateHMSWait == false.
   * If yes, it sends full update back to NameNode and immediately resets
   * fullUpdateNN to false.
   * </ol>
   */

public class SentryPlugin implements SentryPolicyStorePlugin, SigUtils.SigListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryPlugin.class);

  private final AtomicBoolean fullUpdateHMSWait = new AtomicBoolean(false);
  private final AtomicBoolean fullUpdateHMS = new AtomicBoolean(false);
  private final AtomicBoolean fullUpdateNN = new AtomicBoolean(false);

  public static volatile SentryPlugin instance;

  static class PermImageRetriever implements ExternalImageRetriever<PermissionsUpdate> {

    private final SentryStore sentryStore;

    public PermImageRetriever(SentryStore sentryStore) {
      this.sentryStore = sentryStore;
    }

    @Override
    public PermissionsUpdate retrieveFullImage(long currSeqNum) throws Exception {
      try(Timer.Context timerContext =
                  SentryHdfsMetricsUtil.getRetrieveFullImageTimer.time()) {

        SentryHdfsMetricsUtil.getRetrieveFullImageTimer.time();
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
        SentryHdfsMetricsUtil.getPrivilegeChangesHistogram.update(
                tPermUpdate.getPrivilegeChangesSize());
        SentryHdfsMetricsUtil.getRoleChangesHistogram.update(
                tPermUpdate.getRoleChangesSize());
        return permissionsUpdate;
      }
    }
  }

  private UpdateForwarder<PathsUpdate> pathsUpdater;
  private UpdateForwarder<PermissionsUpdate> permsUpdater;
  private final AtomicLong permSeqNum = new AtomicLong(5);
  private PermImageRetriever permImageRetriever;
  private boolean outOfSync = false;
  /*
   * This number is smaller than starting sequence numbers used by NN and HMS
   * so in both cases its effect is to creat appearence of out-of-sync
   * updates on the Sentry server (as if there were no previous updates at all).
   * It, in turn, triggers a) pushing full update from HMS to Sentry and
   * b) pulling full update from Sentry to NameNode.
   */
  private static final long NO_LAST_SEEN_HMS_PATH_SEQ_NUM = 0L;

  /*
   * Call from HMS to get the last known update sequence #.
   */
  long getLastSeenHMSPathSeqNum() {
    if (!fullUpdateHMS.getAndSet(false)) {
      return pathsUpdater.getLastSeen();
    } else {
      LOGGER.info("SIGNAL HANDLING: asking for full update from HMS");
      return NO_LAST_SEEN_HMS_PATH_SEQ_NUM;
    }
  }

  @Override
  public void initialize(Configuration conf, SentryStore sentryStore) throws SentryPluginException {
    final String[] pathPrefixes = conf
        .getStrings(ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES,
            ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES_DEFAULT);
    final int initUpdateRetryDelayMs =
        conf.getInt(ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS,
            ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT);
    permImageRetriever = new PermImageRetriever(sentryStore);

    pathsUpdater = UpdateForwarder.create(conf, new UpdateableAuthzPaths(
        pathPrefixes), new PathsUpdate(0, false), null, 100, initUpdateRetryDelayMs);
    permsUpdater = UpdateForwarder.create(conf,
        new UpdateablePermissions(permImageRetriever), new PermissionsUpdate(0, false),
        permImageRetriever, 100, initUpdateRetryDelayMs);
    LOGGER.info("Sentry HDFS plugin initialized !!");
    instance = this;

    // register signal handler(s) if any signal(s) are configured
    String[] sigs = conf.getStrings(ServerConfig.SENTRY_SERVICE_FULL_UPDATE_SIGNAL, null);
    if (sigs != null && sigs.length != 0) {
      for (String sig : sigs) {
        try {
          LOGGER.info("SIGNAL HANDLING: Registering Signal Handler For " + sig);
          SigUtils.registerSigListener(sig, this);
        } catch (Exception e) {
          LOGGER.error("SIGNAL HANDLING: Signal Handle Registration Failure", e);
        }
      }
    }
  }

  /**
   * Request for update from NameNode.
   * Full update to NameNode should happen only after full update from HMS.
   */
  public List<PathsUpdate> getAllPathsUpdatesFrom(long pathSeqNum) throws Exception {
    if (!fullUpdateNN.get()) {
      // Most common case - Sentry is NOT handling a full update.
      return pathsUpdater.getAllUpdatesFrom(pathSeqNum);
    } else if (!fullUpdateHMSWait.get()) {
      /*
       * Sentry is in the middle of signal-triggered full update.
       * It already got a full update from HMS
       */
      LOGGER.info("SIGNAL HANDLING: sending full update to NameNode");
      fullUpdateNN.set(false); // don't do full NN update till the next signal
      List<PathsUpdate> updates = pathsUpdater.getAllUpdatesFrom(NO_LAST_SEEN_HMS_PATH_SEQ_NUM);
      /*
       * This code branch is only called when Sentry is in the middle of a full update
       * (fullUpdateNN == true) and Sentry has already received full update from HMS
       * (fullUpdateHMSWait == false). It means that the local cache has a full update
       * from HMS.
       *
       * The full update list is expected to contain the last full update as the first
       * element, followed by zero or more subsequent partial updates.
       *
       * Returning NULL, empty, or partial update instead would be unexplainable, so
       * it should be logged.
       */
      if (updates != null) {
        if (!updates.isEmpty()) {
          if (updates.get(0).hasFullImage()) {
            LOGGER.info("SIGNAL HANDLING: Confirmed full update to NameNode");
          } else {
            LOGGER.warn("SIGNAL HANDLING: Sending partial instead of full update to NameNode (???)");
          }
        } else {
          LOGGER.warn("SIGNAL HANDLING: Sending empty instead of full update to NameNode (???)");
        }
      } else {
        LOGGER.warn("SIGNAL HANDLING: returned NULL instead of full update to NameNode (???)");
      }
      return updates;
    } else {
      // Sentry is handling a full update, but not yet received full update from HMS
      LOGGER.warn("SIGNAL HANDLING: sending partial update to NameNode: still waiting for full update from HMS");
      return pathsUpdater.getAllUpdatesFrom(pathSeqNum);
    }
  }

  public List<PermissionsUpdate> getAllPermsUpdatesFrom(long permSeqNum) throws Exception {
    return permsUpdater.getAllUpdatesFrom(permSeqNum);
  }

  /*
   * Handle partial (most common) or full update from HMS
   */
  public void handlePathUpdateNotification(PathsUpdate update)
      throws SentryPluginException {
    pathsUpdater.handleUpdateNotification(update);
    if (!update.hasFullImage()) { // most common case of partial update
      LOGGER.debug("Recieved Authz Path update [" + update.getSeqNum() + "]..");
    } else { // rare case of full update
      LOGGER.warn("Recieved Authz Path FULL update [" + update.getSeqNum() + "]..");
      // indicate that we're ready to send full update to NameNode
      fullUpdateHMSWait.set(false);
    }
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
    if (request.isSetPrivileges()) {
      String roleName = request.getRoleName();
      for (TSentryPrivilege privilege : request.getPrivileges()) {
        if(!("COLUMN".equalsIgnoreCase(privilege.getPrivilegeScope()))) {
          onAlterSentryRoleGrantPrivilegeCore(roleName, privilege);
        }
      }
    }
  }

  private void onAlterSentryRoleGrantPrivilegeCore(String roleName, TSentryPrivilege privilege)
      throws SentryPluginException {
    String authzObj = getAuthzObj(privilege);
    if (authzObj != null) {
      PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
      update.addPrivilegeUpdate(authzObj).putToAddPrivileges(
          roleName, privilege.getAction().toUpperCase());
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
    if (request.isSetPrivileges()) {
      String roleName = request.getRoleName();
      for (TSentryPrivilege privilege : request.getPrivileges()) {
        if(!("COLUMN".equalsIgnoreCase(privilege.getPrivilegeScope()))) {
          onAlterSentryRoleRevokePrivilegeCore(roleName, privilege);
        }
      }
    }
  }

  public boolean isOutOfSync() {
    return outOfSync;
  }

  public void setOutOfSync(boolean outOfSync) {
    this.outOfSync = outOfSync;
  }

  private void onAlterSentryRoleRevokePrivilegeCore(String roleName, TSentryPrivilege privilege)
      throws SentryPluginException {
    String authzObj = getAuthzObj(privilege);
    if (authzObj != null) {
      PermissionsUpdate update = new PermissionsUpdate(permSeqNum.incrementAndGet(), false);
      update.addPrivilegeUpdate(authzObj).putToDelPrivileges(
          roleName, privilege.getAction().toUpperCase());
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

  @Override
  public void onSignal(final String sigName) {
    LOGGER.info("SIGNAL HANDLING: Received signal " + sigName + ", triggering full update");
    fullUpdateHMS.set(true);
    fullUpdateHMSWait.set(true);
    fullUpdateNN.set(true);
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
    return authzObj == null ? null : authzObj.toLowerCase();
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
    return authzObj == null ? null : authzObj.toLowerCase();
  }
}
