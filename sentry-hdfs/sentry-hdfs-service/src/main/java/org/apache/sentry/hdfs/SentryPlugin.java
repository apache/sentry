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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.utils.PubSub;
import org.apache.sentry.core.common.utils.SigUtils;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.thrift.SentryServiceUtil;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TDropPrivilegesRequest;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TRenamePrivilegesRequest;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.persistent.HMSFollower;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sentry.hdfs.ServiceConstants.IMAGE_NUMBER_UPDATE_UNINITIALIZED;
import static org.apache.sentry.hdfs.ServiceConstants.SEQUENCE_NUMBER_UPDATE_UNINITIALIZED;
import static org.apache.sentry.hdfs.ServiceConstants.ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES;
import static org.apache.sentry.hdfs.ServiceConstants.ServerConfig.SENTRY_HDFS_INTEGRATION_PATH_PREFIXES_DEFAULT;
import static org.apache.sentry.hdfs.Updateable.Update;
import static org.apache.sentry.hdfs.service.thrift.sentry_hdfs_serviceConstants.UNUSED_PATH_UPDATE_IMG_NUM;

/**
   * SentryPlugin listens to all sentry permission update events, persists permission
   * changes into database. It also facilitates HDFS synchronization between HMS and NameNode.
   * <p>
   * Synchronization happens via a complete snapshot or partial (incremental) updates.
   * Normally, it is the latter:
   * <ol>
   * <li>
   * Whenever updates happen on HMS, a corresponding notification log is generated,
   * and {@link HMSFollower} will process the notification event and persist it in database.
   * <li>
   * The NameNode periodically asks Sentry for updates. Sentry may return zero
   * or more updates previously received via HMS notification log.
   * </ol>
   * <p>
   * Each individual update is assigned a corresponding sequence number and an image number
   * to synchronize updates between Sentry and NameNode.
   * <p>
   * The image number may be used to identify whether new full updates are persisted and need
   * to be retrieved instead of delta updates.
   * <p>
   * SentryPlugin implements mechanism of triggering full path updates from Sentry to NameNode,
   * to address mission-critical out-of-sync situations that may be encountered in the field.
   * Those out-of-sync situations may not be detectable via the exsiting sequence
   * numbers mechanism (most likely due to the implementation bugs).
   * <p>
   * To trigger full update from Sentry to NameNode, the boolean variable 'fullUpdateNN' is
   * used to ensure that Sentry sends full update to NameNode, and does so only once per
   * triggering event.
   * </ol>
   * The details:
   * <ol>
   * <li>
   * There are two mechanisms to trigger full update: by signal (deprecated) and via WebUI.
   * Both mechanisms are configurable and turned OFF by default.
   * <li>
   * To use signal mechanism, SentryPlugin uses SigUtils to subscribe to specific
   * (configurable) signal that should be delivered to JVM running Sentry server.
   * <li>
   * To use the WebUI mechanism, SentryPlugin uses PubSub which provides publish-subscribe
   * framework. SentryPlugin subscribed to PubSub.Topic.HDFS_SYNC_NN topic.
   * 
   * Upon receiving a signal,  or upon been notified via pub-sub mechanism, fullUpdateNN
   * is set to true.
   * <li>
   * When NameNode calls getAllPathsUpdatesFrom() asking for partial update,
   * Sentry checks if both fullUpdateNN == true. If yes, it sends full update back
   * to NameNode and immediately resets fullUpdateNN to false.
   * </ol>
   */

public class SentryPlugin implements SentryPolicyStorePlugin, SigUtils.SigListener, PubSub.Subscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryPlugin.class);
  private static final String FULL_UPDATE_TRIGGER = "FULL UPDATE TRIGGER: ";

  private final AtomicBoolean fullUpdateNN = new AtomicBoolean(false);
  public static volatile SentryPlugin instance;

  private DBUpdateForwarder<PathsUpdate> pathsUpdater;
  private DBUpdateForwarder<PermissionsUpdate> permsUpdater;

  @Override
  public void initialize(Configuration conf, SentryStore sentryStore) throws SentryPluginException {
    // List of paths managed by Sentry
    String[] prefixes =
            conf.getStrings(SENTRY_HDFS_INTEGRATION_PATH_PREFIXES,
                    SENTRY_HDFS_INTEGRATION_PATH_PREFIXES_DEFAULT);
    PermImageRetriever permImageRetriever = new PermImageRetriever(sentryStore);
    PathImageRetriever pathImageRetriever = new PathImageRetriever(sentryStore, prefixes);
    PermDeltaRetriever permDeltaRetriever = new PermDeltaRetriever(sentryStore);
    PathDeltaRetriever pathDeltaRetriever = new PathDeltaRetriever(sentryStore);
    pathsUpdater = new DBUpdateForwarder<>(pathImageRetriever, pathDeltaRetriever);
    permsUpdater = new DBUpdateForwarder<>(permImageRetriever, permDeltaRetriever);

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

    // subscribe to full update notification
    if (conf.getBoolean(ServerConfig.SENTRY_SERVICE_FULL_UPDATE_PUBSUB, false)) {
      LOGGER.info(FULL_UPDATE_TRIGGER + "subscribing to topic " + PubSub.Topic.HDFS_SYNC_NN.getName());
      PubSub.getInstance().subscribe(PubSub.Topic.HDFS_SYNC_NN, this);
    }
  }

  /**
   * Request for update from NameNode.
   * Full update to NameNode should happen only after full update from HMS.
   */
  public List<PathsUpdate> getAllPathsUpdatesFrom(long pathSeqNum, long pathImgNum) throws Exception {
    if (!fullUpdateNN.get()) {
      // Most common case - Sentry is NOT handling a full update.
      LOGGER.debug("Received request for PATH update from NameNode for pathSeqNum {} and pathImgNum {}", pathSeqNum, pathImgNum);
      return pathsUpdater.getAllUpdatesFrom(pathSeqNum, pathImgNum);
    }

    /*
     * Sentry is in the middle of signal-triggered full update.
     * It already got a full update from HMS
     */
    LOGGER.info(FULL_UPDATE_TRIGGER + "sending full PATH update to NameNode");
    fullUpdateNN.set(false); // don't do full NN update till the next signal
    List<PathsUpdate> updates =
        pathsUpdater.getAllUpdatesFrom(SEQUENCE_NUMBER_UPDATE_UNINITIALIZED, IMAGE_NUMBER_UPDATE_UNINITIALIZED);
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
          LOGGER.info(FULL_UPDATE_TRIGGER + "Confirmed full PATH update to NameNode for pathSeqNum {} and pathImgNum {}", pathSeqNum, pathImgNum);
        } else {
          LOGGER.warn(FULL_UPDATE_TRIGGER + "Sending partial instead of full PATH update to NameNode  for pathSeqNum {} and pathImgNum {} (???)", pathSeqNum, pathImgNum);
        }
      } else {
        LOGGER.warn(FULL_UPDATE_TRIGGER + "Sending empty instead of full PATH update to NameNode  for pathSeqNum {} and pathImgNum {} (???)", pathSeqNum, pathImgNum);
      }
    } else {
      LOGGER.warn(FULL_UPDATE_TRIGGER + "returned NULL instead of full PATH update to NameNode  for pathSeqNum {} and pathImgNum {} (???)", pathSeqNum, pathImgNum);
    }
    return updates;
  }

  public List<PermissionsUpdate> getAllPermsUpdatesFrom(long permSeqNum) throws Exception {
    LOGGER.debug("Received request for PERM update from NameNode for permSeqNum {}", permSeqNum);
    return permsUpdater.getAllUpdatesFrom(permSeqNum, UNUSED_PATH_UPDATE_IMG_NUM);
  }

  @Override
  public Update onAlterSentryRoleAddGroups(
      TAlterSentryRoleAddGroupsRequest request) throws SentryPluginException {
    Preconditions.checkNotNull(request, "request");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onAlterSentryRoleAddGroups: {}", request); // request.toString() provides all details
    }
    PermissionsUpdate update = new PermissionsUpdate();
    TRoleChanges rUpdate = update.addRoleUpdate(request.getRoleName());
    for (TSentryGroup group : request.getGroups()) {
      rUpdate.addToAddGroups(group.getGroupName());
    }

    LOGGER.debug(String.format("onAlterSentryRoleAddGroups, Authz Perm preUpdate[ %s ]",
                  request.getRoleName()));
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onAlterSentryRoleAddGroups: {}", update); // update.toString() provides all details
    }
    return update;
  }

  @Override
  public Update onAlterSentryRoleDeleteGroups(
      TAlterSentryRoleDeleteGroupsRequest request)
          throws SentryPluginException {
    Preconditions.checkNotNull(request, "request");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onAlterSentryRoleDeleteGroups: {}", request); // request.toString() provides all details
    }
    PermissionsUpdate update = new PermissionsUpdate();
    TRoleChanges rUpdate = update.addRoleUpdate(request.getRoleName());
    for (TSentryGroup group : request.getGroups()) {
      rUpdate.addToDelGroups(group.getGroupName());
    }

    LOGGER.debug(String.format("onAlterSentryRoleDeleteGroups, Authz Perm preUpdate [ %s ]",
                  request.getRoleName()));
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onAlterSentryRoleDeleteGroups: {}", update); // update.toString() provides all details
    }
    return update;
  }

  @Override
  public void onAlterSentryRoleGrantPrivilege(TAlterSentryRoleGrantPrivilegeRequest request,
          Map<TSentryPrivilege, Update> privilegesUpdateMap) throws SentryPluginException {
    Preconditions.checkNotNull(request, "request");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onAlterSentryRoleGrantPrivilege: {}", request); // request.toString() provides all details
    }

    if (request.isSetPrivileges()) {
      String roleName = request.getRoleName();

      for (TSentryPrivilege privilege : request.getPrivileges()) {
        if(!("COLUMN".equalsIgnoreCase(privilege.getPrivilegeScope()))) {
          PermissionsUpdate update = onAlterSentryRoleGrantPrivilegeCore(roleName, privilege);
          if (update != null && privilegesUpdateMap != null) {
            privilegesUpdateMap.put(privilege, update);
          }
        }
      }
    }
    if (LOGGER.isTraceEnabled()) {
      // TSentryPrivilege.toString() and update.toString() provides all details
      LOGGER.trace("onAlterSentryRoleGrantPrivilege: {}", privilegesUpdateMap);
    }
  }

  private PermissionsUpdate onAlterSentryRoleGrantPrivilegeCore(String roleName, TSentryPrivilege privilege)
      throws SentryPluginException {
    String authzObj = getAuthzObj(privilege);
    if (authzObj == null) {
      return null;
    }

    PermissionsUpdate update = new PermissionsUpdate();
    update.addPrivilegeUpdate(authzObj).putToAddPrivileges(
        roleName, privilege.getAction().toUpperCase());

    LOGGER.debug(String.format("onAlterSentryRoleGrantPrivilegeCore, Authz Perm preUpdate [ %s ]",
                  authzObj));
    return update;
  }

  @Override
  public Update onRenameSentryPrivilege(TRenamePrivilegesRequest request)
      throws SentryPluginException, SentryInvalidInputException{
    Preconditions.checkNotNull(request, "request");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onRenameSentryPrivilege: {}", request); // request.toString() provides all details
    }
    String oldAuthz = null;
    String newAuthz = null;
    try {
      oldAuthz = SentryServiceUtil.getAuthzObj(request.getOldAuthorizable());
      newAuthz = SentryServiceUtil.getAuthzObj(request.getNewAuthorizable());
    } catch (SentryInvalidInputException failure) {
      LOGGER.error("onRenameSentryPrivilege, Could not rename sentry privilege ", failure);
      throw failure;
    }
    PermissionsUpdate update = new PermissionsUpdate();
    TPrivilegeChanges privUpdate = update.addPrivilegeUpdate(PermissionsUpdate.RENAME_PRIVS);
    privUpdate.putToAddPrivileges(newAuthz, newAuthz);
    privUpdate.putToDelPrivileges(oldAuthz, oldAuthz);

    LOGGER.debug("onRenameSentryPrivilege, Authz Perm preUpdate [ {} ]", oldAuthz);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onRenameSentryPrivilege: {}", update); // update.toString() provides all details
    }
    return update;
  }

  @Override
  public void onAlterSentryRoleRevokePrivilege(TAlterSentryRoleRevokePrivilegeRequest request,
      Map<TSentryPrivilege, Update> privilegesUpdateMap)
          throws SentryPluginException {
    Preconditions.checkNotNull(request, "request");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onAlterSentryRoleRevokePrivilege: {}", request); // request.toString() provides all details
    }

    if (request.isSetPrivileges()) {
      String roleName = request.getRoleName();

      for (TSentryPrivilege privilege : request.getPrivileges()) {
        if(!("COLUMN".equalsIgnoreCase(privilege.getPrivilegeScope()))) {
          PermissionsUpdate update = onAlterSentryRoleRevokePrivilegeCore(roleName, privilege);
          if (update != null && privilegesUpdateMap != null) {
            privilegesUpdateMap.put(privilege, update);
          }
        }
      }
    }
    if (LOGGER.isTraceEnabled()) {
      // TSentryPrivilege.toString() and Update.toString() provides all details
      LOGGER.trace("onAlterSentryRoleRevokePrivilege: {}", privilegesUpdateMap);
    }
  }

  private PermissionsUpdate onAlterSentryRoleRevokePrivilegeCore(String roleName, TSentryPrivilege privilege)
      throws SentryPluginException {
    String authzObj = getAuthzObj(privilege);
    if (authzObj == null) {
      return null;
    }

    PermissionsUpdate update = new PermissionsUpdate();
    update.addPrivilegeUpdate(authzObj).putToDelPrivileges(
        roleName, privilege.getAction().toUpperCase());

    LOGGER.debug("onAlterSentryRoleRevokePrivilegeCore, Authz Perm preUpdate [ {} ]", authzObj);
    return update;
  }

  @Override
  public Update onDropSentryRole(TDropSentryRoleRequest request)
      throws SentryPluginException {
    Preconditions.checkNotNull(request, "request");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onDropSentryRole: {}", request); // request.toString() provides all details
    }
    PermissionsUpdate update = new PermissionsUpdate();
    update.addPrivilegeUpdate(PermissionsUpdate.ALL_AUTHZ_OBJ).putToDelPrivileges(
        request.getRoleName(), PermissionsUpdate.ALL_AUTHZ_OBJ);
    update.addRoleUpdate(request.getRoleName()).addToDelGroups(PermissionsUpdate.ALL_GROUPS);

    LOGGER.debug("onDropSentryRole, Authz Perm preUpdate [ {} ]", request.getRoleName());
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onDropSentryRole: {}", update); // update.toString() provides all details
    }
    return update;
  }

  @Override
  public Update onDropSentryPrivilege(TDropPrivilegesRequest request)
      throws SentryPluginException {
    Preconditions.checkNotNull(request, "request");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onDropSentryPrivilege: {}", request); // request.toString() provides all details
    }

    PermissionsUpdate update = new PermissionsUpdate();
    String authzObj = null;
    try {
       authzObj = SentryServiceUtil.getAuthzObj(request.getAuthorizable());
    } catch (SentryInvalidInputException failure) {
      LOGGER.error("onDropSentryPrivilege, Could not drop sentry privilege "
        + failure.toString(), failure);
      throw new SentryPluginException(failure.getMessage(), failure);
    }
    update.addPrivilegeUpdate(authzObj).putToDelPrivileges(
        PermissionsUpdate.ALL_ROLES, PermissionsUpdate.ALL_ROLES);

    LOGGER.debug("onDropSentryPrivilege, Authz Perm preUpdate [ {} ]", authzObj);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onDropSentryPrivilege: {}", update); // update.toString() provides all details
    }
    return update;
  }

  /**
   * SigUtils.SigListener callback API
   */
  @Override
  public void onSignal(final String sigName) {
    LOGGER.info("SIGNAL HANDLING: Received signal " + sigName + ", triggering full update");
    fullUpdateNN.set(true);
  }

  /**
   * PubSub.Subscriber callback API
   */
  @Override
  public void onMessage(PubSub.Topic topic, String message) {
    Preconditions.checkArgument(topic == PubSub.Topic.HDFS_SYNC_NN, "Unexpected topic %s instead of %s", topic, PubSub.Topic.HDFS_SYNC_NN);
    LOGGER.info(FULL_UPDATE_TRIGGER + "Received [{}, {}] notification", topic, message);
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
}
