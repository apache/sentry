/*
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

import com.codahale.metrics.Timer.Context;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipal;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.hdfs.service.thrift.sentry_hdfs_serviceConstants;
import org.apache.sentry.provider.db.service.persistent.PermissionsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PermImageRetriever obtains a complete snapshot of Sentry permission from a persistent
 * storage and translates it into {@code PermissionsUpdate} that the consumers, such as
 * HDFS NameNode, can understand.
 * <p>
 * It is a thread safe class, as all the underlying database operation is thread safe.
 */
@ThreadSafe
public class PermImageRetriever implements ImageRetriever<PermissionsUpdate> {

  private final SentryStoreInterface sentryStore;

  PermImageRetriever(SentryStoreInterface sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public PermissionsUpdate retrieveFullImage() throws Exception {
    try(Context timerContext =
        SentryHdfsMetricsUtil.getRetrievePermFullImageTimer.time()) {

      // Read the most up-to-date snapshot of Sentry perm information,
      // with a corresponding delta change sequence number.
      PermissionsImage permImage = sentryStore.retrieveFullPermssionsImage();
      long curSeqNum = permImage.getCurSeqNum();
      Map<String, Map<TPrivilegePrincipal, String>> privilegeImage =
          permImage.getPrivilegeImage();
      Map<String, List<String>> roleImage =
          permImage.getRoleImage();

      // Translates the complete Sentry permission snapshot into a PermissionsUpdate.
      // Adds permission mapping for user/roles <role, groups> mapping
      // to be included in the permission update.
      // And label it with the latest delta change sequence number for consumer
      // to be aware of the next delta change it should continue with.
      TPermissionsUpdate tPermUpdate = new TPermissionsUpdate(true, curSeqNum,
          new HashMap<String, TPrivilegeChanges>(),
          new HashMap<String, TRoleChanges>());

      for (Map.Entry<String, Map<TPrivilegePrincipal, String>> privEnt : privilegeImage.entrySet()) {
        String authzObj = privEnt.getKey();
        Map<TPrivilegePrincipal,String> privMap = privEnt.getValue();
        DBUpdateForwarder.translateOwnerPrivileges(privMap);
        tPermUpdate.putToPrivilegeChanges(authzObj, new TPrivilegeChanges(
        authzObj, privMap, new HashMap<TPrivilegePrincipal, String>()));
      }

      for (Map.Entry<String, List<String>> privEnt : roleImage.entrySet()) {
        String role = privEnt.getKey();
        List<String> groups = privEnt.getValue();
        tPermUpdate.putToRoleChanges(role, new TRoleChanges(role, groups,
            new ArrayList<String>()));
      }

      PermissionsUpdate permissionsUpdate = new PermissionsUpdate(tPermUpdate);
      SentryHdfsMetricsUtil.getPrivilegeChangesHistogram.update(
          tPermUpdate.getPrivilegeChangesSize());
      SentryHdfsMetricsUtil.getRoleChangesHistogram.update(
          tPermUpdate.getRoleChangesSize());
      return permissionsUpdate;
    }
  }

  @Override
  public long getLatestImageID() throws Exception {
    return sentry_hdfs_serviceConstants.UNUSED_PATH_UPDATE_IMG_NUM;
  }
}
