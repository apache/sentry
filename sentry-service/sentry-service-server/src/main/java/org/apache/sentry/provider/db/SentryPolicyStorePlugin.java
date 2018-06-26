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

package org.apache.sentry.provider.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.api.service.thrift.TDropPrivilegesRequest;
import org.apache.sentry.api.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.api.service.thrift.TRenamePrivilegesRequest;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;

import java.util.Map;
import java.util.Set;

import static org.apache.sentry.hdfs.Updateable.Update;

/**
 * Interface for processing delta changes of Sentry permission and generate corresponding
 * update. The updates will be persisted into Sentry store afterwards along with the actual
 * operation.
 *
 * TODO: SENTRY-1588: add user level privilege change support. e.g. onAlterSentryRoleDeleteUsers,
 * TODO: onAlterSentryRoleDeleteUsers.
 */
public interface SentryPolicyStorePlugin {

  @SuppressWarnings("serial")
  class SentryPluginException extends SentryUserException {
    public SentryPluginException(String msg) {
      super(msg);
    }
    public SentryPluginException(String msg, Throwable t) {
      super(msg, t);
    }
  }

  void initialize(Configuration conf, SentryStoreInterface sentryStore) throws SentryPluginException;

  Update onAlterSentryRoleAddGroups(TAlterSentryRoleAddGroupsRequest tRequest) throws SentryPluginException;

  Update onAlterSentryRoleDeleteGroups(TAlterSentryRoleDeleteGroupsRequest tRequest) throws SentryPluginException;

  /**
   * Used to create an update when privileges are granted to owner who is a Role
   * @param roleName
   * @param privileges
   * @param privilegesUpdateMap
   * @throws SentryPluginException
   */
  void onAlterSentryRoleGrantPrivilege(String roleName, Set<TSentryPrivilege> privileges,
       Map<TSentryPrivilege, Update> privilegesUpdateMap) throws SentryPluginException;

  /**
   * Used to create an update when privileges are revoked from owner who is a role
   * @param roleName
   * @param privileges
   * @param privilegesUpdateMap
   * @throws SentryPluginException
   */
  void onAlterSentryRoleRevokePrivilege(String roleName, Set<TSentryPrivilege> privileges,
        Map<TSentryPrivilege, Update> privilegesUpdateMap) throws SentryPluginException;

  /**
   * Used to create an update when privileges are granted to user.
   * @param userName
   * @param privileges
   * @param privilegesUpdateMap
   * @throws SentryPluginException
   */
  void onAlterSentryUserGrantPrivilege(String userName, Set<TSentryPrivilege> privileges,
        Map<TSentryPrivilege, Update> privilegesUpdateMap) throws SentryPluginException;

  /**
   * Used to create an update when privileges are revoked from user.
   * @param userName
   * @param privileges
   * @param privilegesUpdateMap
   * @throws SentryPluginException
   */
  void onAlterSentryUserRevokePrivilege(String userName, Set<TSentryPrivilege> privileges,
        Map<TSentryPrivilege, Update> privilegesUpdateMap) throws SentryPluginException;

  Update onDropSentryRole(TDropSentryRoleRequest tRequest) throws SentryPluginException;

  Update onRenameSentryPrivilege(TRenamePrivilegesRequest request)
      throws SentryPluginException, SentryInvalidInputException;

  Update onDropSentryPrivilege(TDropPrivilegesRequest request) throws SentryPluginException;
}
