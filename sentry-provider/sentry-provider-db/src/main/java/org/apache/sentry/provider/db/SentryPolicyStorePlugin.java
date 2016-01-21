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
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TDropPrivilegesRequest;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TRenamePrivilegesRequest;

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

  void initialize(Configuration conf, SentryStore sentryStore) throws SentryPluginException;

  void onAlterSentryRoleAddGroups(TAlterSentryRoleAddGroupsRequest tRequest) throws SentryPluginException;

  void onAlterSentryRoleDeleteGroups(TAlterSentryRoleDeleteGroupsRequest tRequest) throws SentryPluginException;

  void onAlterSentryRoleGrantPrivilege(TAlterSentryRoleGrantPrivilegeRequest tRequest) throws SentryPluginException;

  void onAlterSentryRoleRevokePrivilege(TAlterSentryRoleRevokePrivilegeRequest tRequest) throws SentryPluginException;

  void onDropSentryRole(TDropSentryRoleRequest tRequest) throws SentryPluginException;

  void onRenameSentryPrivilege(TRenamePrivilegesRequest request) throws SentryPluginException;

  void onDropSentryPrivilege(TDropPrivilegesRequest request) throws SentryPluginException;

}
