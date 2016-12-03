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

package org.apache.sentry.provider.db.service.thrift;

import org.apache.hadoop.conf.Configuration;

/**
 * Users wishing to be notified when a metadata changing event occurs
 * should extend this abstract class. All methods which modify the underlying
 * metadata in SentryPolicyStoreProcessor will have a corresponding method
 * on this class. Each method will contain a copy of the request and response
 * object. Therefore any change to the request or response object will be ignored.
 *
 * Sub-classes should be thread-safe.
 */
public abstract class NotificationHandler {

  private final Configuration config;

  public NotificationHandler(Configuration config) throws Exception {
    this.config = config;
  }

  protected Configuration getConf() {
    return config;
  }

  public void create_sentry_role(TCreateSentryRoleRequest request, TCreateSentryRoleResponse response) {
  }

  public void drop_sentry_role(TDropSentryRoleRequest request, TDropSentryRoleResponse response) {
  }

  public void alter_sentry_role_grant_privilege(TAlterSentryRoleGrantPrivilegeRequest request,
                                                TAlterSentryRoleGrantPrivilegeResponse response) {
  }

  public void alter_sentry_role_revoke_privilege(TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response) {
  }

  public void alter_sentry_role_add_groups(TAlterSentryRoleAddGroupsRequest request,
                                           TAlterSentryRoleAddGroupsResponse response) {
  }

  public void alter_sentry_role_delete_groups(TAlterSentryRoleDeleteGroupsRequest request,
                                              TAlterSentryRoleDeleteGroupsResponse response) {
  }
}