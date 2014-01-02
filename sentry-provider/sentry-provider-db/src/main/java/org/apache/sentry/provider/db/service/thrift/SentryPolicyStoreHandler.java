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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.sentry.policystore.api.SentryThriftPolicyService;
import org.apache.sentry.policystore.api.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.policystore.api.TAlterSentryRoleAddGroupsResponse;
import org.apache.sentry.policystore.api.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.policystore.api.TAlterSentryRoleDeleteGroupsResponse;
import org.apache.sentry.policystore.api.TCreateSentryPrivilegeRequest;
import org.apache.sentry.policystore.api.TCreateSentryPrivilegeResponse;
import org.apache.sentry.policystore.api.TCreateSentryRoleRequest;
import org.apache.sentry.policystore.api.TCreateSentryRoleResponse;
import org.apache.sentry.policystore.api.TListSentryRolesRequest;
import org.apache.sentry.policystore.api.TListSentryRolesResponse;
import org.apache.thrift.TException;

public class SentryPolicyStoreHandler implements SentryThriftPolicyService.Iface {
  private final String name;
  private final HiveConf conf;
  public SentryPolicyStoreHandler(String name, HiveConf conf) {
    super();
    this.name = name;
    this.conf = conf;
  }
  @Override
  public TCreateSentryRoleResponse create_sentry_role(
      TCreateSentryRoleRequest request) throws TException {
    return null;
  }
  @Override
  public TCreateSentryPrivilegeResponse create_sentry_privilege(
      TCreateSentryPrivilegeRequest request) throws TException {
    return null;
  }
  @Override
  public TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(
      TAlterSentryRoleAddGroupsRequest request) throws TException {
    return null;
  }
  @Override
  public TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(
      TAlterSentryRoleDeleteGroupsRequest request) throws TException {
    return null;
  }
  @Override
  public TListSentryRolesResponse list_sentry_roles(
      TListSentryRolesRequest request) throws TException {
    return null;
  }

}
