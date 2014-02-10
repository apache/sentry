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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;
import org.apache.thrift.TException;

@SuppressWarnings("unused")
public class SentryPolicyStoreProcessor implements SentryPolicyService.Iface {
  public static final String SENTRY_POLICY_SERVICE_NAME = "SentryPolicyService";

  private final String name;
  private final Configuration conf;

  public SentryPolicyStoreProcessor(String name, Configuration conf) {
    super();
    this.name = name;
    this.conf = conf;
  }
  @Override
  public TCreateSentryRoleResponse create_sentry_role(
      TCreateSentryRoleRequest request) throws TException {
    TCreateSentryRoleResponse resp = new TCreateSentryRoleResponse();
    TSentryResponseStatus status = Status.OK();
    resp.setStatus(status);
    return resp;
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
