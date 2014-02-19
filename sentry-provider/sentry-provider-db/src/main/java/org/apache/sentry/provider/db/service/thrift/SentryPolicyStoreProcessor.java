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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;
import org.apache.thrift.TException;

@SuppressWarnings("unused")
public class SentryPolicyStoreProcessor implements SentryPolicyService.Iface {
  public static final String SENTRY_POLICY_SERVICE_NAME = "SentryService";

  private final String name;
  private final Configuration conf;
  private SentryStore sentryStore;
  private boolean isReady;

  public SentryPolicyStoreProcessor(String name, Configuration conf) {
    super();
    this.name = name;
    this.conf = conf;
    sentryStore = new SentryStore();
    this.isReady = true;
  }

  public void stop() {
    if (isReady) {
      sentryStore.stop();
    }
  }

  private TSentryResponseStatus getStatus(boolean status) {
    return Status.OK();
  }

  @Override
  public TCreateSentryRoleResponse create_sentry_role(
    TCreateSentryRoleRequest request) throws TException {
    TCreateSentryRoleResponse resp = new TCreateSentryRoleResponse();
    TSentryResponseStatus status;
    boolean ret = false;

    try {
      ret = sentryStore.createSentryRole(request.getRole());
      resp.setStatus(Status.OK());
    } catch (Throwable t) {

    }
    return resp;
  }
  @Override
  public TCreateSentryPrivilegeResponse create_sentry_privilege(
    TCreateSentryPrivilegeRequest request) throws TException {
    return null;
  }

  public TDropSentryRoleResponse drop_sentry_role(
    TDropSentryRoleRequest req)  throws TException {
    TDropSentryRoleResponse resp = new TDropSentryRoleResponse();
    TSentryResponseStatus status;
    boolean ret = false;

    try {
      ret = sentryStore.dropSentryRole(req.getRoleName());
      resp.setStatus(Status.OK());
    } catch (Throwable t) {

    }
    return resp;
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
  public TListSentryRolesResponse list_sentry_roles_by_group(
    TListSentryRolesRequest request) throws TException {
    return null;
  }

  @Override
  public TListSentryRolesResponse list_sentry_roles_by_role_name(
    TListSentryRolesRequest request) throws TException {
    TListSentryRolesResponse resp = new TListSentryRolesResponse();
    TSentryResponseStatus status;
    TSentryRole role = null;
    Set<TSentryRole> roleSet = new HashSet<TSentryRole>();
    try {
      role = sentryStore.getSentryRoleByName(request.getRoleName());
      roleSet.add(role);
      resp.setRoles(roleSet);
      resp.setStatus(Status.OK());
    } catch (Throwable t) {
      resp.setRoles(roleSet);
      resp.setStatus(Status.NoSuchObject("Role :" + request.getRoleName() +
                                         "  couldn't be retrieved.", t));
    }
    return resp;
  }
}