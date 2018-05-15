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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

public class TestNotificationHandlerInvoker {

  private Configuration conf;
  private NotificationHandler handler;
  private NotificationHandlerInvoker invoker;

  @Before
  public void setup() throws Exception {
    conf = new Configuration(false);
    handler = Mockito.spy(new NotificationHandler(conf) {});
    invoker = new NotificationHandlerInvoker(conf,
        Lists.newArrayList(new ThrowingNotificationHandler(conf), handler));
  }

  @Test
  public void testCreateSentryRole() throws Exception {
    TCreateSentryRoleRequest request = new TCreateSentryRoleRequest();
    TCreateSentryRoleResponse response = new TCreateSentryRoleResponse();
    invoker.create_sentry_role(request, response);
    Mockito.verify(handler).create_sentry_role(request, response);
  }

  @Test
  public void testDropSentryRole() throws Exception {
    TDropSentryRoleRequest request = new TDropSentryRoleRequest();
    TDropSentryRoleResponse response = new TDropSentryRoleResponse();
    invoker.drop_sentry_role(request, response);
    Mockito.verify(handler).drop_sentry_role(request, response);
  }



  @Test
  public void testAlterSentryRoleAddGroups() throws Exception {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest();
    TAlterSentryRoleAddGroupsResponse response = new TAlterSentryRoleAddGroupsResponse();
    invoker.alter_sentry_role_add_groups(request, response);
    Mockito.verify(handler).alter_sentry_role_add_groups(request, response);
  }

  @Test
  public void testAlterSentryRoleDeleteGroups() throws Exception {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest();
    TAlterSentryRoleDeleteGroupsResponse response = new TAlterSentryRoleDeleteGroupsResponse();
    invoker.alter_sentry_role_delete_groups(request, response);
    Mockito.verify(handler).alter_sentry_role_delete_groups(request, response);
  }

  public static class ThrowingNotificationHandler extends NotificationHandler {
    public ThrowingNotificationHandler(Configuration config) throws Exception {
      super(config);
    }
    @Override
    public void create_sentry_role(TCreateSentryRoleRequest request,
                                   TCreateSentryRoleResponse response) {
      throw new RuntimeException();
    }
    public void drop_sentry_role(TDropSentryRoleRequest request,
                                 TDropSentryRoleResponse response) {
      throw new RuntimeException();
    }
    @Override
    public void alter_sentry_role_add_groups(
            TAlterSentryRoleAddGroupsRequest request,
            TAlterSentryRoleAddGroupsResponse response) {
      throw new RuntimeException();
    }
    @Override
    public void alter_sentry_role_delete_groups(
      TAlterSentryRoleDeleteGroupsRequest request,
      TAlterSentryRoleDeleteGroupsResponse response) {
      throw new RuntimeException();
    }
  }
}
