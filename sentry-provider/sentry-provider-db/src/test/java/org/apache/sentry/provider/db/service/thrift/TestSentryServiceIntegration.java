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

import org.apache.sentry.provider.db.service.thrift.Constants.ServerConfig;
import org.apache.sentry.service.api.TCreateSentryRoleRequest;
import org.apache.sentry.service.api.TCreateSentryRoleResponse;
import org.apache.sentry.service.api.TSentryPrivilege;
import org.apache.sentry.service.api.TSentryRole;
import org.apache.sentry.service.api.TSentryStatus;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSentryServiceIntegration extends SentryServiceIntegrationBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestSentryServiceIntegration.class);

  @Test
  public void testClientServerConnection() throws Exception {
    TCreateSentryRoleRequest req = new TCreateSentryRoleRequest();
    TSentryRole role = new TSentryRole();
    role.setRoleName("admin_r");
    role.setCreateTime(System.currentTimeMillis());
    role.setGrantorPrincipal("test");
    role.setPrivileges(new HashSet<TSentryPrivilege>());
    req.setUserName("admin");
    req.setRole(role);
    TCreateSentryRoleResponse resp = client.createRole(req);
    if (resp.getStatus().getValue() == TSentryStatus.OK) {
      LOGGER.info("Successfully opened connection");
    } else {
      Assert.fail("Received invalid status: " + resp.getStatus().getMessage()
          + ":\nstack:" + resp.getStatus().getStack());
    }
  }
}
