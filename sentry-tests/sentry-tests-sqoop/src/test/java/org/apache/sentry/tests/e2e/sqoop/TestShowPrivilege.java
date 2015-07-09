/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.sqoop;

import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.sqoop.SentrySqoopError;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestShowPrivilege extends AbstractSqoopSentryTestBase {

  @Test
  public void testNotSupportShowOnUser() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrincipal user1 = new MPrincipal("not_support_user1", MPrincipal.TYPE.USER);
    MResource resource1 = new MResource("all", MResource.TYPE.CONNECTOR);
    try {
      client.getPrivilegesByPrincipal(user1, resource1);
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.SHOW_PRIVILEGE_NOT_SUPPORTED_FOR_PRINCIPAL);
    }
  }

  @Test
  public void testNotSupportShowOnGroup() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrincipal group1 = new MPrincipal("not_support_group1", MPrincipal.TYPE.GROUP);
    MResource resource1 = new MResource("all", MResource.TYPE.CONNECTOR);
    try {
      client.getPrivilegesByPrincipal(group1, resource1);
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.SHOW_PRIVILEGE_NOT_SUPPORTED_FOR_PRINCIPAL);
    }
  }

  @Test
  public void testShowPrivileges() throws Exception {
    /**
     * user1 belongs to group group1
     * admin user grant role role1 to group group1
     * admin user grant read privilege on connector all to role role1
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole(ROLE1);
    MPrincipal group1Princ = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MPrincipal role1Princ = new MPrincipal(ROLE1, MPrincipal.TYPE.ROLE);
    MResource allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(allConnector, SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1Princ));
    client.grantPrivilege(Lists.newArrayList(role1Princ), Lists.newArrayList(readPriv));

    // user1 show privilege on role1
    client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getPrivilegesByPrincipal(role1Princ, allConnector).size() == 1);

    // user2 can't show privilege on role1, because user2 doesn't belong to role1
    client = sqoopServerRunner.getSqoopClient(USER2);
    try {
      client.getPrivilegesByPrincipal(role1Princ, allConnector);
      fail("expected SentryAccessDeniedException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryAccessDeniedException");
    }
  }
}
