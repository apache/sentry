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

import org.apache.sentry.sqoop.SentrySqoopError;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MRole;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestRoleOperation extends AbstractSqoopSentryTestBase {

  @Test
  public void testAdminToCreateDeleteRole() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole("create_delete_role_1");
    MRole role2 = new MRole("create_delete_role_2");
    client.createRole(role1);
    client.createRole(role2);
    assertTrue( client.getRoles().size() > 0);
  }

  @Test
  public void testNotAdminToCreateDeleteRole() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole("not_admin_create_delete_role_1");
    MRole role2 = new MRole("not_admin_create_delete_role_2");
    client.createRole(role1);

    client = sqoopServerRunner.getSqoopClient(USER1);
    try {
      client.createRole(role2);
      fail("expected SentryAccessDeniedException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryAccessDeniedException");
    }
    try {
      client.dropRole(role1);
      fail("expected SentryAccessDeniedException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryAccessDeniedException");
    }
  }

  @Test
  public void testCreateExistedRole() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole("create_exist_role_1");
    client.createRole(role1);
    try {
      client.createRole(role1);
      fail("expected SentryAlreadyExistsException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryAlreadyExistsException");
    }
  }

  @Test
  public void testDropNotExistedRole() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    try {
      client.dropRole(new MRole("drop_noexisted_role_1"));
      fail("expected SentryNoSuchObjectException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryNoSuchObjectException");
    }
  }

  @Test
  public void testAdminShowAllRole() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.createRole(new MRole("show_all_role"));
    assertTrue(client.getRoles().size() > 0);
  }

  @Test
  public void testNotAdminShowAllRole() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(USER1);
    try {
      client.getRoles();
      fail("expected SentryAccessDeniedException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryAccessDeniedException");
    }
  }

  @Test
  public void testNotSupportAddRoleToUser() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole("add_to_user_role");
    MPrincipal user1 = new MPrincipal("add_to_user", MPrincipal.TYPE.USER);
    try {
      client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(user1));
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.GRANT_REVOKE_ROLE_NOT_SUPPORT_FOR_PRINCIPAL);
    }
  }

  @Test
  public void testShowRoleOnGroup() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    // admin user grant role1 to group1
    MRole role1 = new MRole(ROLE1);
    client.createRole(role1);
    MPrincipal group1 = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1));
    // admin user grant role2 to group2
    MRole role2 = new MRole(ROLE2);
    client.createRole(role2);
    MPrincipal group2 = new MPrincipal(GROUP2, MPrincipal.TYPE.GROUP);
    client.grantRole(Lists.newArrayList(role2), Lists.newArrayList(group2));

    // use1 can show role on group1
    client = sqoopServerRunner.getSqoopClient(USER1);
    assertEquals(role1.getName(), client.getRolesByPrincipal(group1).get(0).getName());

    // use1 can't show role on group2
    try {
      client.getRolesByPrincipal(group2);
      fail("expected SentryAccessDeniedException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryAccessDeniedException");
    }

    // user2 can show role on group2
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertEquals(role2.getName(), client.getRolesByPrincipal(group2).get(0).getName());

    // use2 can't show role on group1
    try {
      client.getRolesByPrincipal(group1);
      fail("expected SentryAccessDeniedException happend");
    } catch (Exception e) {
      assertCausedMessage(e, "SentryAccessDeniedException");
    }
  }

  @Test
  public void testAddDeleteRoleOnGroup() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    // admin user grant role3 to group3
    MRole role3 = new MRole(ROLE3);
    client.createRole(role3);
    MPrincipal group3 = new MPrincipal(GROUP3, MPrincipal.TYPE.GROUP);
    client.grantRole(Lists.newArrayList(role3), Lists.newArrayList(group3));
    // admin user grant role4 to group4
    MRole role4 = new MRole(ROLE4);
    client.createRole(role4);
    MPrincipal group4 = new MPrincipal(GROUP4, MPrincipal.TYPE.GROUP);
    client.grantRole(Lists.newArrayList(role4), Lists.newArrayList(group4));

    // use3 can show role on group3
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertEquals(role3.getName(), client.getRolesByPrincipal(group3).get(0).getName());

    // user4 can show role on group4
    client = sqoopServerRunner.getSqoopClient(USER4);
    assertEquals(role4.getName(), client.getRolesByPrincipal(group4).get(0).getName());

    /**
     * admin delete role3 from group3
     * admin delete role4 from group4
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.revokeRole(Lists.newArrayList(role3), Lists.newArrayList(group3));
    client.revokeRole(Lists.newArrayList(role4), Lists.newArrayList(group4));

    // use3 show role on group3, empty role list return
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getRolesByPrincipal(group3).isEmpty());

    // use4 show role on group4, empty role list return
    client = sqoopServerRunner.getSqoopClient(USER4);
    assertTrue(client.getRolesByPrincipal(group4).isEmpty());
  }

  @Test
  public void testNotSupportShowRoleonUser() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrincipal user1 = new MPrincipal("showRoleOnUser", MPrincipal.TYPE.USER);
    try {
      client.getRolesByPrincipal(user1);
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.SHOW_GRANT_NOT_SUPPORTED_FOR_PRINCIPAL);
    }
  }
}
