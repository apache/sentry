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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.sqoop.SentrySqoopError;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRevokePrivilege extends AbstractSqoopSentryTestBase {
  @Test
  public void testNotSupportRevokePrivilegeFromUser() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrincipal user1 = new MPrincipal("not_support_revoke_user_1", MPrincipal.TYPE.GROUP);
    MResource  allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(allConnector,SqoopActionConstant.READ, false);
    try {
      client.revokePrivilege(Lists.newArrayList(user1), Lists.newArrayList(readPriv));
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.GRANT_REVOKE_PRIVILEGE_NOT_SUPPORT_FOR_PRINCIPAL);
    }
  }

  @Test
  public void testNotSupportRevokePrivilegeFromGroup() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrincipal group1 = new MPrincipal("not_support_revoke_group_1", MPrincipal.TYPE.GROUP);
    MResource  allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(allConnector,SqoopActionConstant.READ, false);
    try {
      client.revokePrivilege(Lists.newArrayList(group1), Lists.newArrayList(readPriv));
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.GRANT_REVOKE_PRIVILEGE_NOT_SUPPORT_FOR_PRINCIPAL);
    }
  }

  @Test
  public void testRevokeNotExistPrivilege() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole testRole = new MRole("noexist_privilege_role1");
    MPrincipal testPrinc = new MPrincipal(testRole.getName(), MPrincipal.TYPE.ROLE);
    MResource allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readPrivilege = new MPrivilege(allConnector, SqoopActionConstant.READ, false);
    client.createRole(testRole);
    assertTrue(client.getPrivilegesByPrincipal(testPrinc, allConnector).size() == 0);

    client.revokePrivilege(Lists.newArrayList(testPrinc), Lists.newArrayList(readPrivilege));
    assertTrue(client.getPrivilegesByPrincipal(testPrinc, allConnector).size() == 0);
  }


  @Test
  public void testRevokePrivilege() throws Exception {
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
    MPrivilege readPrivilege = new MPrivilege(allConnector, SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1Princ));
    client.grantPrivilege(Lists.newArrayList(role1Princ), Lists.newArrayList(readPrivilege));

    // check user1 has privilege on role1
    client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getPrivilegesByPrincipal(role1Princ, allConnector).size() == 1);

    // admin user revoke read privilege from role1
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.revokePrivilege(Lists.newArrayList(role1Princ), Lists.newArrayList(readPrivilege));

    // check user1 has no privilege on role1
    client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getPrivilegesByPrincipal(role1Princ, allConnector).size() == 0);
  }

  @Test
  public void testRevokeAllPrivilege() throws Exception {
    /**
     * user2 belongs to group group2
     * admin user grant role role2 to group group2
     * admin user grant read and write privilege on connector all to role role2
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role2 = new MRole(ROLE2);
    MPrincipal group2Princ = new MPrincipal(GROUP2, MPrincipal.TYPE.GROUP);
    MPrincipal role2Princ = new MPrincipal(ROLE2, MPrincipal.TYPE.ROLE);
    MResource allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege writePrivilege = new MPrivilege(allConnector, SqoopActionConstant.WRITE, false);
    MPrivilege readPrivilege = new MPrivilege(allConnector, SqoopActionConstant.READ, false);
    client.createRole(role2);
    client.grantRole(Lists.newArrayList(role2), Lists.newArrayList(group2Princ));
    client.grantPrivilege(Lists.newArrayList(role2Princ), Lists.newArrayList(writePrivilege, readPrivilege));

    // check user2 has two privileges on role2
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getPrivilegesByPrincipal(role2Princ, allConnector).size() == 2);

    // admin user revoke all privilege from role2
    MPrivilege allPrivilege = new MPrivilege(allConnector, SqoopActionConstant.ALL_NAME, false);
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.revokePrivilege(Lists.newArrayList(role2Princ), Lists.newArrayList(allPrivilege));

    // check user2 has no privilege on role2
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getPrivilegesByPrincipal(role2Princ, allConnector).size() == 0);
  }

  @Test
  public void testRevokePrivilegeWithAllPrivilegeExist() throws Exception {
    /**
     * user3 belongs to group group3
     * admin user grant role role3 to group group3
     * admin user grant all privilege on connector all to role role3
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role3 = new MRole(ROLE3);
    MPrincipal group3Princ = new MPrincipal(GROUP3, MPrincipal.TYPE.GROUP);
    MPrincipal role3Princ = new MPrincipal(ROLE3, MPrincipal.TYPE.ROLE);
    MResource allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege allPrivilege = new MPrivilege(allConnector, SqoopActionConstant.ALL_NAME, false);
    client.createRole(role3);
    client.grantRole(Lists.newArrayList(role3), Lists.newArrayList(group3Princ));
    client.grantPrivilege(Lists.newArrayList(role3Princ), Lists.newArrayList(allPrivilege));

    // check user3 has one privilege on role3
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getPrivilegesByPrincipal(role3Princ, allConnector).size() == 1);
    // user3 has the all action on role3
    MPrivilege user3Privilege = client.getPrivilegesByPrincipal(role3Princ, allConnector).get(0);
    assertEquals(user3Privilege.getAction(), SqoopActionConstant.ALL_NAME);

    // admin user revoke the read privilege on connector all from role role3
    MPrivilege readPrivilege = new MPrivilege(allConnector, SqoopActionConstant.READ, false);
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.revokePrivilege(Lists.newArrayList(role3Princ), Lists.newArrayList(readPrivilege));

    // check user3 has only the write privilege on role3
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getPrivilegesByPrincipal(role3Princ, allConnector).size() == 1);
    user3Privilege = client.getPrivilegesByPrincipal(role3Princ, allConnector).get(0);
    assertEquals(user3Privilege.getAction().toLowerCase(), SqoopActionConstant.WRITE);
  }
}
