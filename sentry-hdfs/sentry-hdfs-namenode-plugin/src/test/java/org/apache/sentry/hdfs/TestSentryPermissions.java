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


package org.apache.sentry.hdfs;

import java.util.List;

import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipal;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipalType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test suits for components inside SentryPermissions.
 */
public class TestSentryPermissions {

  @Test
  public void testRoleInfoCaseInsensitive() {
    SentryPermissions perm = new SentryPermissions();
    SentryPermissions.RoleInfo roleInfo = new SentryPermissions.RoleInfo("Admin");
    perm.addRoleInfo(roleInfo);

    // RoleInfo is case insensitive.
    Assert.assertNotNull(perm.getRoleInfo("admin"));
    Assert.assertNull(perm.getRoleInfo("doesNotExist"));
  }

  /**
   * Adds group permissions and role info and check is the ACL are properly generated.
   */
  @Test
  public void testSentryRolePermissions() {
    String authorizable = "db1.tb1";
    FsAction fsAction = FsAction.ALL;
    SentryPermissions perms = new SentryPermissions();
    SentryPermissions.RoleInfo roleInfo = new SentryPermissions.RoleInfo("role1");
    roleInfo.addGroup("group1");
    roleInfo.addGroup("group2");
    TPrivilegePrincipal roleEntity = new TPrivilegePrincipal(TPrivilegePrincipalType.ROLE, "role1");

    perms.addRoleInfo(roleInfo);

    SentryPermissions.PrivilegeInfo pInfo = new SentryPermissions.PrivilegeInfo(authorizable);
    pInfo.setPermission(roleEntity, fsAction);

    perms.addPrivilegeInfo(pInfo);

    List<AclEntry> acls  = perms.getAcls(authorizable);
    Assert.assertEquals("Unexpected number of ACL entries received", 2, acls.size());
    Assert.assertEquals("Unexpected permission", fsAction, acls.get(0).getPermission());
  }

  /**
   * Adds user permissions and check is the ACL are properly generated.
   */
  @Test
  public void testSentryUserPermissions() {
    String authorizable = "db1.tb1";
    FsAction fsAction = FsAction.ALL;
    TPrivilegePrincipal userEntity = new TPrivilegePrincipal(TPrivilegePrincipalType.USER, "user1");

    SentryPermissions perms = new SentryPermissions();
    SentryPermissions.PrivilegeInfo pInfo = new SentryPermissions.PrivilegeInfo(authorizable);
    pInfo.setPermission(userEntity, fsAction);

    perms.addPrivilegeInfo(pInfo);

    List<AclEntry> acls  = perms.getAcls(authorizable);
    Assert.assertEquals("Unexpected number of ACL entries received", 1, acls.size());
    Assert.assertEquals("Unexpected permission", fsAction, acls.get(0).getPermission());
  }

  /**
   * Adds aggregated user permissions and check is the ACL are properly generated.
   */
  @Test
  public void testSentryAggregatedUserPermissions() {
    String authorizable = null;
    // Add read permission for database
    authorizable = "db1";
    TPrivilegePrincipal userEntity = new TPrivilegePrincipal(TPrivilegePrincipalType.USER, "user1");

    SentryPermissions perms = new SentryPermissions();
    SentryPermissions.PrivilegeInfo pInfo = new SentryPermissions.PrivilegeInfo(authorizable);
    pInfo.setPermission(userEntity, FsAction.READ_EXECUTE);
    perms.addPrivilegeInfo(pInfo);

    // Add write permission for a particular table in the database.
    authorizable = "db1.tb1";
    pInfo = new SentryPermissions.PrivilegeInfo(authorizable);
    pInfo.setPermission(userEntity, FsAction.WRITE_EXECUTE);
    perms.addPrivilegeInfo(pInfo);

    List<AclEntry> acls  = perms.getAcls(authorizable);
    Assert.assertEquals("Unexpected number of ACL entries received", 1, acls.size());
    Assert.assertEquals("Unexpected permission", FsAction.ALL, acls.get(0).getPermission());
  }

  /**
   * Adds user and group permissions and role info and check is the ACL are properly generated.
   */
  @Test
  public void testSentryPermissions() {
    String authorizable = "db1.tb1";
    FsAction fsAction = FsAction.ALL;
    SentryPermissions perms = new SentryPermissions();
    SentryPermissions.RoleInfo roleInfo = new SentryPermissions.RoleInfo("role1");
    roleInfo.addGroup("group1");
    roleInfo.addGroup("group2");
    TPrivilegePrincipal roleEntity = new TPrivilegePrincipal(TPrivilegePrincipalType.ROLE, "role1");
    TPrivilegePrincipal userEntity = new TPrivilegePrincipal(TPrivilegePrincipalType.USER, "user1");

    perms.addRoleInfo(roleInfo);

    SentryPermissions.PrivilegeInfo pInfo = new SentryPermissions.PrivilegeInfo(authorizable);
    pInfo.setPermission(roleEntity, fsAction);
    pInfo.setPermission(userEntity, fsAction);

    perms.addPrivilegeInfo(pInfo);

    List<AclEntry> acls  = perms.getAcls(authorizable);
    Assert.assertEquals("Unexpected number of ACL entries received", 3, acls.size());
    Assert.assertEquals("Unexpected permission", fsAction, acls.get(0).getPermission());

    int userAclCount = 0;
    int groupAclCount = 0;

    for (AclEntry entry : acls) {
      if(entry.getType() == AclEntryType.GROUP) {
        groupAclCount++;
      } else if (entry.getType() == AclEntryType.USER) {
        userAclCount++;
      }
    }
    Assert.assertEquals("Unexpected number of User ACL", 1, userAclCount);
    Assert.assertEquals("Unexpected number of Group ACL", 2, groupAclCount);
  }
}
