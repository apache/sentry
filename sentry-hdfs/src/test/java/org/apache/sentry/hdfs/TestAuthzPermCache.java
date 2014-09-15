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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.permission.FsAction;
//import org.apache.sentry.hdfs.old.AuthzPermCache;
//import org.apache.sentry.hdfs.old.AuthzPermCache.PrivilegeInfo;
//import org.apache.sentry.hdfs.old.AuthzPermCache.RoleInfo;
import org.junit.Test;

public class TestAuthzPermCache {

//  @Test
//  public void testAuthzAddRemove() throws InterruptedException {
//    DummyAuthzSource src = new DummyAuthzSource();
//    AuthzPermCache authzCache = new AuthzPermCache(10000, src, 0);
//    src.privs.put("db1.tbl11", new PrivilegeInfo("db1.tbl11").setPermission("r1", FsAction.READ_WRITE));
//    src.privs.put("db1.tbl12", new PrivilegeInfo("db1.tbl12").setPermission("r1", FsAction.READ).setPermission("r2", FsAction.WRITE));
//    src.privs.put("db1.tbl13", new PrivilegeInfo("db1.tbl13").setPermission("r2", FsAction.READ).setPermission("r3", FsAction.WRITE));
//    src.roles.put("r1", new RoleInfo("r1").addGroup("g1"));
//    src.roles.put("r2", new RoleInfo("r2").addGroup("g2").addGroup("g1"));
//    src.roles.put("r3", new RoleInfo("r3").addGroup("g3").addGroup("g2").addGroup("g1"));
//    authzCache.handleUpdateNotification(new PermissionsUpdate(10, false));
//    waitToCommit(authzCache);
//
//    assertEquals(FsAction.READ_WRITE, authzCache.getPermissions("db1.tbl11").get("g1"));
//    assertEquals(FsAction.READ_WRITE, authzCache.getPermissions("db1.tbl12").get("g1"));
//    assertEquals(FsAction.WRITE, authzCache.getPermissions("db1.tbl12").get("g2"));
//    assertEquals(FsAction.READ_WRITE, authzCache.getPermissions("db1.tbl13").get("g1"));
//    assertEquals(FsAction.READ_WRITE, authzCache.getPermissions("db1.tbl13").get("g2"));
//    assertEquals(FsAction.WRITE, authzCache.getPermissions("db1.tbl13").get("g3"));
//  }
//
//  private void waitToCommit(AuthzPermCache authzCache) throws InterruptedException {
//    int counter = 0;
//    while(!authzCache.areAllUpdatesCommited()) {
//      Thread.sleep(200);
//      counter++;
//      if (counter > 10000) {
//        fail("Updates taking too long to commit !!");
//      }
//    }
//  }
}
