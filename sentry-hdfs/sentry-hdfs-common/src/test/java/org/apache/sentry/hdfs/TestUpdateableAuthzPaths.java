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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestUpdateableAuthzPaths {

  @Test
  public void testFullUpdate() {
    HMSPaths hmsPaths = createBaseHMSPaths(1, 1);
    assertEquals("db1", hmsPaths.findAuthzObjectExactMatch(new String[]{"db1"}));
    assertEquals("db1.tbl11", hmsPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11"}));
    assertEquals("db1.tbl11", hmsPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part111"}));
    assertEquals("db1.tbl11", hmsPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part112"}));

    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(hmsPaths);
    PathsUpdate update = new PathsUpdate(1, true);
    update.toThrift().setPathsDump(authzPaths.getPathsDump().createPathsDump());

    UpdateableAuthzPaths authzPaths2 = new UpdateableAuthzPaths(new String[] {"/"});
    UpdateableAuthzPaths pre = authzPaths2.updateFull(update);
    assertFalse(pre == authzPaths2);
    authzPaths2 = pre;

    assertEquals("db1", authzPaths2.findAuthzObjectExactMatch(new String[]{"db1"}));
    assertEquals("db1.tbl11", authzPaths2.findAuthzObjectExactMatch(new String[]{"db1", "tbl11"}));
    assertEquals("db1.tbl11", authzPaths2.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part111"}));
    assertEquals("db1.tbl11", authzPaths2.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part112"}));

    // Ensure Full Update wipes old stuff
    UpdateableAuthzPaths authzPaths3 = new UpdateableAuthzPaths(createBaseHMSPaths(2, 1));
    update = new PathsUpdate(2, true);
    update.toThrift().setPathsDump(authzPaths3.getPathsDump().createPathsDump());
    pre = authzPaths2.updateFull(update);
    assertFalse(pre == authzPaths2);
    authzPaths2 = pre;

    assertNull(authzPaths2.findAuthzObjectExactMatch(new String[]{"db1"}));
    assertNull(authzPaths2.findAuthzObjectExactMatch(new String[]{"db1", "tbl11"}));

    assertEquals("db2", authzPaths2.findAuthzObjectExactMatch(new String[]{"db2"}));
    assertEquals("db2.tbl21", authzPaths2.findAuthzObjectExactMatch(new String[]{"db2", "tbl21"}));
    assertEquals("db2.tbl21", authzPaths2.findAuthzObjectExactMatch(new String[]{"db2", "tbl21", "part211"}));
    assertEquals("db2.tbl21", authzPaths2.findAuthzObjectExactMatch(new String[]{"db2", "tbl21", "part212"}));
  }

  @Test
  public void testPartialUpdateAddPath() {
    HMSPaths hmsPaths = createBaseHMSPaths(1, 1);
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(hmsPaths);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // Create table
    PathsUpdate update = new PathsUpdate(2, false);
    TPathChanges pathChange = update.newPathChange("db1.tbl12");
    pathChange.addToAddPaths(PathsUpdate.cleanPath("file:///db1/tbl12"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);
    
    // Add partition
    update = new PathsUpdate(3, false);
    pathChange = update.newPathChange("db1.tbl12");
    pathChange.addToAddPaths(PathsUpdate.cleanPath("file:///db1/tbl12/part121"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);

    // Ensure no change in existing Paths
    assertEquals("db1", authzPaths.findAuthzObjectExactMatch(new String[]{"db1"}));
    assertEquals("db1.tbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11"}));
    assertEquals("db1.tbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part111"}));
    assertEquals("db1.tbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part112"}));

    // Verify new Paths
    assertEquals("db1.tbl12", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl12"}));
    assertEquals("db1.tbl12", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl12", "part121"}));

    // Rename table
    update = new PathsUpdate(4, false);
    update.newPathChange("db1.xtbl11").addToAddPaths(PathsUpdate.cleanPath("file:///db1/xtbl11"));
    update.newPathChange("db1.tbl11").addToDelPaths(PathsUpdate.cleanPath("file:///db1/tbl11"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);

    // Verify name change
    assertEquals("db1", authzPaths.findAuthzObjectExactMatch(new String[]{"db1"}));
    assertEquals("db1.xtbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "xtbl11"}));
    // Explicit set location has to be done on the partition else it will be associated to
    // the old location
    assertEquals("db1.xtbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part111"}));
    assertEquals("db1.xtbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part112"}));
    // Verify other tables are not touched
    assertNull(authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "xtbl12"}));
    assertNull(authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "xtbl12", "part121"}));
    assertEquals("db1.tbl12", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl12"}));
    assertEquals("db1.tbl12", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl12", "part121"}));

  }

  @Test
  public void testPartialUpdateDelPath() {
    HMSPaths hmsPaths = createBaseHMSPaths(1, 1);
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(hmsPaths);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    assertEquals("db1.tbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11"}));
    assertEquals("db1.tbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part111"}));
    
    // Drop partition
    PathsUpdate update = new PathsUpdate(2, false);
    TPathChanges pathChange = update.newPathChange("db1.tbl11");
    pathChange.addToDelPaths(PathsUpdate.cleanPath("file:///db1/tbl11/part111"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);

    // Verify Paths deleted
    assertNull(authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part111"}));

    // Verify rest ok
    assertEquals("db1.tbl11", authzPaths.findAuthzObjectExactMatch(new String[]{"db1", "tbl11", "part112"}));
  }

  private HMSPaths createBaseHMSPaths(int dbNum, int tblNum) {
    String db = "db" + dbNum;
    String tbl = "tbl" + dbNum + "" + tblNum;
    String fullTbl = db + "." + tbl;
    String dbPath = "/" + db;
    String tblPath = "/" + db + "/" + tbl;
    String partPath = tblPath + "/part" + dbNum + "" + tblNum;
    HMSPaths hmsPaths = new HMSPaths(new String[] {"/"});
    hmsPaths._addAuthzObject(db, Lists.newArrayList(dbPath));
    hmsPaths._addAuthzObject(fullTbl, Lists.newArrayList(tblPath));
    hmsPaths._addPathsToAuthzObject(fullTbl, Lists.newArrayList(
        partPath + "1", partPath + "2" ));
    return hmsPaths;
  }

}
