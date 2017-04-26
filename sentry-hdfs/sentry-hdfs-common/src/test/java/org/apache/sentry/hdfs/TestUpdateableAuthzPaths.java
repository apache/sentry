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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.*;

public class TestUpdateableAuthzPaths {

  private List<String> uriToList(String uri) throws SentryMalformedPathException {
    String path = PathsUpdate.parsePath(uri);
    return Lists.newArrayList(path.split("/"));
  }

  @Test
  public void testFullUpdate() {
    HMSPaths hmsPaths = createBaseHMSPaths(1, 1);
    assertTrue(hmsPaths.findAuthzObjectExactMatches(new String[]{"db1"}).contains("db1"));
    assertTrue(hmsPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11"}).contains("db1.tbl11"));
    assertTrue(hmsPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part111"}).contains("db1.tbl11"));
    assertTrue(hmsPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part112"}).contains("db1.tbl11"));

    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(hmsPaths);
    PathsUpdate update = new PathsUpdate(1, true);
    update.toThrift().setPathsDump(authzPaths.getPathsDump().createPathsDump());

    UpdateableAuthzPaths authzPaths2 = new UpdateableAuthzPaths(new String[] {"/"});
    UpdateableAuthzPaths pre = authzPaths2.updateFull(update);
    assertFalse(pre == authzPaths2);
    authzPaths2 = pre;

    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db1"}).contains("db1"));
    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db1", "tbl11"}).contains("db1.tbl11"));
    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part111"}).contains("db1.tbl11"));
    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part112"}).contains("db1.tbl11"));

    // Ensure Full Update wipes old stuff
    UpdateableAuthzPaths authzPaths3 = new UpdateableAuthzPaths(createBaseHMSPaths(2, 1));
    update = new PathsUpdate(2, true);
    update.toThrift().setPathsDump(authzPaths3.getPathsDump().createPathsDump());
    pre = authzPaths2.updateFull(update);
    assertFalse(pre == authzPaths2);
    authzPaths2 = pre;

    assertNull(authzPaths2.findAuthzObjectExactMatches(new String[]{"db1"}));
    assertNull(authzPaths2.findAuthzObjectExactMatches(new String[]{"db1", "tbl11"}));

    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db2"}).contains("db2"));
    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db2", "tbl21"}).contains("db2.tbl21"));
    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db2", "tbl21", "part211"}).contains("db2.tbl21"));
    assertTrue(authzPaths2.findAuthzObjectExactMatches(new String[]{"db2", "tbl21", "part212"}).contains("db2.tbl21"));
  }

  @Test
  public void testPartialUpdateAddPath() throws SentryMalformedPathException{
    HMSPaths hmsPaths = createBaseHMSPaths(1, 1);
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(hmsPaths);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // Create table
    PathsUpdate update = new PathsUpdate(2, false);
    TPathChanges pathChange = update.newPathChange("db1.tbl12");
    pathChange.addToAddPaths(uriToList("hdfs:///db1/tbl12"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);
    
    // Add partition
    update = new PathsUpdate(3, false);
    pathChange = update.newPathChange("db1.tbl12");
    pathChange.addToAddPaths(uriToList("hdfs:///db1/tbl12/part121"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);

    // Ensure no change in existing Paths
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1"}).contains("db1"));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11"}).contains("db1.tbl11"));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part111"}).contains("db1.tbl11"));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part112"}).contains("db1.tbl11"));

    // Verify new Paths
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl12"}).contains("db1.tbl12"));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl12", "part121"}).contains("db1.tbl12"));

    // Rename table
    update = new PathsUpdate(4, false);
    update.newPathChange("db1.xtbl11").addToAddPaths(uriToList("hdfs:///db1/xtbl11"));
    update.newPathChange("db1.tbl11").addToDelPaths(uriToList("hdfs:///db1/tbl11"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);

    // Verify name change
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1"}).contains("db1"));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "xtbl11"}).contains("db1.xtbl11"));
    // When both name and location are changed, old paths should not contain either new or old table name
    assertNull(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part111"}));
    assertNull(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part112"}));
    // Verify other tables are not touched
    assertNull(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "xtbl12"}));
    assertNull(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "xtbl12", "part121"}));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl12"}).contains("db1.tbl12"));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl12", "part121"}).contains("db1.tbl12"));

  }

  @Test
  public void testPartialUpdateDelPath() throws SentryMalformedPathException{
    HMSPaths hmsPaths = createBaseHMSPaths(1, 1);
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(hmsPaths);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11"}).contains("db1.tbl11"));
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part111"}).contains("db1.tbl11"));

    // Drop partition
    PathsUpdate update = new PathsUpdate(2, false);
    TPathChanges pathChange = update.newPathChange("db1.tbl11");
    pathChange.addToDelPaths(uriToList("hdfs:///db1/tbl11/part111"));
    authzPaths.updatePartial(Lists.newArrayList(update), lock);

    // Verify Paths deleted
    assertNull(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part111"}));

    // Verify rest ok
    assertTrue(authzPaths.findAuthzObjectExactMatches(new String[]{"db1", "tbl11", "part112"}).contains("db1.tbl11"));
  }

  @Test
  public void testDefaultDbPath() {
    HMSPaths hmsPaths = new HMSPaths(new String[] {"/user/hive/warehouse"});
    hmsPaths._addAuthzObject("default", Lists.newArrayList("/user/hive/warehouse"));
    assertTrue(hmsPaths.findAuthzObject(new String[]{"user", "hive", "warehouse"}).contains("default"));
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
