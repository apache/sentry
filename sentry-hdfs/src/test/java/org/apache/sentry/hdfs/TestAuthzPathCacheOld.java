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


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
//import org.apache.sentry.hdfs.old.AuthzPathCacheOld;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestAuthzPathCacheOld {

//  @Test
//  public void testGetPathElements() {
//    String[] as2 = AuthzPathCacheOld.getPathElements(new String("/a/b"));
//    String[] as1 = AuthzPathCacheOld.getPathElements(new String("/a/b"));
//    Assert.assertArrayEquals(as1, as2);
//
//    String[] as = AuthzPathCacheOld.getPathElements(new String("/a/b"));
//    Assert.assertArrayEquals(new String[] {"a", "b"}, as);
//
//    as = AuthzPathCacheOld.getPathElements(new String("//a/b"));
//    Assert.assertArrayEquals(new String[]{"a", "b"}, as);
//
//    as = AuthzPathCacheOld.getPathElements(new String("/a//b"));
//    Assert.assertArrayEquals(new String[]{"a", "b"}, as);
//
//    as = AuthzPathCacheOld.getPathElements(new String("/a/b/"));
//    Assert.assertArrayEquals(new String[]{"a", "b"}, as);
//
//    as = AuthzPathCacheOld.getPathElements(new String("//a//b//"));
//    Assert.assertArrayEquals(new String[]{"a", "b"}, as);
//  }
//
//  @Test
//  public void testGetPathsElements() {
//    String[][] as1 = AuthzPathCacheOld.gePathsElements(
//        new String[]{new String("/a/b")});
//    String[][] as2 = AuthzPathCacheOld.gePathsElements(
//        new String[]{new String("/a/b")});
//    Assert.assertEquals(as1.length, as2.length);
//    Assert.assertArrayEquals(as1[0], as2[0]);
//  }
//
//  @Test
//  public void testEntryType() {
//    Assert.assertTrue(AuthzPathCacheOld.EntryType.DIR.isRemoveIfDangling());
//    Assert.assertFalse(AuthzPathCacheOld.EntryType.PREFIX.isRemoveIfDangling());
//    Assert.assertTrue(
//        AuthzPathCacheOld.EntryType.AUTHZ_OBJECT.isRemoveIfDangling());
//  }
//  
//  @Test
//  public void testRootEntry() {
//    AuthzPathCacheOld.Entry root = AuthzPathCacheOld.Entry.createRoot(false);
//    root.toString();
//    Assert.assertNull(root.getParent());
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.DIR, root.getType());
//    // NOTE : This was causing some problems during serialization.. so dissabling 
////    Assert.assertNull(root.getPathElement());
//    Assert.assertNull(root.getAuthzObj());
//    Assert.assertEquals(Path.SEPARATOR, root.getFullPath());
//    Assert.assertTrue(root.getChildren().isEmpty());
//    root.delete();
//    try {
//      root.find(null, true);
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//    try {
//      root.find(new String[0], true);
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//    try {
//      root.find(null, false);
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//    try {
//      root.find(new String[0], false);
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//    Assert.assertEquals(root, root.find(new String[]{"a"}, true));
//    Assert.assertNull(root.find(new String[]{"a"}, false));
//    Assert.assertNull(root.findPrefixEntry(new String[]{"a"}));
//
//    root.delete();
//  }
//
//  @Test
//  public void testRootPrefixEntry() {
//    AuthzPathCacheOld.Entry root = AuthzPathCacheOld.Entry.createRoot(true);
//    root.toString();
//
//    Assert.assertEquals(root, root.find(new String[]{"a"}, true));
//    Assert.assertEquals(null, root.find(new String[]{"a"}, false));
//    Assert.assertEquals(root, root.findPrefixEntry(new String[]{"a"}));
//    Assert.assertEquals(root, root.findPrefixEntry(new String[]{"a", "b"}));
//
//    try {
//      root.createPrefix(new String[]{"a"});
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//  }
//
//  @Test
//  public void testImmediatePrefixEntry() {
//    AuthzPathCacheOld.Entry root = AuthzPathCacheOld.Entry.createRoot(false);
//    AuthzPathCacheOld.Entry entry = root.createPrefix(new String[] {"a"});
//    entry.toString();
//    
//    Assert.assertEquals(1, root.getChildren().size());
//
//    Assert.assertEquals(root, entry.getParent());
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.PREFIX, entry.getType());
//    Assert.assertEquals("a", entry.getPathElement());
//    Assert.assertNull(entry.getAuthzObj());
//    Assert.assertEquals(Path.SEPARATOR + "a", entry.getFullPath());
//    Assert.assertTrue(entry.getChildren().isEmpty());
//
//    Assert.assertEquals(entry, root.find(new String[]{"a"}, true));
//    Assert.assertEquals(entry, root.find(new String[]{"a"}, false));
//    Assert.assertEquals(entry, root.findPrefixEntry(new String[]{"a"}));
//    Assert.assertEquals(entry, root.findPrefixEntry(new String[]{"a", "b"}));
//
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b"}, true));
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "c"}, true));
//    Assert.assertNull(root.find(new String[]{"a", "b"}, false));
//
//    Assert.assertNull(root.find(new String[]{"b"}, false));
//    Assert.assertNull(root.findPrefixEntry(new String[]{"b"}));
//
//    try {
//      root.createPrefix(new String[]{"a", "b"});
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//
//    try {
//      root.createPrefix(new String[]{"a", "b", "c"});
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//
//    entry.delete();
//    Assert.assertTrue(root.getChildren().isEmpty());
//  }
//
//  @Test
//  public void testFurtherPrefixEntry() {
//    AuthzPathCacheOld.Entry root = AuthzPathCacheOld.Entry.createRoot(false);
//    AuthzPathCacheOld.Entry entry = root.createPrefix(new String[]{"a", "b"});
//    entry.toString();
//
//    Assert.assertEquals(1, root.getChildren().size());
//
//    Assert.assertEquals(root, entry.getParent().getParent());
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.PREFIX, entry.getType());
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.DIR, 
//        entry.getParent().getType());
//    Assert.assertEquals("b", entry.getPathElement());
//    Assert.assertEquals("a", entry.getParent().getPathElement());
//    Assert.assertNull(entry.getAuthzObj());
//    Assert.assertNull(entry.getParent().getAuthzObj());
//    Assert.assertEquals(Path.SEPARATOR + "a" + Path.SEPARATOR + "b", 
//        entry.getFullPath());
//    Assert.assertEquals(Path.SEPARATOR + "a", entry.getParent().getFullPath());
//    Assert.assertTrue(entry.getChildren().isEmpty());
//    Assert.assertEquals(1, entry.getParent().getChildren().size());
//
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b"}, true));
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b"}, false));
//    Assert.assertEquals(entry, root.findPrefixEntry(new String[]{"a", "b"}));
//    Assert.assertNull(root.findPrefixEntry(new String[]{"a"}));
//
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "c"}, true));
//    Assert.assertNull(root.find(new String[]{"a", "b", "c"}, false));
//
//    try {
//      root.createPrefix(new String[]{"a", "b"});
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//
//    try {
//      root.createPrefix(new String[]{"a", "b", "c"});
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//
//    entry.delete();
//    Assert.assertTrue(root.getChildren().isEmpty());
//  }
//
//  @Test
//  public void testImmediateAuthzEntry() {
//    AuthzPathCacheOld.Entry root = AuthzPathCacheOld.Entry.createRoot(false);
//    AuthzPathCacheOld.Entry prefix = root.createPrefix(new String[]{"a", "b"});
//
//    AuthzPathCacheOld.Entry entry = root.createAuthzObjPath(
//        new String[]{"a", "b", "p1"}, "A");
//    Assert.assertEquals(prefix, entry.getParent());
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.AUTHZ_OBJECT, entry.getType());
//    Assert.assertEquals("p1", entry.getPathElement());
//    Assert.assertEquals("A", entry.getAuthzObj());
//    Assert.assertEquals(Path.SEPARATOR + "a" + Path.SEPARATOR + "b" + 
//            Path.SEPARATOR + "p1", entry.getFullPath());
//
//    try {
//      root.createPrefix(new String[]{"a", "b", "p1", "c"});
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "p1"}, true));
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "p1"}, false));
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "p1", "c"}, 
//        true));
//    Assert.assertNull(root.find(new String[]{"a", "b", "p1", "c"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "p1"}));
//
//    root.find(new String[]{"a", "b", "p1"}, true).delete();
//    Assert.assertNull(root.find(new String[]{"a", "b", "p1"}, false));
//    Assert.assertNotNull(root.find(new String[]{"a", "b"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "p1"}));
//
//  }
//
//  @Test
//  public void testFurtherAuthzEntry() {
//    AuthzPathCacheOld.Entry root = AuthzPathCacheOld.Entry.createRoot(false);
//    AuthzPathCacheOld.Entry prefix = root.createPrefix(new String[]{"a", "b"});
//
//    AuthzPathCacheOld.Entry entry = root.createAuthzObjPath(
//        new String[]{"a", "b", "t", "p1"}, "A");
//    Assert.assertEquals(prefix, entry.getParent().getParent());
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.AUTHZ_OBJECT, entry.getType());
//    Assert.assertEquals("p1", entry.getPathElement());
//    Assert.assertEquals("A", entry.getAuthzObj());
//    Assert.assertEquals(Path.SEPARATOR + "a" + Path.SEPARATOR + "b" +
//        Path.SEPARATOR + "t" + Path.SEPARATOR + "p1", entry.getFullPath());
//
//    try {
//      root.createPrefix(new String[]{"a", "b", "p1", "t", "c"});
//      Assert.fail();
//    } catch (IllegalArgumentException ex) {
//      //NOP
//    }
//
//    AuthzPathCacheOld.Entry ep2 = root.createAuthzObjPath(
//        new String[]{"a", "b", "t", "p1", "p2"}, "A");
//
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.AUTHZ_OBJECT, entry.getType());
//    Assert.assertEquals("p1", entry.getPathElement());
//    Assert.assertEquals("A", entry.getAuthzObj());
//
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.AUTHZ_OBJECT, ep2.getType());
//    Assert.assertEquals("p2", ep2.getPathElement());
//    Assert.assertEquals("A", entry.getAuthzObj());
//
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "t", "p1"}, 
//        true));
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "t", "p1"}, 
//        false));
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "t", "p1", "c"},
//        true));
//    Assert.assertNull(root.find(new String[]{"a", "b", "t", "p1", "c"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "t", "p1"}));
//
//    Assert.assertEquals(ep2, root.find(new String[]{"a", "b", "t", "p1", "p2"},
//        true));
//    Assert.assertEquals(ep2, root.find(new String[]{"a", "b", "t", "p1", "p2"},
//        false));
//    Assert.assertEquals(ep2, root.find(new String[]{"a", "b", "t", "p1", "p2", "c"},
//        true));
//    Assert.assertNull(root.find(new String[]{"a", "b", "t", "p1", "p2", "c"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "t", "p1", "p2"}));
//
//    root.find(new String[]{"a", "b", "t", "p1"}, false).delete();
//
//    Assert.assertEquals(entry, root.find(new String[]{"a", "b", "t", "p1"},
//        true));
//    Assert.assertEquals(AuthzPathCacheOld.EntryType.DIR, entry.getType());
//    Assert.assertNull(entry.getAuthzObj());
//
//    Assert.assertNotNull(root.find(new String[]{"a", "b", "t", "p1"}, false));
//    Assert.assertNotNull(root.find(new String[]{"a", "b", "t"}, false));
//    Assert.assertNotNull(root.find(new String[]{"a", "b"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "t", "p1"}));
//
//    root.find(new String[]{"a", "b", "t", "p1", "p2"}, false).delete();
//    Assert.assertNull(root.find(new String[]{"a", "b", "t", "p1"}, false));
//    Assert.assertNull(root.find(new String[]{"a", "b", "t"}, false));
//    Assert.assertNotNull(root.find(new String[]{"a", "b"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "t", "p1"}));
//
//  }
//
//  @Test
//  public void testMultipleAuthzEntry() {
//    AuthzPathCacheOld.Entry root = AuthzPathCacheOld.Entry.createRoot(false);
//    AuthzPathCacheOld.Entry prefix = root.createPrefix(new String[]{"a", "b"});
//
//    AuthzPathCacheOld.Entry e1 = root.createAuthzObjPath(
//        new String[]{"a", "b", "t", "p1"}, "A");
//    AuthzPathCacheOld.Entry e2 = root.createAuthzObjPath(
//        new String[]{"a", "b", "t", "p2"}, "A");
//
//
//    Assert.assertEquals(e1, root.find(new String[]{"a", "b", "t", "p1"}, true));
//    Assert.assertEquals(e1, root.find(new String[]{"a", "b", "t", "p1"}, 
//        false));
//    Assert.assertEquals(e1, root.find(new String[]{"a", "b", "t", "p1", "c"},
//        true));
//    Assert.assertNull(root.find(new String[]{"a", "b", "t", "p1", "c"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "t", "p1"}));
//
//    Assert.assertEquals(e2, root.find(new String[]{"a", "b", "t", "p2"}, true));
//    Assert.assertEquals(e2, root.find(new String[]{"a", "b", "t", "p2"}, 
//        false));
//    Assert.assertEquals(e2, root.find(new String[]{"a", "b", "t", "p2", "c"},
//        true));
//    Assert.assertNull(root.find(new String[]{"a", "b", "t", "p2", "c"}, false));
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "t", "p2"}));
//
//    root.find(new String[]{"a", "b", "t", "p1"}, true).delete();
//    Assert.assertNull(root.find(new String[]{"a", "b", "t", "p1"}, false));
//    Assert.assertNotNull(root.find(new String[]{"a", "b", "t"}, false));
//
//    root.find(new String[]{"a", "b", "t", "p2"}, true).delete();
//    Assert.assertNull(root.find(new String[]{"a", "b", "t", "p2"}, false));
//    Assert.assertNull(root.find(new String[]{"a", "b", "t"}, false));
//    Assert.assertNotNull(root.find(new String[]{"a", "b"}, false));
//
//    Assert.assertEquals(prefix, root.findPrefixEntry(
//        new String[]{"a", "b", "t", "p3"}));
//  }
//
//  @Test
//  public void testUpdateHandling() throws Exception {
//    DummyHMSClient mock = new DummyHMSClient();
//    Database db1 = mock.addDb("db1", "/db1");
//    Table tbl11 = mock.addTable(db1, "tbl11", "/db1/tbl11");
//    mock.addPartition(db1, tbl11, "/db1/tbl11/part111");
//    mock.addPartition(db1, tbl11, "/db1/tbl11/part112");
//    AuthzPathCacheOld AuthzPathUpdater = new AuthzPathCacheOld(mock, new String[]{"/db1"}, 10000);
//
//    // Trigger Initial refresh (full dump)
//    AuthzPathUpdater.handleUpdateNotification(new PathsUpdate(10, null));
//    waitToCommit(AuthzPathUpdater);
//    assertEquals("db1.tbl11", AuthzPathUpdater.findAuthzObject("/db1/tbl11/part111".split("^/")[1].split("/")));
//    assertEquals("db1.tbl11", AuthzPathUpdater.findAuthzObject("/db1/tbl11/part112".split("^/")[1].split("/")));
//
//    // Handle preUpdate from HMS plugin
//    PathsUpdate update = new PathsUpdate(11, null);
//    update.addPathUpdate("db1.tbl12").addPath("/db1/tbl12").addPath("/db1/tbl12/part121");
//    update.addPathUpdate("db1.tbl11").delPath("/db1/tbl11/part112");
//
//    // Ensure JSON serialization is working :
//    assertEquals(PathsUpdate.toJsonString(update), 
//        PathsUpdate.toJsonString(
//            PathsUpdate.fromJsonString(
//                PathsUpdate.toJsonString(update))));
//
//    AuthzPathUpdater.handleUpdateNotification(update);
//    waitToCommit(AuthzPathUpdater);
//    assertNull(AuthzPathUpdater.findAuthzObject("/db1/tbl11/part112".split("^/")[1].split("/"), false));
//    assertEquals("db1.tbl12", AuthzPathUpdater.findAuthzObject("/db1/tbl12/part121".split("^/")[1].split("/")));
//
//    // Add more entries to HMS
//    Table tbl13 = mock.addTable(db1, "tbl13", "/db1/tbl13");
//    mock.addPartition(db1, tbl13, "/db1/tbl13/part131");
//
//    // Simulate missed preUpdate (Send empty preUpdate with seqNum 13)
//    // On missed preUpdate, refresh again
//    AuthzPathUpdater.handleUpdateNotification(new PathsUpdate(13, null));
//    waitToCommit(AuthzPathUpdater);
//    assertEquals("db1.tbl13", AuthzPathUpdater.findAuthzObject("/db1/tbl13/part131".split("^/")[1].split("/")));
//  }
//
//  @Test
//  public void testGetUpdatesFromSrcCache() throws InterruptedException {
//    DummyHMSClient mock = new DummyHMSClient();
//    Database db1 = mock.addDb("db1", "/db1");
//    Table tbl11 = mock.addTable(db1, "tbl11", "/db1/tbl11");
//    mock.addPartition(db1, tbl11, "/db1/tbl11/part111");
//    mock.addPartition(db1, tbl11, "/db1/tbl11/part112");
//
//    // This would live in the Sentry Service
//    AuthzPathCacheOld srcCache = new AuthzPathCacheOld(mock, new String[]{"/db1"}, 10000);
//
//    // Trigger Initial full Image fetch
//    srcCache.handleUpdateNotification(new PathsUpdate(10, null));
//    waitToCommit(srcCache);
//
//    // This entity would live in the NN plugin : a downstream cache with no updateLog
//    AuthzPathCacheOld destCache = new AuthzPathCacheOld(null, new String[]{"/db1"}, 0);
//
//    // Adapter to pull updates from upstream cache to downstream Cache
//    DummyAdapter<PathsUpdate> adapter = new DummyAdapter<PathsUpdate>(destCache, srcCache);
//    adapter.getDestToPullUpdatesFromSrc();
//    waitToCommit(destCache);
//    // Check if NN plugin received the updates from Sentry Cache
//    assertEquals("db1.tbl11", destCache.findAuthzObject("/db1/tbl11/part111".split("^/")[1].split("/")));
//    assertEquals("db1.tbl11", destCache.findAuthzObject("/db1/tbl11/part112".split("^/")[1].split("/")));
//
//    // Create Upsteram HMS preUpdate
//    PathsUpdate update = new PathsUpdate(11, null);
//    update.addPathUpdate("db1.tbl12").addPath("/db1/tbl12").addPath("/db1/tbl12/part121");
//    update.addPathUpdate("db1.tbl11").delPath("/db1/tbl11/part112");
//
//    // Send Update to Upstream Cache
//    srcCache.handleUpdateNotification(update);
//    waitToCommit(srcCache);
//    // Pull preUpdate to downstream Cache
//    adapter.getDestToPullUpdatesFromSrc();
//    waitToCommit(destCache);
//
//    assertNull(srcCache.findAuthzObject("/db1/tbl11/part112".split("^/")[1].split("/"), false));
//    assertNull(destCache.findAuthzObject("/db1/tbl11/part112".split("^/")[1].split("/"), false));
//    assertEquals("db1.tbl11", destCache.findAuthzObject("/db1/tbl11/part112".split("^/")[1].split("/")));
//    assertEquals("db1.tbl12", destCache.findAuthzObject("/db1/tbl12/part121".split("^/")[1].split("/")));
//  }
//
////  @Test(expected = IllegalArgumentException.class)
////  public void testAuthzPathUpdaterRootPrefix() {
////    AuthzPathCacheOld cache = new AuthzPathCacheOld(new String[]{"/", "/b/c"});
////  }
//  
//  @Test
//  public void testAuthzPathUpdater() {
//    AuthzPathCacheOld cache = new AuthzPathCacheOld(null, new String[] { "/a", "/b/c"}, 0);
//    Assert.assertTrue(cache.isUnderPrefix("/a".split("^/")[1].split("/")));
//    Assert.assertTrue(cache.isUnderPrefix("/a/x".split("^/")[1].split("/")));
//    Assert.assertTrue(cache.isUnderPrefix("/b/c/".split("^/")[1].split("/")));
//    Assert.assertFalse(cache.isUnderPrefix("/x".split("^/")[1].split("/")));
//
//    Assert.assertNull((cache.findAuthzObject("/a/x".split("^/")[1].split("/"))));
//    Assert.assertNull((cache.findAuthzObject("/x".split("^/")[1].split("/"))));
//    
//    cache.addAuthzObject("T", Arrays.asList("/a/T/p1", "/a/T/p2"));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p2".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1/x".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1/x/x".split("^/")[1].split("/")));
//    Assert.assertNull((cache.findAuthzObject("/a/T/p3".split("^/")[1].split("/"))));
//
//    cache.addPathsToAuthzObject("T", Arrays.asList("/a/T/p3"));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p2".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1/x".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1/x/x".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p3".split("^/")[1].split("/")));
//
//    cache.deletePathsFromAuthzObject("T", Arrays.asList("/a/T/p2"));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1".split("^/")[1].split("/")));
//    Assert.assertNull((cache.findAuthzObject("/a/T/p2".split("^/")[1].split("/"))));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1/x".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p1/x/x".split("^/")[1].split("/")));
//    Assert.assertEquals("T", cache.findAuthzObject("/a/T/p3".split("^/")[1].split("/")));
//
//    cache.deleteAuthzObject("T");
//    Assert.assertNull((cache.findAuthzObject("/a/T/p1".split("^/")[1].split("/"))));
//    Assert.assertNull((cache.findAuthzObject("/a/T/p2".split("^/")[1].split("/"))));
//    Assert.assertNull((cache.findAuthzObject("/a/T/p3".split("^/")[1].split("/"))));
//  }
//
//  private void waitToCommit(AuthzPathCacheOld hmsCache) throws InterruptedException {
//    int counter = 0;
//    while(!hmsCache.areAllUpdatesCommited()) {
//      Thread.sleep(200);
//      counter++;
//      if (counter > 10000) {
//        fail("Updates taking too long to commit !!");
//      }
//    }
//  }
  
}
