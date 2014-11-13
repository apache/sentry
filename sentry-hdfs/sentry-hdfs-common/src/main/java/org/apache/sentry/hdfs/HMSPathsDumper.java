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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sentry.hdfs.HMSPaths.Entry;
import org.apache.sentry.hdfs.HMSPaths.EntryType;
import org.apache.sentry.hdfs.service.thrift.TPathEntry;
import org.apache.sentry.hdfs.service.thrift.TPathsDump;

public class HMSPathsDumper implements AuthzPathsDumper<HMSPaths> {

  private final HMSPaths hmsPaths;

  static class Tuple {
    final TPathEntry entry;
    final int id;
    Tuple(TPathEntry entry, int id) {
      this.entry = entry;
      this.id = id;
    }
  }

  public HMSPathsDumper(HMSPaths hmsPaths) {
    this.hmsPaths = hmsPaths;
  }

  @Override
  public TPathsDump createPathsDump() {
    AtomicInteger counter = new AtomicInteger(0);
    Map<Integer, TPathEntry> idMap = new HashMap<Integer, TPathEntry>();
    Tuple tRootTuple =
        createTPathEntry(hmsPaths.getRootEntry(), counter, idMap);
    idMap.put(tRootTuple.id, tRootTuple.entry);
    cloneToTPathEntry(hmsPaths.getRootEntry(), tRootTuple.entry, counter, idMap);
    return new TPathsDump(tRootTuple.id, idMap);
  }

  private void cloneToTPathEntry(Entry parent, TPathEntry tParent,
      AtomicInteger counter, Map<Integer, TPathEntry> idMap) {
    for (Entry child : parent.getChildren().values()) {
      Tuple childTuple = createTPathEntry(child, counter, idMap);
      tParent.getChildren().add(childTuple.id);
      cloneToTPathEntry(child, childTuple.entry, counter, idMap);
    }
  }

  private Tuple createTPathEntry(Entry entry, AtomicInteger idCounter,
      Map<Integer, TPathEntry> idMap) {
    int myId = idCounter.incrementAndGet();
    TPathEntry tEntry = new TPathEntry(entry.getType().getByte(),
        entry.getPathElement(), new HashSet<Integer>());
    if (entry.getAuthzObj() != null) {
      tEntry.setAuthzObj(entry.getAuthzObj());
    }
    idMap.put(myId, tEntry);
    return new Tuple(tEntry, myId);
  }

  @Override
  public HMSPaths initializeFromDump(TPathsDump pathDump) {
    HMSPaths hmsPaths = new HMSPaths(this.hmsPaths.getPrefixes());
    TPathEntry tRootEntry = pathDump.getNodeMap().get(pathDump.getRootId());
    Entry rootEntry = hmsPaths.getRootEntry();
//    Entry rootEntry = new Entry(null, tRootEntry.getPathElement(),
//        EntryType.fromByte(tRootEntry.getType()), tRootEntry.getAuthzObj());
    Map<String, Set<Entry>> authzObjToPath = new HashMap<String, Set<Entry>>();
    cloneToEntry(tRootEntry, rootEntry, pathDump.getNodeMap(), authzObjToPath,
        rootEntry.getType() == EntryType.PREFIX);
    hmsPaths.setRootEntry(rootEntry);
    hmsPaths.setAuthzObjToPathMapping(authzObjToPath);
    return hmsPaths;
  }

  private void cloneToEntry(TPathEntry tParent, Entry parent,
      Map<Integer, TPathEntry> idMap, Map<String,
      Set<Entry>> authzObjToPath, boolean hasCrossedPrefix) {
    for (Integer id : tParent.getChildren()) {
      TPathEntry tChild = idMap.get(id);
      Entry child = null;
      boolean isChildPrefix = hasCrossedPrefix;
      if (!hasCrossedPrefix) {
        child = parent.getChildren().get(tChild.getPathElement());
        // If we havn't reached a prefix entry yet, then child should
        // already exists.. else it is not part of the prefix
        if (child == null) continue;
        isChildPrefix = child.getType() == EntryType.PREFIX;
      }
      if (child == null) {
        child = new Entry(parent, tChild.getPathElement(),
            EntryType.fromByte(tChild.getType()), tChild.getAuthzObj());
      }
      if (child.getAuthzObj() != null) {
        Set<Entry> paths = authzObjToPath.get(child.getAuthzObj());
        if (paths == null) {
          paths = new HashSet<Entry>();
          authzObjToPath.put(child.getAuthzObj(), paths);
        }
        paths.add(child);
      }
      parent.getChildren().put(child.getPathElement(), child);
      cloneToEntry(tChild, child, idMap, authzObjToPath, isChildPrefix);
    }
  }

}
