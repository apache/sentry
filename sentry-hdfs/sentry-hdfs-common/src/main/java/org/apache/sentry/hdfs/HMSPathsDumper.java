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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sentry.hdfs.HMSPaths.Entry;
import org.apache.sentry.hdfs.HMSPaths.EntryType;
import org.apache.sentry.hdfs.service.thrift.TPathEntry;
import org.apache.sentry.hdfs.service.thrift.TPathsDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HMSPathsDumper implements AuthzPathsDumper<HMSPaths> {

  private final HMSPaths hmsPaths;

  private static final Logger LOG = LoggerFactory.getLogger(HMSPathsDumper.class);

  static class Tuple {
    private final TPathEntry entry;
    private final int id;
    Tuple(TPathEntry entry, int id) {
      this.entry = entry;
      this.id = id;
    }
  }

  public HMSPathsDumper(HMSPaths hmsPaths) {
    this.hmsPaths = hmsPaths;
  }

  @Override
  public TPathsDump createPathsDump(boolean minimizeSize) {
    DupDetector dups = null;
    if (minimizeSize) {
      dups = new DupDetector();
      dups.detectDupPathElements(hmsPaths.getRootEntry());
    }

    AtomicInteger counter = new AtomicInteger(0);
    Map<Integer, TPathEntry> idMap = new HashMap<Integer, TPathEntry>();
    Tuple tRootTuple =
        createTPathEntry(hmsPaths.getRootEntry(), counter, idMap, dups);
    idMap.put(tRootTuple.id, tRootTuple.entry);
    cloneToTPathEntry(
        hmsPaths.getRootEntry(), tRootTuple.entry, counter, idMap, dups);
    TPathsDump dump = new TPathsDump(tRootTuple.id, idMap);

    String stringDupMsg = "";
    if (minimizeSize) {
      String[] dupStringValues = dups.getDupStringValues();
      dump.setDupStringValues(Arrays.asList(dupStringValues));
      stringDupMsg = String.format(" %d total path strings, %d duplicate strings found, " +
          "compacted to %d unique strings.", dups.nTotalStrings, dups.nDupStrings,
          dupStringValues.length);
    }
    LOG.info("Paths Dump created." + stringDupMsg);

    return dump;
  }

  private void cloneToTPathEntry(Entry parent, TPathEntry tParent,
      AtomicInteger counter, Map<Integer, TPathEntry> idMap, DupDetector dups) {
    for (Entry child : parent.childrenValues()) {
      Tuple childTuple = createTPathEntry(child, counter, idMap, dups);
      tParent.addToChildren(childTuple.id);
      cloneToTPathEntry(child, childTuple.entry, counter, idMap, dups);
    }
  }

  private Tuple createTPathEntry(Entry entry, AtomicInteger idCounter,
      Map<Integer, TPathEntry> idMap, DupDetector dups) {
    int myId = idCounter.incrementAndGet();
    List<Integer> children = entry.hasChildren() ?
        new ArrayList<Integer>(entry.numChildren()) : Collections.<Integer>emptyList();
    String pathElement = entry.getPathElement();
    String sameOrReplacementId =
        dups != null ? dups.getReplacementString(pathElement) : pathElement;
    TPathEntry tEntry = new TPathEntry(entry.getType().getByte(), sameOrReplacementId, children);
    if (!entry.isAuthzObjsEmpty()) {
      tEntry.setAuthzObjs(new ArrayList<>(entry.getAuthzObjs()));
    }
    idMap.put(myId, tEntry);
    return new Tuple(tEntry, myId);
  }

  @Override
  public HMSPaths initializeFromDump(TPathsDump pathDump) {
    HMSPaths newHmsPaths = new HMSPaths(this.hmsPaths.getPrefixes());
    TPathEntry tRootEntry = pathDump.getNodeMap().get(pathDump.getRootId());
    Entry rootEntry = newHmsPaths.getRootEntry();
    Map<String, Set<Entry>> authzObjToPath = new HashMap<String, Set<Entry>>();
    cloneToEntry(tRootEntry, rootEntry, pathDump.getNodeMap(), authzObjToPath,
        pathDump.getDupStringValues(), rootEntry.getType() == EntryType.PREFIX);
    newHmsPaths.setRootEntry(rootEntry);
    newHmsPaths.setAuthzObjToEntryMapping(authzObjToPath);

    return newHmsPaths;
  }

  private void cloneToEntry(TPathEntry tParent, Entry parent,
      Map<Integer, TPathEntry> idMap, Map<String, Set<Entry>> authzObjToPath,
      List<String> dupStringValues, boolean hasCrossedPrefix) {
    for (Integer id : tParent.getChildren()) {
      TPathEntry tChild = idMap.get(id);

      String tChildPathElement = tChild.getPathElement();
      if (tChildPathElement.charAt(0) == DupDetector.REPLACEMENT_STRING_PREFIX) {
        int dupStrIdx = Integer.parseInt(tChildPathElement.substring(1), 16);
        tChildPathElement = dupStringValues.get(dupStrIdx);
      }

      Entry child = null;
      boolean isChildPrefix = hasCrossedPrefix;
      if (!hasCrossedPrefix) {
        child = parent.getChild(tChildPathElement);
        // If we haven't reached a prefix entry yet, then child should
        // already exists.. else it is not part of the prefix
        if (child == null) {
          continue;
        }
        isChildPrefix = child.getType() == EntryType.PREFIX;
        // Handle case when prefix entry has an authzObject
        // For Eg (default table mapped to /user/hive/warehouse)
        if (isChildPrefix) {
          child.addAuthzObjs(tChild.getAuthzObjs());
        }
      }
      if (child == null) {
        child = new Entry(parent, tChildPathElement,
            EntryType.fromByte(tChild.getType()), tChild.getAuthzObjs());
      }
      if (!child.isAuthzObjsEmpty()) {
        for (String authzObj: child.getAuthzObjs()) {
          Set<Entry> paths = authzObjToPath.get(authzObj);
          if (paths == null) {
            paths = new HashSet<>();
            authzObjToPath.put(authzObj, paths);
          }
          paths.add(child);
        }
      }
      parent.putChild(child.getPathElement(), child);
      cloneToEntry(tChild, child, idMap, authzObjToPath,
          dupStringValues, isChildPrefix);
    }
  }

  /**
   * This class wraps a customized hash map that allows us to detect (most of)
   * the duplicate strings in the given tree of HMSPaths$Entry objects. The
   * hash map has fixed size to avoid bloating memory, especially in the
   * situation when there are many strings but little or no duplication. Fixed
   * table size also means that the maximum length of replacement IDs that we
   * return for duplicate strings, such as ":123", is relatively small, and thus
   * they can be effectively used to substitute duplicate strings that are just
   * slightly longer. The IDs are generated using a running index within the
   * table, so they start from ":0", but eventually can get long if the table
   * is too big.
   *
   * After calling methods in this class to detect duplicates and then to
   * obtain a possible encoded substitute for each string, getDupStringValues()
   * should be called to obtain the auxiliary string array, which contains
   * the real values of encoded duplicate strings.
   */
  private static class DupDetector {
    // The prefix that we use to distinguish between real path element
    // strings and replacement string IDs used for duplicate strings
    static final char REPLACEMENT_STRING_PREFIX = ':';
    // Hash map size chosen as a compromise between not using too much memory
    // and catching enough of duplicate strings. Should be a power of two.
    private static final int TABLE_SIZE = 16 * 1024;
    // We assume that an average replacement string looks like ":123".
    // We don't encode strings shorter than this length, because it's likely
    // that the resulting gain will be negative.
    private static final int AVG_ID_LENGTH = 4;
    // We replace a string in TPathsDump with an id only if it occurs in the
    // message at least this number of times.
    private static final int MIN_NUM_DUPLICATES = 2;
    // Strings in TPathsDump that we check for duplication. Since our table has
    // fixed size, strings with the same hashcode fall into the same table slot,
    // and the string that was added last "wins".
    private final String[] keys = new String[TABLE_SIZE];
    // During the analysis phase, each value is the number of occurrences
    // of the respective key string. Then it's the position of that string
    // in the serialized auxiliary string array.
    private final int[] values = new int[TABLE_SIZE];
    // Size of the auxiliary string array - essentially the number of duplicate
    // strings that we detected and encoded.
    private int auxArraySize;
    // For statistics/debugging
    int nTotalStrings, nDupStrings;

    /**
     * Finds duplicate strings in the tree of Entry objects with the given root,
     * and fills the internal hash map for subsequent use.
     */
    void detectDupPathElements(Entry root) {
      inspectEntry(root);

      // Iterate through the table, remove Strings that are not duplicate,
      // and associate each duplicate one with its position in the final
      // serialized auxiliary string array.
      for (int i = 0; i < TABLE_SIZE; i++) {
        if (keys[i] != null) {
          if (values[i] >= MIN_NUM_DUPLICATES) {
            values[i] = auxArraySize++;
            nDupStrings += values[i];
          } else {  // No duplication for this string
            keys[i] = null;
            values[i] = -1;  // Just to mark invalid slots
          }
        }
      }
    }

    /**
     * For the given original string, returns a shorter substitute string ID,
     * such as ":123". The ID starts with a symbol not allowed in normal HDFS
     * pathElements, allowing us to later distinguish between normal and
     * substitute pathElements. The ID (in hexadecimal format, to make IDs
     * shorter) is the index of the original string in the array returned by
     * getDupStringValues().
     *
     * @param pathElement a string that may be duplicate
     * @return if pathElement was previously found to be duplicate, returns
     *         the replacement ID as described above. Otherwise, returns the
     *         input string itself.
     */
    String getReplacementString(String pathElement) {
      int slot = pathElement.hashCode() & (TABLE_SIZE - 1);
      return pathElement.equals(keys[slot]) ?
        REPLACEMENT_STRING_PREFIX + Integer.toHexString(values[slot]) : pathElement;
    }

    /**
     * Returns the array of strings that have duplicates and therefore should
     * be substituted with respective IDs. See {@link #getReplacementString}
     */
    String[] getDupStringValues() {
      String[] auxArray = new String[auxArraySize];
      int pos = 0;
      for (int i = 0; i < TABLE_SIZE; i++) {
        if (keys[i] != null) {
          auxArray[pos++] = keys[i];
        }
      }
      return auxArray;
    }

    private void inspectEntry(Entry entry) {
      nTotalStrings++;
      String pathElement = entry.getPathElement();
      if (pathElement.length() > AVG_ID_LENGTH) {
        // In the serialized data, it doesn't make sense to replace string origS
        // with idS if origS is shorter than the average length of idS.
        int slot = pathElement.hashCode() & (TABLE_SIZE - 1);
        if (pathElement.equals(keys[slot])) {
          values[slot]++;
        } else {
          // This slot is currently empty, or there is a hash collision.
          // Either way, put pathElement there and reset the entry.
          keys[slot] = pathElement;
          values[slot] = 1;
        }
      }

      for (Entry child : entry.childrenValues()) {
        inspectEntry(child);
      }
    }
  }
}
