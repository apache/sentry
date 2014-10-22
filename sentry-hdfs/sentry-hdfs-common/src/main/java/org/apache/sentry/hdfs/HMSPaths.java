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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A non thread-safe implementation of {@link AuthzPaths}. It abstracts over the
 * core data-structures used to efficiently handle request from clients of 
 * the {@link AuthzPaths} paths. All updates to this class is handled by the
 * thread safe {@link UpdateableAuthzPaths} class
 */
public class HMSPaths implements AuthzPaths {

  @VisibleForTesting
  static List<String> getPathElements(String path) {
    path = path.trim();
    if (path.charAt(0) != Path.SEPARATOR_CHAR) {
      throw new IllegalArgumentException("It must be an absolute path: " + 
          path);
    }
    List<String> list = new ArrayList<String>(32);
    int idx = 0;
    int found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    while (found > -1) {
      if (found > idx) {
        list.add(path.substring(idx, found));
      }
      idx = found + 1;
      found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    }
    if (idx < path.length()) {
      list.add(path.substring(idx));
    }
    return list;
  }

  @VisibleForTesting
  static List<List<String>> gePathsElements(List<String> paths) {
    List<List<String>> pathsElements = new ArrayList<List<String>>(paths.size());
    for (String path : paths) {
      pathsElements.add(getPathElements(path));
    }
    return pathsElements;
  }

  @VisibleForTesting
  enum EntryType {
    DIR(true),
    PREFIX(false),
    AUTHZ_OBJECT(false);

    private boolean removeIfDangling;

    private EntryType(boolean removeIfDangling) {
      this.removeIfDangling = removeIfDangling;
    }

    public boolean isRemoveIfDangling() {
      return removeIfDangling;
    }

    public byte getByte() {
      return (byte)toString().charAt(0);
    }
    
    public static EntryType fromByte(byte b) {
      switch (b) {
      case ((byte)'D'):
        return DIR;
      case ((byte)'P'):
        return PREFIX;
      case ((byte)'A'):
        return AUTHZ_OBJECT;
      default:
        return null;
      }
    }
  }

  @VisibleForTesting
  static class Entry {
    private Entry parent;
    private EntryType type;
    private String pathElement;
    private String authzObj;
    private final Map<String, Entry> children;

    Entry(Entry parent, String pathElement, EntryType type,
        String authzObj) {
      this.parent = parent;
      this.type = type;
      this.pathElement = pathElement;
      this.authzObj = authzObj;
      children = new HashMap<String, Entry>();
    }

    private void setAuthzObj(String authzObj) {
      this.authzObj = authzObj;
    }

    private void setType(EntryType type) {
      this.type = type;
    }

    protected void removeParent() {
      parent = null;
    }

    public String toString() {
      return String.format("Entry[fullPath: %s, type: %s, authObject: %s]",
          getFullPath(), type, authzObj);
    }

    private Entry createChild(List<String> pathElements, EntryType type,
        String authzObj) {
      Entry entryParent = this;
      for (int i = 0; i < pathElements.size() - 1; i++) {
        String pathElement = pathElements.get(i);
        Entry child = entryParent.getChildren().get(pathElement);
        if (child == null) {
          child = new Entry(entryParent, pathElement, EntryType.DIR, null);
          entryParent.getChildren().put(pathElement, child);
        }
        entryParent = child;
      }
      String lastPathElement = pathElements.get(pathElements.size() - 1);
      Entry child = entryParent.getChildren().get(lastPathElement);
      if (child == null) {
        child = new Entry(entryParent, lastPathElement, type, authzObj);
        entryParent.getChildren().put(lastPathElement, child);
      } else if (type == EntryType.AUTHZ_OBJECT &&
          child.getType() == EntryType.DIR) {
        // if the entry already existed as dir, we change it  to be a authz obj
        child.setAuthzObj(authzObj);
        child.setType(EntryType.AUTHZ_OBJECT);
      }
      return child;
    }

    public static Entry createRoot(boolean asPrefix) {
      return new Entry(null, "/", (asPrefix) 
                                   ? EntryType.PREFIX : EntryType.DIR, null);
    }

    private String toPath(List<String> arr) {
      StringBuilder sb = new StringBuilder();
      for (String s : arr) {
        sb.append(Path.SEPARATOR).append(s);
      }
      return sb.toString();
    }

    public Entry createPrefix(List<String> pathElements) {
      Entry prefix = findPrefixEntry(pathElements);
      if (prefix != null) {
        throw new IllegalArgumentException(String.format(
            "Cannot add prefix '%s' under an existing prefix '%s'", 
            toPath(pathElements), prefix.getFullPath()));
      }
      return createChild(pathElements, EntryType.PREFIX, null);
    }

    public Entry createAuthzObjPath(List<String> pathElements, String authzObj) {
      Entry entry = null;
      Entry prefix = findPrefixEntry(pathElements);
      if (prefix != null) {
        // we only create the entry if is under a prefix, else we ignore it
        entry = createChild(pathElements, EntryType.AUTHZ_OBJECT, authzObj);
      }
      return entry;
    }

    public void delete() {
      if (getParent() != null) {
        if (getChildren().isEmpty()) {
          getParent().getChildren().remove(getPathElement());
          getParent().deleteIfDangling();
          parent = null;
        } else {
          // if the entry was for an authz object and has children, we
          // change it to be a dir entry.
          if (getType() == EntryType.AUTHZ_OBJECT) {
            setType(EntryType.DIR);
            setAuthzObj(null);
          }
        }
      }
    }

    private void deleteIfDangling() {
      if (getChildren().isEmpty() && getType().isRemoveIfDangling()) {
        delete();
      }
    }

    public Entry getParent() {
      return parent;
    }

    public EntryType getType() {
      return type;
    }

    public String getPathElement() {
      return pathElement;
    }

    public String getAuthzObj() {
      return authzObj;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Entry> getChildren() {
      return children;
    }

    public Entry findPrefixEntry(List<String> pathElements) {
      Preconditions.checkArgument(pathElements != null,
          "pathElements cannot be NULL");
      return (getType() == EntryType.PREFIX) 
             ? this : findPrefixEntry(pathElements, 0);
    }

    private Entry findPrefixEntry(List<String> pathElements, int index) {
      Entry prefixEntry = null;
      if (index == pathElements.size()) {
        prefixEntry = null;
      } else {
        Entry child = getChildren().get(pathElements.get(index));
        if (child != null) {
          if (child.getType() == EntryType.PREFIX) {
            prefixEntry = child;
          } else {
            prefixEntry = child.findPrefixEntry(pathElements, index + 1);
          }
        }
      }
      return prefixEntry;
    }

    public Entry find(String[] pathElements, boolean isPartialMatchOk) {
      Preconditions.checkArgument(
          pathElements != null && pathElements.length > 0,
          "pathElements cannot be NULL or empty");
      return find(pathElements, 0, isPartialMatchOk, null);
    }

    private Entry find(String[] pathElements, int index,
        boolean isPartialMatchOk, Entry lastAuthObj) {
      Entry found = null;
      if (index == pathElements.length) {
        if (isPartialMatchOk && (getType() == EntryType.AUTHZ_OBJECT)) {
          found = this;
        }
      } else {
        Entry child = getChildren().get(pathElements[index]);
        if (child != null) {
          if (index == pathElements.length - 1) {
            found = (child.getType() == EntryType.AUTHZ_OBJECT) ? child : lastAuthObj;
          } else {
            found = child.find(pathElements, index + 1, isPartialMatchOk,
                (child.getType() == EntryType.AUTHZ_OBJECT) ? child : lastAuthObj);
          }
        } else {
          if (isPartialMatchOk) {
            found = lastAuthObj;
          }
        }
      }
      return found;
    }

    public String getFullPath() {
      String path = getFullPath(this, new StringBuilder()).toString();
      if (path.isEmpty()) {
        path = Path.SEPARATOR;
      }
      return path;
    }

    private StringBuilder getFullPath(Entry entry, StringBuilder sb) {
      if (entry.getParent() != null) {
        getFullPath(entry.getParent(), sb).append(Path.SEPARATOR).append(
            entry.getPathElement());
      }
      return sb;
    }

  }

  private volatile Entry root;
  private String[] prefixes;
  private Map<String, Set<Entry>> authzObjToPath;

  public HMSPaths(String[] pathPrefixes) {
    boolean rootPrefix = false;
    this.prefixes = pathPrefixes;
    for (String pathPrefix : pathPrefixes) {
      rootPrefix = rootPrefix || pathPrefix.equals(Path.SEPARATOR);
    }
    if (rootPrefix && pathPrefixes.length > 1) {
      throw new IllegalArgumentException(
          "Root is a path prefix, there cannot be other path prefixes");
    }
    root = Entry.createRoot(rootPrefix);
    if (!rootPrefix) {
      for (String pathPrefix : pathPrefixes) {
        root.createPrefix(getPathElements(pathPrefix));
      }
    }
    authzObjToPath = new HashMap<String, Set<Entry>>();
  }

  void _addAuthzObject(String authzObj, List<String> authzObjPaths) {
    addAuthzObject(authzObj, gePathsElements(authzObjPaths));
  }

  void addAuthzObject(String authzObj, List<List<String>> authzObjPathElements) {
    Set<Entry> previousEntries = authzObjToPath.get(authzObj);
    Set<Entry> newEntries = new HashSet<Entry>(authzObjPathElements.size());
    for (List<String> pathElements : authzObjPathElements) {
      Entry e = root.createAuthzObjPath(pathElements, authzObj);
      if (e != null) {
        newEntries.add(e);
      } else {
        // LOG WARN IGNORING PATH, no prefix
      }
    }
    authzObjToPath.put(authzObj, newEntries);
    if (previousEntries != null) {
      previousEntries.removeAll(newEntries);
      if (!previousEntries.isEmpty()) {
        for (Entry entry : previousEntries) {
          entry.delete();
        }
      }
    }
  }

  void addPathsToAuthzObject(String authzObj,
      List<List<String>> authzObjPathElements, boolean createNew) {
    Set<Entry> entries = authzObjToPath.get(authzObj);
    if (entries != null) {
      Set<Entry> newEntries = new HashSet<Entry>(authzObjPathElements.size());
      for (List<String> pathElements : authzObjPathElements) {
        Entry e = root.createAuthzObjPath(pathElements, authzObj);
        if (e != null) {
          newEntries.add(e);
        } else {
          // LOG WARN IGNORING PATH, no prefix
        }
      }
      entries.addAll(newEntries);
    } else {
      if (createNew) {
        addAuthzObject(authzObj, authzObjPathElements);
      }
      // LOG WARN object does not exist
    }
  }

  void _addPathsToAuthzObject(String authzObj, List<String> authzObjPaths) {
    addPathsToAuthzObject(authzObj, gePathsElements(authzObjPaths), false);
  }

  void addPathsToAuthzObject(String authzObj, List<List<String>> authzObjPaths) {
    addPathsToAuthzObject(authzObj, authzObjPaths, false);
  }

  void deletePathsFromAuthzObject(String authzObj,
      List<List<String>> authzObjPathElements) {
    Set<Entry> entries = authzObjToPath.get(authzObj);
    if (entries != null) {
      Set<Entry> toDelEntries = new HashSet<Entry>(authzObjPathElements.size());
      for (List<String> pathElements : authzObjPathElements) {
        Entry entry = root.find(
            pathElements.toArray(new String[pathElements.size()]), false);
        if (entry != null) {
          entry.delete();
          toDelEntries.add(entry);
        } else {
          // LOG WARN IGNORING PATH, it was not in registered
        }
      }
      entries.removeAll(toDelEntries);
    } else {
      // LOG WARN object does not exist
    }
  }

  void deleteAuthzObject(String authzObj) {
    Set<Entry> entries = authzObjToPath.remove(authzObj);
    if (entries != null) {
      for (Entry entry : entries) {
        entry.delete();
      }
    }
  }

  @Override
  public String findAuthzObject(String[] pathElements) {
    return findAuthzObject(pathElements, true);
  }

  @Override
  public String findAuthzObjectExactMatch(String[] pathElements) {
    return findAuthzObject(pathElements, false);
  }

  public String findAuthzObject(String[] pathElements, boolean isPartialOk) {
    // Handle '/'
    if ((pathElements == null)||(pathElements.length == 0)) return null;
    String authzObj = null;
    Entry entry = root.find(pathElements, isPartialOk);
    if (entry != null) {
      authzObj = entry.getAuthzObj();
    }
    return authzObj;
  }

  boolean renameAuthzObject(String oldName, List<String> oldPathElems,
      String newName, List<String> newPathElems) {
    // Handle '/'
    if ((oldPathElems == null)||(oldPathElems.size() == 0)) return false;
    Entry entry =
        root.find(oldPathElems.toArray(new String[oldPathElems.size()]), false);
    if ((entry != null)&&(entry.getAuthzObj().equals(oldName))) {
      // Update pathElements
      for (int i = newPathElems.size() - 1; i > -1; i--) {
        if (entry.parent != null) {
          if (!entry.pathElement.equals(newPathElems.get(i))) {
            entry.parent.getChildren().put(newPathElems.get(i), entry);
            entry.parent.getChildren().remove(entry.pathElement);
          }
        }
        entry.pathElement = newPathElems.get(i);
        entry = entry.parent;
      }
      // This would be true only for table rename
      if (!oldName.equals(newName)) {
        Set<Entry> eSet = authzObjToPath.get(oldName);
        authzObjToPath.put(newName, eSet);
        for (Entry e : eSet) {
          if (e.getAuthzObj().equals(oldName)) {
            e.setAuthzObj(newName);
          }
        }
        authzObjToPath.remove(oldName);
      }
    }
    return true;
  }

  @Override
  public boolean isUnderPrefix(String[] pathElements) {
    return root.findPrefixEntry(Lists.newArrayList(pathElements)) != null;
  }

  // Used by the serializer
  String[] getPrefixes() {
    return prefixes;
  }

  Entry getRootEntry() {
    return root;
  }

  void setRootEntry(Entry root) {
    this.root = root;
  }

  void setAuthzObjToPathMapping(Map<String, Set<Entry>> mapping) {
    authzObjToPath = mapping;
  }

  @Override
  public HMSPathsDumper getPathsDump() {
    return new HMSPathsDumper(this);
  }

}
