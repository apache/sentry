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

import java.util.*;

import com.google.common.base.Joiner;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A non thread-safe implementation of {@link AuthzPaths}. It abstracts over the
 * core data-structures used to efficiently handle request from clients of 
 * the {@link AuthzPaths} paths. All updates to this class is handled by the
 * thread safe {@link UpdateableAuthzPaths} class
 */
public class HMSPaths implements AuthzPaths {

  private static Logger LOG = LoggerFactory.getLogger(HMSPaths.class);

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
  static List<List<String>> getPathsElements(List<String> paths) {
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

    // A set of authorizable objects associated with this entry. Authorizable
    // object should be case insensitive.
    private Set<String> authzObjs;

    // Path of child element to the path entry mapping.
    // e.g. 'b' -> '/a/b'
    private final Map<String, Entry> children;

    Entry(Entry parent, String pathElement, EntryType type,
        String authzObj) {
      this.parent = parent;
      this.type = type;
      this.pathElement = pathElement;
      this.authzObjs = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
      addAuthzObj(authzObj);
      children = new HashMap<String, Entry>();
    }

    Entry(Entry parent, String pathElement, EntryType type,
          Set<String> authzObjs) {
      this.parent = parent;
      this.type = type;
      this.pathElement = pathElement;
      this.authzObjs = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
      addAuthzObjs(authzObjs);
      children = new HashMap<String, Entry>();
    }

    // Get all the mapping of the children element to
    // the path entries.
    public Map<String, Entry> getChildren() {
      return children;
    }

    void clearAuthzObjs() {
      authzObjs = new HashSet<String>();
    }

    void removeAuthzObj(String authzObj) {
      authzObjs.remove(authzObj);
    }

    void addAuthzObj(String authzObj) {
      if (authzObj != null) {
        authzObjs.add(authzObj);
      }
    }

    void addAuthzObjs(Set<String> authzObjs) {
      if (authzObjs != null) {
        for (String authObj : authzObjs) {
          this.authzObjs.add(authObj);
        }
      }
    }

    private void setType(EntryType type) {
      this.type = type;
    }

    protected void removeParent() {
      parent = null;
    }

    public String toString() {
      return String.format("Entry[fullPath: %s, type: %s, authObject: %s]",
          getFullPath(), type, Joiner.on(",").join(authzObjs));
    }

    /**
     * Create a child entry based on the path, type and authzObj that
     * associates with it.
     *
     * @param  pathElements  a path split into segments.
     * @param  type the type of the child entry.
     * @param  authzObj the authorizable Object associates with the entry.
     * @return  Returns the child entry.
     */
    private Entry createChild(List<String> pathElements, EntryType type,
        String authzObj) {

      // Parent entry is the current referring one.
      Entry entryParent = this;

      // Creates the entry based on the path elements (if not found) until reaches its
      // direct parent.
      for (int i = 0; i < pathElements.size() - 1; i++) {

        String pathElement = pathElements.get(i);
        Entry child = entryParent.getChildren().get(pathElement);

        if (child == null) {
          child = new Entry(entryParent, pathElement, EntryType.DIR, (String) null);
          entryParent.getChildren().put(pathElement, child);
        }

        entryParent = child;
      }

      String lastPathElement = pathElements.get(pathElements.size() - 1);
      Entry child = entryParent.getChildren().get(lastPathElement);

      // Create the child entry if not found. If found and the entry is
      // already a prefix or authzObj type, then only add the authzObj.
      // If the entry already existed as dir, we change it to be a authzObj,
      // and add the authzObj.
      if (child == null) {
        child = new Entry(entryParent, lastPathElement, type, authzObj);
        entryParent.getChildren().put(lastPathElement, child);
      } else if (type == EntryType.AUTHZ_OBJECT &&
          (child.getType() == EntryType.PREFIX || child.getType() == EntryType.AUTHZ_OBJECT)) {
        child.addAuthzObj(authzObj);
      } else if (type == EntryType.AUTHZ_OBJECT &&
          child.getType() == EntryType.DIR) {
        child.addAuthzObj(authzObj);
        child.setType(EntryType.AUTHZ_OBJECT);
      }

      return child;
    }

    public static Entry createRoot(boolean asPrefix) {
      return new Entry(null, "/", (asPrefix)
                                   ? EntryType.PREFIX : EntryType.DIR, (String) null);
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
      } else {
        LOG.info("Skipping to create authzObjPath as it is outside of prefix. authObj = " + authzObj
        + " pathElements=" + pathElements);
      }
      return entry;
    }

    public void deleteAuthzObject(String authzObj) {
      if (getParent() != null) {
        if (getChildren().isEmpty()) {

          // Remove the authzObj on the path entry. If the path
          // entry no longer maps to any authzObj, removes the
          // entry recursively.
          authzObjs.remove(authzObj);
          if (authzObjs.size() == 0) {
            getParent().getChildren().remove(getPathElement());
            getParent().deleteIfDangling();
            parent = null;
          }
        } else {

          // if the entry was for an authz object and has children, we
          // change it to be a dir entry. And remove the authzObj on
          // the path entry.
          if (getType() == EntryType.AUTHZ_OBJECT) {
            setType(EntryType.DIR);
            authzObjs.remove(authzObj);
          }
        }
      }
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
            clearAuthzObjs();
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

    public Set<String> getAuthzObjs() {
      return authzObjs;
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
        if (isPartialMatchOk && (getAuthzObjs().size() != 0)) {
          found = this;
        }
      } else {
        Entry child = getChildren().get(pathElements[index]);
        if (child != null) {
          if (index == pathElements.length - 1) {
            found = (child.getAuthzObjs().size() != 0) ? child : lastAuthObj;
          } else {
            found = child.find(pathElements, index + 1, isPartialMatchOk,
                (child.getAuthzObjs().size() != 0) ? child : lastAuthObj);
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

  // The hive authorized objects to path entries mapping.
  // One authorized object can map to a set of path entries.
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

    authzObjToPath = new TreeMap<String, Set<Entry>>(String.CASE_INSENSITIVE_ORDER);
  }

  void _addAuthzObject(String authzObj, List<String> authzObjPaths) {
    addAuthzObject(authzObj, getPathsElements(authzObjPaths));
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
          entry.deleteAuthzObject(authzObj);
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
          LOG.info("Path outside prefix");
        }
      }
      entries.addAll(newEntries);
    } else {
      if (createNew) {
        addAuthzObject(authzObj, authzObjPathElements);
      } else {
        LOG.warn("Path was not added to AuthzObject, could not find key in authzObjToPath. authzObj = " + authzObj +
                " authzObjPathElements=" + authzObjPathElements);
      }
    }
  }

  void _addPathsToAuthzObject(String authzObj, List<String> authzObjPaths) {
    addPathsToAuthzObject(authzObj, getPathsElements(authzObjPaths), false);
  }

  void addPathsToAuthzObject(String authzObj, List<List<String>> authzObjPaths) {
    addPathsToAuthzObject(authzObj, authzObjPaths, false);
  }

  /*
  1. Removes authzObj from all entries corresponding to the authzObjPathElements
  ( which also deletes the entry if no more authObjs to that path and does it recursively upwards)
  2. Removes it from value of authzObjToPath Map for this authzObj key, does not reset entries to null even if entries is empty
   */
  void deletePathsFromAuthzObject(String authzObj,
      List<List<String>> authzObjPathElements) {
    Set<Entry> entries = authzObjToPath.get(authzObj);
    if (entries != null) {
      Set<Entry> toDelEntries = new HashSet<Entry>(authzObjPathElements.size());
      for (List<String> pathElements : authzObjPathElements) {
        Entry entry = root.find(
            pathElements.toArray(new String[pathElements.size()]), false);
        if (entry != null) {
          entry.deleteAuthzObject(authzObj);
          toDelEntries.add(entry);
        } else {
          LOG.info("Path was not deleted from AuthzObject, path not registered. This is possible for implicit partition locations. authzObj = " + authzObj + " authzObjPathElements=" + authzObjPathElements);
        }
      }
      entries.removeAll(toDelEntries);
    } else {
      LOG.info("Path was not deleted from AuthzObject, could not find key in authzObjToPath. authzObj = " + authzObj +
              " authzObjPathElements=" + authzObjPathElements);
    }
  }

  void deleteAuthzObject(String authzObj) {
    Set<Entry> entries = authzObjToPath.remove(authzObj);
    if (entries != null) {
      for (Entry entry : entries) {
        entry.deleteAuthzObject(authzObj);
      }
    }
  }

  @Override
  public Set<String> findAuthzObject(String[] pathElements) {
    return findAuthzObject(pathElements, true);
  }

  @Override
  public Set<String> findAuthzObjectExactMatches(String[] pathElements) {
    return findAuthzObject(pathElements, false);
  }

  /**
   * Based on the isPartialOk flag, returns all authorizable Objects
   * (database/table/partition) associated with the path, or if no match
   * is found returns the first ancestor that has the associated
   * authorizable objects.
   *
   * @param pathElements A path split into segments.
   * @param isPartialOk Flag that indicates if patial path match is Ok or not.
   * @return Returns a set of authzObjects authzObject associated with this path.
   */
  public Set<String> findAuthzObject(String[] pathElements, boolean isPartialOk) {
    // Handle '/'
    if ((pathElements == null)||(pathElements.length == 0)) return null;
    Entry entry = root.find(pathElements, isPartialOk);
    return (entry != null) ? entry.getAuthzObjs() : null;
  }

  /*
  Following condition should be true: oldName != newName
  If oldPath == newPath, Example: rename external table (only HMS meta data is updated)
    => new_table.add(new_path), new_table.add(old_table_partition_paths), old_table.dropAllPaths.
  If oldPath != newPath, Example: rename managed table (HMS metadata is updated as well as physical files are moved to new location)
    => new_table.add(new_path), old_table.dropAllPaths.
  */
  void renameAuthzObject(String oldName, List<List<String>> oldPathElems,
      String newName, List<List<String>> newPathElems) {

    if ( oldPathElems == null || oldPathElems.size() == 0 || newPathElems == null || newPathElems.size() == 0
            || newName == null || oldName == null || oldName == newName) {
      LOG.warn("Unexpected state in renameAuthzObject, inputs invalid: oldName=" + oldName + " newName=" + newName +
              " oldPath=" + oldPathElems + " newPath=" + newPathElems);
      return;
    }

    //new_table.add(new_path)
    addPathsToAuthzObject(newName, newPathElems, true);

    //if oldPath == newPath, that is path has not changed as part of rename and hence new table needs to have old paths
    // => new_table.add(old_table_partition_paths)
    List<String> oldPathElements = oldPathElems.get(0);
    List<String> newPathElements = newPathElems.get(0);
    boolean samePaths = false;
    if(oldPathElements.size() == newPathElements.size()) {
      samePaths = true;
      for(int i=0; i<oldPathElements.size() ; i++) {
        if (!newPathElements.get(i).equalsIgnoreCase(oldPathElements.get(i))) {
          samePaths = false;
        }
      }
    }
    if(samePaths) {
      Set<Entry> eSet = authzObjToPath.get(oldName);
      if (eSet == null) {
        LOG.warn("Unexpected state in renameAuthzObject, cannot find oldName in authzObjToPath: oldName=" + oldName + " newName=" + newName +
                " oldPath=" + oldPathElems + " newPath=" + newPathElems);
      } else {
        authzObjToPath.put(newName, eSet);
        for (Entry e : eSet) {
          if (e.getAuthzObjs().contains(oldName)) {
            e.addAuthzObj(newName);
          } else {
            LOG.warn("Unexpected state in renameAuthzObject, authzObjToPath has an entry <oldName,entries> where one of the entry does not have oldName : oldName=" + oldName + " newName=" + newName +
                    " oldPath=" + oldPathElems + " newPath=" + newPathElems);
          }
        }
      }
    }

    //old_table.dropAllPaths
    deleteAuthzObject(oldName);
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
