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

  private static final Logger LOG = LoggerFactory.getLogger(HMSPaths.class);

  @VisibleForTesting
  static List<String> getPathElements(String path) {
    String trimmedPath = path.trim();
    if (trimmedPath.charAt(0) != Path.SEPARATOR_CHAR) {
      throw new IllegalArgumentException("It must be an absolute path: " +
          trimmedPath);
    }
    List<String> list = new ArrayList<String>(32);
    int idx = 0;
    int found = trimmedPath.indexOf(Path.SEPARATOR_CHAR, idx);
    while (found > -1) {
      if (found > idx) {
        list.add(trimmedPath.substring(idx, found));
      }
      idx = found + 1;
      found = trimmedPath.indexOf(Path.SEPARATOR_CHAR, idx);
    }
    if (idx < trimmedPath.length()) {
      list.add(trimmedPath.substring(idx));
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

  // used for more compact logging
  static List<String> assemblePaths(List<List<String>> pathElements) {
    if (pathElements == null) {
      return Collections.emptyList();
    }
    List<String> paths = new ArrayList<>(pathElements.size());
    for (List<String> path : pathElements) {
      StringBuffer sb = new StringBuffer();
      for (String elem : path) {
        sb.append(Path.SEPARATOR_CHAR).append(elem);
      }
      paths.add(sb.toString());
    }
    return paths;
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

  /**
   * Entry represents a node in the tree that {@see HMSPaths} uses to organize the auth objects.
   * This tree maps the entries in the filesystem namespace in HDFS, and the auth objects are
   * associated to each entry.
   *
   * Each individual entry in the tree contains a children map that maps the path element
   * (filename) to the child entry.
   *
   * For example, for a HDFS file or directory, "hdfs://foo/bar", it is presented in HMSPaths as the
   * following tree, of which the root node is {@link HMSPaths#root}.
   *
   * Entry("/", children: {
   *   "foo": Entry("foo", children: {
   *            "bar": Entry("bar"),
   *            "zoo": Entry("zoo"),
   *        }),
   *   "tmp": Entry("tmp"),
   *   ...
   * });
   *
   * Note that the URI scheme is not presented in the tree.
   */
  @VisibleForTesting
  static class Entry {
    private Entry parent;
    private EntryType type;
    private String pathElement;

    // A set (or single object when set size is 1) of authorizable objects associated
    // with this entry. Authorizable object should be case insensitive. The set is
    // allocated lazily to avoid wasting memory due to empty sets.
    private Object authzObjs;

    // Path of child element to the path entry mapping, e.g. 'b' -> '/a/b'
    // This is allocated lazily to avoid wasting memory due to empty maps.
    private Map<String, Entry> children;

    /**
     * Construct an Entry with one authzObj.
     *
     * @param parent the parent node. If not specified, this entry is a root entry.
     * @param pathElement the path element of this entry on the tree.
     * @param type Entry type.
     * @param authzObj the authzObj.
     */
    Entry(Entry parent, String pathElement, EntryType type, String authzObj) {
      this.parent = parent;
      this.type = type;
      this.pathElement = pathElement.intern();
      addAuthzObj(authzObj);
    }

    /**
     * Construct an Entry with a set of authz objects.
     * @param parent the parent node. If not specified, this entry is a root entry.
     * @param pathElement the path element of this entry on the tree.
     * @param type entry type.
     * @param authzObjs a collection of authz objects.
     */
    Entry(Entry parent, String pathElement, EntryType type, Collection<String> authzObjs) {
      this.parent = parent;
      this.type = type;
      this.pathElement = pathElement.intern();
      addAuthzObjs(authzObjs);
    }

    Entry getChild(String pathElement) {
      if (children == null) {
        return null;
      }
      return children.get(pathElement);
    }

    void putChild(String pathElement, Entry entry) {
      if (children == null) {
        // We allocate this map lazily and with small initial capacity to avoid
        // memory waste due to empty and underpopulated maps.
        children = new HashMap<>(2);
      }
      LOG.debug("[putChild]Adding {} as child to {}", entry.toString(), this.toString());
      children.put(pathElement.intern(), entry);
    }

    Entry removeChild(String pathElement) {
      LOG.debug("[removeChild]Removing {} from children {}", pathElement, childrenValues());
      return children.remove(pathElement);
    }

    boolean hasChildren() { return children != null && !children.isEmpty(); }

    int numChildren() { return children == null ? 0 : children.size(); }

    Collection<Entry> childrenValues() {
      return children != null ? children.values() : Collections.<Entry>emptyList();
    }

    void clearAuthzObjs() {
      LOG.debug("Clearing authzObjs from {}", this.toString());
      authzObjs = null;
    }

    void removeAuthzObj(String authzObj) {
      LOG.debug("Removing {} from {}", authzObj, authzObjs);
      if (authzObjs != null) {
        if (authzObjs instanceof Set) {
          Set<String> authzObjsSet = (Set<String>) authzObjs;
          authzObjsSet.remove(authzObj);
          if (authzObjsSet.size() == 1) {
            authzObjs = authzObjsSet.iterator().next();
          }
        } else if (authzObjs.equals(authzObj)){
          authzObjs = null;
        }
      }
    }

    void addAuthzObj(String authzObj) {
      LOG.debug("Adding {} to {}", authzObj, this.toString());
      if (authzObj != null) {
        if (authzObjs == null) {
          authzObjs = authzObj;
        } else {
          Set<String> authzObjsSet;
          if (authzObjs instanceof String) {
            if (authzObjs.equals(authzObj)) {
              return;
            } else {
              authzObjs = authzObjsSet = newTreeSetWithElement((String) authzObjs);
            }
          } else {
            authzObjsSet = (Set) authzObjs;
          }
          authzObjsSet.add(authzObj.intern());
        }
      }
      LOG.debug("Added {} to {}", authzObj, this.toString());
    }

    void addAuthzObjs(Collection<String> authzObjs) {
      LOG.debug("Adding {} to {}", authzObjs, this.toString());
      if (authzObjs != null) {
        for (String authzObj : authzObjs) {
          addAuthzObj(authzObj.intern());
        }
      }
    }

    private void setType(EntryType type) {
      this.type = type;
    }

    protected void removeParent() {
      parent = null;
    }

    @Override
    public String toString() {
      return String.format("Entry[%s:%s -> authObj: %s]",
          type, getFullPath(), authzObjsToString());
    }

    private String authzObjsToString() {
      if (authzObjs == null) {
        return "";
      } else if (authzObjs instanceof String) {
        return (String) authzObjs;
      } else {
        return Joiner.on(",").join((Set) authzObjs);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }

      Entry other = (Entry) obj;
      if (parent == null) {
        if (other.parent != null) {
          return false;
        }
      } else if (!parent.equals(other.parent)) {
        return false;
      }

      if (type == null) {
        if (other.type != null) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }

      if (pathElement == null) {
        if (other.pathElement != null) {
          return false;
        }
      } else if (!pathElement.equals(other.pathElement)) {
        return false;
      }

      if (authzObjs == null) {
        if (other.authzObjs != null) {
          return false;
        }
      } else if (!authzObjs.equals(other.authzObjs)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((parent == null) ? 0 : parent.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      result = prime * result + ((pathElement == null) ? 0 : pathElement.hashCode());
      result = prime * result + ((authzObjs == null) ? 0 : authzObjs.hashCode());

      return result;
    }

    /**
     * Create all missing parent entries for an given entry, and return the parent entry for
     * the entry.
     *
     * For example, if {@code pathElements} is ["a", "b", "c"], it creates entries "/a" and "/a/b"
     * if they do not exist, and then returns "/a/b" as the parent entry.
     *
     * @param pathElements path elements of the entry.
     * @return the direct parent entry of the given entry.
     */
    private Entry createParent(List<String> pathElements) {
      Entry parent = this;
      LOG.debug("[createParent]Trying to create entires for {} ", pathElements);
      // The loop is resilient to 0 or 1 element list.
      for (int i = 0; i < pathElements.size() - 1; i++) {
        String elem = pathElements.get(i);
        Entry child = parent.getChild(elem);

        if (child == null) {
          child = new Entry(parent, elem, EntryType.DIR, (String) null);
          parent.putChild(elem, child);
          LOG.debug("[createParent] Entry {} created", child.toString());
        }

        parent = child;
      }
      return parent;
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
      LOG.debug("[createChild] Creating child for {} with path {}", authzObj, pathElements);
      // Create all the parent entries on the path if they do not exist.
      Entry entryParent = createParent(pathElements);

      String lastPathElement = pathElements.get(pathElements.size() - 1);
      Entry child = entryParent.getChild(lastPathElement);

      // Create the child entry if not found. If found and the entry is
      // already a prefix or authzObj type, then only add the authzObj.
      // If the entry already existed as dir, we change it to be a authzObj,
      // and add the authzObj.
      if (child == null) {
        child = new Entry(entryParent, lastPathElement, type, authzObj);
        entryParent.putChild(lastPathElement, child);
        LOG.debug("Created child entry {}", child);
      } else if (type == EntryType.AUTHZ_OBJECT &&
          (child.getType() == EntryType.PREFIX || child.getType() == EntryType.AUTHZ_OBJECT)) {
        child.addAuthzObj(authzObj);
        LOG.debug("[createChild] Found Child {}, updated authzObj", child.toString());
      } else if (type == EntryType.AUTHZ_OBJECT &&
          child.getType() == EntryType.DIR) {
        child.addAuthzObj(authzObj);
        child.setType(EntryType.AUTHZ_OBJECT);
        LOG.debug("[createChild] Found Child {}, updated authzObj", child.toString());
        LOG.debug("[createChild] Updating type to", child.getType().toString());
      }

      return child;
    }

    public static Entry createRoot(boolean asPrefix) {
      LOG.debug("Creating entry for root");
      return new Entry(null, "/", asPrefix
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
      LOG.debug("Creating entries for prefix paths", pathElements.toString());
      Entry prefix = findPrefixEntry(pathElements);
      if (prefix != null) {
        throw new IllegalArgumentException(String.format(
            "%s: createPrefix(%s): cannot add prefix under an existing prefix '%s'", 
            this, pathElements, prefix.getFullPath()));
      }
      return createChild(pathElements, EntryType.PREFIX, null);
    }

    public Entry createAuthzObjPath(List<String> pathElements, String authzObj) {
      Entry entry = null;
      LOG.debug("createAuthzObjPath authzObj:{} paths: {}", authzObj, pathElements);
      Entry prefix = findPrefixEntry(pathElements);
      if (prefix != null) {
        // we only create the entry if is under a prefix, else we ignore it
        entry = createChild(pathElements, EntryType.AUTHZ_OBJECT, authzObj);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("%s: createAuthzObjPath(%s, %s): outside of prefix, skipping",
            this, authzObj, pathElements));
        }
      }
      return entry;
    }

    /**
     * Delete this entry from its parent.
     */
    private void deleteFromParent() {
      LOG.debug("[deleteFromParent] Attempting to remove path: {}", this.getFullPath());
      if (getParent() != null) {
        LOG.debug("Child in Parent Entry with path: {} is removed", getParent().getFullPath());
        getParent().removeChild(getPathElement());
        getParent().deleteIfDangling();
        parent = null;
      } else {
        LOG.warn("Parent for {} not found", this.toString());
      }
    }

    public void deleteAuthzObject(String authzObj) {
      LOG.debug("[deleteAuthzObject] Removing authObj:{} from path {}", authzObj, this.toString());
      if (getParent() != null) {
        if (!hasChildren()) {

          // Remove the authzObj on the path entry. If the path
          // entry no longer maps to any authzObj, removes the
          // entry recursively.
          if (authzObjs != null) {
            removeAuthzObj(authzObj);
          }
          if (authzObjs == null) {
            LOG.debug("Deleting path {}", this.toString());
            deleteFromParent();
          }
        } else {

          // if the entry was for an authz object and has children, we
          // change it to be a dir entry. And remove the authzObj on
          // the path entry.
          if (getType() == EntryType.AUTHZ_OBJECT) {
            LOG.debug("Entry with path: {} is changed to DIR", this.getFullPath());
            setType(EntryType.DIR);
            if (authzObjs != null) {
              removeAuthzObj(authzObj);
            }
          }
        }
      }
    }

    /**
     * Move this Entry under the new parent.
     * @param newParent the new parent.
     * @param pathElem the path element on the new path.
     *
     * @return true if success. Returns false if the target with the same name already exists.
     */
    private void moveTo(Entry newParent, String pathElem) {
      LOG.debug("Moving {} as a child to {}", this.toString(), newParent.toString());
      Preconditions.checkNotNull(newParent);
      Preconditions.checkArgument(!pathElem.isEmpty());
      if (newParent.getChild(pathElem) != null) {
        LOG.warn(String.format(
            "Attempt to move %s to %s: entry with the same name %s already exists",
            this, newParent, pathElem));
        return;
      }
      deleteFromParent();
      parent = newParent;
      parent.putChild(pathElem, this);
      pathElement = pathElem.intern();
    }

    public void delete() {
      if (getParent() != null) {
        if (!hasChildren()) {
          deleteFromParent();
        } else {
          // if the entry was for an authz object and has children, we
          // change it to be a dir entry.
          if (getType() == EntryType.AUTHZ_OBJECT) {
            setType(EntryType.DIR);
            clearAuthzObjs();
            LOG.debug("Entry with path: {} is changed to DIR", this.getFullPath());

          }
        }
      }
    }

    private void deleteIfDangling() {
      if (!hasChildren() && getType().isRemoveIfDangling()) {
        LOG.debug("Deleting {} as it is dangling", this.toString());
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

    /**
     * @return the set of auth objects. The returned set should be used only
     * for querying, not for any modifications. If you just want to find out
     * the set size or whether it's empty, use the specialized getAuthzObjsSize()
     * and isAuthzObjsEmpty() methods that performs better.
     */
    Set<String> getAuthzObjs() {
      if (authzObjs != null) {
        if (authzObjs instanceof Set) {
          return (Set<String>) authzObjs;
        } else {
          return newTreeSetWithElement((String) authzObjs);
        }
      } else {
        return Collections.<String>emptySet();
      }
    }

    int getAuthzObjsSize() {
      if (authzObjs != null) {
        if (authzObjs instanceof Set) {
          return ((Set<String>) authzObjs).size();
        } else {
          return 1;
        }
      } else {
        return 0;
      }
    }

    boolean isAuthzObjsEmpty() {
      return authzObjs == null;
    }

    private Set<String> newTreeSetWithElement(String el) {
      Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
      result.add(el);
      return result;
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
        Entry child = getChild(pathElements.get(index));
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
        if (isPartialMatchOk && !isAuthzObjsEmpty()) {
          found = this;
        }
      } else {
        Entry child = getChild(pathElements[index]);
        if (child != null) {
          if (index == pathElements.length - 1) {
            found = (!child.isAuthzObjsEmpty()) ? child : lastAuthObj;
          } else {
            found = child.find(pathElements, index + 1, isPartialMatchOk,
                (!child.isAuthzObjsEmpty()) ? child : lastAuthObj);
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
  private Map<String, Set<Entry>> authzObjToEntries;

  public HMSPaths() {
    LOG.info(toString() + " (default) Initialized");
  }

  public HMSPaths(String[] pathPrefixes) {
    boolean rootPrefix = false;
    // Copy the array to avoid external modification
    this.prefixes = Arrays.copyOf(pathPrefixes, pathPrefixes.length);
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

    authzObjToEntries = new TreeMap<String, Set<Entry>>(String.CASE_INSENSITIVE_ORDER);
    LOG.info("Sentry managed prefixes: " + prefixes.toString());
  }

  void _addAuthzObject(String authzObj, List<String> authzObjPaths) {
    addAuthzObject(authzObj, getPathsElements(authzObjPaths));
  }

  void addAuthzObject(String authzObj, List<List<String>> authzObjPathElements) {
    LOG.debug("Number of Objects: {}", authzObjToEntries.size());
    LOG.debug(String.format("%s addAuthzObject(%s, %s)",
              this, authzObj, assemblePaths(authzObjPathElements)));
    Set<Entry> previousEntries = authzObjToEntries.get(authzObj);
    Set<Entry> newEntries = new HashSet<Entry>(authzObjPathElements.size());
    for (List<String> pathElements : authzObjPathElements) {
      Entry e = root.createAuthzObjPath(pathElements, authzObj);
      if (e != null) {
        newEntries.add(e);
      } else {
        LOG.warn(String.format("%s addAuthzObject(%s, %s):" +
          " Ignoring path %s, no prefix",
          this, authzObj, assemblePaths(authzObjPathElements), pathElements));
      }
    }
    authzObjToEntries.put(authzObj, newEntries);
    LOG.debug("Path entries for {} are {}", authzObj, newEntries.toString());
    if (previousEntries != null) {
      previousEntries.removeAll(newEntries);
      if (!previousEntries.isEmpty()) {
        for (Entry entry : previousEntries) {
          LOG.debug("Removing stale path {}", entry.toString());
          entry.deleteAuthzObject(authzObj);
        }
      }
    }
  }

  void addPathsToAuthzObject(String authzObj,
      List<List<String>> authzObjPathElements, boolean createNew) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("%s addPathsToAuthzObject(%s, %s, %b)",
              this, authzObj, assemblePaths(authzObjPathElements), createNew));
    }
    Set<Entry> entries = authzObjToEntries.get(authzObj);
    if (entries != null) {
      Set<Entry> newEntries = new HashSet<Entry>(authzObjPathElements.size());
      for (List<String> pathElements : authzObjPathElements) {
        Entry e = root.createAuthzObjPath(pathElements, authzObj);
        if (e != null) {
          newEntries.add(e);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%s addPathsToAuthzObject(%s, %s, %b):" +
              " Cannot create authz obj for path %s because it is outside of prefix", 
              this, authzObj, assemblePaths(authzObjPathElements), createNew, pathElements));
          }
        }
      }
      entries.addAll(newEntries);
      LOG.debug("[addPathsToAuthzObject]Updated path entries for {} are {}", authzObj, entries.toString());
    } else {
      if (createNew) {
        LOG.debug("No paths found for Object:{}, Adding new", authzObj);
        addAuthzObject(authzObj, authzObjPathElements);
      } else {
        LOG.warn(String.format("%s addPathsToAuthzObject(%s, %s, %b):" +
          " Path was not added to AuthzObject, could not find key in authzObjToPath",
          this, authzObj, assemblePaths(authzObjPathElements), createNew));
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
    Set<Entry> entries = authzObjToEntries.get(authzObj);
    if (entries != null) {
      LOG.debug("[deletePathsFromAuthzObject] Paths for {} before delete are {}", authzObj, entries.toString());
      Set<Entry> toDelEntries = new HashSet<Entry>(authzObjPathElements.size());
      for (List<String> pathElements : authzObjPathElements) {
        Entry entry = root.find(
            pathElements.toArray(new String[pathElements.size()]), false);
        if (entry != null) {
          entry.deleteAuthzObject(authzObj);
          toDelEntries.add(entry);
        } else {
          LOG.warn(String.format("%s deletePathsFromAuthzObject(%s, %s):" +
            " Path %s was not deleted from AuthzObject, path not registered." +
            " This is possible for implicit partition locations",
            this, authzObj, assemblePaths(authzObjPathElements), pathElements));
        }
      }
      entries.removeAll(toDelEntries);
      LOG.debug("[deletePathsFromAuthzObject] Paths for {} after are {}", authzObj, entries.toString());
    } else {
      LOG.warn(String.format("%s deletePathsFromAuthzObject(%s, %s):" +
        " Path was not deleted from AuthzObject, could not find key in authzObjToPath",
        this, authzObj, assemblePaths(authzObjPathElements)));
    }
  }

    void deleteAuthzObject(String authzObj) {
      LOG.debug(String.format("%s deleteAuthzObject(%s)", this, authzObj));
      LOG.debug("Number of Objects: {}", authzObjToEntries.size());
    Set<Entry> entries = authzObjToEntries.remove(authzObj);
    if (entries != null) {
      for (Entry entry : entries) {
        entry.deleteAuthzObject(authzObj);
      }
    }
  }

  Set<String> findAuthzObject(List<String> pathElements) {
    return findAuthzObject(pathElements.toArray(new String[0]));
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
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("%s findAuthzObject(%s, %b)",
        this, Arrays.toString(pathElements), isPartialOk));
    }
    // Handle '/'
    if (pathElements == null || pathElements.length == 0) {
        return null;
    }
    Entry entry = root.find(pathElements, isPartialOk);
    Set<String> authzObjSet = (entry != null) ? entry.getAuthzObjs() : null;
    if ((authzObjSet == null || authzObjSet.isEmpty()) && LOG.isDebugEnabled()) {
      LOG.debug(String.format("%s findAuthzObject(%s, %b) - no authzObject found",
        this, Arrays.toString(pathElements), isPartialOk));
    }
    return authzObjSet;
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
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("%s renameAuthzObject({%s, %s} -> {%s, %s})",
        this, oldName, assemblePaths(oldPathElems), newName, assemblePaths(newPathElems)));
    }
    if (oldPathElems == null || oldPathElems.isEmpty() ||
        newPathElems == null || newPathElems.isEmpty() ||
        newName == null || newName.equals(oldName)) {
      LOG.warn(String.format("%s renameAuthzObject({%s, %s} -> {%s, %s})" +
        ": invalid inputs, skipping",
        this, oldName, assemblePaths(oldPathElems), newName, assemblePaths(newPathElems)));
      return;
    }

    // if oldPath == newPath, that is path has not changed as part of rename and hence new table
    // needs to have old paths => new_table.add(old_table_partition_paths)
    List<String> oldPathElements = oldPathElems.get(0);
    List<String> newPathElements = newPathElems.get(0);
    if (!oldPathElements.equals(newPathElements)) {
      Entry oldEntry = root.find(oldPathElements.toArray(new String[0]), false);
      Entry newParent = root.createParent(newPathElements);

      if (oldEntry == null) {
        LOG.warn(String.format("%s Moving old paths for renameAuthzObject({%s, %s} -> {%s, %s}) is skipped. Cannot find entry for old name",
            this, oldName, assemblePaths(oldPathElems), newName, assemblePaths(newPathElems)));
      } else {
        oldEntry.moveTo(newParent, newPathElements.get(newPathElements.size() - 1));
      }
    }

    // Re-write authObj from oldName to newName.
    Set<Entry> entries = authzObjToEntries.get(oldName);
    if (entries == null) {
      LOG.warn(String.format("%s renameAuthzObject({%s, %s} -> {%s, %s}):" +
        " cannot find oldName %s in authzObjToPath",
        this, oldName, assemblePaths(oldPathElems), newName, assemblePaths(newPathElems), oldName));
    } else {
      authzObjToEntries.put(newName, entries);
      for (Entry e : entries) {
        e.addAuthzObj(newName);

        if (e.getAuthzObjs().contains(oldName)) {
          e.removeAuthzObj(oldName);
        } else {
          LOG.warn(String.format("%s renameAuthzObject({%s, %s} -> {%s, %s}):" +
            " Unexpected state: authzObjToPath has an " +
            "entry %s where one of the authz objects does not have oldName",
            this, oldName, assemblePaths(oldPathElems), newName, assemblePaths(newPathElems), e));
        }
      }
    }

    // old_table.dropAllPaths
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

  void setAuthzObjToEntryMapping(Map<String, Set<Entry>> mapping) {
    authzObjToEntries = mapping;
  }

  /**
   * For logging: collect all path entries into a list.
   *
   * Each Entry has informative toString() implementation,
   * so we can print the returned value directly.
   *
   * Non-recursive traversal.
   */
  public Collection<Entry> getAllEntries() {
    Collection<Entry> entries = new ArrayList<>(); 
    Stack<Entry> stack = new Stack<>();
    stack.push(root);
    while (!stack.isEmpty()) {
      Entry entry = stack.pop();
      entries.add(entry);
      for (Entry child : entry.childrenValues()) { // handles entry.children == null
        stack.push(child);
      }
    }
    return entries;
  }

  @Override
  public HMSPathsDumper getPathsDump() {
    return new HMSPathsDumper(this);
  }

  @Override
  public String toString() {
    return String.format("%s:%s", getClass().getSimpleName(), Arrays.toString(prefixes));
  }

  public String dumpContent() {
    return toString() + ": " + getAllEntries();
  }

}
