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
 * See the License for the specific language governing permission and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.AuthorizationProvider;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class SentryAuthorizationProvider
    extends AuthorizationProvider implements Configurable {

  static class SentryAclFeature extends AclFeature {
    public SentryAclFeature(ImmutableList<AclEntry> entries) {
      super(entries);
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(SentryAuthorizationProvider.class);
  private static final String WARN_VISIBILITY = 
      " The result won't be visible when the path is managed by Sentry";

  private boolean started;
  private Configuration conf;
  private AuthorizationProvider defaultAuthzProvider;
  private String user;
  private String group;
  private FsPermission permission;
  private boolean originalAuthzAsAcl;
  private SentryAuthorizationInfo authzInfo;

  public SentryAuthorizationProvider() {
    this(null);
  }

  @VisibleForTesting
  SentryAuthorizationProvider(SentryAuthorizationInfo authzInfo) {
    this.authzInfo = authzInfo;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public synchronized void start() {
    if (started) {
      throw new IllegalStateException("Provider already started");
    }
    started = true;
    try {
      if (!conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, false)) {
        throw new RuntimeException("HDFS ACLs must be enabled");
      }

      defaultAuthzProvider = AuthorizationProvider.get();
      defaultAuthzProvider.start();
      // Configuration is read from hdfs-sentry.xml and NN configuration, in
      // that order of precedence.
      Configuration newConf = new Configuration(this.conf);
      newConf.addResource(SentryAuthorizationConstants.CONFIG_FILE);
      user = newConf.get(SentryAuthorizationConstants.HDFS_USER_KEY,
          SentryAuthorizationConstants.HDFS_USER_DEFAULT);
      group = newConf.get(SentryAuthorizationConstants.HDFS_GROUP_KEY,
          SentryAuthorizationConstants.HDFS_GROUP_DEFAULT);
      permission = FsPermission.createImmutable(
          (short) newConf.getLong(SentryAuthorizationConstants.HDFS_PERMISSION_KEY,
              SentryAuthorizationConstants.HDFS_PERMISSION_DEFAULT)
      );
      originalAuthzAsAcl = newConf.getBoolean(
          SentryAuthorizationConstants.INCLUDE_HDFS_AUTHZ_AS_ACL_KEY,
          SentryAuthorizationConstants.INCLUDE_HDFS_AUTHZ_AS_ACL_DEFAULT);

      LOG.info("Starting");
      LOG.info("Config: hdfs-user[{}] hdfs-group[{}] hdfs-permission[{}] " +
          "include-hdfs-authz-as-acl[{}]", new Object[]
          {user, group, permission, originalAuthzAsAcl});

      if (authzInfo == null) {
        authzInfo = new SentryAuthorizationInfo(newConf);
      }
      authzInfo.start();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public synchronized void stop() {
    LOG.debug("Stopping");
    authzInfo.stop();
    defaultAuthzProvider.stop();
    defaultAuthzProvider = null;
  }

  @Override
  public void setSnaphottableDirs(Map<INodeAuthorizationInfo, Integer>
      snapshotableDirs) {
    defaultAuthzProvider.setSnaphottableDirs(snapshotableDirs);
  }

  @Override
  public void addSnapshottable(INodeAuthorizationInfo dir) {
    defaultAuthzProvider.addSnapshottable(dir);
  }

  @Override
  public void removeSnapshottable(INodeAuthorizationInfo dir) {
    defaultAuthzProvider.removeSnapshottable(dir);
  }

  @Override
  public void createSnapshot(INodeAuthorizationInfo dir, int snapshotId)
      throws IOException{
    defaultAuthzProvider.createSnapshot(dir, snapshotId);
  }

  @Override
  public void removeSnapshot(INodeAuthorizationInfo dir, int snapshotId)
      throws IOException {
    defaultAuthzProvider.removeSnapshot(dir, snapshotId);
  }

  @Override
  public void checkPermission(String user, Set<String> groups,
      INodeAuthorizationInfo[] inodes, int snapshotId,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException, UnresolvedLinkException {
    defaultAuthzProvider.checkPermission(user, groups, inodes, snapshotId,
        doCheckOwner, ancestorAccess, parentAccess, access, subAccess,
        ignoreEmptyDir);
  }

  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  private String[] getPathElements(INodeAuthorizationInfo node) {
    return getPathElements(node, 0);
  }

  private String[] getPathElements(INodeAuthorizationInfo node, int idx) {
    String[] paths;
    INodeAuthorizationInfo parent = node.getParent();
    if (parent == null) {
      paths = idx > 0 ? new String[idx] : EMPTY_STRING_ARRAY;
    } else {
      paths = getPathElements(parent, idx + 1);
      paths[paths.length - 1 - idx] = node.getLocalName();
    }
    return paths;
  }

  private boolean isSentryManaged(final String[] pathElements) {
    return authzInfo.isSentryManaged(pathElements);
  }

  private boolean isSentryManaged(INodeAuthorizationInfo node) {
    String[] pathElements = getPathElements(node);
    return isSentryManaged(pathElements);
  }

  @Override
  public void setUser(INodeAuthorizationInfo node, String user) {
    // always fall through to defaultAuthZProvider, 
    // issue warning when the path is sentry managed
    if (isSentryManaged(node)) {
      LOG.warn("### setUser {} (sentry managed path) to {}, update HDFS." +
          WARN_VISIBILITY,
          node.getFullPathName(), user);
    }
    defaultAuthzProvider.setUser(node, user);
  }

  @Override
  public String getUser(INodeAuthorizationInfo node, int snapshotId) {
    return isSentryManaged(node)?
        this.user : defaultAuthzProvider.getUser(node, snapshotId);
  }

  @Override
  public void setGroup(INodeAuthorizationInfo node, String group) {
    // always fall through to defaultAuthZProvider, 
    // issue warning when the path is sentry managed
    if (isSentryManaged(node)) {
      LOG.warn("### setGroup {} (sentry managed path) to {}, update HDFS." +
          WARN_VISIBILITY,
          node.getFullPathName(), group);
    }
    defaultAuthzProvider.setGroup(node, group);
  }

  @Override
  public String getGroup(INodeAuthorizationInfo node, int snapshotId) {
    return isSentryManaged(node)?
        this.group : defaultAuthzProvider.getGroup(node, snapshotId);
  }

  @Override
  public void setPermission(INodeAuthorizationInfo node, FsPermission permission) {
    // always fall through to defaultAuthZProvider, 
    // issue warning when the path is sentry managed
    if (isSentryManaged(node)) {
      LOG.warn("### setPermission {} (sentry managed path) to {}, update HDFS." +
          WARN_VISIBILITY,
          node.getFullPathName(), permission.toString());
    }
    defaultAuthzProvider.setPermission(node, permission);
  }

  @Override
  public FsPermission getFsPermission(
      INodeAuthorizationInfo node, int snapshotId) {
    FsPermission returnPerm;
    String[] pathElements = getPathElements(node);
    if (!isSentryManaged(pathElements)) {
      returnPerm = defaultAuthzProvider.getFsPermission(node, snapshotId);
    } else {
      returnPerm = this.permission;
      // Handle case when prefix directory is itself associated with an
      // authorizable object (default db directory in hive)
      // An executable permission needs to be set on the the prefix directory
      // in this case.. else, subdirectories (which map to other dbs) will
      // not be travesible.
      for (String [] prefixPath : authzInfo.getPathPrefixes()) {
        if (Arrays.equals(prefixPath, pathElements)) {
          returnPerm = FsPermission.createImmutable((short)(returnPerm.toShort() | 0x01));
          break;
        }
      }
    }
    return returnPerm;
  }

  private List<AclEntry> createAclEntries(String user, String group,
      FsPermission permission) {
    List<AclEntry> list = new ArrayList<AclEntry>();
    AclEntry.Builder builder = new AclEntry.Builder();
    FsPermission fsPerm = new FsPermission(permission);
    builder.setName(user);
    builder.setType(AclEntryType.USER);
    builder.setScope(AclEntryScope.ACCESS);
    builder.setPermission(fsPerm.getUserAction());
    list.add(builder.build());
    builder.setName(group);
    builder.setType(AclEntryType.GROUP);
    builder.setScope(AclEntryScope.ACCESS);
    builder.setPermission(fsPerm.getGroupAction());
    list.add(builder.build());
    builder.setName(null);
    return list;
  }
  /*
  Returns hadoop acls if
  - Not managed
  - Not stale and not an auth obj
  Returns hive:hive
  - If stale
  Returns sentry acls
  - Otherwise, if not stale and auth obj
   */
  @Override
  public AclFeature getAclFeature(INodeAuthorizationInfo node, int snapshotId) {
    AclFeature f = null;
    String[] pathElements = getPathElements(node);
    String p = Arrays.toString(pathElements);
    boolean isPrefixed = false;
    boolean isStale = false;
    boolean hasAuthzObj = false;
    Map<String, AclEntry> aclMap = null;
    if (!authzInfo.isUnderPrefix(pathElements)) {
      isPrefixed = false;
      f = defaultAuthzProvider.getAclFeature(node, snapshotId);
    } else if (!authzInfo.doesBelongToAuthzObject(pathElements)) {
      isPrefixed = true;
      f = defaultAuthzProvider.getAclFeature(node, snapshotId);
    } else {
      isPrefixed = true;
      hasAuthzObj = true;
      aclMap = new HashMap<String, AclEntry>();
      if (originalAuthzAsAcl) {
        String newUser = defaultAuthzProvider.getUser(node, snapshotId);
        String newGroup = getDefaultProviderGroup(node, snapshotId);
        FsPermission perm = defaultAuthzProvider.getFsPermission(node, snapshotId);
        addToACLMap(aclMap, createAclEntries(newUser, newGroup, perm));
      } else {
        addToACLMap(aclMap,
            createAclEntries(this.user, this.group, this.permission));
      }
      if (!authzInfo.isStale()) {
        isStale = false;
        addToACLMap(aclMap, authzInfo.getAclEntries(pathElements));
        f = new SentryAclFeature(ImmutableList.copyOf(aclMap.values()));
      } else {
        isStale = true;
        f = new SentryAclFeature(ImmutableList.copyOf(aclMap.values()));
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("### getAclEntry \n[" + p + "] : ["
          + "isPreifxed=" + isPrefixed
          + ", isStale=" + isStale
          + ", hasAuthzObj=" + hasAuthzObj
          + ", origAuthzAsAcl=" + originalAuthzAsAcl + "]\n"
          + "[" + (aclMap == null ? "null" : aclMap) + "]\n"
          + "[" + (f == null ? "null" : f.getEntries()) + "]\n");
    }
    return f;
  }

  private void addToACLMap(Map<String, AclEntry> map,
      Collection<AclEntry> entries) {
    for (AclEntry ent : entries) {
      String key = (ent.getName() == null ? "" : ent.getName())
          + ent.getScope() + ent.getType();
      AclEntry aclEntry = map.get(key);
      if (aclEntry == null) {
        map.put(key, ent);
      } else {
        map.put(key,
            new AclEntry.Builder().
            setName(ent.getName()).
            setScope(ent.getScope()).
            setType(ent.getType()).
            setPermission(ent.getPermission().or(aclEntry.getPermission())).
            build());
      }
    }
  }

  private String getDefaultProviderGroup(INodeAuthorizationInfo node,
      int snapshotId) {
    String newGroup = defaultAuthzProvider.getGroup(node, snapshotId);
    INodeAuthorizationInfo pNode = node.getParent();
    while (newGroup == null && pNode != null) {
      newGroup = defaultAuthzProvider.getGroup(pNode, snapshotId);
      pNode = pNode.getParent();
    }
    return newGroup;
  }

  /*
   * Check if the given node has ACL, remove the ACL if so. Issue a warning
   * message when the node doesn't have ACL and warn is true.
   * TODO: We need this to maintain backward compatibility (not throw error in
   * some cases). We may remove this when we release sentry major version.
   */
  private void checkAndRemoveHdfsAcl(INodeAuthorizationInfo node,
      boolean warn) {
    AclFeature f = defaultAuthzProvider.getAclFeature(node,
        Snapshot.CURRENT_STATE_ID);
    if (f != null) {
      defaultAuthzProvider.removeAclFeature(node);
    } else {
      if (warn) {
        LOG.warn("### removeAclFeature is requested on {}, but it does not " +
            "have any acl.", node);
      }
    }
  }

  @Override
  public void removeAclFeature(INodeAuthorizationInfo node) {
    // always fall through to defaultAuthZProvider, 
    // issue warning when the path is sentry managed
    if (isSentryManaged(node)) {
      LOG.warn("### removeAclFeature {} (sentry managed path), update HDFS." +
          WARN_VISIBILITY,
          node.getFullPathName());
      // For Sentry-managed paths, client code may try to remove a 
      // non-existing ACL, ignore the request with a warning if the ACL
      // doesn't exist
      checkAndRemoveHdfsAcl(node, true);
    } else {
      defaultAuthzProvider.removeAclFeature(node);
    }
  }

  @Override
  public void addAclFeature(INodeAuthorizationInfo node, AclFeature f) {
    // always fall through to defaultAuthZProvider, 
    // issue warning when the path is sentry managed
    if (isSentryManaged(node)) {
      LOG.warn("### addAclFeature {} (sentry managed path) {}, update HDFS." +
          WARN_VISIBILITY,
          node.getFullPathName(), f.toString());
      // For Sentry-managed path, remove ACL silently before adding new ACL
      checkAndRemoveHdfsAcl(node, false);
    }
    defaultAuthzProvider.addAclFeature(node, f);
  }

}
