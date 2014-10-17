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
import org.apache.hadoop.hdfs.server.namenode.DefaultAuthorizationProvider;
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

  private static Logger LOG = 
      LoggerFactory.getLogger(SentryAuthorizationProvider.class);

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

      defaultAuthzProvider = new DefaultAuthorizationProvider();
      defaultAuthzProvider.start();
      // Configuration is read from hdfs-sentry.xml and NN configuration, in
      // that order of precedence.
      Configuration conf = new Configuration(this.conf);
      conf.addResource(SentryAuthorizationConstants.CONFIG_FILE);
      user = conf.get(SentryAuthorizationConstants.HDFS_USER_KEY,
          SentryAuthorizationConstants.HDFS_USER_DEFAULT);
      group = conf.get(SentryAuthorizationConstants.HDFS_GROUP_KEY,
          SentryAuthorizationConstants.HDFS_GROUP_DEFAULT);
      permission = FsPermission.createImmutable(
          (short) conf.getLong(SentryAuthorizationConstants.HDFS_PERMISSION_KEY,
              SentryAuthorizationConstants.HDFS_PERMISSION_DEFAULT)
      );
      originalAuthzAsAcl = conf.getBoolean(
          SentryAuthorizationConstants.INCLUDE_HDFS_AUTHZ_AS_ACL_KEY,
          SentryAuthorizationConstants.INCLUDE_HDFS_AUTHZ_AS_ACL_DEFAULT);

      LOG.info("Starting");
      LOG.info("Config: hdfs-user[{}] hdfs-group[{}] hdfs-permission[{}] " +
          "include-hdfs-authz-as-acl[{}]", new Object[]
          {user, group, permission, originalAuthzAsAcl});

      if (authzInfo == null) {
        authzInfo = new SentryAuthorizationInfo(conf);
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
      paths = (idx > 0) ? new String[idx] : EMPTY_STRING_ARRAY;
    } else {
      paths = getPathElements(parent, idx + 1);
      paths[paths.length - 1 - idx] = node.getLocalName();
    }
    return paths;
  }

  @Override
  public void setUser(INodeAuthorizationInfo node, String user) {
    defaultAuthzProvider.setUser(node, user);
  }

  @Override
  public String getUser(INodeAuthorizationInfo node, int snapshotId) {
    String user;
    String[] pathElements = getPathElements(node);
    if (!authzInfo.isManaged(pathElements)) {
      user = defaultAuthzProvider.getUser(node, snapshotId);
    } else {
      if (!authzInfo.isStale()) {
        if (authzInfo.doesBelongToAuthzObject(pathElements)) {
          user = this.user;
        } else {
          user = defaultAuthzProvider.getUser(node, snapshotId);
        }
      } else {
        user = this.user;
      }
    }
    return user;
  }

  @Override
  public void setGroup(INodeAuthorizationInfo node, String group) {
    defaultAuthzProvider.setGroup(node, group);
  }

  @Override
  public String getGroup(INodeAuthorizationInfo node, int snapshotId) {
    String group;
    String[] pathElements = getPathElements(node);
    if (!authzInfo.isManaged(pathElements)) {
      group = defaultAuthzProvider.getGroup(node, snapshotId);
    } else {
      if (!authzInfo.isStale()) {
        if (authzInfo.doesBelongToAuthzObject(pathElements)) {
          group = this.group;
        } else {
          group = defaultAuthzProvider.getGroup(node, snapshotId);
        }
      } else {
        group = this.group;
      }
    }
    return group;
  }

  @Override
  public void setPermission(INodeAuthorizationInfo node,
      FsPermission permission) {
    defaultAuthzProvider.setPermission(node, permission);
  }

  @Override
  public FsPermission getFsPermission(
      INodeAuthorizationInfo node, int snapshotId) {
    FsPermission permission;
    String[] pathElements = getPathElements(node);
    if (!authzInfo.isManaged(pathElements)) {
      permission = defaultAuthzProvider.getFsPermission(node, snapshotId);
    } else {
      if (!authzInfo.isStale()) {
        if (authzInfo.doesBelongToAuthzObject(pathElements)) {
          permission = this.permission;
        } else {
          permission = defaultAuthzProvider.getFsPermission(node, snapshotId);
        }
      } else {
        permission = this.permission;
      }
    }
    return permission;
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
    builder.setType(AclEntryType.OTHER);
    builder.setScope(AclEntryScope.ACCESS);
    builder.setPermission(fsPerm.getOtherAction());
    list.add(builder.build());
    return list;
  }

  @Override
  public AclFeature getAclFeature(INodeAuthorizationInfo node, int snapshotId) {
    AclFeature f = null;
    String[] pathElements = getPathElements(node);
    String p = Arrays.toString(pathElements);
    boolean isManaged = false;
    boolean isStale = false;
    boolean hasAuthzObj = false;
    if (!authzInfo.isManaged(pathElements)) {
      isManaged = false;
      f = defaultAuthzProvider.getAclFeature(node, snapshotId);
    } else {
      isManaged = true;
      List<AclEntry> list = new ArrayList<AclEntry>();
      if (originalAuthzAsAcl) {
        String user = defaultAuthzProvider.getUser(node, snapshotId);
        String group = defaultAuthzProvider.getGroup(node, snapshotId);
        INodeAuthorizationInfo pNode = node.getParent();
        while  (group == null && pNode != null) {
          group = defaultAuthzProvider.getGroup(pNode, snapshotId);
          pNode = pNode.getParent();
        }
        FsPermission perm = defaultAuthzProvider.getFsPermission(node, snapshotId);
        list.addAll(createAclEntries(user, group, perm));
      }
      if (!authzInfo.isStale()) { 
        isStale = false;
        if (authzInfo.doesBelongToAuthzObject(pathElements)) {
          hasAuthzObj = true;
          list.addAll(authzInfo.getAclEntries(pathElements));
          f = new SentryAclFeature(ImmutableList.copyOf(list));
        } else {
          hasAuthzObj = false;
          f = defaultAuthzProvider.getAclFeature(node, snapshotId);
        }
      } else {
        isStale = true;
        f = new SentryAclFeature(ImmutableList.copyOf(list));
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("### getAclEntry [" + (p == null ? "null" : p) + "] : ["
          + "isManaged=" + isManaged
          + ", isStale=" + isStale
          + ", hasAuthzObj=" + hasAuthzObj
          + ", origAuthzAsAcl=" + originalAuthzAsAcl + "]"
          + "[" + (f == null ? "null" : f.getEntries()) + "]");
    }
    return f;
  }

  @Override
  public void removeAclFeature(INodeAuthorizationInfo node) {
    AclFeature aclFeature = node.getAclFeature(CURRENT_STATE_ID);
    if (aclFeature.getClass() != SentryAclFeature.class) {
      defaultAuthzProvider.removeAclFeature(node);
    }
  }

  @Override
  public void addAclFeature(INodeAuthorizationInfo node, AclFeature f) {
    String[] pathElements = getPathElements(node);
    if (!authzInfo.isManaged(pathElements)) {
      defaultAuthzProvider.addAclFeature(node, f);
    }
  }

//  @Override 
//  public boolean doesAllowChanges(INodeAuthorizationInfo node) {
//    String[] pathElements = getPathElements(node);
//    if (!authzInfo.isManaged(pathElements)) {
//      return defaultAuthzProvider.doesAllowChanges(node);
//    }
//    return !authzInfo.doesBelongToAuthzObject(getPathElements(node));
//  }

}
