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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class SentryAuthorizationProvider 
    extends INodeAttributeProvider implements Configurable {
  
  static class SentryAclFeature extends AclFeature {
    public SentryAclFeature(ImmutableList<AclEntry> entries) {
      super(entries);
    }
  }

  private static Logger LOG = 
      LoggerFactory.getLogger(SentryAuthorizationProvider.class);

  private boolean started;
  private Configuration conf;
  private String user;
  private String group;
  private short permission;
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

      // Configuration is read from hdfs-sentry.xml and NN configuration, in
      // that order of precedence.
      Configuration conf = new Configuration(this.conf);
      conf.addResource(SentryAuthorizationConstants.CONFIG_FILE);
      user = conf.get(SentryAuthorizationConstants.HDFS_USER_KEY,
          SentryAuthorizationConstants.HDFS_USER_DEFAULT);
      group = conf.get(SentryAuthorizationConstants.HDFS_GROUP_KEY,
          SentryAuthorizationConstants.HDFS_GROUP_DEFAULT);
      permission = (short) conf.getLong(
          SentryAuthorizationConstants.HDFS_PERMISSION_KEY,
          SentryAuthorizationConstants.HDFS_PERMISSION_DEFAULT);
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
  }

  private static final AclFeature EMPTY_ACL_FEATURE =
      new AclFeature(AclFeature.EMPTY_ENTRY_LIST);

  private class INodeAttributesX implements INodeAttributes {
    private boolean useDefault;
    private INodeAttributes node;
    private AclFeature aclFeature;
    
    public INodeAttributesX(boolean useDefault, INodeAttributes node,
        AclFeature aclFeature) {
      this.node = node;
      this.useDefault = useDefault;
      this.aclFeature = aclFeature;
    }
    
    @Override
    public boolean isDirectory() {
      return node.isDirectory();
    }

    @Override
    public byte[] getLocalNameBytes() {
      return node.getLocalNameBytes();
    }

    public String getUserName() {
      return (useDefault) ? node.getUserName() : user;
    }

    @Override
    public String getGroupName() {
      return (useDefault) ? node.getGroupName() : group;
    }

    @Override
    public FsPermission getFsPermission() {
      return (useDefault) ? node.getFsPermission()
                          : new FsPermission(getFsPermissionShort());
    }

    @Override
    public short getFsPermissionShort() {
      return (useDefault) ? node.getFsPermissionShort()
                          : (short) getPermissionLong();
    }

    @Override
    public long getPermissionLong() {
      return (useDefault) ? node.getPermissionLong() : permission;
    }

    @Override
    public AclFeature getAclFeature() {
      AclFeature feature;
      if (useDefault) {
        feature = node.getAclFeature();
        if (feature == null) {
          feature = EMPTY_ACL_FEATURE;
        }
      } else {
        feature = aclFeature;
      }
      return feature;
    }

    @Override
    public XAttrFeature getXAttrFeature() {
      return node.getXAttrFeature();
    }

    @Override
    public long getModificationTime() {
      return node.getModificationTime();
    }

    @Override
    public long getAccessTime() {
      return node.getAccessTime();
    }
  }
  
  @Override
  public INodeAttributes getAttributes(String[] pathElements,
      INodeAttributes node) {
    if (authzInfo.isManaged(pathElements)) {
      boolean stale = authzInfo.isStale();
      AclFeature aclFeature = getAclFeature(pathElements, node, stale);
      if (!stale) {
        if (authzInfo.doesBelongToAuthzObject(pathElements)) {
          node = new INodeAttributesX(false, node, aclFeature);
        }
      } else {
        node = new INodeAttributesX(true, node, aclFeature);
      }
    }
    return node;
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

  public AclFeature getAclFeature(String[] pathElements, INodeAttributes node, 
      boolean stale) {
    AclFeature f = null;
    String p = Arrays.toString(pathElements);
    boolean hasAuthzObj = false;
    List<AclEntry> list = new ArrayList<AclEntry>();
    if (originalAuthzAsAcl) {
      String user = node.getUserName();
      String group = node.getGroupName();
      FsPermission perm = node.getFsPermission();
      list.addAll(createAclEntries(user, group, perm));
    }
    if (!stale) { 
      if (authzInfo.doesBelongToAuthzObject(pathElements)) {
        hasAuthzObj = true;
        list.addAll(authzInfo.getAclEntries(pathElements));
        f = new SentryAclFeature(ImmutableList.copyOf(list));
      }
    } else {
      f = new SentryAclFeature(ImmutableList.copyOf(list));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("### getAclEntry [" + p + "] : ["
          + "isStale=" + stale
          + ",hasAuthzObj=" + hasAuthzObj
          + ",origAtuhzAsAcl=" + originalAuthzAsAcl + "]"
          + "[" + (f == null ? "null" : f.getEntries()) + "]");
    }
    return f;
  }

}
