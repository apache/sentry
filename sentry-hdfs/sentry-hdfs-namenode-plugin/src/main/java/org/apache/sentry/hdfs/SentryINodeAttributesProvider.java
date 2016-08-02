/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permission and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SentryINodeAttributesProvider extends INodeAttributeProvider
        implements Configurable {

  private static Logger LOG =
          LoggerFactory.getLogger(SentryINodeAttributesProvider.class);

  static class SentryAclFeature extends AclFeature {
    public SentryAclFeature(ImmutableList<AclEntry> entries) {
      super(AclEntryStatusFormat.toInt(entries));
    }
  }

  class SentryPermissionEnforcer implements AccessControlEnforcer {
    private final AccessControlEnforcer ace;

    SentryPermissionEnforcer(INodeAttributeProvider.AccessControlEnforcer ace) {
      this.ace = ace;
    }

    @Override
    public void checkPermission(String fsOwner, String supergroup,
                                UserGroupInformation callerUgi,
                                INodeAttributes[] inodeAttrs,
                                INode[] inodes, byte[][] pathByNameArr,
                                int snapshotId, String path,
                                int ancestorIndex, boolean doCheckOwner,
                                FsAction ancestorAccess,
                                FsAction parentAccess, FsAction access,
                                FsAction subAccess,
                                boolean ignoreEmptyDir) throws
            AccessControlException {
      String[] pathElems = getPathElems(pathByNameArr);
      if (pathElems != null && (pathElems.length > 1) && ("".equals(pathElems[0]))) {
        pathElems = Arrays.copyOfRange(pathElems, 1, pathElems.length);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Enforcing Permission : + " + Lists
              .newArrayList(fsOwner, supergroup, callerUgi.getShortUserName(),
                      Arrays.toString(callerUgi.getGroupNames()),
                      Arrays.toString(pathElems), ancestorAccess,
                      parentAccess, access, subAccess, ignoreEmptyDir));
      }
      ace.checkPermission(fsOwner, supergroup, callerUgi,
              inodeAttrs, inodes,
              pathByNameArr, snapshotId, path, ancestorIndex,
              doCheckOwner,
              ancestorAccess, parentAccess, access, subAccess,
              ignoreEmptyDir);
    }

    private String[] getPathElems(byte[][] pathByName) {
      String[] retVal = new String[pathByName.length];
      for (int i = 0; i < pathByName.length; i++) {
        retVal[i] = (pathByName[i] != null) ? DFSUtil.bytes2String
                (pathByName[i]) : "";
      }
      return retVal;
    }
  }

  public class SentryINodeAttributes implements INodeAttributes {

    private final INodeAttributes defaultAttributes;
    private final String[] pathElements;

    public SentryINodeAttributes(INodeAttributes defaultAttributes, String[]
            pathElements) {
      this.defaultAttributes = defaultAttributes;
      this.pathElements = pathElements;
    }

    @Override
    public boolean isDirectory() {
      return defaultAttributes.isDirectory();
    }

    @Override
    public byte[] getLocalNameBytes() {
      return defaultAttributes.getLocalNameBytes();
    }

    @Override
    public String getUserName() {
      return isSentryManaged(pathElements)?
          SentryINodeAttributesProvider.this.user : defaultAttributes.getUserName();
    }

    @Override
    public String getGroupName() {
      return isSentryManaged(pathElements)?
          SentryINodeAttributesProvider.this.group : defaultAttributes.getGroupName();
    }

    @Override
    public FsPermission getFsPermission() {
      FsPermission permission;

      if (!isSentryManaged(pathElements)) {
        permission = defaultAttributes.getFsPermission();
      } else {
        FsPermission returnPerm = SentryINodeAttributesProvider.this.permission;
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
        permission = returnPerm;
      }
      return permission;
    }

    @Override
    public short getFsPermissionShort() {
      return getFsPermission().toShort();
    }

    @Override
    public long getPermissionLong() {
      PermissionStatus permissionStatus = new PermissionStatus(getUserName(),
              getGroupName(), getFsPermission());
      // No other way to get the long permission currently
      return new INodeDirectory(0l, null, permissionStatus, 0l)
              .getPermissionLong();
    }

    /**
     * Returns hadoop acls if
     *  - Not managed
     *  - Not stale and not an auth obj
     * Returns hive:hive
     *  - If stale
     * Returns sentry acls
     *  - Otherwise, if not stale and auth obj
     **/
    @Override
    public AclFeature getAclFeature() {
      AclFeature aclFeature;
      String p = Arrays.toString(pathElements);
      boolean isPrefixed = false;
      boolean isStale = false;
      boolean hasAuthzObj = false;
      Map<String, AclEntry> aclMap = null;

      // If path is not under prefix, return hadoop acls.
      if (!authzInfo.isUnderPrefix(pathElements)) {
        isPrefixed = false;
        aclFeature = defaultAttributes.getAclFeature();
      } else if (!authzInfo.doesBelongToAuthzObject(pathElements)) {
        // If path is not managed, return hadoop acls.
        isPrefixed = true;
        aclFeature = defaultAttributes.getAclFeature();
      } else {
        // If path is managed, add original hadoop permission if originalAuthzAsAcl true.
        isPrefixed = true;
        hasAuthzObj = true;
        aclMap = new HashMap<String, AclEntry>();
        if (originalAuthzAsAcl) {
          String user = defaultAttributes.getUserName();
          String group = defaultAttributes.getGroupName();
          FsPermission perm = defaultAttributes.getFsPermission();
          addToACLMap(aclMap, createAclEntries(user, group, perm));
        } else {
          // else add hive:hive
          addToACLMap(aclMap, createAclEntries(user, group, permission));
        }
        if (!authzInfo.isStale()) {
          // if not stale return sentry acls.
          isStale = false;
          addToACLMap(aclMap, authzInfo.getAclEntries(pathElements));
          aclFeature = new SentryAclFeature(ImmutableList.copyOf(aclMap.values()));
        } else {
          // if stale return hive:hive
          isStale = true;
          aclFeature = new SentryAclFeature(ImmutableList.copyOf(aclMap.values()));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("### getAclEntry \n[" + (p == null ? "null" : p) + "] : ["
            + "isPreifxed=" + isPrefixed
            + ", isStale=" + isStale
            + ", hasAuthzObj=" + hasAuthzObj
            + ", origAuthzAsAcl=" + originalAuthzAsAcl + "]\n"
            + "[" + (aclMap == null ? "null" : aclMap) + "]\n");
      }
      return aclFeature;
    }

    @Override
    public XAttrFeature getXAttrFeature() {
      return defaultAttributes.getXAttrFeature();
    }

    @Override
    public long getModificationTime() {
      return defaultAttributes.getModificationTime();
    }

    @Override
    public long getAccessTime() {
      return defaultAttributes.getAccessTime();
    }
  }

  private boolean started;
  private SentryAuthorizationInfo authzInfo;
  private String user;
  private String group;
  private FsPermission permission;
  private boolean originalAuthzAsAcl;
  private Configuration conf;

  public SentryINodeAttributesProvider() {
  }

  private boolean isSentryManaged(final String[] pathElements) {
    return authzInfo.isSentryManaged(pathElements);
  }

  @VisibleForTesting
  SentryINodeAttributesProvider(SentryAuthorizationInfo authzInfo) {
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
  public void start() {
    if (started) {
      throw new IllegalStateException("Provider already started");
    }
    started = true;
    try {
      if (!conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
              false)) {
        throw new RuntimeException("HDFS ACLs must be enabled");
      }
      Configuration conf = new Configuration(this.conf);
      conf.addResource(SentryAuthorizationConstants.CONFIG_FILE);
      user = conf.get(SentryAuthorizationConstants.HDFS_USER_KEY,
              SentryAuthorizationConstants.HDFS_USER_DEFAULT);
      group = conf.get(SentryAuthorizationConstants.HDFS_GROUP_KEY,
              SentryAuthorizationConstants.HDFS_GROUP_DEFAULT);
      permission = FsPermission.createImmutable(
              (short) conf.getLong(SentryAuthorizationConstants
                              .HDFS_PERMISSION_KEY,
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
  public void stop() {
    LOG.debug("Stopping");
    authzInfo.stop();
  }

  @Override
  public INodeAttributes getAttributes(String[] pathElements,
                                       INodeAttributes inode) {
    Preconditions.checkNotNull(pathElements);
    pathElements = "".equals(pathElements[0]) && pathElements.length > 1 ?
            Arrays.copyOfRange(pathElements, 1, pathElements.length) :
            pathElements;
    return isSentryManaged(pathElements) ? new SentryINodeAttributes
            (inode, pathElements) : inode;
  }

  @Override
  public AccessControlEnforcer getExternalAccessControlEnforcer
          (AccessControlEnforcer defaultEnforcer) {
    return new SentryPermissionEnforcer(defaultEnforcer);
  }

  private static void addToACLMap(Map<String, AclEntry> map,
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
                        setPermission(ent.getPermission().or(aclEntry
                                .getPermission())).
                        build());
      }
    }
  }

  private static List<AclEntry> createAclEntries(String user, String group,
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
}
