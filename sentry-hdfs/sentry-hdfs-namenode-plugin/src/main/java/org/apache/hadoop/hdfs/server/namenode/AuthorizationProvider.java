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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Implementations of this interface are called from within an 
 * <code>inode</code> to set or return authorization related information.
 * <p/>
 * The HDFS default implementation, {@link DefaultAuthorizationProvider} uses
 * the <code>inode</code> itself to retrieve and store information.
 * <p/>
 * A custom implementation may use a different authorization store and enforce
 * the permission check using alternate logic.
 * <p/>
 * It is expected that an implementation of the provider will not call external 
 * systems or realize expensive computations on any of the methods defined by 
 * the provider interface as they are typically invoked within remote client 
 * filesystem operations.
 * <p/>
 * If calls to external systems are required, they should be done 
 * asynchronously from the provider methods.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class AuthorizationProvider {

  private static final ThreadLocal<Boolean> CLIENT_OP_TL =
      new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
          return Boolean.FALSE;
        }
      };

  static void beginClientOp() {
    CLIENT_OP_TL.set(Boolean.TRUE);
  }

  static void endClientOp() {
    CLIENT_OP_TL.set(Boolean.FALSE);
  }

  private static AuthorizationProvider provider;

  /**
   * Return the authorization provider singleton for the NameNode.
   * 
   * @return the authorization provider
   */
  public static AuthorizationProvider get() {
    return provider;  
  }

  /**
   * Set the authorization provider singleton for the NameNode. The 
   * provider must be started (before being set) and stopped by the setter.
   * 
   * @param authzProvider the authorization provider
   */
  static void set(AuthorizationProvider authzProvider) {
    provider = authzProvider;
  }

  /**
   * Constant that indicates current state (as opposed to a particular snapshot 
   * ID) when retrieving authorization information from the provider.
   */
  public static final int CURRENT_STATE_ID = Snapshot.CURRENT_STATE_ID;

  /**
   * This interface exposes INode read-only information relevant for 
   * authorization decisions.
   * 
   * @see AuthorizationProvider
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public interface INodeAuthorizationInfo {

    /**
     * Return the inode unique ID. This value never changes.
     * 
     * @return the inode unique ID.
     */
    public long getId();

    /**
     * Return the inode path element name. This value may change.
     * @return the inode path element name.
     */
    public String getLocalName();

    /**
     * Return the parent inode. This value may change.
     * 
     * @return the parent inode.
     */
    public INodeAuthorizationInfo getParent();

    /**
     * Return the inode full path. This value may change.
     *
     * @return the inode full path
     */
    public String getFullPathName();

    /**
     * Return if the inode is a directory or not.
     *
     * @return <code>TRUE</code> if the inode is a directory, 
     * <code>FALSE</code> otherwise.
     */
    public boolean isDirectory();

    /**
     * Return the inode user for the specified snapshot.
     * 
     * @param snapshotId a snapshot ID or {@link #CURRENT_STATE_ID} for latest 
     * value.
     * @return the inode user for the specified snapshot.
     */
    public String getUserName(int snapshotId);

    /**
     * Return the inode group for the specified snapshot.
     *
     * @param snapshotId a snapshot ID or {@link #CURRENT_STATE_ID} for latest
     * value.
     * @return the inode group for the specified snapshot.
     */
    public String getGroupName(int snapshotId);

    /**
     * Return the inode permission for the specified snapshot.
     *
     * @param snapshotId a snapshot ID or {@link #CURRENT_STATE_ID} for latest
     * value.
     * @return the inode permission for the specified snapshot.
     */
    public FsPermission getFsPermission(int snapshotId);

    /**
     * Return the inode ACL feature for the specified snapshot.
     *
     * @param snapshotId a snapshot ID or {@link #CURRENT_STATE_ID} for latest
     * value.
     * @return the inode ACL feature for the specified snapshot.
     */
    public AclFeature getAclFeature(int snapshotId);
    
  }

  /**
   * Indicates if the current provider method invocation is part of a client 
   * operation or it is an internal NameNode call (i.e. a FS image or an edit 
   * log  operation).
   * 
   * @return <code>TRUE</code> if the provider method invocation is being 
   * done as part of a client operation, <code>FALSE</code> otherwise.
   */
  protected final boolean isClientOp() {
    return CLIENT_OP_TL.get() == Boolean.TRUE;
  }

  /**
   * Initialize the provider. This method is called at NameNode startup 
   * time.
   */
  public void start() {    
  }

  /**
   * Shutdown the provider. This method is called at NameNode shutdown time.
   */
  public void stop() {    
  }

  /**
   * Set all currently snapshot-able directories and their corresponding last 
   * snapshot ID. This method is called at NameNode startup.
   * <p/>
   * A provider implementation that keeps authorization information on per 
   * snapshot basis can use this call to initialize/re-sync its information with
   * the NameNode snapshot-able directories information.
   * 
   * @param snapshotableDirs a map with all the currently snapshot-able 
   * directories and their corresponding last snapshot ID
   */
  public void setSnaphottableDirs(Map<INodeAuthorizationInfo, Integer> 
      snapshotableDirs) {
  }

  /**
   * Add a directory as snapshot-able.
   * <p/>
   * A provider implementation that keeps authorization information on per 
   * snapshot basis can use this call to prepare itself for snapshots on the
   * specified directory.
   * 
   * @param dir snapshot-able directory to add
   */
  public void addSnapshottable(INodeAuthorizationInfo dir) {
  }

  /**
   * Remove a directory as snapshot-able.
   * <p/>
   * A provider implementation that keeps authorization information on per 
   * snapshot basis can use this call to clean up any snapshot on the
   * specified directory.
   *
   * @param dir snapshot-able directory to remove
   */
  public void removeSnapshottable(INodeAuthorizationInfo dir) {
  }

  /**
   * Create a snapshot for snapshot-able directory.
   * <p/>
   * A provider implementation that keeps authorization information on per
   * snapshot basis can use this call to perform any snapshot related 
   * bookkeeping on the specified directory because of the snapshot creation.
   *
   * @param dir directory to make a snapshot of
   * @param snapshotId the snapshot ID to create
   */
  public void createSnapshot(INodeAuthorizationInfo dir, int snapshotId)
      throws IOException {    
  }
  
  /**
   * Remove a snapshot for snapshot-able directory.
   * <p/>
   * A provider implementation that keeps authorization information on per
   * snapshot basis can use this call to perform any snapshot related
   * bookkeeping on the specified directory because of the snapshot removal.
   *
   * @param dir directory to remove a snapshot from
   * @param snapshotId the snapshot ID to remove
   */
  public void removeSnapshot(INodeAuthorizationInfo dir, int snapshotId)
      throws IOException {
  }
  
  /**
   * Set the user for an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   * @param user user name
   */
  public abstract void setUser(INodeAuthorizationInfo node, String user);

  /**
   * Get the user of an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   * @param snapshotId snapshot ID of the inode to get the user from
   * @return the user of the inode
   */
  public abstract String getUser(INodeAuthorizationInfo node, int snapshotId);

  /**
   * Set teh group of an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   * @param group group name
   */
  public abstract void setGroup(INodeAuthorizationInfo node, String group);

  /**
   * Get the group of an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   *
   * @param node inode
   * @param snapshotId snapshot ID of the inode to get the group from
   * @return the group of the inode
   */
  public abstract String getGroup(INodeAuthorizationInfo node, int snapshotId);

  /**
   * Set the permission of an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   * @param permission the permission to set
   */
  public abstract void setPermission(INodeAuthorizationInfo node, 
      FsPermission permission);

  /**
   * Get the permission of an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   * @param snapshotId snapshot ID of the inode to get the permission from
   * @return the permission of the inode
   */
  public abstract FsPermission getFsPermission(INodeAuthorizationInfo node, 
      int snapshotId);

  /**
   * Get the ACLs of an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   * @param snapshotId snapshot ID of the inode to get the ACLs from
   * @return the ACLs of the inode
   */
  public abstract AclFeature getAclFeature(INodeAuthorizationInfo node, 
      int snapshotId);

  /**
   * Remove the ACLs of an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   */
  public abstract void removeAclFeature(INodeAuthorizationInfo node);

  /**
   * Add ACLs to an inode.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * 
   * @param node inode
   * @param f the ACLs of the inode
   */
  public abstract void addAclFeature(INodeAuthorizationInfo node, AclFeature f);

  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   * <p/>
   * This method is always call within a Filesystem LOCK.
   * <p/>
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * <p/>
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   * <p/>
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   *
   * @param user user ot check permissions against
   * @param groups groups of the user to check permissions against
   * @param inodes inodes of the path to check permissions
   * @param snapshotId snapshot ID to check permissions
   * @param doCheckOwner Require user to be the owner of the path?
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess The access required by the parent of the path.
   * @param access The access required by the path.
   * @param subAccess If path is a directory,
   * it is the access required of the path and all the sub-directories.
   * If path is not a directory, there is no effect.
   * @param ignoreEmptyDir Ignore permission checking for empty directory?
   * @throws AccessControlException
   * @throws UnresolvedLinkException
   */
  public abstract void checkPermission(String user, Set<String> groups,
      INodeAuthorizationInfo[] inodes, int snapshotId,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException, UnresolvedLinkException;

}
