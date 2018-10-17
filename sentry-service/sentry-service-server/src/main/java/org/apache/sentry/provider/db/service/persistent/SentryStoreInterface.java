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

package org.apache.sentry.provider.db.service.persistent;

import com.codahale.metrics.Gauge;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sentry.SentryOwnerInfo;
import org.apache.sentry.api.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.api.service.thrift.TSentryAuthorizable;
import org.apache.sentry.api.service.thrift.TSentryGroup;
import org.apache.sentry.api.service.thrift.TSentryMappingData;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.api.service.thrift.TSentryPrivilegeMap;
import org.apache.sentry.api.service.thrift.TSentryRole;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.UniquePathsUpdate;
import org.apache.sentry.hdfs.Updateable.Update;
import org.apache.sentry.provider.db.service.classification.InterfaceAudience.Private;
import org.apache.sentry.provider.db.service.model.MAuthzPathsMapping;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryHmsNotification;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.service.common.ServiceConstants.SentryPrincipalType;

/**
 * Interface for backend sentry store.
 * NOTE: No guarantee is provided as to reliability or stability across any level of release
 * granularity. This interface can change any time for both major and minor release.
 */
@Private
public interface SentryStoreInterface {

  /**
   * Assign a given role to a set of groups.
   *
   * @param grantorPrincipal grantorPrincipal currently is not used.
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @throws Exception
   */
  void alterSentryRoleAddGroups(final String grantorPrincipal,
                                final String roleName,
                                final Set<TSentryGroup> groupNames) throws Exception;

  /**
   * Assign a given role to a set of users.
   *
   * @param roleName the role to be assigned to the users.
   * @param userNames the list of users to be added to the role,
   * @throws Exception
   */
  void alterSentryRoleAddUsers(final String roleName,
                               final Set<String> userNames) throws Exception;

  /**
   * Revoke a given role to a set of groups.
   *
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @throws Exception
   */
  void alterSentryRoleDeleteGroups(final String roleName,
                                   final Set<TSentryGroup> groupNames) throws Exception;

  /**
   * Revoke a given role from a set of users.
   *
   * @param roleName the role to be revoked from the users.
   * @param userNames the list of users to be revoked from the role,
   * @throws Exception
   */
  void alterSentryRoleDeleteUsers(final String roleName,
                                  final Set<String> userNames) throws Exception;
  /**
   * Alter a given sentry role to grant a set of privileges.
   * Internally calls alterSentryRoleGrantPrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName Role name
   * @param privileges Set of privileges
   * @throws Exception
   */
  void alterSentryRoleGrantPrivileges(final String grantorPrincipal,
                                      final String roleName,
                                      final Set<TSentryPrivilege> privileges) throws Exception;

  /**
   * Alter a given sentry role to revoke a set of privileges.
   * Internally calls alterSentryRoleRevokePrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param tPrivileges a Set of privileges
   * @throws Exception
   *
   */
  void alterSentryRoleRevokePrivileges(final String grantorPrincipal,
                                       final String roleName,
                                       final Set<TSentryPrivilege> tPrivileges)
    throws Exception;

  /**
   * Create a sentry role and persist it. Role name is the primary key for the
   * role, so an attempt to create a role which exists fails with JDO exception.
   *
   * @param roleName: Name of the role being persisted.
   *    The name is normalized.
   * @throws Exception
   */
  void createSentryRole(final String roleName) throws Exception;

  /**
   * Drop the given privilege from all roles.
   *
   * @param tAuthorizable the given authorizable object.
   * @throws Exception
   */
  void dropPrivilege(final TSentryAuthorizable tAuthorizable) throws Exception;

  /**
   * Drop a given sentry role.
   *
   * @param roleName the given role name
   * @throws Exception
   */
  void dropSentryRole(final String roleName) throws Exception;

  /**
   * Get role names for groups.
   * @param groups the given group names
   * @return set of role names for the given groups.
   * @throws Exception
   */
  Set<String> getRoleNamesForGroups(final Set<String> groups) throws Exception;

  /**
   * Thrift sentry privileges by authorizables.
   * @param groups
   * @param activeRoles
   * @param authHierarchy
   * @param isAdmin
   * @return Map of thrift sentry privileges by authorizables.
   * @throws Exception
   */
  TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(
    Set<String> groups, TSentryActiveRoleSet activeRoles,
    TSentryAuthorizable authHierarchy, boolean isAdmin)
    throws Exception;

  /**
   * Get all privileges associated with the authorizable and input users
   * @param userNames the users to get their privileges
   * @param authHierarchy the authorizables
   * @param isAdmin true: user is admin; false: is not admin
   * @return the privilege map. The key is user name
   * @throws Exception
   */
  TSentryPrivilegeMap listSentryPrivilegesByAuthorizableForUser(
    Set<String> userNames,
    TSentryAuthorizable authHierarchy,
    boolean isAdmin) throws Exception;

  /**
   * Gets sentry privilege objects for criteria from the persistence layer
   * @param entityType : the type of the entity (required)
   * @param entityNames : entity names to look up (required)
   * @param authHierarchy : filter push down based on auth hierarchy (optional)
   * @return : Set of thrift sentry privilege objects
   * @throws SentryInvalidInputException
   */
  Set<TSentryPrivilege> getTSentryPrivileges(SentryPrincipalType entityType,
                                             Set<String> entityNames,
                                             TSentryAuthorizable authHierarchy)
    throws Exception;

  /**
   * Gets sentry privilege objects for a given roleName from the persistence layer
   * @param roleName : roleName to look up
   * @return : Set of thrift sentry privilege objects
   * @throws Exception
   */
  Set<TSentryPrivilege> getAllTSentryPrivilegesByRoleName(String roleName)
    throws Exception;

  /**
   * Gets sentry privilege objects for a given userName from the persistence layer
   * @param userName : userName to look up
   * @return : Set of thrift sentry privilege objects
   * @throws Exception
   */
  Set<TSentryPrivilege> getAllTSentryPrivilegesByUserName(String userName) throws Exception;

  /**
   * Gets sentry privileges granted to the given user and groups from the
   * persistence layer. This method is only intent to be used for provider.
   * @param groups the set of group names
   * @param users the set of user names
   * @param roleSet the active roleSet
   * @param authHierarchy filter push down based on auth hierarchy (optional)
   * @return a set of sentry privilege string belongs to the given users
   *         and groups.
   * @throws Exception
   */
  Set<String> listSentryPrivilegesForProvider(Set<String> groups,
                                              Set<String> users,
                                              TSentryActiveRoleSet roleSet,
                                              TSentryAuthorizable authHierarchy)
    throws Exception;

  /**
   * Similar to {@link SentryStoreInterface#listSentryPrivilegesForProvider(Set, Set,
   * TSentryActiveRoleSet, TSentryAuthorizable)}, but returns a set of thrift sentry
   * privilege objects instead.
   * @param groups the set of group names
   * @param users the set of user names
   * @param roleSet the active roleSet
   * @param authHierarchy filter push down based on auth hierarchy (optional)
   * @return a set of thrift sentry privilege objects belongs to the given
   *         users and groups.
   * @throws Exception
   */
  Set<TSentryPrivilege> listSentryPrivilegesByUsersAndGroups(Set<String> groups,
                                                             Set<String> users,
                                                             TSentryActiveRoleSet roleSet,
                                                             TSentryAuthorizable authHierarchy)
    throws Exception;

  /**
   * True if the given set of group
   * @param groups
   * @param users
   * @param roleSet
   * @param server
   * @return
   * @throws Exception
   */
  boolean hasAnyServerPrivileges(Set<String> groups,
                                 Set<String> users,
                                 TSentryActiveRoleSet roleSet,
                                 String server) throws Exception;

  /**
   * Return set of roles corresponding to the groups provided.<p>
   *
   * If groups contain a null group, return all available roles.<p>
   *
   * Everything is done in a single transaction so callers get a
   * fully-consistent view of the roles, so this can be called at the same tie as
   * some other method that modifies groups or roles.<p>
   *
   * <em><b>NOTE:</b> This function is performance-critical, so before you modify it, make
   * sure to measure performance effect. It is called every time when PolicyClient
   * (Hive or Impala) tries to get list of roles.
   * </em>
   *
   * @param groupNames Set of Sentry groups. Can contain {@code null}
   *                  in which case all roles should be returned
   * @param checkAllGroups If false, raise SentryNoSuchObjectException
   *                      if one of the groups is not available, otherwise
   *                      ignore non-existent groups
   * @return Set of TSentryRole toles corresponding to the given set of groups.
   * @throws SentryNoSuchObjectException if one of the groups is not present and
   * checkAllGroups is not set.
   * @throws Exception if DataNucleus operation fails.
   */
  Set<TSentryRole> getTSentryRolesByGroupName(final Set<String> groupNames,
                                              final boolean checkAllGroups)
    throws Exception;


  Set<TSentryRole> getTSentryRolesByUserNames(final Set<String> users)
    throws Exception;

  /**
   * Rename the privilege for all roles. Drop the old privilege name and create the new one.
   *
   * @param oldTAuthorizable the old authorizable name needs to be renamed.
   * @param newTAuthorizable the new authorizable name
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  void renamePrivilege(final TSentryAuthorizable oldTAuthorizable,
                       final TSentryAuthorizable newTAuthorizable) throws Exception;


  /**
   * @return mapping data for [role,privilege] with the specific auth object
   */
  Map<String, Set<TSentryPrivilege>> getRoleNameTPrivilegesMap(final String dbName,
                                                               final String tableName)
    throws Exception;

  /** get mapping datas for [group,role], [user,role] with the specific roles */
  List<Map<String, Set<String>>> getGroupUserRoleMapList(final Collection<String> roleNames)
    throws Exception;

  /**
   * Import the sentry mapping data.
   *
   * @param tSentryMappingData
   *        Include 2 maps to save the mapping data, the following is the example of the data
   *        structure:
   *        for the following mapping data:
   *        user1=role1,role2
   *        user2=role2,role3
   *        group1=role1,role2
   *        group2=role2,role3
   *        role1=server=server1->db=db1
   *        role2=server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2
   *        role3=server=server1->url=hdfs://localhost/path
   *
   *        The GroupRolesMap in TSentryMappingData will be saved as:
   *        {
   *        TSentryGroup(group1)={role1, role2},
   *        TSentryGroup(group2)={role2, role3}
   *        }
   *        The UserRolesMap in TSentryMappingData will be saved as:
   *        {
   *        TSentryUser(user1)={role1, role2},
   *        TSentryGroup(user2)={role2, role3}
   *        }
   *        The RolePrivilegesMap in TSentryMappingData will be saved as:
   *        {
   *        role1={TSentryPrivilege(server=server1->db=db1)},
   *        role2={TSentryPrivilege(server=server1->db=db1->table=tbl1),
   *        TSentryPrivilege(server=server1->db=db1->table=tbl2)},
   *        role3={TSentryPrivilege(server=server1->url=hdfs://localhost/path)}
   *        }
   * @param isOverwriteForRole
   *        The option for merging or overwriting the existing data during import, true for
   *        overwriting, false for merging
   */
  void importSentryMetaData(final TSentryMappingData tSentryMappingData,
                            final boolean isOverwriteForRole) throws Exception;

  /**
   * Removes all the information related to HMS Objects from sentry store.
   */
  void clearHmsPathInformation() throws Exception;

  /**
   * Stop Sentry Store
   */
  void stop();

  /**
   * Set the notification ID of last processed HMS notification and remove all
   * subsequent notifications stored.
   */
  void setLastProcessedNotificationID(final Long notificationId) throws Exception;

  /**
   * Checks if a notification was already processed by searching for the hash value
   * on the MSentryPathChange table.
   *
   * @param hash A SHA-1 hex hash that represents a unique notification
   * @return True if the notification was already processed; False otherwise
   */
  boolean isNotificationProcessed(final String hash) throws Exception;

  /**
   * Tells if there are any records in MSentryHmsNotification
   *
   * @return true if there are no entries in <code>MSentryHmsNotification</code>
   * false if there are entries
   * @throws Exception
   */
  boolean isHmsNotificationEmpty() throws Exception;

  /**
   * Get the notification ID of last processed path delta change.
   *
   * @return the notification ID of latest path change. If no change
   *         found then return 0.
   */
  Long getLastProcessedNotificationID() throws Exception;

  /**
   * Set the notification ID of last processed HMS notification.
   */
  void persistLastProcessedNotificationID(final Long notificationId) throws Exception;

  /**
   * Set persistent update deltas
   * @param persistUpdateDeltas
   */
  void setPersistUpdateDeltas(boolean persistUpdateDeltas);

  /**
   * Purge delta change tables, {@link MSentryPermChange} and {@link MSentryPathChange}.
   * The number of deltas to keep is configurable
   */
  void purgeDeltaChangeTables();

  /**
   * Purge hms notification id table , {@link MSentryHmsNotification}.
   * The number of notifications id's to be kept is based on configuration
   * sentry.server.delta.keep.count
   */
  void purgeNotificationIdTable();

  /**
   * Return counter wait
   * @return
   */
  CounterWait getCounterWait();

  // Metrics

  /**
   * @return number of roles
   */
  Gauge<Long> getRoleCountGauge();

  /**
   * @return number of groups
   */
  Gauge<Long> getGroupCountGauge();

  /**
   * @return number of threads waiting for HMS notifications to be processed
   */
  Gauge<Integer> getHMSWaitersCountGauge();

  /**
   * @return Number of privileges
   */
  Gauge<Long> getPrivilegeCountGauge();

  /**
   * @return current value of last processed notification ID
   */
  Gauge<Long> getLastNotificationIdGauge();

  /**
   * @return ID of the path snapshot
   */
  Gauge<Long> getLastPathsSnapshotIdGauge();

  /**
   * @return Permissions change ID
   */
  Gauge<Long> getPermChangeIdGauge();

  /**
   * @return Path change id
   */
  Gauge<Long> getPathChangeIdGauge();

  /**
   * @return Number of objects persisted
   */
  Gauge<Long> getAuthzObjectsCountGauge();

  /**
   * @return Number of objects persisted
   */
  Gauge<Long> getAuthzPathsCountGauge();

  /**
   * Assign a given role to a set of groups. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param grantorPrincipal grantorPrincipal currently is not used.
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @param update the corresponding permission delta update
   * @throws Exception
   */
  void alterSentryRoleAddGroups(final String grantorPrincipal,
                                final String roleName,
                                final Set<TSentryGroup> groupNames,
                                final Update update) throws Exception;

  /**
   * Revoke a given role to a set of groups. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param roleName the role to be assigned to the groups.
   * @param groupNames the list of groups to be added to the role,
   * @param update the corresponding permission delta update
   * @throws Exception
   */
  void alterSentryRoleDeleteGroups(final String roleName,
                                   final Set<TSentryGroup> groupNames,
                                   final Update update) throws Exception;

  /**
   * Alter a given sentry role to grant a set of privileges, as well as persist the
   * corresponding permission change to MSentryPermChange table in a single transaction.
   * Internally calls alterSentryRoleGrantPrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param privileges a Set of privileges
   * @param privilegesUpdateMap the corresponding <privilege, DeltaTransactionBlock> map
   * @throws Exception
   *
   */
  void alterSentryRoleGrantPrivileges(final String grantorPrincipal,
                                      final String roleName,
                                      final Set<TSentryPrivilege> privileges,
                                      final Map<TSentryPrivilege, Update> privilegesUpdateMap)
    throws Exception;

  /**
   * Alter a given sentry role to revoke a set of privileges, as well as persist the
   * corresponding permission change to MSentryPermChange table in a single transaction.
   * Internally calls alterSentryRoleRevokePrivilege.
   *
   * @param grantorPrincipal User name
   * @param roleName the given role name
   * @param tPrivileges a Set of privileges
   * @param privilegesUpdateMap the corresponding <privilege, Update> map
   * @throws Exception
   *
   */
  void alterSentryRoleRevokePrivileges(final String grantorPrincipal,
                                       final String roleName, final Set<TSentryPrivilege> tPrivileges,
                                       final Map<TSentryPrivilege, Update> privilegesUpdateMap)
    throws Exception;

  /**
   * Drop the given privilege from all roles. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param tAuthorizable the given authorizable object.
   * @param update the corresponding permission delta update.
   * @throws Exception
   */
  void dropPrivilege(final TSentryAuthorizable tAuthorizable,
                     final Update update) throws Exception;

  /**
   * Drop a given sentry role. As well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   *
   * @param roleName the given role name
   * @param update the corresponding permission delta update
   * @throws Exception
   */
  void dropSentryRole(final String roleName, final Update update) throws Exception;

  /**
   * Retrieves an up-to-date sentry permission snapshot.
   * <p>
   * It reads hiveObj to &lt role, privileges &gt mapping from {@link MSentryPrivilege}
   * table and role to groups mapping from {@link MSentryGroup}.
   * It also gets the changeID of latest delta update, from {@link MSentryPathChange}, that
   * the snapshot corresponds to.
   *
   * @return a {@link PathsImage} contains the mapping of hiveObj to
   *         &lt role, privileges &gt and the mapping of role to &lt Groups &gt.
   *         For empty image returns
   *         {@link org.apache.sentry.core.common.utils.SentryConstants#EMPTY_CHANGE_ID}
   *         and empty maps.
   * @throws Exception
   */
  PermissionsImage retrieveFullPermssionsImage() throws Exception;

  /**
   * Retrieves an up-to-date hive paths snapshot.
   * The image only contains PathsDump in it.
   * <p>
   * It reads hiveObj to paths mapping from {@link MAuthzPathsMapping} table and
   * gets the changeID of latest delta update, from {@link MSentryPathChange}, that
   * the snapshot corresponds to.
   *
   * @param prefixes path of Sentry managed prefixes. Ignore any path outside the prefix.
   * @return an up-to-date hive paths snapshot contains mapping of hiveObj to &lt Paths &gt.
   *         For empty image return
   *         {@link org.apache.sentry.core.common.utils.SentryConstants#EMPTY_CHANGE_ID}
   *         and a empty map.
   * @throws Exception
   */
  PathsUpdate retrieveFullPathsImageUpdate(final String[] prefixes) throws Exception;

  /**
   * Rename the privilege for all roles. Drop the old privilege name and create the new one.
   * As well as persist the corresponding permission change to MSentryPermChange table in a
   * single transaction.
   *
   * @param oldTAuthorizable the old authorizable name needs to be renamed.
   * @param newTAuthorizable the new authorizable name
   * @param update the corresponding permission delta update.
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  void renamePrivilege(final TSentryAuthorizable oldTAuthorizable,
                       final TSentryAuthorizable newTAuthorizable,
                       final Update update)
    throws Exception;

  /**
   * Gets a list of MSentryPermChange objects greater than or equal to the given ChangeID.
   * If there is any path delta missing in {@link MSentryPermChange} table, an empty list is returned.
   *
   * @param changeID Requested changeID
   * @return a list of MSentryPathChange objects. May be empty.
   * @throws Exception
   */
  List<MSentryPermChange> getMSentryPermChanges(final long changeID) throws Exception;

  /**
   * Checks if any MSentryPermChange object exists with the given changeID.
   *
   * @param changeID
   * @return true if found the MSentryPermChange object, otherwise false.
   * @throws Exception
   */
  Boolean permChangeExists(final long changeID) throws Exception;

  /**
   * Gets the last processed change ID for perm delta changes.
   * @return latest perm change ID.
   */
  Long getLastProcessedPermChangeID() throws Exception;

  /**
   * Gets a list of MSentryPathChange objects greater than or equal to the given changeID.
   * If there is any path delta missing in {@link MSentryPathChange} table, an empty list is returned.
   *
   * @param changeID  Requested changeID
   * @return a list of MSentryPathChange objects. May be empty.
   * @throws Exception
   */
  List<MSentryPathChange> getMSentryPathChanges(final long changeID) throws Exception;

  /**
   * Checks if any MSentryPathChange object exists with the given changeID.
   *
   * @param changeID
   * @return true if found the MSentryPathChange object, otherwise false.
   * @throws Exception
   */
  Boolean pathChangeExists(final long changeID) throws Exception;

  /**
   * Gets the last processed change ID for path delta changes.
   *
   * @return latest path change ID.
   */
  Long getLastProcessedPathChangeID() throws Exception;

  /**
   * Persist an up-to-date HMS snapshot into Sentry DB in a single transaction with its latest
   * notification ID
   *
   * @param authzPaths paths to be be persisted
   * @param notificationID the latest notificationID associated with the snapshot
   * @throws Exception
   */
  void persistFullPathsImage(final Map<String, Collection<String>> authzPaths,
                             final long notificationID) throws Exception;

  /**
   * Adds the authzObj and with a set of paths into the authzObj -> [Paths] mapping.
   * As well as persist the corresponding delta path change to MSentryPathChange
   * table in a single transaction.
   *
   * @param authzObj an authzObj
   * @param paths a set of paths need to be added into the authzObj -> [Paths] mapping
   * @param update the corresponding path delta update
   * @throws Exception
   */
  void addAuthzPathsMapping(final String authzObj,
                            final Collection<String> paths,
                            final UniquePathsUpdate update) throws Exception;

  /**
   * Deletes all entries of the given authzObj from the authzObj -> [Paths] mapping.
   * As well as persist the corresponding delta path change to MSentryPathChange
   * table in a single transaction.
   *
   * @param authzObj an authzObj to be deleted
   * @param update the corresponding path delta update
   */
  void deleteAllAuthzPathsMapping(final String authzObj, final UniquePathsUpdate update)
    throws Exception;

  /**
   * Deletes a set of paths belongs to given authzObj from the authzObj -> [Paths] mapping.
   * As well as persist the corresponding delta path change to MSentryPathChange
   * table in a single transaction.
   *
   * @param authzObj an authzObj
   * @param paths a set of paths need to be deleted from the authzObj -> [Paths] mapping
   * @param update the corresponding path delta update
   */
  void deleteAuthzPathsMapping(final String authzObj,
                               final Iterable<String> paths,
                               final UniquePathsUpdate update) throws Exception;

  /**
   * Renames the existing authzObj to a new one in the authzObj -> [Paths] mapping,
   * but keeps its paths mapping as-is. As well as persist the corresponding delta path
   * change to MSentryPathChange table in a single transaction.
   *
   * @param oldObj the existing authzObj
   * @param newObj the new name to be changed to
   * @param update the corresponding path delta update
   */
  void renameAuthzObj(final String oldObj,
                      final String newObj,
                      final UniquePathsUpdate update) throws Exception;

  /**
   * Renames the existing authzObj to a new one in the authzObj -> [Paths] mapping.
   * And updates its existing path with a new path, while keeps the rest of its paths
   * untouched if there is any. As well as persist the corresponding delta path
   * change to MSentryPathChange table in a single transaction.
   *
   * @param oldObj the existing authzObj
   * @param newObj the new name to be changed to
   * @param oldPath a existing path of the given authzObj
   * @param newPath a new path to be changed to
   * @param update the corresponding path delta update
   */
  void renameAuthzPathsMapping(final String oldObj, final String newObj, final String oldPath,
                               final String newPath, final UniquePathsUpdate update)
    throws Exception;

  /**
   * Updates authzObj -> [Paths] mapping to replace an existing path with a new one
   * given an authzObj. As well as persist the corresponding delta path change to
   * MSentryPathChange table in a single transaction.
   *
   * @param authzObj an authzObj
   * @param oldPath the existing path maps to the given authzObj
   * @param newPath a new path to replace the existing one
   * @param update the corresponding path delta update
   * @throws Exception
   */
  void updateAuthzPathsMapping(final String authzObj, final String oldPath,
                               final String newPath, final UniquePathsUpdate update)
    throws Exception;

  /**
   * Tells if there are any records in MAuthzPathsSnapshotId
   *
   * @return true if there are no entries in <code>MAuthzPathsSnapshotId</code>
   * false if there are entries
   * @throws Exception
   */
  boolean isAuthzPathsSnapshotEmpty() throws Exception;

  /**
   * Gets the last processed HMS snapshot ID for path delta changes.
   *
   * @return latest path change ID.
   */
  long getLastProcessedImageID() throws Exception;

  /**
   * Alter a give sentry user/role to set owner privilege, as well as persist the corresponding
   * permission change to MSentryPermChange table in a single transaction.
   * Creates User, if it is not already there.
   * Internally calls alterSentryGrantPrivilege.
   * @param entityName Entity name to which permissions should be granted.
   * @param entityType Entity Type
   * @param privilege Privilege to be granted
   * @param update DeltaTransactionBlock
   * @throws Exception
   */
  void alterSentryGrantOwnerPrivilege(final String entityName, SentryPrincipalType entityType,
                                      final TSentryPrivilege privilege,
                                      final Update update) throws Exception;

  /**
   * List the Owners for an authorizable
   * @param authorizable Authorizable
   * @return List of owner for an authorizable
   * @throws Exception
   */
  List<SentryOwnerInfo> listOwnersByAuthorizable(TSentryAuthorizable authorizable)
    throws Exception;

  /**
   * Updates the owner privileges by revoking owner privileges to an authorizable and adding new
   * privilege based on the arguments provided.
   * @param tAuthorizable Authorizable to which owner privilege should be granted.
   * @param ownerName
   * @param entityType
   * @param updates Delta Updates.
   * @throws Exception
   */
  void updateOwnerPrivilege(final TSentryAuthorizable tAuthorizable,
                                         String ownerName,  SentryPrincipalType entityType,
                                         final List<Update> updates) throws Exception;

  /**
   * Returns all roles and privileges found on the Sentry database.
   *
   * @return A mapping between role and privileges in the form [roleName, set<privileges>].
   *         If a role does not have privileges, then an empty set is returned for that role.
   *         If no roles are found, then an empty map object is returned.
   */
  Map<String, Set<TSentryPrivilege>> getAllRolesPrivileges() throws Exception;

  /**
   * Returns all users and privileges found on the Sentry database.
   *
   * @return A mapping between user and privileges in the form [userName, set<privileges>].
   *         If a user does not have privileges, then an empty set is returned for that user.
   *         If no users are found, then an empty map object is returned.
   */
  Map<String, Set<TSentryPrivilege>> getAllUsersPrivileges() throws Exception;
}
