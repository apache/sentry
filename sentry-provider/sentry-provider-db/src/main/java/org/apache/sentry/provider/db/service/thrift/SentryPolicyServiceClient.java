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

package org.apache.sentry.provider.db.service.thrift;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;

public interface SentryPolicyServiceClient extends AutoCloseable {

  public void createRole(String requestorUserName, String roleName) throws SentryUserException;

  public void dropRole(String requestorUserName, String roleName) throws SentryUserException;

  public void dropRoleIfExists(String requestorUserName, String roleName)
      throws SentryUserException;

  public Set<TSentryRole> listRolesByGroupName(String requestorUserName, String groupName)
      throws SentryUserException;

  public Set<TSentryPrivilege> listAllPrivilegesByRoleName(String requestorUserName, String roleName)
      throws SentryUserException;

  /**
   * Gets sentry privilege objects for a given roleName using the Sentry service
   *
   * @param requestorUserName : user on whose behalf the request is issued
   * @param roleName : roleName to look up
   * @param authorizable : authorizable Hierarchy (server->db->table etc)
   * @return Set of thrift sentry privilege objects
   * @throws SentryUserException
   */
  public Set<TSentryPrivilege> listPrivilegesByRoleName(String requestorUserName, String roleName,
      List<? extends Authorizable> authorizable) throws SentryUserException;

  public Set<TSentryRole> listRoles(String requestorUserName) throws SentryUserException;

  public Set<TSentryRole> listUserRoles(String requestorUserName) throws SentryUserException;

  public TSentryPrivilege grantURIPrivilege(String requestorUserName, String roleName,
      String server, String uri) throws SentryUserException;

  public TSentryPrivilege grantURIPrivilege(String requestorUserName, String roleName,
      String server, String uri, Boolean grantOption) throws SentryUserException;

  public void grantServerPrivilege(String requestorUserName, String roleName, String server,
      String action) throws SentryUserException;

  public TSentryPrivilege grantServerPrivilege(String requestorUserName, String roleName,
      String server, Boolean grantOption) throws SentryUserException;

  public TSentryPrivilege grantServerPrivilege(String requestorUserName, String roleName,
      String server, String action, Boolean grantOption) throws SentryUserException;

  public TSentryPrivilege grantDatabasePrivilege(String requestorUserName, String roleName,
      String server, String db, String action) throws SentryUserException;

  public TSentryPrivilege grantDatabasePrivilege(String requestorUserName, String roleName,
      String server, String db, String action, Boolean grantOption) throws SentryUserException;

  public TSentryPrivilege grantTablePrivilege(String requestorUserName, String roleName,
      String server, String db, String table, String action) throws SentryUserException;

  public TSentryPrivilege grantTablePrivilege(String requestorUserName, String roleName,
      String server, String db, String table, String action, Boolean grantOption)
      throws SentryUserException;

  public TSentryPrivilege grantColumnPrivilege(String requestorUserName, String roleName,
      String server, String db, String table, String columnName, String action)
      throws SentryUserException;

  public TSentryPrivilege grantColumnPrivilege(String requestorUserName, String roleName,
      String server, String db, String table, String columnName, String action, Boolean grantOption)
      throws SentryUserException;

  public Set<TSentryPrivilege> grantColumnsPrivileges(String requestorUserName, String roleName,
      String server, String db, String table, List<String> columnNames, String action)
      throws SentryUserException;

  public Set<TSentryPrivilege> grantColumnsPrivileges(String requestorUserName, String roleName,
      String server, String db, String table, List<String> columnNames, String action,
      Boolean grantOption) throws SentryUserException;

  public void revokeURIPrivilege(String requestorUserName, String roleName, String server,
      String uri) throws SentryUserException;

  public void revokeURIPrivilege(String requestorUserName, String roleName, String server,
      String uri, Boolean grantOption) throws SentryUserException;

  public void revokeServerPrivilege(String requestorUserName, String roleName, String server,
      String action) throws SentryUserException;

  public void revokeServerPrivilege(String requestorUserName, String roleName, String server,
      String action, Boolean grantOption) throws SentryUserException;

  public void revokeServerPrivilege(String requestorUserName, String roleName, String server,
      boolean grantOption) throws SentryUserException;

  public void revokeDatabasePrivilege(String requestorUserName, String roleName, String server,
      String db, String action) throws SentryUserException;

  public void revokeDatabasePrivilege(String requestorUserName, String roleName, String server,
      String db, String action, Boolean grantOption) throws SentryUserException;

  public void revokeTablePrivilege(String requestorUserName, String roleName, String server,
      String db, String table, String action) throws SentryUserException;

  public void revokeTablePrivilege(String requestorUserName, String roleName, String server,
      String db, String table, String action, Boolean grantOption) throws SentryUserException;

  public void revokeColumnPrivilege(String requestorUserName, String roleName, String server,
      String db, String table, String columnName, String action) throws SentryUserException;

  public void revokeColumnPrivilege(String requestorUserName, String roleName, String server,
      String db, String table, String columnName, String action, Boolean grantOption)
      throws SentryUserException;

  public void revokeColumnsPrivilege(String requestorUserName, String roleName, String server,
      String db, String table, List<String> columns, String action) throws SentryUserException;

  public void revokeColumnsPrivilege(String requestorUserName, String roleName, String server,
      String db, String table, List<String> columns, String action, Boolean grantOption)
      throws SentryUserException;

  public Set<String> listPrivilegesForProvider(Set<String> groups, ActiveRoleSet roleSet,
      Authorizable... authorizable) throws SentryUserException;

  public void grantRoleToGroup(String requestorUserName, String groupName, String roleName)
      throws SentryUserException;

  public void revokeRoleFromGroup(String requestorUserName, String groupName, String roleName)
      throws SentryUserException;

  public void grantRoleToGroups(String requestorUserName, String roleName, Set<String> groups)
      throws SentryUserException;

  public void revokeRoleFromGroups(String requestorUserName, String roleName, Set<String> groups)
      throws SentryUserException;

  public void dropPrivileges(String requestorUserName,
      List<? extends Authorizable> authorizableObjects) throws SentryUserException;

  public void renamePrivileges(String requestorUserName,
      List<? extends Authorizable> oldAuthorizables, List<? extends Authorizable> newAuthorizables)
      throws SentryUserException;

  public Map<TSentryAuthorizable, TSentryPrivilegeMap> listPrivilegsbyAuthorizable(
      String requestorUserName, Set<List<? extends Authorizable>> authorizables,
      Set<String> groups, ActiveRoleSet roleSet) throws SentryUserException;

  /**
   * Returns the configuration value in the sentry server associated with propertyName, or if
   * propertyName does not exist, the defaultValue. There is no "requestorUserName" because this is
   * regarded as an internal interface.
   *
   * @param propertyName Config attribute to search for
   * @param defaultValue String to return if not found
   * @return The value of the propertyName
   * @throws SentryUserException
   */
  public String getConfigValue(String propertyName, String defaultValue) throws SentryUserException;

  /**
   * Requests the sentry server to synchronize all HMS notification events up to the specified id.
   * The sentry server will return once it have processed the id specified..
   *
   * @param id Requested HMS notification ID.
   * @return The most recent processed notification ID.
   */
  public long syncNotifications(long id) throws SentryUserException;
}
