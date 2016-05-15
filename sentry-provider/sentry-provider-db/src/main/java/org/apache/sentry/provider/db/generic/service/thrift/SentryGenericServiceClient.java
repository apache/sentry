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
package org.apache.sentry.provider.db.generic.service.thrift;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;

public interface SentryGenericServiceClient {

  /**
   * Create a sentry role
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @throws SentryUserException
   */
  public void createRole(String requestorUserName, String roleName,
      String component) throws SentryUserException;

  public void createRoleIfNotExist(String requestorUserName,
      String roleName, String component) throws SentryUserException;

  /**
   * Drop a sentry role
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @throws SentryUserException
   */
  public void dropRole(String requestorUserName, String roleName,
      String component) throws SentryUserException;

  public void dropRoleIfExists(String requestorUserName, String roleName,
      String component) throws SentryUserException;

  /**
   * add a sentry role to groups.
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param groups: The name of groups
   * @throws SentryUserException
   */
  public void addRoleToGroups(String requestorUserName, String roleName,
      String component, Set<String> groups) throws SentryUserException;

  /**
   * delete a sentry role from groups.
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param groups: The name of groups
   * @throws SentryUserException
   */
  public void deleteRoleToGroups(String requestorUserName, String roleName,
      String component, Set<String> groups) throws SentryUserException;

  /**
   * grant privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  public void grantPrivilege(String requestorUserName, String roleName,
      String component, TSentryPrivilege privilege) throws SentryUserException;

  /**
   * revoke privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName: Name of the role
   * @param component: The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  public void revokePrivilege(String requestorUserName, String roleName,
      String component, TSentryPrivilege privilege) throws SentryUserException;

  /**
   * drop privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param component: The request is issued to which component
   * @param privilege
   * @throws SentryUserException
   */
  public void dropPrivilege(String requestorUserName,String component,
      TSentryPrivilege privilege) throws SentryUserException;

  /**
   * rename privilege
   * @param requestorUserName: user on whose behalf the request is issued
   * @param component: The request is issued to which component
   * @param serviceName: The Authorizable belongs to which service
   * @param oldAuthorizables
   * @param newAuthorizables
   * @throws SentryUserException
   */
  public void renamePrivilege(String requestorUserName, String component,
      String serviceName, List<? extends Authorizable> oldAuthorizables,
      List<? extends Authorizable> newAuthorizables) throws SentryUserException;

  /**
   * Gets sentry role objects for a given groupName using the Sentry service
   * @param requestorUserName : user on whose behalf the request is issued
   * @param groupName : groupName to look up ( if null returns all roles for groups related to requestorUserName)
   * @param component: The request is issued to which component
   * @return Set of thrift sentry role objects
   * @throws SentryUserException
   */
  public Set<TSentryRole> listRolesByGroupName(
      String requestorUserName,
      String groupName,
      String component)
  throws SentryUserException;

  public Set<TSentryRole> listUserRoles(String requestorUserName, String component)
      throws SentryUserException;

  public Set<TSentryRole> listAllRoles(String requestorUserName, String component)
      throws SentryUserException;

  /**
   * Gets sentry privileges for a given roleName and Authorizable Hierarchy using the Sentry service
   * @param requestorUserName: user on whose behalf the request is issued
   * @param roleName:
   * @param component: The request is issued to which component
   * @param serviceName
   * @param authorizables
   * @return
   * @throws SentryUserException
   */
  public Set<TSentryPrivilege> listPrivilegesByRoleName(
      String requestorUserName, String roleName, String component,
      String serviceName, List<? extends Authorizable> authorizables)
      throws SentryUserException;

  public Set<TSentryPrivilege> listPrivilegesByRoleName(
      String requestorUserName, String roleName, String component,
      String serviceName) throws SentryUserException;

  /**
   * get sentry permissions from provider as followings:
   * @param: component: The request is issued to which component
   * @param: serviceName: The privilege belongs to which service
   * @param: roleSet
   * @param: groupNames
   * @param: the authorizables
   * @returns the set of permissions
   * @throws SentryUserException
   */
  public Set<String> listPrivilegesForProvider(String component,
      String serviceName, ActiveRoleSet roleSet, Set<String> groups,
      List<? extends Authorizable> authorizables) throws SentryUserException;

  public void close();

  /**
   * Get sentry privileges based on valid active roles and the authorize objects. Note that
   * it is client responsibility to ensure the requestor username, etc. is not impersonated.
   *
   * @param component: The request respond to which component.
   * @param serviceName: The name of service.
   * @param requestorUserName: The requestor user name.
   * @param authorizablesSet: The set of authorize objects. One authorize object is represented
   *     as a string. e.g resourceType1=resourceName1->resourceType2=resourceName2->resourceType3=resourceName3.
   * @param groups: The requested groups.
   * @param roleSet: The active roles set.
   *
   * @returns The mapping of authorize objects and TSentryPrivilegeMap(<role, set<privileges>).
   * @throws SentryUserException
   */
  Map<String, TSentryPrivilegeMap> listPrivilegsbyAuthorizable(String component,
      String serviceName, String requestorUserName, Set<String> authorizablesSet,
      Set<String> groups, ActiveRoleSet roleSet) throws SentryUserException;

}
