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
package org.apache.sentry.provider.db.generic.service.persistent;

import java.util.List;
import java.util.Set;

import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;

/**
 * Sentry store for persistent the authorize object to database
 */
public interface SentryStoreLayer {
  /**
   * Create a role
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param requestor: User on whose behalf the request is launched
   * @throws Exception
   */
  Object createRole(String component, String role,
      String requestor) throws Exception;

  /**
   * Drop a role
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param requestor: user on whose behalf the request is launched
   * @throws Exception
   */
  Object dropRole(String component, String role,
      String requestor) throws Exception;

  /**
   * Add a role to groups.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param groups: The name of groups
   * @param requestor: User on whose behalf the request is issued
   * @throws Exception
   */
  Object alterRoleAddGroups(String component, String role,
      Set<String> groups, String requestor) throws Exception;

  /**
   * Delete a role from groups.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param groups: The name of groups
   * @param requestor: User on whose behalf the request is launched
   * @throws Exception
   */
  Object alterRoleDeleteGroups(String component, String role,
      Set<String> groups, String requestor) throws Exception;

  /**
   * Grant a privilege to role.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param privilege: The privilege object will be granted
   * @param grantorPrincipal: User on whose behalf the request is launched
   * @throws Exception
   */
  Object alterRoleGrantPrivilege(String component, String role,
      PrivilegeObject privilege, String grantorPrincipal) throws Exception;

  /**
   * Revoke a privilege from role.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param privilege: The privilege object will revoked
   * @param grantorPrincipal: User on whose behalf the request is launched
   * @throws Exception
   */
  Object alterRoleRevokePrivilege(String component, String role,
      PrivilegeObject privilege, String grantorPrincipal) throws Exception;

  /**
   * Rename privilege
   *
   * @param component: The request respond to which component
   * @param service: The name of service
   * @param oldAuthorizables: The old list of authorize objects
   * @param newAuthorizables: The new list of authorize objects
   * @param requestor: User on whose behalf the request is launched
   * @throws Exception
   */
  Object renamePrivilege(
      String component, String service, List<? extends Authorizable> oldAuthorizables,
      List<? extends Authorizable> newAuthorizables, String requestor) throws Exception;

  /**
   * Drop privilege
   * @param component: The request respond to which component
   * @param privilege: The privilege will be dropped
   * @param requestor: User on whose behalf the request is launched
   * @throws Exception
   */
  Object dropPrivilege(String component, PrivilegeObject privilege,
      String requestor) throws Exception;

  /**
   * Get roles
   * @param component: The request respond to which component
   * @param groups: The name of groups
   * @returns the set of roles
   * @throws Exception
   */
  Set<String> getRolesByGroups(String component, Set<String> groups) throws Exception;

  /**
   * Get groups
   * @param component: The request respond to which component
   * @param roles: The name of roles
   * @returns the set of groups
   * @throws Exception
   */
  Set<String> getGroupsByRoles(String component, Set<String> roles) throws Exception;

  /**
   * Get privileges
   * @param component: The request respond to which component
   * @param roles: The name of roles
   * @returns the set of privileges
   * @throws Exception
   */
  Set<PrivilegeObject> getPrivilegesByRole(String component, Set<String> roles) throws Exception;

  /**
   * get sentry privileges from provider as followings:
   * @param component: The request respond to which component
   * @param service: The name of service
   * @param roles: The name of roles
   * @param groups: The name of groups
   * @param authorizables: The list of authorize objects
   * @returns the set of privileges
   * @throws Exception
   */

  public Set<PrivilegeObject> getPrivilegesByProvider(String component, String service,Set<String> roles,
       Set<String> groups, List<? extends Authorizable> authorizables)
       throws Exception;

  /**
   * Get all roles name.
   *
   * @returns The set of roles name,
   */
  Set<String> getAllRoleNames();

  /**
   * Get sentry privileges based on valid active roles and the authorize objects.
   *
   * @param component: The request respond to which component
   * @param service: The name of service
   * @param validActiveRoles: The valid active roles
   * @param authorizables: The list of authorize objects
   * @returns The set of MSentryGMPrivilege
   * @throws Exception
   */
  Set<MSentryGMPrivilege> getPrivilegesByAuthorizable(String component, String service,
      Set<String> validActiveRoles, List<? extends Authorizable> authorizables)
      throws Exception;

  /**
   * close sentryStore
   */
  public void close();

}
