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

import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.persistent.CommitContext;

/**
 * Sentry store for persistent the authorize object to database
 */
public interface SentryStoreLayer {
  /**
   * Create a role
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param requestor: User on whose behalf the request is launched
   * @returns commit context used for notification handlers
   * @throws SentryAlreadyExistsException
   */
  CommitContext createRole(String component, String role,
      String requestor) throws SentryAlreadyExistsException;

  /**
   * Drop a role
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param requestor: user on whose behalf the request is launched
   * @returns commit context used for notification handlers
   * @throws SentryNoSuchObjectException
   */
  CommitContext dropRole(String component, String role,
      String requestor) throws SentryNoSuchObjectException;

  /**
   * Add a role to groups.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param groups: The name of groups
   * @param requestor: User on whose behalf the request is issued
   * @returns commit context used for notification handlers
   * @throws SentryNoSuchObjectException
   */
  CommitContext alterRoleAddGroups(String component, String role,
      Set<String> groups, String requestor) throws SentryNoSuchObjectException;

  /**
   * Delete a role from groups.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param groups: The name of groups
   * @param requestor: User on whose behalf the request is launched
   * @returns commit context used for notification handlers
   * @throws SentryNoSuchObjectException
   */
  CommitContext alterRoleDeleteGroups(String component, String role,
      Set<String> groups, String requestor) throws SentryNoSuchObjectException;

  /**
   * Grant a privilege to role.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param privilege: The privilege object will be granted
   * @param grantorPrincipal: User on whose behalf the request is launched
   * @returns commit context Used for notification handlers
   * @throws SentryUserException
   */
  CommitContext alterRoleGrantPrivilege(String component, String role,
      PrivilegeObject privilege, String grantorPrincipal) throws SentryUserException;

  /**
   * Revoke a privilege from role.
   * @param component: The request respond to which component
   * @param role: The name of role
   * @param privilege: The privilege object will revoked
   * @param grantorPrincipal: User on whose behalf the request is launched
   * @returns commit context used for notification handlers
   * @throws SentryUserException
   */
  CommitContext alterRoleRevokePrivilege(String component, String role,
      PrivilegeObject privilege, String grantorPrincipal) throws SentryUserException;

  /**
   * Rename privilege
   *
   * @param component: The request respond to which component
   * @param service: The name of service
   * @param oldAuthorizables: The old list of authorize objects
   * @param newAuthorizables: The new list of authorize objects
   * @param requestor: User on whose behalf the request is launched
   * @returns commit context used for notification handlers
   * @throws SentryUserException
   */
  CommitContext renamePrivilege(
      String component, String service, List<? extends Authorizable> oldAuthorizables,
      List<? extends Authorizable> newAuthorizables, String requestor) throws SentryUserException;

  /**
   * Drop privilege
   * @param component: The request respond to which component
   * @param privilege: The privilege will be dropped
   * @param requestor: User on whose behalf the request is launched
   * @returns commit context used for notification handlers
   * @throws SentryUserException
   */
  CommitContext dropPrivilege(String component, PrivilegeObject privilege,
      String requestor) throws SentryUserException;

  /**
   * Get roles
   * @param component: The request respond to which component
   * @param groups: The name of groups
   * @returns the set of roles
   * @throws SentryUserException
   */
  Set<String> getRolesByGroups(String component, Set<String> groups) throws SentryUserException;

  /**
   * Get groups
   * @param component: The request respond to which component
   * @param roles: The name of roles
   * @returns the set of groups
   * @throws SentryUserException
   */
  Set<String> getGroupsByRoles(String component, Set<String> roles) throws SentryUserException;

  /**
   * Get privileges
   * @param component: The request respond to which component
   * @param roles: The name of roles
   * @returns the set of privileges
   * @throws SentryUserException
   */
  Set<PrivilegeObject> getPrivilegesByRole(String component, Set<String> roles) throws SentryUserException;

  /**
   * get sentry privileges from provider as followings:
   * @param component: The request respond to which component
   * @param service: The name of service
   * @param roles: The name of roles
   * @param groups: The name of groups
   * @param authorizables: The list of authorize objects
   * @returns the set of privileges
   * @throws SentryUserException
   */

  Set<PrivilegeObject> getPrivilegesByProvider(String component, String service, Set<String> roles,
       Set<String> groups, List<? extends Authorizable> authorizables)
       throws SentryUserException;

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
   * @throws SentryUserException
   */
  Set<MSentryGMPrivilege> getPrivilegesByAuthorizable(String component, String service,
      Set<String> validActiveRoles, List<? extends Authorizable> authorizables)
      throws SentryUserException;

  /**
   * close sentryStore
   */
  void close();

}
