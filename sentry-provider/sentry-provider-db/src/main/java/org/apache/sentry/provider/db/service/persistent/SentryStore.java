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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilegeMap;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.service.thrift.TStoreSnapshot;

/**
 * Base interface All SentryStore implementations must implement
 *
 */
public interface SentryStore {

  public Configuration getConfiguration();

  public CommitContext createSentryRole(String roleName)
      throws SentryAlreadyExistsException;

  public CommitContext alterSentryRoleGrantPrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege privilege)
      throws SentryUserException;
  
  public CommitContext alterSentryRoleGrantPrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> privileges)
      throws SentryUserException;

  public CommitContext alterSentryRoleRevokePrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege tPrivilege) throws SentryUserException;

  public CommitContext alterSentryRoleRevokePrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> tPrivileges)
          throws SentryUserException;

  public CommitContext dropSentryRole(String roleName)
      throws SentryNoSuchObjectException;
  
  public CommitContext alterSentryRoleAddGroups(String grantorPrincipal,
      String roleName, Set<TSentryGroup> groupNames)
          throws SentryNoSuchObjectException;

  public CommitContext alterSentryRoleDeleteGroups(String roleName,
      Set<TSentryGroup> groupNames) throws SentryNoSuchObjectException;

  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(
      Set<String> groups, TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable authHierarchy, boolean isAdmin)
      throws SentryInvalidInputException;

  public Set<TSentryPrivilege> getAllTSentryPrivilegesByRoleName(String roleName)
      throws SentryNoSuchObjectException;

  public Set<TSentryPrivilege> getTSentryPrivileges(Set<String> roleNames,
      TSentryAuthorizable authHierarchy) throws SentryInvalidInputException;

  public Set<TSentryRole> getTSentryRolesByGroupName(Set<String> groupNames,
      boolean checkAllGroups) throws SentryNoSuchObjectException;

  public Set<String> getRoleNamesForGroups(Set<String> groups);

  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet) throws SentryInvalidInputException;

  public Set<String> listSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy)
          throws SentryInvalidInputException;

  public boolean hasAnyServerPrivileges(Set<String> groups,
      TSentryActiveRoleSet roleSet, String server);

  public String getSentryVersion() throws SentryNoSuchObjectException,
  SentryAccessDeniedException;

  public void setSentryVersion(String newVersion, String verComment)
      throws SentryNoSuchObjectException, SentryAccessDeniedException;

  public void dropPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryNoSuchObjectException, SentryInvalidInputException;

  public void renamePrivilege(TSentryAuthorizable tAuthorizable,
      TSentryAuthorizable newTAuthorizable)
      throws SentryNoSuchObjectException, SentryInvalidInputException;

  public Map<String, HashMap<String, String>> retrieveFullPrivilegeImage();

  public Map<String, LinkedList<String>> retrieveFullRoleImage();

  public Set<String> getGroupsForRole(String roleName);

  public long getRoleCount();

  public long getPrivilegeCount();

  public long getGroupCount();

  public void stop();

  public TStoreSnapshot toSnapshot();

  public void fromSnapshot(TStoreSnapshot snapshot);
}
