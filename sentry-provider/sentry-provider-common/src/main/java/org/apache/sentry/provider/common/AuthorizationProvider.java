/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.common;

import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.policy.common.PolicyEngine;

/**
 * Implementations of AuthorizationProvider must be threadsafe.
 */
@ThreadSafe
public interface AuthorizationProvider {

  public static String SENTRY_PROVIDER = "sentry.provider";

  /***
   * Returns validate subject privileges on given Authorizable object
   *
   * @param subject: UserID to validate privileges
   * @param authorizableHierarchy : List of object according to namespace hierarchy.
   *        eg. Server->Db->Table or Server->Function
   *        The privileges will be validated from the higher to lower scope
   * @param actions : Privileges to validate
   * @param roleSet : Roles which should be used when obtaining privileges
   * @return
   *        True if the subject is authorized to perform requested action on the given object
   */
  public boolean hasAccess(Subject subject, List<? extends Authorizable> authorizableHierarchy,
      Set<? extends Action> actions, ActiveRoleSet roleSet);

  /***
   * Get the GroupMappingService used by the AuthorizationProvider
   *
   * @return GroupMappingService used by the AuthorizationProvider
   */
  public GroupMappingService getGroupMapping();

  /***
   * Validate the policy file format for syntax and semantic errors
   * @param strictValidation
   * @throws SentryConfigurationException
   */
  public void validateResource(boolean strictValidation) throws SentryConfigurationException;

  /***
   * Returns the list privileges for the given subject
   * @param subject
   * @return
   * @throws SentryConfigurationException
   */
  public Set<String> listPrivilegesForSubject(Subject subject) throws SentryConfigurationException;

  /**
   * Returns the list privileges for the given group
   * @param groupName
   * @return
   * @throws SentryConfigurationException
   */
  public Set<String> listPrivilegesForGroup(String groupName) throws SentryConfigurationException;

  /***
   * Returns the list of missing privileges of the last access request
   * @return
   */
  public List<String> getLastFailedPrivileges();

  /**
   * Frees any resources held by the the provider
   */
  public void close();

  /**
   * Get the policy engine
   */
  public PolicyEngine getPolicyEngine();
}
