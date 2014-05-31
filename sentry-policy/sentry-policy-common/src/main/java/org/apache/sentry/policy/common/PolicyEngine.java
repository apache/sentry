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

package org.apache.sentry.policy.common;

import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;

import com.google.common.collect.ImmutableSet;
/**
 * Implementations of this interface are expected to be thread safe
 * after construction.
 */
@ThreadSafe
public interface PolicyEngine {

  /**
   * The privilege factory to use in order to compare privileges in {@link getPermission}.
   * This is typically a factory that returns a privilege used to evaluate wildcards.
   * @return the privilege factory
   */
  public PrivilegeFactory getPrivilegeFactory();

  /**
   * Get privileges associated with a group. Returns Strings which can be resolved
   * by the caller. Strings are returned to separate the PolicyFile class from the
   * type of privileges used in a policy file. Additionally it is possible further
   * processing of the privileges is needed before resolving to a privilege object.
   * @param group name
   * @param active role-set
   * @return non-null immutable set of privileges
   */
  public ImmutableSet<String> getAllPrivileges(Set<String> groups, ActiveRoleSet roleSet)
      throws SentryConfigurationException;

  /**
   * Get privileges associated with a group. Returns Strings which can be resolved
   * by the caller. Strings are returned to separate the PolicyFile class from the
   * type of privileges used in a policy file. Additionally it is possible further
   * processing of the privileges is needed before resolving to a privilege object.
   * @param group name
   * @param active role-set
   * @param authorizable Hierarchy (Can be null)
   * @return non-null immutable set of privileges
   */
  public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy)
      throws SentryConfigurationException;

  public void close();

  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException;
}
