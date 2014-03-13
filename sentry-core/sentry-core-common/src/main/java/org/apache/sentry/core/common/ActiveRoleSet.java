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
package org.apache.sentry.core.common;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * Some authorization schemes allow users to select a particular
 * set of roles they want active at any give time. For example,
 * SQL systems often all ALL, NONE, or a subset of roles.
 */
public class ActiveRoleSet {
  public static final ActiveRoleSet ALL = new ActiveRoleSet(true);
  private final boolean allRoles;
  private final ImmutableSet<String> roles;

  public ActiveRoleSet(boolean allRoles) {
    this(allRoles, new HashSet<String>());
  }

  public ActiveRoleSet(Set<String> roles) {
    this(false, ImmutableSet.copyOf(roles));
  }

  private ActiveRoleSet(boolean allRoles, Set<String> roles) {
    this.allRoles = allRoles;
    ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();
    for (String role : roles) {
      setBuilder.add(role.toLowerCase());
    }
    this.roles = setBuilder.build();
  }

  /**
   * Returns true if this active role set contains role. This can be the result
   * of either this role set implying all roles or containing role.
   * @param role
   * @return true if this active role set contains role
   */
  public boolean containsRole(String role) {
    return allRoles || roles.contains(role.toLowerCase());
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("ActiveRoleSet = [ roles = ");
    if (allRoles) {
      builder.append("ALL");
    } else {
      builder.append(roles);
    }
    return builder.append(" ").toString();
  }
}
