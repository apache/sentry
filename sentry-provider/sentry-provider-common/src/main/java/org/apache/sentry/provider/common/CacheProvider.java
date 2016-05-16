/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.sentry.provider.common;

import com.google.common.collect.ImmutableSet;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;

import java.util.Map;
import java.util.Set;

public class CacheProvider {
  private TableCache cache;
  private volatile boolean initialized = false;

  public void initialize(TableCache cache) {
    if (initialized) {
      throw new IllegalStateException("CacheProvider has already been initialized, cannot be initialized twice.");
    }
    this.cache = cache;
    this.initialized = true;
  }

  public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet,
                                            Authorizable... authorizableHierarchy) {
    if (!initialized) {
      throw new IllegalStateException("CacheProvider has not been properly initialized");
    }
    ImmutableSet.Builder<String> resultBuilder = ImmutableSet.builder();
    for (String groupName : groups) {
      for (Map.Entry<String, Set<String>> row : cache.getCache().row(groupName).entrySet()) {
        if (roleSet.containsRole(row.getKey())) {
          // TODO: SENTRY-1245: Filter by Authorizables, if provided
          resultBuilder.addAll(row.getValue());
        }
      }
    }
    return resultBuilder.build();
  }

  public ImmutableSet<String> getRoles(Set<String> groups, ActiveRoleSet roleSet) {
    if (!initialized) {
      throw new IllegalStateException("CacheProvider has not been properly initialized");
    }
    ImmutableSet.Builder<String> resultBuilder = ImmutableSet.builder();
    if (groups != null) {
      for (String groupName : groups) {
        for (Map.Entry<String, Set<String>> row : cache.getCache().row(groupName)
            .entrySet()) {
          if (roleSet.containsRole(row.getKey())) {
            resultBuilder.add(row.getKey());
          }
        }
      }
    }
    return resultBuilder.build();
  }
}
