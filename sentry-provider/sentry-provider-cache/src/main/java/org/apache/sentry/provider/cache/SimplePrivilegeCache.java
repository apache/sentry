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
package org.apache.sentry.provider.cache;

import org.apache.sentry.core.common.ActiveRoleSet;

import java.util.HashSet;
import java.util.Set;

/*
 * The class is used for saving and getting user's privileges when do the hive command like "show tables".
 * This will enhance the performance for the hive metadata filter.
 */
public class SimplePrivilegeCache implements PrivilegeCache {

  private Set<String> cachedPrivileges;

  public SimplePrivilegeCache(Set<String> cachedPrivileges) {
    this.cachedPrivileges = cachedPrivileges;
  }

  // return the cached privileges
  @Override
  public Set<String> listPrivileges(Set<String> groups, ActiveRoleSet roleSet) {
    if (cachedPrivileges == null) {
      cachedPrivileges = new HashSet<String>();
    }
    return cachedPrivileges;
  }

  @Override
  public void close() {
    if (cachedPrivileges != null) {
      cachedPrivileges.clear();
    }
  }

  @Override
  public Set<String> listPrivileges(Set<String> groups, Set<String> users, ActiveRoleSet roleSet) {
    if (cachedPrivileges == null) {
      cachedPrivileges = new HashSet<String>();
    }
    return cachedPrivileges;
  }
}
