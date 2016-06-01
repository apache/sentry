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
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;

import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;

/*
 * The class is used for saving and getting user's privileges when do the hive command like "show tables".
 * This will enhance the performance for the hive metadata filter. This class is not thread safe.
 */
public class SimplePrivilegeCache implements PrivilegeCache {

  private Set<String> cachedPrivileges;

  // <Authorizable, Set<PrivilegeObject>> map, this is a cache for mapping authorizable
  // to corresponding set of privilege objects.
  // e.g. (server=server1->database=b1, (server=server1->database=b1->action=insert))
  private final Map<String, Set<String>> cachedAuthzPrivileges = new HashMap<>();

  // <AuthorizableType, Set<AuthorizableValue>> wild card map
  private final Map<String, Set<String>> wildCardAuthz = new HashMap<>();

  public SimplePrivilegeCache(Set<String> cachedPrivileges) {
    this.cachedPrivileges = cachedPrivileges;

    for (String cachedPrivilege : cachedPrivileges) {
      Privilege privilege = new CommonPrivilege(cachedPrivilege);
      List<KeyValue> authorizable = privilege.getAuthorizable();
      String authzString = getAuthzString(authorizable);
      updateWildCardAuthzMap(authorizable);

      Set<String> authzPrivileges = cachedAuthzPrivileges.get(authzString);
      if (authzPrivileges == null) {
        authzPrivileges = new HashSet();
        cachedAuthzPrivileges.put(authzString, authzPrivileges);
      }
      authzPrivileges.add(cachedPrivilege);
    }
  }

  private String getAuthzString(List<KeyValue> authoriable) {
    List<KeyValue> authz = new LinkedList<>();
    for (KeyValue auth : authoriable) {

      // For authorizable e.g. sever=server1->uri=hdfs://namenode:8020/path/,
      // use sever=server1 as the key of cachedAuthzPrivileges, since
      // cannot do string matchinf on URI paths.
      if (!AuthorizableType.URI.toString().equalsIgnoreCase(auth.getKey())) {
        authz.add(auth);
      }
    }

    return SentryConstants.AUTHORIZABLE_JOINER.join(authz);
  }

  private void updateWildCardAuthzMap(List<KeyValue> authz) {
    for (KeyValue auth : authz) {
      String authKey = auth.getKey().toLowerCase();
      String authValue = auth.getValue().toLowerCase();
      Set<String> authzValue = wildCardAuthz.get(authKey);

      if (authzValue != null ) {
        if (!authzValue.contains(authValue)) {
          authzValue.add(authValue);
        }
      } else {
        authzValue = new HashSet<>();
        authzValue.add(authValue);
        wildCardAuthz.put(authKey, authzValue);
      }
    }
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

  @Override
  public Set<String> listPrivileges(Set<String> groups, Set<String> users, ActiveRoleSet roleSet,
      Authorizable... authorizationHierarchy) {
    Set<String> privileges = new HashSet<>();
    Set<StringBuilder> authzKeys = getAuthzKeys(authorizationHierarchy);
    for (StringBuilder authzKey : authzKeys) {
      if (cachedAuthzPrivileges.get(authzKey.toString()) != null) {
        privileges.addAll(cachedAuthzPrivileges.get(authzKey.toString()));
      }
    }

    return privileges;
  }

  /**
   * Get authoriables from the <Authorizable, Set<PrivilegeObject>> cache map,
   * based on the authorizable hierarchy. This logic follows Privilege.implies.
   * e.g. given authorizable hierarchy:server=server1->db=db1, returns matched
   * privileges including server=server1;server=*;server=server1->db=db1;server=server1->db=*.
   * @param authorizationHierarchy
   * @return
   */
  private Set<StringBuilder> getAuthzKeys(Authorizable... authorizationHierarchy) {
    Set<StringBuilder> targets = new HashSet<>();
    for (Authorizable auth : authorizationHierarchy) {
      String authzType = auth.getTypeName().toLowerCase();
      String authzName = auth.getName().toLowerCase();

      // No op for URI authorizable type.
      if (authzType.equalsIgnoreCase(AuthorizableType.URI.toString())) {
        continue;
      }
      // If authorizable name is a wild card, need to add all possible authorizable objects
      // basesd on the authorizable type.
      if (authzName.equals(SentryConstants.RESOURCE_WILDCARD_VALUE) ||
          authzName.equals(SentryConstants.RESOURCE_WILDCARD_VALUE_SOME)||
          authzName.equals(SentryConstants.RESOURCE_WILDCARD_VALUE_ALL)) {
        Set<String> wildcardValues = wildCardAuthz.get(authzType);

        if (wildcardValues != null && wildcardValues.size() > 0) {
          Set<StringBuilder> newTargets = new HashSet<>(targets);
          for (StringBuilder target : targets) {
            for (String wildcardValue : wildcardValues) {
              newTargets.add(addAuthz(target, authzType, wildcardValue));
            }
          }

          targets = newTargets;
        } else {
          return targets;
        }
      } else {
        if (targets.isEmpty()) {
          targets.add(addAuthz(new StringBuilder(), authzType, authzName));

          // Add wild card * search, e.g server=*, server=ALL
          targets.add(addAuthz(new StringBuilder(), authzType,
              SentryConstants.RESOURCE_WILDCARD_VALUE.toLowerCase()));
          targets.add(addAuthz(new StringBuilder(), authzType,
              SentryConstants.RESOURCE_WILDCARD_VALUE_ALL.toLowerCase()));
        } else {
          Set<StringBuilder> newTargets = new HashSet<>(targets);

          for (StringBuilder target : targets) {
            newTargets.add(addAuthz(target, authzType, authzName));

            // Add wild card * search, e.g server=server1->db=*, server=server1->db=ALL
            newTargets.add(addAuthz(target, authzType,
                SentryConstants.RESOURCE_WILDCARD_VALUE.toLowerCase()));
            newTargets.add(addAuthz(target, authzType,
                SentryConstants.RESOURCE_WILDCARD_VALUE_ALL.toLowerCase()));
          }

          targets = newTargets;
        }
      }
    }

    return targets;
  }

  private StringBuilder addAuthz(StringBuilder authorizable, String authzType, String authzName) {
    StringBuilder newAuthrizable = new StringBuilder(authorizable);
    if (newAuthrizable.length() > 0) {
      newAuthrizable.append(SentryConstants.AUTHORIZABLE_SEPARATOR);
      newAuthrizable.append(SentryConstants.KV_JOINER.join(authzType, authzName));
    } else {
      newAuthrizable.append(SentryConstants.KV_JOINER.join(authzType, authzName));
    }

    return newAuthrizable;
  }
}
