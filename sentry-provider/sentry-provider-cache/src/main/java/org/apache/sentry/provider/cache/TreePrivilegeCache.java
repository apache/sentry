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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.policy.common.PrivilegeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class is used to cache user's privileges at the hive command such as "show tables".
 * This cache enhances the performance for the hive metadata filter. This class is not thread safe.
 * It keeps the privilege objects and organizes them based on their hierarchy
 */
public class TreePrivilegeCache implements FilteredPrivilegeCache {

  private final Set<String> cachedPrivileges;
  private final PrivilegeFactory privilegeFactory;
  private final Map<String, TreePrivilegeNode> cachedPrivilegeMap;

  private static final Logger LOGGER = LoggerFactory
    .getLogger(TreePrivilegeCache.class);

  public TreePrivilegeCache(Set<String> cachedPrivileges, PrivilegeFactory inPrivilegeFactory) {
    if (cachedPrivileges == null) {
      cachedPrivileges = new HashSet<String>();
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Created with privileges {}", cachedPrivileges);
    }

    this.cachedPrivileges = cachedPrivileges;
    this.privilegeFactory = inPrivilegeFactory;
    this.cachedPrivilegeMap = createPrivilegeMap(cachedPrivileges);
  }

  @Override
  public Set<String> listPrivileges(Set<String> groups, ActiveRoleSet roleSet) {
    return cachedPrivileges;
  }

  @Override
  public Set<String> listPrivileges(Set<String> groups, Set<String> users, ActiveRoleSet roleSet) {
    return cachedPrivileges;
  }

  @Override
  public Set<String> listPrivileges(Set<String> groups, Set<String> users, ActiveRoleSet roleSet,
    Authorizable... authorizationhierarchy) {
    Set<Privilege> privilegeObjects = listPrivilegeObjects(groups, users, roleSet, authorizationhierarchy);

    return privilegeObjects.stream()
      .filter(priObj -> priObj != null)
      .map(priObj -> priObj.toString())
      .collect(Collectors.toSet());
  }

  @Override
  public Set<Privilege> listPrivilegeObjects(Set<String> groups, Set<String> users,
    ActiveRoleSet roleSet, Authorizable... authorizationhierarchy) {
    Set<String> topResourceValues = getTopLevelResourceValues(authorizationhierarchy);
    Set<Privilege> targetSet = new HashSet<>();
    for (String topResourceValue : topResourceValues) {
      if (StringUtils.isEmpty(topResourceValue)) {
        continue;
      }

      TreePrivilegeNode topNode = cachedPrivilegeMap.get(topResourceValue);
      if (topNode == null) {
        continue;
      }

      targetSet.addAll(topNode.listPrivilegeObjects(0, authorizationhierarchy));
    }

    return targetSet;
  }

  @Override
  public void close() {
    // Keep the privileges to be consistent with cache implementation in Impala
  }

  private Privilege getPrivilegeObject(String priString) {
    if (privilegeFactory != null) {
      return privilegeFactory.createPrivilege(priString);
    }

    return new CommonPrivilege(priString);
  }

  private Map<String, TreePrivilegeNode> createPrivilegeMap(Set<String> cachedPrivileges) {
    Map<String, TreePrivilegeNode> privilegeNodeMap = new HashMap<>();

    for (String priString : cachedPrivileges) {
      Privilege currPrivilege = getPrivilegeObject(priString);

      String topKey = getTopLevelResourceValue(currPrivilege);
      if (StringUtils.isEmpty(topKey)) {
        LOGGER.warn("The top level authorizable of privilege {} is null", priString);
        continue;
      }

      TreePrivilegeNode matchedNode = privilegeNodeMap.get(topKey);
      if (matchedNode == null) {
        matchedNode = new TreePrivilegeNode();
        privilegeNodeMap.put(topKey, matchedNode);
      }

      matchedNode.addPrivilege(currPrivilege, 0);
    }

    return privilegeNodeMap;
  }

  private String getTopLevelResourceValue(Privilege inPrivilege) {
    return TreePrivilegeNode.getResourceValue(0, inPrivilege);
  }

  private String getTopLevelResourceValue(Authorizable[] authorizables) {
    return TreePrivilegeNode.getResourceValue(0, authorizables);
  }

  private Set<String> getTopLevelResourceValues(Authorizable[] authorizables) {
    Set<String> keys = new HashSet<>();
    keys.add(SentryConstants.RESOURCE_WILDCARD_VALUE.toLowerCase());
    keys.add(SentryConstants.RESOURCE_WILDCARD_VALUE_SOME.toLowerCase());
    keys.add(SentryConstants.RESOURCE_WILDCARD_VALUE_ALL.toLowerCase());

    String topKey = getTopLevelResourceValue(authorizables);
    if (!StringUtils.isEmpty(topKey)) {
      keys.add(topKey);
    }

    return keys;
  }
}
