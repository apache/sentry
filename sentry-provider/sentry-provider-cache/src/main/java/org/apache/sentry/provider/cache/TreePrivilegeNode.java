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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.policy.common.Privilege;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to store value of the <key, value> used in TreePrivilegeCache
 */
public class TreePrivilegeNode {
  Set<Privilege> ownPrivileges;
  Set<Privilege> childWildcardPrivileges;

  /** the key of childPrivileges is the resource value of the next level hierarchy, i.e., child resource value
   *  For example, a privilege "server=server1->db=db1->table=table2->column=col3->action=select"
   *  will be put into the following data structure
   *  TreePrivilegeCache.cachedPrivilegeMap[server1]
   *    -> TreePrivilegeNode.childPrivileges[db1]        (partIndex = 0, for node of resource value: server1)
   *      -> TreePrivilegeNode.childPrivileges[table2]   (partIndex = 1, for node of resource value: db1)
   *        -> TreePrivilegeNode.childPrivileges[col3]   (partIndex = 2, for node of resource value: table2)
   *          -> TreePrivilegeNode.ownPrivileges         (partIndex = 3, for node of resource value: col3)
   *
   *  A privilege "server=server1->db=db1->table=table2->column=*->action=select"
   *  will be put into the following data structure
   *  TreePrivilegeCache.cachedPrivilegeMap[server1]
   *    -> TreePrivilegeNode.childPrivileges[db1]        (partIndex = 0, for node of resource value: server1)
   *      -> TreePrivilegeNode.childPrivileges[table2]   (partIndex = 1, for node of resource value: db1)
   *        -> TreePrivilegeNode.childWildcardPrivileges (partIndex = 2, for node of resource value: table2)
   *
   */
  Map<String, TreePrivilegeNode> childPrivileges;

  private static final Logger LOGGER = LoggerFactory
    .getLogger(TreePrivilegeNode.class);

  public TreePrivilegeNode () {
  }

  public void addPrivilege(final Privilege inPrivilege, int partIndex) {
    if (isOwnPrivilege(inPrivilege, partIndex)) {
      if (ownPrivileges == null) {
        ownPrivileges = new HashSet<>();
      }

      ownPrivileges.add(inPrivilege);
      return;
    }

    // find the child resource value, which is used as key in childPrivileges
    String childResourceValue = getResourceValue(partIndex + 1, inPrivilege);
    if (StringUtils.isEmpty(childResourceValue)) {
      LOGGER.warn("Child resource value at index [{}] of privilege {} is null", partIndex, inPrivilege.toString());
      return;
    }

    if (isResourceValueWildcard(childResourceValue)) {
      if (childWildcardPrivileges == null) {
        childWildcardPrivileges = new HashSet<>();
      }

      childWildcardPrivileges.add(inPrivilege);
      return;
    }

    if (childPrivileges == null) {
      childPrivileges = new HashMap<>();
    }

    TreePrivilegeNode childNode = childPrivileges.get(childResourceValue);
    if (childNode == null) {
      childNode = new TreePrivilegeNode();
      childPrivileges.put(childResourceValue, childNode);
    }

    childNode.addPrivilege(inPrivilege, partIndex + 1);
  }

  /**
   * Return the set of privileges that could match the authorizable, including own, child wild card, and
   * matched child privileges.
   * @param authorizationhierarchy list of authorizable in the order of server, db, table, column.
   * @param partIndex the current index of the list of authorizable
   * @return
   */
  public Set<Privilege> listPrivilegeObjects(int partIndex, Authorizable... authorizationhierarchy) {
    if (authorizationhierarchy.length < partIndex + 1) {
      return null;
    }

    Set<Privilege> targetSet = new HashSet<>();
    if (ownPrivileges != null) {
      targetSet.addAll(ownPrivileges);
    }

    if ((childWildcardPrivileges != null) && (authorizationhierarchy.length > partIndex + 1)) {
      // only add when the child authorizable is included
      targetSet.addAll(childWildcardPrivileges);
    }

    Set<Privilege> childPrivileges = listChildPrivilegeObjects(partIndex, authorizationhierarchy);

    if (childPrivileges != null) {
      targetSet.addAll(childPrivileges);
    }

    return targetSet;
  }

  // Check if there is child to process
  // true: yes; false: reach to the end of the hierarchy, and there is no more child to process
  private static boolean hasChild(int partIndex, int totalLevel) {
    if (totalLevel <= partIndex + 1) {
      return false;
    }

    return true;
  }

  // Check if there is own data to process
  // true: yes; false: reach to the end of the hierarchy, and there is no more data to process
  private static boolean hasOwn(int partIndex, int totalLevel) {
    if (totalLevel < partIndex + 1) {
      return false;
    }

    return true;
  }

  /**
   * Return the set of privileges that could match the authorizable, only including matched child privileges.
   * @param partIndex
   * @param authorizationhierarchy
   * @return
   */
  private Set<Privilege> listChildPrivilegeObjects(int partIndex, Authorizable... authorizationhierarchy) {

    if (!hasChild(partIndex, authorizationhierarchy.length)) {
      return null;
    }

    String childKey = getResourceValue(partIndex + 1, authorizationhierarchy);
    if (StringUtils.isEmpty(childKey)) {
      return null;
    }

    if (isResourceValueWildcard(childKey)) {
      // the authorizable for child is wildcard, so return the privileges of all children.
      return listAllChildPrivilegeObjects(partIndex, authorizationhierarchy);
    }

    // the authorizable for child is for a specific child, return its own privileges.
    if (childPrivileges == null) {
      return null;
    }
    
    TreePrivilegeNode childNode = childPrivileges.get(childKey);
    if (childNode == null) {
      return null;
    }

    return childNode.listPrivilegeObjects(partIndex + 1, authorizationhierarchy);
  }

  private Set<Privilege> listAllChildPrivilegeObjects(int partIndex, Authorizable... authorizationhierarchy) {
    if (childPrivileges == null) {
      return null;
    }

    Set<Privilege> targetPrivileges = new HashSet<>();

    for (TreePrivilegeNode childNode : childPrivileges.values()) {
      Set<Privilege> childSet = childNode.listPrivilegeObjects(partIndex + 1, authorizationhierarchy);
      if ((childSet == null) || (childSet.size() == 0)) {
        continue;
      }

      targetPrivileges.addAll(childSet);
    }

    return targetPrivileges;
  }

  private boolean isOwnPrivilege(Privilege inPrivilege, int partIndex) {
    List<KeyValue> parts = inPrivilege.getParts();
    if (!hasChild(partIndex, parts.size())) {
      return true;
    }

    // check child resource type
    String partType = parts.get(partIndex + 1).getKey();
    if (SentryConstants.PRIVILEGE_NAME.equalsIgnoreCase(partType)) {
      // the next part is action, not authorizable
      return true;
    }

    if (AuthorizableType.URI.toString().equalsIgnoreCase(partType)) {
      // the next part is uri
      return true;
    }

    return false;
  }

  public static boolean isResourceValueWildcard(String resourceValue) {
    if (StringUtils.isEmpty(resourceValue)) {
      return false;
    }

    if (SentryConstants.RESOURCE_WILDCARD_VALUE.equalsIgnoreCase(resourceValue)) {
      return true;
    }

    if (SentryConstants.RESOURCE_WILDCARD_VALUE_SOME.equalsIgnoreCase(resourceValue)) {
      return true;
    }

    if (SentryConstants.RESOURCE_WILDCARD_VALUE_ALL.equalsIgnoreCase(resourceValue)) {
      return true;
    }

    return false;
  }

  public static String getResourceValue(int partIndex, Authorizable[] authorizables) {
    if ((authorizables == null) || (authorizables.length == 0) ) {
      return null;
    }

    if (!hasOwn(partIndex, authorizables.length)) {
      return null;
    }

    Authorizable ownPart = authorizables[partIndex];

    if (ownPart == null) {
      return null;
    }

    return ownPart.getName().toLowerCase();
  }

  public static String getResourceValue(int partIndex, Privilege inPrivilege) {
    List<KeyValue> parts = inPrivilege.getParts();

    if (parts == null) {
      return null;
    }

    if (!hasOwn(partIndex, parts.size())) {
      return null;
    }

    KeyValue ownPart = parts.get(partIndex);

    if (ownPart == null) {
      return null;
    }

    return ownPart.getValue().toLowerCase();
  }
}
