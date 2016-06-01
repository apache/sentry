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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;
import org.apache.sentry.core.common.ImplyMethodType;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.common.utils.SentryConstants;

import java.util.ArrayList;
import java.util.List;

// The class is used to compare the privilege
public class CommonPrivilege implements Privilege {

  private ImmutableList<KeyValue> parts;

  public CommonPrivilege(String privilegeStr) {
    privilegeStr = Strings.nullToEmpty(privilegeStr).trim();
    if (privilegeStr.isEmpty()) {
      throw new IllegalArgumentException("Privilege string cannot be null or empty.");
    }
    List<KeyValue> parts = Lists.newArrayList();
    for (String authorizable : SentryConstants.AUTHORIZABLE_SPLITTER.trimResults().split(
            privilegeStr)) {
      if (authorizable.isEmpty()) {
        throw new IllegalArgumentException("Privilege '" + privilegeStr + "' has an empty section");
      }
      parts.add(new KeyValue(authorizable));
    }
    if (parts.isEmpty()) {
      throw new AssertionError("Should never occur: " + privilegeStr);
    }
    this.parts = ImmutableList.copyOf(parts);
  }

  @Override
  public boolean implies(Privilege privilege, Model model) {
    // By default only supports comparisons with other IndexerWildcardPermissions
    if (!(privilege instanceof CommonPrivilege)) {
      return false;
    }

    List<KeyValue> otherParts = ((CommonPrivilege) privilege).getParts();
    if(parts.equals(otherParts)) {
      return true;
    }

    int index = 0;
    for (KeyValue otherPart : otherParts) {
      // If this privilege has less parts than the other privilege, everything
      // after the number of parts contained
      // in this privilege is automatically implied, so return true
      if (parts.size() - 1 < index) {
        return true;
      } else {
        KeyValue part = parts.get(index);
        String policyKey = part.getKey();
        // are the keys even equal
        if(!policyKey.equalsIgnoreCase(otherPart.getKey())) {
          // Support for action inheritance from parent to child
          if (SentryConstants.PRIVILEGE_NAME.equalsIgnoreCase(policyKey)) {
            continue;
          }
          return false;
        }

        // do the imply for action
        if (SentryConstants.PRIVILEGE_NAME.equalsIgnoreCase(policyKey)) {
          if (!impliesAction(part.getValue(), otherPart.getValue(), model.getBitFieldActionFactory())) {
            return false;
          }
        } else {
          if (!impliesResource(model.getImplyMethodMap().get(policyKey.toLowerCase()),
                  part.getValue(), otherPart.getValue())) {
            return false;
          }
        }

        index++;
      }
    }

    // If this privilege has more parts than the other parts, only imply it if
    // all of the other parts are wildcards
    for (; index < parts.size(); index++) {
      KeyValue part = parts.get(index);
      if (!SentryConstants.PRIVILEGE_WILDCARD_VALUE.equals(part.getValue())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public List<KeyValue> getAuthorizable() {
    List<KeyValue> authorizable = new ArrayList<>();

    for (KeyValue part : parts) {

      // Authorizeable is the same as privileges but should exclude action
      if (!SentryConstants.PRIVILEGE_NAME.equalsIgnoreCase(part.getKey())) {
        KeyValue keyValue = new KeyValue(part.getKey().toLowerCase(),
            part.getValue().toLowerCase());
        authorizable.add(keyValue);
      }
    }

    return authorizable;
  }

  // The method is used for compare the value of resource by the ImplyMethodType.
  // for Hive, databaseName, tableName, columnName will be compared using String.equal(wildcard support)
  //           url will be compared using PathUtils.impliesURI
  private boolean impliesResource(ImplyMethodType implyMethodType, String policyValue, String requestValue) {
    // wildcard support, "*", "+", "all"("+" and "all" are for backward compatibility) are represented as wildcard
    // if requestValue is wildcard, means privilege request is to match with any value of given resource
    if (SentryConstants.RESOURCE_WILDCARD_VALUE.equals(policyValue)
            || SentryConstants.RESOURCE_WILDCARD_VALUE.equals(requestValue)
            || SentryConstants.RESOURCE_WILDCARD_VALUE_ALL.equalsIgnoreCase(policyValue)
            || SentryConstants.RESOURCE_WILDCARD_VALUE_ALL.equalsIgnoreCase(requestValue)
            || SentryConstants.RESOURCE_WILDCARD_VALUE_SOME.equals(requestValue)) {
      return true;
    }

    // compare as the url
    if (ImplyMethodType.URL == implyMethodType) {
      return PathUtils.impliesURI(policyValue, requestValue);
    } else if (ImplyMethodType.STRING_CASE_SENSITIVE == implyMethodType) {
      // compare as the string case sensitive
      return policyValue.equals(requestValue);
    }
    // default: compare as the string case insensitive
    return policyValue.equalsIgnoreCase(requestValue);
  }

  // The method is used for compare the action for the privilege model.
  // for Hive, the action will be select, insert, etc.
  // for Solr, the action will be update, query, etc.
  private boolean impliesAction(String policyValue, String requestValue,
                                BitFieldActionFactory bitFieldActionFactory) {
    BitFieldAction currentAction = bitFieldActionFactory.getActionByName(policyValue);
    BitFieldAction requestAction = bitFieldActionFactory.getActionByName(requestValue);
    // the action in privilege is not supported
    if (currentAction == null || requestAction == null) {
      return false;
    }
    return currentAction.implies(requestAction);
  }

  @Override
  public String toString() {
    return SentryConstants.AUTHORIZABLE_JOINER.join(parts);
  }

  public List<KeyValue> getParts() {
    return parts;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CommonPrivilege) {
      CommonPrivilege cp = (CommonPrivilege) o;
      return parts.equals(cp.parts);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return parts.hashCode();
  }
}
