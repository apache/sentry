/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// copied from apache shiro

package org.apache.sentry.policy.search;

import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_SPLITTER;

import java.util.List;

import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.policy.common.PrivilegeFactory;
import org.apache.sentry.provider.file.KeyValue;
import org.apache.sentry.provider.file.PolicyFileConstants;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SearchWildcardPrivilege implements Privilege {

  private final ImmutableList<KeyValue> parts;

  public SearchWildcardPrivilege(String wildcardString) {
    wildcardString = Strings.nullToEmpty(wildcardString).trim();
    if (wildcardString.isEmpty()) {
      throw new IllegalArgumentException("Wildcard string cannot be null or empty.");
    }
    List<KeyValue>parts = Lists.newArrayList();
    for (String authorizable : AUTHORIZABLE_SPLITTER.trimResults().split(wildcardString)) {
      if (authorizable.isEmpty()) {
        throw new IllegalArgumentException("Privilege '" + wildcardString + "' has an empty section");
      }
      parts.add(new KeyValue(authorizable));
    }
    if (parts.isEmpty()) {
      throw new AssertionError("Should never occur: " + wildcardString);
    }
    this.parts = ImmutableList.copyOf(parts);
  }


  @Override
  public boolean implies(Privilege p) {
    // By default only supports comparisons with other SearchWildcardPermissions
    if (!(p instanceof SearchWildcardPrivilege)) {
      return false;
    }

    SearchWildcardPrivilege wp = (SearchWildcardPrivilege) p;

    List<KeyValue> otherParts = wp.parts;
    if(equals(wp)) {
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
        // are the keys even equal
        if(!part.getKey().equalsIgnoreCase(otherPart.getKey())) {
          return false;
        }
        if (!impliesKeyValue(part, otherPart)) {
          return false;
        }
        index++;
      }
    }
    // If this privilege has more parts than
    // the other parts, only imply it if
    // all of the other parts are wildcards
    for (; index < parts.size(); index++) {
      KeyValue part = parts.get(index);
      if (!part.getValue().equals(SearchConstants.ALL)) {
        return false;
      }
    }

    return true;
  }

  private boolean impliesKeyValue(KeyValue policyPart, KeyValue requestPart) {
    Preconditions.checkState(policyPart.getKey().equalsIgnoreCase(requestPart.getKey()),
        "Please report, this method should not be called with two different keys");
    if(policyPart.getValue().equals(SearchConstants.ALL) || policyPart.equals(requestPart)) {
      return true;
    } else if (!PolicyFileConstants.PRIVILEGE_NAME.equalsIgnoreCase(policyPart.getKey())
        && SearchConstants.ALL.equalsIgnoreCase(requestPart.getValue())) {
      /* privilege request is to match with any object of given type */
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return AUTHORIZABLE_JOINER.join(parts);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SearchWildcardPrivilege) {
      SearchWildcardPrivilege wp = (SearchWildcardPrivilege) o;
      return parts.equals(wp.parts);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return parts.hashCode();
  }

  public static class SearchWildcardPrivilegeFactory implements PrivilegeFactory {
    @Override
    public Privilege createPrivilege(String privilege) {
      return new SearchWildcardPrivilege(privilege);
    }
  }
}
