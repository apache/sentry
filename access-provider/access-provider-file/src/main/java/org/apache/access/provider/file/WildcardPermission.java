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

package org.apache.access.provider.file;

import static org.apache.access.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.access.provider.file.PolicyFileConstants.AUTHORIZABLE_SPLITTER;

import java.io.Serializable;
import java.util.List;

import org.apache.access.core.AccessConstants;
import org.apache.access.core.Authorizable.AuthorizableType;
import org.apache.shiro.authz.Permission;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

// XXX this class is made ugly by the fact that Action is not a Authorizable.
public class WildcardPermission implements Permission, Serializable {

  private static final long serialVersionUID = -6785051263922740818L;
  private final ImmutableList<KeyValue> parts;

  public WildcardPermission(String wildcardString) {
    wildcardString = Strings.nullToEmpty(wildcardString).trim().toLowerCase();
    if (wildcardString.isEmpty()) {
      throw new IllegalArgumentException("Wildcard string cannot be null or empty.");
    }
    List<KeyValue>parts = Lists.newArrayList();
    for (String authorizable : AUTHORIZABLE_SPLITTER.split(wildcardString)) {
      authorizable = authorizable.trim().toLowerCase();
      if (authorizable.isEmpty()) {
        throw new IllegalArgumentException("Portion of " + wildcardString + " is invalid");
      }
      parts.add(new KeyValue(authorizable));
    }
    if (parts.isEmpty()) {
      throw new AssertionError("Should never occur: " + wildcardString);
    }
    this.parts = ImmutableList.copyOf(parts);
  }


  @Override
  public boolean implies(Permission p) {
    // By default only supports comparisons with other WildcardPermissions
    if (!(p instanceof WildcardPermission)) {
      return false;
    }

    WildcardPermission wp = (WildcardPermission) p;

    List<KeyValue> otherParts = wp.parts;
    if(equals(wp)) {
      return true;
    }
    int index = 0;
    for (KeyValue otherPart : otherParts) {
      // If this permission has less parts than the other permission, everything
      // after the number of parts contained
      // in this permission is automatically implied, so return true
      if (parts.size() - 1 < index) {
        return true;
      } else {
        KeyValue part = parts.get(index);
        // are the keys even equal
        if(!part.getKey().equals(otherPart.getKey())) {
          return false;
        }
        // in order to imply, the values either have to be equal or our value has
        // to be ALL
        if (!implies(part, otherPart)) {
          return false;
        }
        index++;
      }
    }
    // If this permission has more parts than
    // the other parts, only imply it if
    // all of the other parts are wildcards
    for (; index < parts.size(); index++) {
      KeyValue part = parts.get(index);
      if (!part.getValue().equals(AccessConstants.ALL)) {
        return false;
      }
    }

    return true;
  }

  private boolean implies(KeyValue policyPart, KeyValue requestPart) {
    Preconditions.checkState(policyPart.getKey().equalsIgnoreCase(requestPart.getKey()),
        "Please report, this method should not be called with two different keys");
    if(policyPart.getValue().equals(AccessConstants.ALL) || policyPart.equals(requestPart)) {
      return true;
    } else if(policyPart.getKey().equalsIgnoreCase(AuthorizableType.URI.name())) {
      /*
       * URI is a a special case. For URI's, /a implies /a/b.
       * Therefore the test is "/a/b".startsWith("/a");
       */
      return requestPart.getValue().startsWith(policyPart.getValue());
    }
    return false;
  }


  @Override
  public String toString() {
    return AUTHORIZABLE_JOINER.join(parts);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof WildcardPermission) {
      WildcardPermission wp = (WildcardPermission) o;
      return parts.equals(wp.parts);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return parts.hashCode();
  }
}