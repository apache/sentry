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

package org.apache.sentry.provider.file;

import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_SPLITTER;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.sentry.core.AccessConstants;
import org.apache.sentry.core.Authorizable.AuthorizableType;
import org.apache.shiro.authz.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

// XXX this class is made ugly by the fact that Action is not a Authorizable.
public class WildcardPermission implements Permission, Serializable {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(WildcardPermission.class);
  private static final long serialVersionUID = -6785051263922740818L;

  private final ImmutableList<KeyValue> parts;

  public WildcardPermission(String wildcardString) {
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
        if(!part.getKey().equalsIgnoreCase(otherPart.getKey())) {
          return false;
        }
        if (!impliesKeyValue(part, otherPart)) {
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

  private boolean impliesKeyValue(KeyValue policyPart, KeyValue requestPart) {
    Preconditions.checkState(policyPart.getKey().equalsIgnoreCase(requestPart.getKey()),
        "Please report, this method should not be called with two different keys");
    if(policyPart.getValue().equals(AccessConstants.ALL) || policyPart.equals(requestPart)) {
      return true;
    } else if (!PolicyFileConstants.PRIVILEGE_NAME.equalsIgnoreCase(policyPart.getKey())
        && AccessConstants.ALL.equalsIgnoreCase(requestPart.getValue())) {
      /* permission request is to match with any object of given type */
      return true;
    } else if(policyPart.getKey().equalsIgnoreCase(AuthorizableType.URI.name())) {
      return impliesURI(policyPart.getValue(), requestPart.getValue());
    }
    return false;
  }

  /**
   * URI is a a special case. For URI's, /a implies /a/b.
   * Therefore the test is "/a/b".startsWith("/a");
   */
  @VisibleForTesting
  protected static boolean impliesURI(String policy, String request) {
    try {
      URI policyURI = new URI(new StrSubstitutor(System.getProperties()).replace(policy));
      URI requestURI = new URI(request);
      if(policyURI.getScheme() == null || policyURI.getPath() == null) {
        LOGGER.warn("Policy URI " + policy + " is not valid. Either no scheme or no path.");
        return false;
      }
      if(requestURI.getScheme() == null || requestURI.getPath() == null) {
        LOGGER.warn("Request URI " + request + " is not valid. Either no scheme or no path.");
        return false;
      }
      // schemes are equal &&
      // request path does not contain relative parts /a/../b &&
      // request path starts with policy path &&
      // authorities (nullable) are equal
      String requestPath = requestURI.getPath() + File.separator;
      String policyPath = policyURI.getPath() + File.separator;
      if(policyURI.getScheme().equals(requestURI.getScheme()) &&
          requestURI.getPath().equals(new URI(request).normalize().getPath()) &&
          requestPath.startsWith(policyPath) &&
          Strings.nullToEmpty(policyURI.getAuthority()).equals(Strings.nullToEmpty(requestURI.getAuthority()))) {
        return true;
      }
      return false;
    } catch (URISyntaxException e) {
      LOGGER.warn("Request URI " + request + " is not a URI", e);
      return false;
    }
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