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
package org.apache.access.provider.file;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.shiro.config.Ini;
import org.apache.shiro.util.PermissionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class SimplePolicy implements Policy {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimplePolicy.class);

  private static final String GROUPS = "groups";
  private static final String ROLES = "roles";

  private final String resourcePath;
  private final AtomicReference<ConcurrentMap<String, ImmutableSet<String>>> privilegesRef;

  public SimplePolicy(String resourcePath) {
    this.resourcePath = resourcePath;
    this.privilegesRef = new AtomicReference<ConcurrentMap<String, ImmutableSet<String>>>();
    this.privilegesRef.set(new ConcurrentHashMap<String, ImmutableSet<String>>());
    parse();
  }

  /**
   * Parse the resource. Should not be used in the normal course
   */
  protected void parse() {
    LOGGER.info("Parsing " + resourcePath);
    Ini ini = Ini.fromResourcePath(resourcePath);
    Ini.Section privilegesSection = ini.getSection(ROLES);
    boolean invalidConfiguration = false;
    if (privilegesSection == null) {
      LOGGER.warn("Section {} empty for {}", ROLES, resourcePath);
      invalidConfiguration = true;
    }
    Ini.Section groupsSection = ini.getSection(GROUPS);
    if (groupsSection == null) {
      LOGGER.warn("Section {} empty for {}", GROUPS, resourcePath);
      invalidConfiguration = true;
    }
    if (!invalidConfiguration) {
      process(privilegesSection, groupsSection);
    }
  }

  private void process(Ini.Section rolesSection, Ini.Section groupsSection) {
    ConcurrentMap<String, ImmutableSet<String>> privileges = new ConcurrentHashMap<String, ImmutableSet<String>>();
    Multimap<String, String> roleNameToPrivilegeMap = HashMultimap
        .create();
    for (Map.Entry<String, String> entry : rolesSection.entrySet()) {
      String roleName = Strings.nullToEmpty(entry.getKey()).trim();
      String roleValue = Strings.nullToEmpty(entry.getValue()).trim();
      boolean invalidConfiguration = false;
      if (roleName.isEmpty()) {
        LOGGER.warn("Empty role name encountered in {}", resourcePath);
        invalidConfiguration = true;
      }
      if (roleValue.isEmpty()) {
        LOGGER.warn("Empty role value encountered in {}", resourcePath);
        invalidConfiguration = true;
      }
      if (roleNameToPrivilegeMap.containsKey(roleName)) {
        LOGGER.warn("Role {} defined twice in {}", roleName,
            resourcePath);
      }
      if (!invalidConfiguration) {
        Set<String> permissions = PermissionUtils
            .toPermissionStrings(roleValue);
        if (permissions != null) {
          roleNameToPrivilegeMap.putAll(roleName, permissions);
        }
      }
    }
    Splitter commaSplitter = Splitter.on(",").omitEmptyStrings().trimResults();
    for (Map.Entry<String, String> entry : groupsSection.entrySet()) {
      String groupName = Strings.nullToEmpty(entry.getKey()).trim();
      String groupPrivileges = Strings.nullToEmpty(entry.getValue()).trim();
      Collection<String> resolvedGroupPrivileges = Sets.newHashSet();
      for (String roleName : commaSplitter.split(groupPrivileges)) {
        if (roleNameToPrivilegeMap.containsKey(roleName)) {
          resolvedGroupPrivileges.addAll(roleNameToPrivilegeMap
              .get(roleName));
        } else {
          LOGGER.warn("Role {} for group {} does not exist in privileges section in {}",
              new Object[] { roleName, groupName, resourcePath });
        }
      }
      privileges.put(groupName, ImmutableSet.copyOf(resolvedGroupPrivileges));
    }
    this.privilegesRef.set(privileges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getPermissions(String group) {
    ImmutableSet<String> result = privilegesRef.get().get(Strings.nullToEmpty(group)
        .trim());
    if (result == null) {
      return ImmutableSet.of();
    }
    return result;
  }
}