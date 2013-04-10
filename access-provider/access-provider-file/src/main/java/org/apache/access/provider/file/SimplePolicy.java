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

import static org.apache.access.provider.file.PolicyFileConstants.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.access.core.Authorizable;
import org.apache.access.core.Database;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.util.PermissionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class SimplePolicy implements Policy {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimplePolicy.class);

  private static final String DATABASES = "databases";
  private static final String GROUPS = "groups";
  private static final String ROLES = "roles";

  private final String resourcePath;
  private final AtomicReference<Roles> rolesReference;

  public SimplePolicy(String resourcePath) {
    this.resourcePath = resourcePath;
    this.rolesReference = new AtomicReference<Roles>();
    this.rolesReference.set(new Roles());
    parse();
  }

  /**
   * Parse the resource. Should not be used in the normal course
   */
  protected void parse() {
    LOGGER.info("Parsing " + resourcePath);
    Roles roles = new Roles();
    try {
      Ini ini = Ini.fromResourcePath(resourcePath);
      ImmutableSetMultimap<String, String> globalRoles;
      Map<String, ImmutableSetMultimap<String, String>> perDatabaseRoles = Maps.newHashMap();
      globalRoles = parseIni(null, ini);
      Ini.Section filesSection = ini.getSection(DATABASES);
      if(filesSection == null) {
        LOGGER.info("Section " + DATABASES + " needs no further processing");
      } else {
        for(Map.Entry<String, String> entry : filesSection.entrySet()) {
          String database = Strings.nullToEmpty(entry.getKey()).trim().toLowerCase();
          String file = Strings.nullToEmpty(entry.getValue()).trim();
          try {
            LOGGER.info("Parsing " + file);
            perDatabaseRoles.put(database, parseIni(database, Ini.fromResourcePath(file)));
          } catch (Exception e) {
            LOGGER.error("Error processing key " + entry.getKey() + ", skipping " + entry.getValue(), e);
            throw e;
          }
        }
      }
      roles = new Roles(globalRoles, ImmutableMap.copyOf(perDatabaseRoles));
    } catch (Exception e) {
      LOGGER.error("Error processing file, ignoring " + resourcePath, e);
    }
    rolesReference.set(roles);
  }

  private ImmutableSetMultimap<String, String> parseIni(String database, Ini ini) {
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
      return parsePermissions(database, privilegesSection, groupsSection);
    }
    return ImmutableSetMultimap.of();
  }

  private ImmutableSetMultimap<String, String> parsePermissions(@Nullable String database,
      Ini.Section rolesSection, Ini.Section groupsSection) {
    ImmutableSetMultimap.Builder<String, String> resultBuilder = ImmutableSetMultimap.builder();
    Multimap<String, String> roleNameToPrivilegeMap = HashMultimap
        .create();
    List<? extends RoleValidator> validators = Lists.newArrayList(
        new ServersAllIsInvalid(),
        new DatabaseMustMatch(),
        new DatabaseRequiredInRole());
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
      Set<String> roles = PermissionUtils
          .toPermissionStrings(roleValue);
      if (!invalidConfiguration && roles != null) {
        for(String role : roles) {
          for(RoleValidator validator : validators) {
            validator.validate(database, role.trim());
          }
        }
        roleNameToPrivilegeMap.putAll(roleName, roles);
      }
    }
    Splitter roleSplitter = ROLE_SPLITTER.omitEmptyStrings().trimResults();
    for (Map.Entry<String, String> entry : groupsSection.entrySet()) {
      String groupName = Strings.nullToEmpty(entry.getKey()).trim();
      String groupPrivileges = Strings.nullToEmpty(entry.getValue()).trim();
      Collection<String> resolvedGroupPrivileges = Sets.newHashSet();
      for (String roleName : roleSplitter.split(groupPrivileges)) {
        if (roleNameToPrivilegeMap.containsKey(roleName)) {
          resolvedGroupPrivileges.addAll(roleNameToPrivilegeMap
              .get(roleName));
        } else {
          LOGGER.warn("Role {} for group {} does not exist in privileges section in {}",
              new Object[] { roleName, groupName, resourcePath });
        }
      }
      resultBuilder.putAll(groupName, resolvedGroupPrivileges);
    }
    return resultBuilder.build();
  }



  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSetMultimap<String, String> getPermissions(List<Authorizable> authorizables, List<String> groups) {
    Roles roles = rolesReference.get();
    String database = null;
    for(Authorizable authorizable : authorizables) {
      if(authorizable instanceof Database) {
        database = authorizable.getName();
      }
    }
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Getting permissions for {} via {}", groups, database);
    }
    ImmutableSetMultimap.Builder<String, String> resultBuilder = ImmutableSetMultimap.builder();
    for(String group : groups) {
      resultBuilder.putAll(group, roles.getRoles(database, group));
    }
    return resultBuilder.build();
  }
}