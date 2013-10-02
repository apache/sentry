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
package org.apache.sentry.provider.file;

import static org.apache.sentry.provider.file.PolicyFileConstants.DATABASES;
import static org.apache.sentry.provider.file.PolicyFileConstants.GROUPS;
import static org.apache.sentry.provider.file.PolicyFileConstants.ROLES;
import static org.apache.sentry.provider.file.PolicyFileConstants.ROLE_SPLITTER;
import static org.apache.sentry.provider.file.PolicyFileConstants.USERS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.core.AccessURI;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.Database;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.util.PermissionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class SimplePolicyParser {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimplePolicyParser.class);



  private final FileSystem fileSystem;
  private final Path resourcePath;
  private final List<Path> perDbResources = Lists.newArrayList();
  private final AtomicReference<Roles> rolesReference;
  private final RolesFactory rolesFactory;
  private final Configuration conf;
  private final List<? extends RoleValidator> validators;

  public SimplePolicyParser(String resourcePath, RolesFactory rolesFactory, List<? extends RoleValidator> validators) throws IOException {
    this(new Configuration(), new Path(resourcePath), rolesFactory, validators);
  }

  @VisibleForTesting
  public SimplePolicyParser(Configuration conf, Path resourcePath, RolesFactory rolesFactory, List<? extends RoleValidator> validators) throws IOException {
    this.resourcePath = resourcePath;
    this.fileSystem = resourcePath.getFileSystem(conf);
    this.rolesReference = new AtomicReference<Roles>();
    this.rolesReference.set(rolesFactory.createRoles());
    this.conf = conf;
    this.rolesFactory = rolesFactory;
    this.validators = validators;
    parse();
  }

  /**
   * Parse the resource. Should not be used in the normal course
   */
  protected void parse() {
    LOGGER.info("Parsing " + resourcePath);
    Roles roles = rolesFactory.createRoles();
    try {
      perDbResources.clear();
      Ini ini = PolicyFiles.loadFromPath(fileSystem, resourcePath);
      if(LOGGER.isDebugEnabled()) {
        for(String sectionName : ini.getSectionNames()) {
          LOGGER.debug("Section: " + sectionName);
          Ini.Section section = ini.get(sectionName);
          for(String key : section.keySet()) {
            String value = section.get(key);
            LOGGER.debug(key + " = " + value);
          }
        }
      }
      ImmutableSetMultimap<String, String> globalRoles;
      Map<String, ImmutableSetMultimap<String, String>> perDatabaseRoles = Maps.newHashMap();
      globalRoles = parseIni(null, ini);
      Ini.Section filesSection = ini.getSection(DATABASES);
      if(filesSection == null) {
        LOGGER.info("Section " + DATABASES + " needs no further processing");
      } else {
        for(Map.Entry<String, String> entry : filesSection.entrySet()) {
          String database = Strings.nullToEmpty(entry.getKey()).trim().toLowerCase();
          Path perDbPolicy = new Path(Strings.nullToEmpty(entry.getValue()).trim());
          if(isRelative(perDbPolicy)) {
            perDbPolicy = new Path(resourcePath.getParent(), perDbPolicy);
          }
          try {
            LOGGER.info("Parsing " + perDbPolicy);
            Ini perDbIni = PolicyFiles.loadFromPath(perDbPolicy.getFileSystem(conf), perDbPolicy);
            if(perDbIni.containsKey(USERS)) {
              throw new ConfigurationException("Per-db policy files cannot contain " + USERS + " section");
            }
            if(perDbIni.containsKey(DATABASES)) {
              throw new ConfigurationException("Per-db policy files cannot contain " + DATABASES + " section");
            }
            ImmutableSetMultimap<String, String> currentDbRoles = parseIni(database, perDbIni);
            perDatabaseRoles.put(database, currentDbRoles);
            perDbResources.add(perDbPolicy);
          } catch (Exception e) {
            LOGGER.error("Error processing key " + entry.getKey() + ", skipping " + entry.getValue(), e);
          }
        }
      }
      roles = rolesFactory.createRoles(globalRoles, ImmutableMap.copyOf(perDatabaseRoles));
    } catch (Exception e) {
      LOGGER.error("Error processing file, ignoring " + resourcePath, e);
    }
    rolesReference.set(roles);
  }
  /**
   * Relative for our purposes is no scheme, no authority
   * and a non-absolute path portion.
   */
  private boolean isRelative(Path path) {
    URI uri = path.toUri();
    return uri.getAuthority() == null && uri.getScheme() == null && !path.isUriPathAbsolute();
  }

  protected long getModificationTime() throws IOException {
    // if resource path has been deleted, throw all exceptions
    long result = fileSystem.getFileStatus(resourcePath).getModificationTime();
    for(Path perDbPolicy : perDbResources) {
      try {
        result = Math.max(result, fileSystem.getFileStatus(perDbPolicy).getModificationTime());
      } catch (FileNotFoundException e) {
        // if a per-db file has been deleted, wait until the main
        // policy file has been updated before refreshing
      }
    }
    return result;
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

  public ImmutableSet<String> getRoles(@Nullable String database, String group, Boolean isURI) {
    return rolesReference.get().getRoles(database, group, isURI);
  }
}
