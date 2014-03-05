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

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.policy.common.PrivilegeUtils;
import org.apache.sentry.policy.common.PrivilegeValidator;
import org.apache.sentry.policy.common.PrivilegeValidatorContext;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.shiro.config.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class SimpleFileProviderBackend implements ProviderBackend {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleFileProviderBackend.class);

  private final FileSystem fileSystem;
  private final Path resourcePath;
  private final Configuration conf;
  private final List<String> configErrors;
  private final List<String> configWarnings;
  private final SetMultimap<String, String> groupToPrivilegeMap;

  private ImmutableList<PrivilegeValidator> validators;
  private boolean allowPerDatabaseSection;
  private volatile boolean initialized;

  public SimpleFileProviderBackend(String resourcePath) throws IOException {
    this(new Configuration(), new Path(resourcePath));
  }

  public SimpleFileProviderBackend(Configuration conf, String resourcePath) throws IOException {
    this(conf, new Path(resourcePath));
  }

  public SimpleFileProviderBackend(Configuration conf, Path resourcePath) throws IOException {
    this.resourcePath = resourcePath;
    this.fileSystem = resourcePath.getFileSystem(conf);
    this.groupToPrivilegeMap = HashMultimap.create();
    this.conf = conf;
    this.configErrors = Lists.newArrayList();
    this.configWarnings = Lists.newArrayList();
    this.validators = ImmutableList.of();
    this.allowPerDatabaseSection = true;
    this.initialized = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(ProviderBackendContext context) {
    if (initialized) {
      throw new IllegalStateException("Backend has already been initialized, cannot be initialized twice");
    }
    this.validators = context.getValidators();
    this.allowPerDatabaseSection = context.isAllowPerDatabase();
    parse();
    this.initialized = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups) {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
    ImmutableSet.Builder<String> resultBuilder = ImmutableSet.builder();
    for (String group : groups) {
      resultBuilder.addAll(groupToPrivilegeMap.get(group));
    }
    return resultBuilder.build();
  }

  @Override
  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
    List<String> localConfigErrors = Lists.newArrayList(configErrors);
    List<String> localConfigWarnings = Lists.newArrayList(configWarnings);
    if ((strictValidation && !localConfigWarnings.isEmpty()) || !localConfigErrors.isEmpty()) {
      localConfigErrors.add("Failed to process global policy file " + resourcePath);
      SentryConfigurationException e = new SentryConfigurationException("");
      e.setConfigErrors(localConfigErrors);
      e.setConfigWarnings(localConfigWarnings);
      throw e;
    }
  }

  private void parse() {
    configErrors.clear();
    configWarnings.clear();
    SetMultimap<String, String> groupToPrivilegeMapTemp = HashMultimap.create();
    Ini ini;
    LOGGER.info("Parsing " + resourcePath);
    try {
      try {
        ini = PolicyFiles.loadFromPath(fileSystem, resourcePath);
      } catch (IOException e) {
        configErrors.add("Failed to read policy file " + resourcePath +
          " Error: " + e.getMessage());
        throw new SentryConfigurationException("Error loading policy file " + resourcePath, e);
      } catch (IllegalArgumentException e) {
        configErrors.add("Failed to read policy file " + resourcePath +
          " Error: " + e.getMessage());
        throw new SentryConfigurationException("Error loading policy file " + resourcePath, e);
      }

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
      groupToPrivilegeMapTemp.putAll(parseIni(null, ini, validators, resourcePath));
      Ini.Section filesSection = ini.getSection(DATABASES);
      if(filesSection == null) {
        LOGGER.info("Section " + DATABASES + " needs no further processing");
      } else if (!allowPerDatabaseSection) {
        String msg = "Per-db policy file is not expected in this configuration.";
        throw new SentryConfigurationException(msg);
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
              configErrors.add("Per-db policy file cannot contain " + USERS + " section in " +  perDbPolicy);
              throw new SentryConfigurationException("Per-db policy files cannot contain " + USERS + " section");
            }
            if(perDbIni.containsKey(DATABASES)) {
              configErrors.add("Per-db policy files cannot contain " + DATABASES
                  + " section in " + perDbPolicy);
              throw new SentryConfigurationException("Per-db policy files cannot contain " + DATABASES + " section");
            }
            groupToPrivilegeMapTemp.putAll(parseIni(database, perDbIni, validators, perDbPolicy));
          } catch (Exception e) {
            configErrors.add("Failed to read per-DB policy file " + perDbPolicy +
               " Error: " + e.getMessage());
            LOGGER.error("Error processing key " + entry.getKey() + ", skipping " + entry.getValue(), e);
          }
        }
      }
      groupToPrivilegeMap.clear();
      groupToPrivilegeMap.putAll(groupToPrivilegeMapTemp);
    } catch (Exception e) {
      configErrors.add("Error processing file " + resourcePath + e.getMessage());
      LOGGER.error("Error processing file, ignoring " + resourcePath, e);
    }
  }

  /**
   * Relative for our purposes is no scheme, no authority
   * and a non-absolute path portion.
   */
  private boolean isRelative(Path path) {
    URI uri = path.toUri();
    return uri.getAuthority() == null && uri.getScheme() == null && !path.isUriPathAbsolute();
  }

  private ImmutableSetMultimap<String, String> parseIni(String database, Ini ini,
      List<? extends PrivilegeValidator> validators, Path policyPath) {
    Ini.Section privilegesSection = ini.getSection(ROLES);
    boolean invalidConfiguration = false;
    if (privilegesSection == null) {
      String errMsg = String.format("Section %s empty for %s", ROLES, policyPath);
      LOGGER.warn(errMsg);
      configErrors.add(errMsg);
      invalidConfiguration = true;
    }
    Ini.Section groupsSection = ini.getSection(GROUPS);
    if (groupsSection == null) {
      String warnMsg = String.format("Section %s empty for %s", GROUPS, policyPath);
      LOGGER.warn(warnMsg);
      configErrors.add(warnMsg);
      invalidConfiguration = true;
    }
    if (!invalidConfiguration) {
      return parsePrivileges(database, privilegesSection, groupsSection, validators, policyPath);
    }
    return ImmutableSetMultimap.of();
  }

  private ImmutableSetMultimap<String, String> parsePrivileges(@Nullable String database,
      Ini.Section rolesSection, Ini.Section groupsSection, List<? extends PrivilegeValidator> validators,
      Path policyPath) {
    ImmutableSetMultimap.Builder<String, String> resultBuilder = ImmutableSetMultimap.builder();
    Multimap<String, String> roleNameToPrivilegeMap = HashMultimap
        .create();
    for (Map.Entry<String, String> entry : rolesSection.entrySet()) {
      String roleName = Strings.nullToEmpty(entry.getKey()).trim();
      String roleValue = Strings.nullToEmpty(entry.getValue()).trim();
      boolean invalidConfiguration = false;
      if (roleName.isEmpty()) {
        String errMsg = String.format("Empty role name encountered in %s", policyPath);
        LOGGER.warn(errMsg);
        configErrors.add(errMsg);
        invalidConfiguration = true;
      }
      if (roleValue.isEmpty()) {
        String errMsg = String.format("Empty role value encountered in %s", policyPath);
        LOGGER.warn(errMsg);
        configErrors.add(errMsg);
        invalidConfiguration = true;
      }
      if (roleNameToPrivilegeMap.containsKey(roleName)) {
        String warnMsg = String.format("Role %s defined twice in %s", roleName, policyPath);
        LOGGER.warn(warnMsg);
        configWarnings.add(warnMsg);
      }
      Set<String> privileges = PrivilegeUtils.toPrivilegeStrings(roleValue);
      if (!invalidConfiguration && privileges != null) {
        for(String privilege : privileges) {
          for(PrivilegeValidator validator : validators) {
            validator.validate(new PrivilegeValidatorContext(database, privilege.trim()));
          }
        }
        roleNameToPrivilegeMap.putAll(roleName, privileges);
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
          String warnMsg = String.format("Role %s for group %s does not exist in privileges section in %s",
                  roleName, groupName, policyPath);
          LOGGER.warn(warnMsg);
          configWarnings.add(warnMsg);
        }
      }
      resultBuilder.putAll(groupName, resolvedGroupPrivileges);
    }
    return resultBuilder.build();
  }
}
