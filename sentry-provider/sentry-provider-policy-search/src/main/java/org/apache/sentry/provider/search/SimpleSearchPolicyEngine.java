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
package org.apache.sentry.provider.search;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import org.apache.shiro.config.ConfigurationException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.provider.common.PermissionFactory;
import org.apache.sentry.provider.common.PolicyEngine;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.Roles;
import org.apache.sentry.provider.common.RoleValidator;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;

/**
 * A PolicyEngine for a search service.
 */
public class SimpleSearchPolicyEngine implements PolicyEngine {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleSearchPolicyEngine.class);

  private ProviderBackend providerBackend;

  public SimpleSearchPolicyEngine(ProviderBackend providerBackend) {
    List<? extends RoleValidator> validators =
      Lists.newArrayList(new CollectionRequiredInRole());
    this.providerBackend = providerBackend;
    this.providerBackend.process(validators);

    if (!this.providerBackend.getRoles().getPerDatabaseRoles().isEmpty()) {
      throw new ConfigurationException(
        "SimpleSearchPolicyEngine does not support per-database roles, " +
        "but per-database roles were specified.  Ignoring.");
    }
  }

  /*
   * Note: finalize is final because constructor throws exception, see:
   * OBJ11-J.
   */
  public final void finalize() {
    // do nothing
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PermissionFactory getPermissionFactory() {
    return new SearchWildcardPermission.SearchWildcardPermissionFactory();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSetMultimap<String, String> getPermissions(List<? extends Authorizable> authorizables, List<String> groups) {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Getting permissions for {}", groups);
    }
    ImmutableSetMultimap.Builder<String, String> resultBuilder = ImmutableSetMultimap.builder();
    for(String group : groups) {
      resultBuilder.putAll(group, getSearchRoles(group,providerBackend.getRoles()));
    }
    ImmutableSetMultimap<String, String> result = resultBuilder.build();
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("result = " + result);
    }
    return result;
  }

  private ImmutableSet<String> getSearchRoles(String group, Roles roles) {
    ImmutableSetMultimap<String, String> globalRoles = roles.getGlobalRoles();
    ImmutableSet.Builder<String> resultBuilder = ImmutableSet.builder();

    if(globalRoles.containsKey(group)) {
      resultBuilder.addAll(globalRoles.get(group));
    }
    ImmutableSet<String> result = resultBuilder.build();
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Group {}, Result {}",
          new Object[]{ group, result});
    }
    return result;
  }
}
