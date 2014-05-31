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
package org.apache.sentry.policy.db;

import java.util.List;
import java.util.Set;

import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.policy.common.PrivilegeFactory;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.policy.common.PrivilegeValidator;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SimpleDBPolicyEngine implements PolicyEngine {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleDBPolicyEngine.class);

  public final static String ACCESS_ALLOW_URI_PER_DB_POLICYFILE = "sentry.allow.uri.db.policyfile";

  private final ProviderBackend providerBackend;

  public SimpleDBPolicyEngine(String serverName, ProviderBackend providerBackend) {
    this.providerBackend = providerBackend;
    ProviderBackendContext context = new ProviderBackendContext();
    context.setAllowPerDatabase(true);
    context.setValidators(createPrivilegeValidators(serverName));
    this.providerBackend.initialize(context);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PrivilegeFactory getPrivilegeFactory() {
    return new DBWildcardPrivilege.DBWildcardPrivilegeFactory();
  }



  @Override
  public ImmutableSet<String> getAllPrivileges(Set<String> groups,
      ActiveRoleSet roleSet) throws SentryConfigurationException {
    return getPrivileges(groups, roleSet, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy)
      throws SentryConfigurationException {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Getting permissions for {}", groups);
    }
    ImmutableSet<String> result = providerBackend.getPrivileges(groups, roleSet, authorizableHierarchy);
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("result = " + result);
    }
    return result;
  }

  @Override
  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException {
    this.providerBackend.validatePolicy(strictValidation);
  }

  @Override
  public void close() {
    if (providerBackend != null) {
      providerBackend.close();
    }
  }

  public static ImmutableList<PrivilegeValidator> createPrivilegeValidators(String serverName) {
    return ImmutableList.<PrivilegeValidator>of(new ServersAllIsInvalid(), new DatabaseMustMatch(),
        new DatabaseRequiredInPrivilege(), new ServerNameMustMatch(serverName));
  }
}
