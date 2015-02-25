/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.generic.service.thrift;

import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * when Solr integration with Database store, this backend will communicate with Sentry service to get
 * privileges according to the requested groups
 *
 */
public class SearchProviderBackend implements ProviderBackend {
  private static final Logger LOGGER = LoggerFactory.getLogger(SearchProviderBackend.class);
  private final Configuration conf;
  private final Subject subject;
  private volatile boolean initialized = false;

  public SearchProviderBackend(Configuration conf, String resourcePath) throws Exception {
    this.conf = conf;
    /**
     * Who create the searchProviderBackend, this subject will been used the requester to communicate
     * with Sentry Service
     */
    subject = new Subject(UserGroupInformation.getCurrentUser()
        .getShortUserName());
  }

  @Override
  public void initialize(ProviderBackendContext context) {
    if (initialized) {
      throw new IllegalStateException("SearchProviderBackend has already been initialized, cannot be initialized twice");
    }
    this.initialized = true;
  }

  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups,
      ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    if (!initialized) {
      throw new IllegalStateException("SearchProviderBackend has not been properly initialized");
    }
    SearchPolicyServiceClient client = null;
    try {
      client = getClient();
      return ImmutableSet.copyOf(client.listPrivilegesForProvider(roleSet, groups, Arrays.asList(authorizableHierarchy)));
    } catch (SentryUserException e) {
      String msg = "Unable to obtain privileges from server: " + e.getMessage();
      LOGGER.error(msg, e);
    } catch (Exception e) {
      String msg = "Unable to obtain client:" + e.getMessage();
      LOGGER.error(msg, e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
    return ImmutableSet.of();
  }

  @Override
  public ImmutableSet<String> getRoles(Set<String> groups, ActiveRoleSet roleSet) {
    if (!initialized) {
      throw new IllegalStateException("SearchProviderBackend has not been properly initialized");
    }
    SearchPolicyServiceClient client = null;
    try {
      Set<TSentryRole> tRoles = Sets.newHashSet();
      client = getClient();
      //get the roles according to group
      for (String group : groups) {
        tRoles.addAll(client.listRolesByGroupName(subject.getName(), group));
      }
      Set<String> roles = Sets.newHashSet();
      for (TSentryRole tRole : tRoles) {
        roles.add(tRole.getRoleName());
      }
      return ImmutableSet.copyOf(roleSet.isAll() ? roles : Sets.intersection(roles, roleSet.getRoles()));
    } catch (SentryUserException e) {
      String msg = "Unable to obtain roles from server: " + e.getMessage();
      LOGGER.error(msg, e);
    } catch (Exception e) {
      String msg = "Unable to obtain client:" + e.getMessage();
      LOGGER.error(msg, e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
    return ImmutableSet.of();
  }

  public SearchPolicyServiceClient getClient() throws Exception {
    return new SearchPolicyServiceClient(conf);
  }

  /**
   * SearchProviderBackend does nothing in the validatePolicy()
   */
  @Override
  public void validatePolicy(boolean strictValidation)
      throws SentryConfigurationException {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
  }

  @Override
  public void close() {
  }
}