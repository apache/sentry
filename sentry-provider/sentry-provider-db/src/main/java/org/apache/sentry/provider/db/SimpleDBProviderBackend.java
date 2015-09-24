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
package org.apache.sentry.provider.db;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class SimpleDBProviderBackend implements ProviderBackend {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleDBProviderBackend.class);

  private Configuration conf;

  public SimpleDBProviderBackend(Configuration conf, String resourcePath) throws Exception {
    // DB Provider doesn't use policy file path
    this(conf);
  }

  public SimpleDBProviderBackend(Configuration conf) throws Exception {
    this.conf = conf;
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(ProviderBackendContext context) {
    //Noop
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    return getPrivileges(1, groups, roleSet, authorizableHierarchy);
  }

  private ImmutableSet<String> getPrivileges(int retryCount, Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    SentryPolicyServiceClient policyServiceClient = null;
    try {
      policyServiceClient = SentryServiceClientFactory.create(conf);
    } catch (Exception e) {
      LOGGER.error("Error connecting to Sentry ['{}'] !!",
          e.getMessage());
    }
    if(policyServiceClient!= null) {
      try {
        return ImmutableSet.copyOf(policyServiceClient.listPrivilegesForProvider(groups, roleSet, authorizableHierarchy));
      } catch (Exception e) {
        if (retryCount > 0) {
          return getPrivileges(retryCount - 1, groups, roleSet, authorizableHierarchy);
        } else {
          String msg = "Unable to obtain privileges from server: " + e.getMessage();
          LOGGER.error(msg, e);
        }
      } finally {
        if(policyServiceClient != null) {
          policyServiceClient.close();
        }
      }
    }
    return ImmutableSet.of();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getRoles(Set<String> groups, ActiveRoleSet roleSet) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void close() {
    //Noop
  }
  
  /**
   * SimpleDBProviderBackend does not implement validatePolicy()
   */
  @Override
  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException {
  //Noop
  }
}

