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
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class SimpleDBProviderBackend implements ProviderBackend {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleDBProviderBackend.class);

  private Configuration conf;
  private int retryCount;
  private int retryIntervalSec;

  public SimpleDBProviderBackend(Configuration conf, String resourcePath) throws Exception {
    // DB Provider doesn't use policy file path
    this(conf);
  }

  public SimpleDBProviderBackend(Configuration conf) throws Exception {
    this.conf = conf;
    this.retryCount = conf.getInt(ServiceConstants.ClientConfig.RETRY_COUNT_CONF, ServiceConstants.ClientConfig.RETRY_COUNT_DEFAULT);
    this.retryIntervalSec = conf.getInt(ServiceConstants.ClientConfig.RETRY_INTERVAL_SEC_CONF, ServiceConstants.ClientConfig.RETRY_INTERVAL_SEC_DEFAULT);
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
    return getPrivileges(retryCount, groups, roleSet, authorizableHierarchy);
  }

  private ImmutableSet<String> getPrivileges(int retryCount, Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    int retries = Math.max(retryCount + 1, 1); // if customer configs retryCount as Integer.MAX_VALUE, try only once
    while (retries > 0) {
      retries--;
      try (SentryPolicyServiceClient policyServiceClient =
                   SentryServiceClientFactory.create(conf)) {
		return ImmutableSet.copyOf(policyServiceClient.listPrivilegesForProvider(groups, roleSet, authorizableHierarchy));
       } catch (Exception e) {
        //TODO: differentiate transient errors and permanent errors
        String msg = "Unable to obtain privileges from server: " + e.getMessage() + ".";
        if (retries > 0) {
          LOGGER.warn(msg +  " Will retry for " + retries + " time(s)");
        } else {
          LOGGER.error(msg, e);
        }
        if (retries > 0) {
          try {
            Thread.sleep(retryIntervalSec * 1000);
          } catch (InterruptedException e1) {
            LOGGER.info("Sleeping is interrupted.", e1);
          }
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

