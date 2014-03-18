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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

public class SimpleDBProviderBackend implements ProviderBackend {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleDBProviderBackend.class);

  private final SentryPolicyServiceClient policyServiceClient;

  private volatile boolean initialized;

  public SimpleDBProviderBackend(Configuration conf, String resourcePath) throws IOException {
    this(conf, new Path(resourcePath));
  }

  public SimpleDBProviderBackend(Configuration conf, Path resourcePath) throws IOException {
    this(new SentryPolicyServiceClient(conf));
  }

  @VisibleForTesting
  public SimpleDBProviderBackend(SentryPolicyServiceClient policyServiceClient) throws IOException {
    this.initialized = false;
    this.policyServiceClient = policyServiceClient;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(ProviderBackendContext context) {
    if (initialized) {
      throw new IllegalStateException("Backend has already been initialized, cannot be initialized twice");
    }
    this.initialized = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet) {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
    try {
      return ImmutableSet.copyOf(policyServiceClient.listPrivileges(groups, roleSet));
    } catch (SentryUserException e) {
      String msg = "Unable to obtain privileges from server: " + e.getMessage();
      LOGGER.error(msg, e);
    }
    return ImmutableSet.of();
  }

  @Override
  public void close() {
    if (policyServiceClient != null) {
      policyServiceClient.close();
    }
  }

  /**
   * SimpleDBProviderBackend does not implement validatePolicy()
   */
  @Override
  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
    // db provider does not implement validation
  }
}