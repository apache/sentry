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

package org.apache.sentry.provider.cache;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;

import com.google.common.collect.ImmutableSet;

public class SimpleCacheProviderBackend implements ProviderBackend {

  private PrivilegeCache cacheHandle;
  private Configuration conf;

  public SimpleCacheProviderBackend(Configuration conf, String resourcePath) {
    this.conf = conf;
  }

  @Override
  public void initialize(ProviderBackendContext context) {
    cacheHandle = (PrivilegeCache) context.getBindingHandle();
    assert cacheHandle != null;
  }

  private boolean initialized() {
    return cacheHandle != null;
  }

  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups,
      ActiveRoleSet roleSet) {
    if (!initialized()) {
      throw new IllegalStateException(
          "Backend has not been properly initialized");
    }
    return ImmutableSet.copyOf(cacheHandle.listPrivileges(groups,
        roleSet));
  }

  @Override
  public ImmutableSet<String> getRoles(Set<String> groups, ActiveRoleSet roleSet) {
    if (!initialized()) {
      throw new IllegalStateException(
          "Backend has not been properly initialized");
    }
    throw new UnsupportedOperationException(
        "getRoles() is not supported by Cache provider");
  }

  @Override
  public void validatePolicy(boolean strictValidation)
      throws SentryConfigurationException {
    if (!initialized()) {
      throw new IllegalStateException(
          "Backend has not been properly initialized");
    }
    throw new UnsupportedOperationException(
        "validatePolicy() is not supported by Cache provider");
  }

  @Override
  public void close() {
    cacheHandle.close();
  }

}
