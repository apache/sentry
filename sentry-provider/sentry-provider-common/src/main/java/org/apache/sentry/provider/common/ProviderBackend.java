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
package org.apache.sentry.provider.common;

import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.SentryConfigurationException;

import com.google.common.collect.ImmutableSet;

/**
 * Interface for getting roles from a specific provider backend. Implementations
 * are expected to be thread safe after initialize() has
 * been called.
 */
@ThreadSafe
public interface ProviderBackend {

  /**
   * Set the privilege validators to be used on the backend. This is required
   * because the Backend must be created before the policy engine and only the
   * policy engine knows the validators. Ideally we could change but since
   * both the policy engine and backend are exposed via configuration properties
   * that would be backwards incompatible.
   * @param validators
   */
  public void initialize(ProviderBackendContext context);

  /**
   * Get the privileges from the backend.
   */
  public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet);

  /**
   * If strictValidation is true then an error is thrown for warnings
   * as well as errors.
   *
   * @param strictValidation
   * @throws SentryConfigurationException
   */
  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException;

  public void close();
}
