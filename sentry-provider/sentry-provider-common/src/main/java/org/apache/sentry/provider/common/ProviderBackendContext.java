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

import org.apache.sentry.policy.common.PrivilegeValidator;

import com.google.common.collect.ImmutableList;

public class ProviderBackendContext {

  private boolean allowPerDatabase;
  private ImmutableList<PrivilegeValidator> validators;
  private Object bindingHandle;

  public ProviderBackendContext() {
    validators = ImmutableList.of();
  }

  public boolean isAllowPerDatabase() {
    return allowPerDatabase;
  }

  public void setAllowPerDatabase(boolean allowPerDatabase) {
    this.allowPerDatabase = allowPerDatabase;
  }

  public ImmutableList<PrivilegeValidator> getValidators() {
    return validators;
  }

  public void setValidators(ImmutableList<PrivilegeValidator> validators) {
    if (validators == null) {
      validators = ImmutableList.of();
    }
    this.validators = validators;
  }

  public Object getBindingHandle() {
    return bindingHandle;
  }

  public void setBindingHandle(Object bindingHandle) {
    this.bindingHandle = bindingHandle;
  }

}
