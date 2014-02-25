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
package org.apache.sentry.core.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.shiro.config.ConfigurationException;

public class SentryConfigurationException extends ConfigurationException {
  private List<String> configErrors = new ArrayList<String>();
  private List<String> configWarnings = new ArrayList<String>();

  public boolean hasWarnings() {
    return !configWarnings.isEmpty();
  }

  public boolean hasErrors() {
    return !configErrors.isEmpty();
  }

  public SentryConfigurationException() {
    super();
  }

  public SentryConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public SentryConfigurationException(String message) {
    super(message);
  }

  public SentryConfigurationException(Throwable cause) {
    super(cause);
  }

  public List<String> getConfigErrors() {
    return configErrors;
  }

  public void setConfigErrors(List<String> configErrors) {
    this.configErrors = configErrors;
  }

  public List<String> getConfigWarnings() {
    return configWarnings;
  }

  public void setConfigWarnings(List<String> configWarnings) {
    this.configWarnings = configWarnings;
  }
}
