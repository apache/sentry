/*
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
 *
 */

package org.apache.sentry.server.provider.webservice;


import org.apache.sentry.spi.Provider;
import org.apache.sentry.spi.ProviderFactory;
import org.apache.sentry.spi.Spi;

/**
 * Service Provider definition for Sentry Web Services
 */
public class WebServiceSpi implements Spi {

  public static final String ID = "sentry-web-service";

  @Override
  public String getName() {
    return ID;
  }

  @Override
  public Class<? extends Provider> getProviderClass() {
    return WebServiceProvider.class;
  }

  @Override
  public Class<? extends ProviderFactory> getProviderFactoryClass() {
    return WebServiceProviderFactory.class;
  }
}
