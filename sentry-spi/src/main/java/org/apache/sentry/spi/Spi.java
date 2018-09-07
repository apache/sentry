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

package org.apache.sentry.spi;

/**
 * This is the main Service Provider Interface Definition.  Custom SPIs can be implemented by
 * implementing this interface and providing a reference to that implementation in the
 * META-INF/service/org.apache.sentry.spi.Spi file.
 *
 * This was borrowed from and inspired by the Keycloak SPI implmentation
 * http://www.keycloak.org
 * original Author <a href="mailto:sthorger@redhat.com">Stian Thorgersen</a>
 */
public interface Spi {

  String getName();

  Class<? extends Provider> getProviderClass();

  Class<? extends ProviderFactory> getProviderFactoryClass();
}
