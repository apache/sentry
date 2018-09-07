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

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * This is the default Provider loader.  It loads from the same classloader as is passed to it.
 * 
 * This was borrowed from and inspired by the Keycloak SPI implmentation
 * http://www.keycloak.org
 * original Author <a href="mailto:sthorger@redhat.com">Stian Thorgersen</a>
 */
public class DefaultProviderLoader implements ProviderLoader {

  private ClassLoader classLoader;

  public DefaultProviderLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public List<Spi> loadSpis() {
    LinkedList<Spi> list = new LinkedList<>();
    for (Spi spi : ServiceLoader.load(Spi.class, classLoader)) {
      list.add(spi);
    }
    return list;
  }

  @Override
  public List<ProviderFactory> load(Spi spi) {
    LinkedList<ProviderFactory> list = new LinkedList<>();
    for (ProviderFactory f : ServiceLoader.load(spi.getProviderFactoryClass(), classLoader)) {
      list.add(f);
    }
    return list;
  }

}
