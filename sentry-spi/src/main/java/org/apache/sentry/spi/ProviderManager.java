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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;



/**
 * The ProviderManager that can retrieve instances of ProviderFactory for an SPI.
 *
 * This was borrowed from and inspired by the Keycloak SPI implmentation
 * http://www.keycloak.org
 * original Author <a href="mailto:sthorger@redhat.com">Stian Thorgersen</a>
 *
 */
@Slf4j
public class ProviderManager {

  private static ProviderManager instance;
  private Map<String, Spi> spiMap = new HashMap<>();
  private List<ProviderLoader> loaders = new LinkedList<ProviderLoader>();
  private ListMultimap<Class<? extends Provider>, ProviderFactory> cache = ArrayListMultimap
      .create();

  private ProviderManager(ClassLoader baseClassLoader) {
    loaders.add(new DefaultProviderLoader(baseClassLoader));
    loadSpis();
  }

  public static ProviderManager getInstance() {
    if (instance == null) {
      instance = new ProviderManager(ProviderManager.class.getClassLoader());
    }
    return instance;
  }

  private synchronized void loadSpis() {
    for (ProviderLoader loader : loaders) {
      List<Spi> spis = loader.loadSpis();
      if (spis != null) {
        for (Spi spi : spis) {
          spiMap.put(spi.getName(), spi);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public synchronized <T extends ProviderFactory> List<T> load(String service) {
    if (!spiMap.containsKey(service)) {
      throw new RuntimeException(String.format("SPI Definition for Service %s not found", service));
    }

    Spi spi = spiMap.get(service);
    if (!cache.containsKey(spi.getProviderClass())) {
      LOGGER.debug("Loading Service {}", spi.getName());
      IdentityHashMap factoryClasses = new IdentityHashMap();
      for (ProviderLoader loader : loaders) {
        List<ProviderFactory> f = loader.load(spi);
        if (f != null) {
          for (ProviderFactory pf : f) {
            // make sure there are no duplicates
            if (!factoryClasses.containsKey(pf.getClass())) {
              LOGGER.debug("Service {} provider {} loaded", spi.getName(), pf.getId());
              cache.put(spi.getProviderClass(), pf);
              factoryClasses.put(pf.getClass(), pf);
            }
          }
        }
      }
    }
    List<T> rtn = (List<T>) cache.get(spi.getProviderClass());
    return rtn == null ? Collections.EMPTY_LIST : rtn;
  }

  public synchronized <T extends ProviderFactory> T load(String service, String providerId) {
    for (ProviderFactory f : load(service)) {
      if (f.getId().equals(providerId)) {
        return (T) f;
      }
    }
    return null;
  }
}
