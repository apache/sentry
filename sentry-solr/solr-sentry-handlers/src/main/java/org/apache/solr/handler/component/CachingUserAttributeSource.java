/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Decorator class for any other UserAttributeSource which implements a simple cache around the UserAttributeSource
 * The cache avoids repeated calls to the UAS upon repeated Solr queries
 */
public class CachingUserAttributeSource implements UserAttributeSource {

  private static final Logger LOG = LoggerFactory.getLogger(CachingUserAttributeSource.class);
  private final LoadingCache<String, Multimap<String, String>> cache;

  /**
   * @param userAttributeSource {@link UserAttributeSource} being decorated
   * @param ttlSeconds Time To Live (seconds) for the cache before entries will be aged
   * @param maxCacheSize The maximum number of entries the cache should contain before entries are evicted
   */
  public CachingUserAttributeSource(final UserAttributeSource userAttributeSource, long ttlSeconds, long maxCacheSize) {
    this(userAttributeSource, ttlSeconds, maxCacheSize, null);
  }

  /**
   * @param userAttributeSource {@link UserAttributeSource} being decorated
   * @param ttlSeconds Time To Live (seconds) for the cache before entries will be aged
   * @param maxCacheSize The maximum number of entries the cache should contain before entries are evicted
   * @param ticker A {@link Ticker} used for testing cache expiry. If null the system clock will be used
   */
  @VisibleForTesting
  /* default */ CachingUserAttributeSource(final UserAttributeSource userAttributeSource, long ttlSeconds, long maxCacheSize, Ticker ticker) {
    LOG.debug("Creating cached user attribute source, userAttributeSource={}, ttlSeconds={}, maxCacheSize={}", userAttributeSource, ttlSeconds, maxCacheSize);
    CacheLoader<String, Multimap<String, String>> cacheLoader = new CacheLoader<String, Multimap<String, String>>() {
      public Multimap<String, String> load(String userName) {
        LOG.debug("User attribute cache miss for user: {}", userName);
        return userAttributeSource.getAttributesForUser(userName);
      }
    };
    CacheBuilder builder = CacheBuilder.newBuilder().expireAfterWrite(ttlSeconds, TimeUnit.SECONDS).maximumSize(maxCacheSize);
    if (ticker != null) {
      builder.ticker(ticker);
    }
    cache = builder.build(cacheLoader);
  }

  @Override
  public Multimap<String, String> getAttributesForUser(String userName) {
    try {
      return cache.get(userName);
    } catch (ExecutionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error getting user attributes from cache", e);
    }
  }

  @Override
  public Class<? extends UserAttributeSourceParams> getParamsClass() {
    return null;
  }

  @Override
  public void init(UserAttributeSourceParams params, Collection<String> attributes) {
  }
}
