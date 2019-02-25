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

import com.google.common.base.Ticker;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Arrays;

import static org.mockito.Mockito.times;

public class CachingUserAttributeSourceTest {

  @Mock
  private UserAttributeSource mockUserAttributeSource;

  @Before
  public void setup() {
      MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testWithMocks(){

    // configure mock LDAP response
    Multimap<String, String> mockUserAttributes = LinkedListMultimap.create();
    mockUserAttributes.putAll("attr1", Arrays.asList("A", "B", "C"));
    mockUserAttributes.put("attr2", "DEF");
    mockUserAttributes.put("attr3", "3");
    Mockito.when(mockUserAttributeSource.getAttributesForUser(Mockito.<String>any())).thenReturn(mockUserAttributes);

    CachingUserAttributeSource cachingUserAttributeSource = new CachingUserAttributeSource(mockUserAttributeSource, SolrAttrBasedFilter.CACHE_TTL_DEFAULT, SolrAttrBasedFilter.CACHE_MAX_SIZE_DEFAULT);

    // call caching source a bunch of times...
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user1");

    // ... but make sure underlying source only got called once
    Mockito.verify(mockUserAttributeSource, times(1)).getAttributesForUser("user1");

  }

  @Test
  public void testCacheSizeWithMocks(){

    // configure mock LDAP response
    Multimap<String, String> mockUserAttributes = LinkedListMultimap.create();
    mockUserAttributes.putAll("attr1", Arrays.asList("A", "B", "C"));
    mockUserAttributes.put("attr2", "DEF");
    mockUserAttributes.put("attr3", "3");
    Mockito.when(mockUserAttributeSource.getAttributesForUser(Mockito.<String>any())).thenReturn(mockUserAttributes);

    CachingUserAttributeSource cachingUserAttributeSource = new CachingUserAttributeSource(mockUserAttributeSource, SolrAttrBasedFilter.CACHE_TTL_DEFAULT, 3);

    // call caching source a bunch of times...
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user2");
    cachingUserAttributeSource.getAttributesForUser("user3");
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user2");
    cachingUserAttributeSource.getAttributesForUser("user3");

    // ... but make sure underlying source only got called once
    Mockito.verify(mockUserAttributeSource, times(1)).getAttributesForUser("user1");
    Mockito.verify(mockUserAttributeSource, times(1)).getAttributesForUser("user2");
    Mockito.verify(mockUserAttributeSource, times(1)).getAttributesForUser("user3");

    // Now request a fourth user and therefore age out user1
    cachingUserAttributeSource.getAttributesForUser("user4");
    cachingUserAttributeSource.getAttributesForUser("user1");

    Mockito.verify(mockUserAttributeSource, times(2)).getAttributesForUser("user1");


  }

  @Test
  public void testCacheTtlWithMocks(){

    // configure mock LDAP response
    Multimap<String, String> mockUserAttributes = LinkedListMultimap.create();
    mockUserAttributes.putAll("attr1", Arrays.asList("A", "B", "C"));
    mockUserAttributes.put("attr2", "DEF");
    mockUserAttributes.put("attr3", "3");
    Mockito.when(mockUserAttributeSource.getAttributesForUser(Mockito.<String>any())).thenReturn(mockUserAttributes);

    // Create a cache with a 1s TTL
    FastForwardTicker time = new FastForwardTicker();
    CachingUserAttributeSource cachingUserAttributeSource = new CachingUserAttributeSource(mockUserAttributeSource, 1, 100, time);

    // call caching source a bunch of times...
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user2");
    cachingUserAttributeSource.getAttributesForUser("user3");
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user2");
    cachingUserAttributeSource.getAttributesForUser("user3");

    // ... but make sure underlying source only got called once
    Mockito.verify(mockUserAttributeSource, times(1)).getAttributesForUser("user1");
    Mockito.verify(mockUserAttributeSource, times(1)).getAttributesForUser("user2");
    Mockito.verify(mockUserAttributeSource, times(1)).getAttributesForUser("user3");

    // "Wait" for 2 seconds
    time.fastForward(Duration.ofSeconds(2));

    // Now let the cache age out the entries
    cachingUserAttributeSource.getAttributesForUser("user1");
    cachingUserAttributeSource.getAttributesForUser("user2");
    cachingUserAttributeSource.getAttributesForUser("user3");

    Mockito.verify(mockUserAttributeSource, times(2)).getAttributesForUser("user1");
    Mockito.verify(mockUserAttributeSource, times(2)).getAttributesForUser("user2");
    Mockito.verify(mockUserAttributeSource, times(2)).getAttributesForUser("user3");

  }

  private static class FastForwardTicker extends Ticker {
    private long tnanos = 0L;

    public void fastForward(Duration interval){
      tnanos += interval.toNanos();
    }

    @Override
    public long read() {
      return tnanos;
    }
  }
}
