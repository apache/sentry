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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests around the NoAuthorizationProvider
 */
public class TestNoAuthorizationProvider {

  @Test
  public void testNoAuthorizationProvider() {
    NoAuthorizationProvider nap = new NoAuthorizationProvider();
    assertFalse(nap.hasAccess(null, null, null, null));

    GroupMappingService gms = nap.getGroupMapping();
    try {
      assertEquals(gms.getGroups(null).size(), 0);
      assertEquals(gms.getGroups("").size(), 0);
      assertEquals(gms.getGroups("a").size(), 0);
    } catch (SentryGroupNotFoundException e) {
      Assert.fail("SentryGroupsNotFoundException should not be thrown");
    }
  }
}
