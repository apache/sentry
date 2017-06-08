/**
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
 */

package org.apache.sentry.core.common.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestSentryUtils {

  @Test
  public void testCollapseNumsToString() throws Exception {
    assertEquals("Collapsed string should match",
                 SentryUtils.collapseNumsToString(Arrays.asList(1L)),
                 "[1]");

    assertEquals("Collapsed string should match",
                 SentryUtils.collapseNumsToString(Arrays.asList(1L, 2L)),
                 "[1, 2]");

    assertEquals("Collapsed string should match",
                 SentryUtils.collapseNumsToString(Arrays.asList(1L, 2L, 4L)),
                 "[1, 2, 4]");

    assertEquals("Collapsed string should match",
                 SentryUtils.collapseNumsToString(Arrays.asList(1L, 2L, 4L, 5L)),
                 "[1, 2, 4, 5]");

    assertEquals("Collapsed string should match",
                 SentryUtils.collapseNumsToString(Arrays.asList(1L, 2L, 4L, 5L, 6L)),
                 "[1, 2, 4-6]");

    assertEquals("Collapsed string should match",
                 SentryUtils.collapseNumsToString(Arrays.asList(1L, 2L, 4L, 5L, 6L, 8L)),
                 "[1, 2, 4-6, 8]");

    assertEquals("Collapsed string should just return empty string",
                 SentryUtils.collapseNumsToString(Collections.<Long>emptyList()),
                 "[]");
  }
}
