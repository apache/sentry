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
package org.apache.sentry.tests.e2e.solr.db.integration;

import java.util.Arrays;
import java.util.List;

import com.carrotsearch.randomizedtesting.ThreadFilter;


/**
 * This class suppresses the known thread leaks in the Sentry service.
 * It should be removed after these thread leaks are fixed.
 */
public class SentryServiceThreadLeakFilter implements ThreadFilter {
  private static final List<String> KNOWN_LEAKED_THREAD_NAMES =
      Arrays.asList(
          "BoneCP-keep-alive-scheduler",
          "BoneCP-pool-watch-thread",
          "SentryService-0",
          "BoneCP-release-thread-helper-thread",
          "com.google.common.base.internal.Finalizer",
          "derby.rawStoreDaemon");

  @Override
  public boolean reject(Thread arg0) {
    String tName = arg0.getName();
    return tName.startsWith("Timer") || KNOWN_LEAKED_THREAD_NAMES.contains(tName);
  }
}
