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

package org.apache.sentry.tests.e2e.hdfs;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

@Ignore ("Disable sentry HA tests for now")
public class TestHDFSIntegrationWithHA extends TestHDFSIntegrationBase {
  @BeforeClass
  public static void setup() throws Exception {
    TestHDFSIntegrationBase.testSentryHA = true;
    TestHDFSIntegrationBase.setup();
  }

  // Disable the following tests for HA mode. Need to reenable them
  // once HA is ready.
  @Test
  public void testMissingScheme() throws Throwable {
    ignoreCleanUp = true;
  }

  @Test
  public void testAuthzObjOnMultipleTables() throws Throwable {
    ignoreCleanUp = true;
  }
}
