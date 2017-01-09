/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.tests.e2e.metastore;

import org.junit.BeforeClass;


/**
 * Make sure we are able to capture all HMS object and path changes using Hive's default JSONMessageFactory and
 * Sentry's JSON deserializer. This would make sure Sentry is able to read the Notification logs written by
 * Hive's JSONMessageFactory.
 */
public class TestDefaultMessageFactorySentryDeserializer extends TestSentryMessageFactorySentryDeserializer {

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    setMetastoreListener = true;
    useDefaultMessageFactory = true;
    enableNotificationLog = true;
    AbstractMetastoreTestWithStaticConfiguration.setupTestStaticConfiguration();
    setupClass();
  }
}

