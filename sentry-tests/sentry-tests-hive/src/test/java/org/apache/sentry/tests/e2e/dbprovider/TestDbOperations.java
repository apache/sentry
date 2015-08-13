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
package org.apache.sentry.tests.e2e.dbprovider;

import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.TestOperations;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestDbOperations extends TestOperations{
  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
  }
  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    System.setProperty(SentryHiveConstants.ALLOW_ALL_DDL_FOR_TEST, "true");
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();

  }

  @AfterClass
  public static void tearDownTestStaticConfiguration() throws Exception {
    AbstractTestWithStaticConfiguration.tearDownTestStaticConfiguration();
    System.setProperty(SentryHiveConstants.ALLOW_ALL_DDL_FOR_TEST, "false");
  }
}
