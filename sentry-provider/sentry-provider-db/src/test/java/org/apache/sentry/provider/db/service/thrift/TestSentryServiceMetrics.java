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
 * Unless createRequired by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.thrift;

import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.*;

public class TestSentryServiceMetrics extends SentryServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {
    kerberos = false;
    beforeSetup();
    setupConf();
    startSentryService();
    afterSetup();
  }

  //Overriding this method as the tests do not require a client handle
  @Override
  @Before
  public void before() throws Exception {

  }
  /* SENTRY-1319 */
  @Test
  public void testSentryServiceGauges() throws Throwable {
    //More Cases to be added once Sentry HA is implemented

    //Check for gauges with the server handle.
    Assert.assertEquals(new Boolean(false),server.getIsHAGauge().getValue());
    Assert.assertEquals(new Boolean(true),server.getIsActiveGauge().getValue());
  }

  //Overriding this method as the client handle does not exist.
  @Override
  @After
  public void after() {

  }

}
