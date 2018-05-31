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
package org.apache.sentry.api.service.thrift;

import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test various kerberos related stuff on the SentryService side
 */
public class TestSentryServiceWithKerberos extends SentryServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {
    SERVER_KERBEROS_NAME = "sentry/_HOST@" + REALM;
    SentryServiceIntegrationBase.setup();
  }

  @Override
  @Before
  public void before() throws Exception {
  }

  @Override
  @After
  public void after() {
  }

  /**
   * Test that we are correctly substituting "_HOST" if/when needed.
   *
   * @throws Exception
   */
  @Test
  public void testHostSubstitution() throws Exception {
    // We just need to ensure that we are able to correct connect to the server
    connectToSentryService();
  }

}
