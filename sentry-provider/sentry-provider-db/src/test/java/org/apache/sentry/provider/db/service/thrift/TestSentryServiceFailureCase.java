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

package org.apache.sentry.provider.db.service.thrift;

import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class TestSentryServiceFailureCase extends SentryServiceIntegrationBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestSentryServiceFailureCase.class);
  private static final String PEER_CALLBACK_FAILURE = "Peer indicated failure: Problem with callback handler";

  @BeforeClass
  public static void setup() throws Exception {
    kerberos = true;
    beforeSetup();
    setupConf();
    conf.set(ServerConfig.ALLOW_CONNECT, "");
    startSentryService();
    afterSetup();
  }

  @Override
  @Before
  public void before() throws Exception {
  }

  @Override
  @After
  public void after() {
  }

  @Test
  public void testClientServerConnectionFailure()  throws Exception {
    try {
      connectToSentryService();
      String requestorUserName = ADMIN_USER;
      client.listRoles(requestorUserName);
      Assert.fail("Failed to receive Exception");
    } catch(Exception e) {
      LOGGER.info("Excepted exception", e);
      Throwable cause = e.getCause();
      if (cause == null) {
        throw e;
      }
      String msg = "Exception message: " + cause.getMessage();
      Assert.assertTrue(msg, Strings.nullToEmpty(cause.getMessage())
          .contains(PEER_CALLBACK_FAILURE));
    }
  }
}
