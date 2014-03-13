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

import java.security.PrivilegedActionException;

import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSentryServiceFailureCase extends SentryServiceIntegrationBase {

  @Before @Override
  public void setup() throws Exception {
    beforeSetup();
    setupConf();
    conf.set(ServerConfig.ALLOW_CONNECT, "");
    startSentryService();
    afterSetup();
  }

  @Test(expected = PrivilegedActionException.class)
  public void testClientServerConnectionFailure()  throws Exception {
    connectToSentryService();
    Assert.fail("Failed to receive Exception");
  }
}
