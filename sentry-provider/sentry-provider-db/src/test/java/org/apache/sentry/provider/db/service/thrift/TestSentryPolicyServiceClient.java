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

package org.apache.sentry.provider.db.service.thrift;

import java.util.Set;

import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestSentryPolicyServiceClient extends SentryServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {
    beforeSetup();
    setupConf();
    startSentryService();
    afterSetup();
    kerberos = false;
  }

  @Test
  public void testConnectionWhenReconnect() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_r";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName);
        client.createRole(requestorUserName, roleName);
        client.listRoles(requestorUserName);
        stopSentryService();
        server = new SentryServiceFactory().create(conf);
        startSentryService();
        client.listRoles(requestorUserName);
        client.dropRole(requestorUserName, roleName);
      }
    });
  }

}