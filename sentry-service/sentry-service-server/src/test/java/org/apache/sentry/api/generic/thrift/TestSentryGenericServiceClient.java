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

package org.apache.sentry.api.generic.thrift;

import java.util.Set;

import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestSentryGenericServiceClient extends SentryGenericServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {
    SentryServiceIntegrationBase.beforeSetup();
    SentryServiceIntegrationBase.setupConf();
    SentryServiceIntegrationBase.startSentryService();
    SentryServiceIntegrationBase.afterSetup();
    SentryServiceIntegrationBase.kerberos = false;
  }

  @Test
  public void testConnectionWhenReconnect() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = SentryServiceIntegrationBase.ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(SentryServiceIntegrationBase.ADMIN_GROUP);
        String roleName = "admin_r";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName, "solr");
        client.createRole(requestorUserName, roleName, "solr");
        stopSentryService();
        SentryServiceIntegrationBase.server = SentryServiceFactory.create(SentryServiceIntegrationBase.conf);
        SentryServiceIntegrationBase.startSentryService();
        client.dropRole(requestorUserName, roleName, "solr");
      }
    });
  }

}
