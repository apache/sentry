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


import java.io.File;
import java.util.Set;

import org.apache.sentry.core.common.utils.PolicyFile;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Test various kerberos related stuff on the SentryService side
 */
public class TestSentryServiceForHAWithKerberos extends SentryServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {
    SentryServiceIntegrationBase.kerberos = true;
    SentryServiceIntegrationBase.haEnabled = true;
    SentryServiceIntegrationBase.SERVER_KERBEROS_NAME = "sentry/_HOST@" + SentryServiceIntegrationBase.REALM;
    SentryServiceIntegrationBase.beforeSetup();
    SentryServiceIntegrationBase.setupConf();
    SentryServiceIntegrationBase.startSentryService();
    SentryServiceIntegrationBase.afterSetup();
  }

  @Override
  @Before
  public void before() throws Exception {
    policyFilePath = new File(SentryServiceIntegrationBase.dbDir, "local_policy_file.ini");
    SentryServiceIntegrationBase.conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
      policyFilePath.getPath());
    policyFile = new PolicyFile();
    connectToSentryService();
  }

  @Test
  public void testCreateRole() throws Exception {
    runTestAsSubject(new TestOperation(){
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = SentryServiceIntegrationBase.ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(SentryServiceIntegrationBase.ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();
        String roleName = "admin_r";
        client.dropRoleIfExists(requestorUserName, roleName);
        client.createRole(requestorUserName, roleName);
        client.dropRole(requestorUserName, roleName);
      }
    });
  }
}
