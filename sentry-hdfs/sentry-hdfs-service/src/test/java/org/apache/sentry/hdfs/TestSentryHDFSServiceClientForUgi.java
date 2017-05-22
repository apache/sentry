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

package org.apache.sentry.hdfs;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceIntegrationBase;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

public class TestSentryHDFSServiceClientForUgi extends SentryHdfsServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {
    kerberos = true;
    beforeSetup();
    setupConf();
    startSentryService();
    afterSetup();
  }

  public static void setupConf() throws Exception {
    // If kerberos is enabled, SentryTransportFactory should make sure that
    // HADOOP_SECURITY_AUTHENTICATION is appropriately configured.
    SentryGenericServiceIntegrationBase.setupConf();
    conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_KERBEROS);
    conf.set(ServerConfig.SECURITY_USE_UGI_TRANSPORT, "true");
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "simple");
    UserGroupInformation.setConfiguration(conf);
  }

  /**
   * Test UserGroupInformationInitializer
   * <p>
   * Ensures that SentryTransportFactory is making sure that HADOOP_SECURITY_AUTHENTICATION
   * is appropriately configured and UserGroupInformation is initialized accordingly
   * by validating the static information in UserGroupInformation Class
   *
   * @throws Exception
   */

  @Test
  public void testUserGroupInformationInitializer() throws Exception {
    kerberos = false;
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        assert UserGroupInformation.isSecurityEnabled();
      }
    });
  }
}