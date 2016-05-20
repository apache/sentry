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
package org.apache.sentry.provider.db.generic.service.thrift;

import java.security.PrivilegedExceptionAction;
import java.util.Set;

import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryGenericServiceIntegrationBase extends SentryServiceIntegrationBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryGenericServiceIntegrationBase.class);
  protected static final String SOLR = "SOLR";
  protected SentryGenericServiceClient client;

 /**
   * use the generic client to connect sentry service
   */
  @Override
  public void connectToSentryService() throws Exception {
    // The client should already be logged in when running in solr
    // therefore we must manually login in the integration tests
    final SentryGenericServiceClientFactory clientFactory;
    if (kerberos) {
      this.client = clientUgi.doAs( new PrivilegedExceptionAction<SentryGenericServiceClient>() {
        @Override
        public SentryGenericServiceClient run() throws Exception {
          return SentryGenericServiceClientFactory.create(conf);
        }
      });
    } else {
      this.client = SentryGenericServiceClientFactory.create(conf);
    }
  }

  @After
  public void after() {
    try {
      runTestAsSubject(new TestOperation(){
        @Override
        public void runTestAsSubject() throws Exception {
          Set<TSentryRole> tRoles = client.listAllRoles(ADMIN_USER, SOLR);
          for (TSentryRole tRole : tRoles) {
            client.dropRole(ADMIN_USER, tRole.getRoleName(), SOLR);
          }
          if(client != null) {
            client.close();
          }
        }
      });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    } finally {
      policyFilePath.delete();
    }
  }
}
