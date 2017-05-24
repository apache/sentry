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

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Test sentry service with a larger message size than the server's or client's thrift max message size.
 */
public class TestSentryServiceWithInvalidMsgSize extends SentryServiceIntegrationBase {
  private final Set<String> REQUESTER_USER_GROUP_NAMES = Sets.newHashSet(ADMIN_GROUP);
  private final String ROLE_NAME = "admin_r";

  /**
   * Test the case when the message size is larger than the client's thrift max message size.
   */
  @Test
  public void testClientWithSmallMaxMsgSize() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        SentryServiceClientFactory oldFactory = SentryServiceClientFactory.factoryReset(null);
        Configuration confWithSmallMaxMsgSize = new Configuration(conf);
        confWithSmallMaxMsgSize.setLong(ServiceConstants.ClientConfig.SENTRY_POLICY_CLIENT_THRIFT_MAX_MESSAGE_SIZE, 20);
        // create a client with a small thrift max message size
        SentryPolicyServiceClient clientWithSmallMaxMsgSize = SentryServiceClientFactory.create(confWithSmallMaxMsgSize);

        setLocalGroupMapping(ADMIN_USER, REQUESTER_USER_GROUP_NAMES);
        writePolicyFile();

        boolean exceptionThrown = false;
        try {
          // client throws exception when message size is larger than the client's thrift max message size.
          clientWithSmallMaxMsgSize.listRoles(ADMIN_USER);
        } catch (SentryUserException e) {
          exceptionThrown = true;
          Assert.assertTrue(e.getMessage().contains("Thrift exception occurred"));
          Assert.assertTrue(e.getCause().getMessage().contains("Length exceeded max allowed"));
        } finally {
          Assert.assertEquals(true, exceptionThrown);
          clientWithSmallMaxMsgSize.close();
          SentryServiceClientFactory.factoryReset(oldFactory);
        }

        // client can still talk with sentry server when message size is smaller.
        client.dropRoleIfExists(ADMIN_USER, ROLE_NAME);
        client.listRoles(ADMIN_USER);
        client.createRole(ADMIN_USER, ROLE_NAME);
        client.listRoles(ADMIN_USER);
      }
    });
  }

  /**
   * Test the case when the message size is larger than the server's thrift max message size.
   */
  @Test
  public void testServerWithSmallMaxMsgSize() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Configuration confWithSmallMaxMsgSize = new Configuration(conf);
        confWithSmallMaxMsgSize.setLong(ServiceConstants.ServerConfig.SENTRY_POLICY_SERVER_THRIFT_MAX_MESSAGE_SIZE,
            50);
        stopSentryService();

        // create a server with a small max thrift message size
        server = new SentryServiceFactory().create(confWithSmallMaxMsgSize);
        startSentryService();

        setLocalGroupMapping(ADMIN_USER, REQUESTER_USER_GROUP_NAMES);
        writePolicyFile();

        // client can talk with server when message size is smaller.
        client.listRoles(ADMIN_USER);
        client.createRole(ADMIN_USER, ROLE_NAME);

        boolean exceptionThrown = false;
        try {
          // client throws exception when message size is larger than the server's thrift max message size.
          client.grantServerPrivilege(ADMIN_USER, ROLE_NAME, "server", false);
        } catch (SentryUserException e) {
          exceptionThrown = true;
          Assert.assertTrue(e.getCause().getMessage().contains("org.apache.thrift.transport.TTransportException"));
        } finally {
          Assert.assertEquals(true, exceptionThrown);
        }

        // client can still talk with sentry server when message size is smaller.
        Set<TSentryRole> roles = client.listRoles(ADMIN_USER);
        Assert.assertTrue(roles.size() == 1);
        Assert.assertEquals(ROLE_NAME, roles.iterator().next().getRoleName());
      }
    });
  }
}
