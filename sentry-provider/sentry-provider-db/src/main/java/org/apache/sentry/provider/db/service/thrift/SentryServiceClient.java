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

import java.net.URI;

import org.apache.sentry.service.api.SentryThriftService;
import org.apache.sentry.service.api.TCreateSentryRoleRequest;
import org.apache.sentry.service.api.TCreateSentryRoleResponse;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryServiceClient {

  private SentryThriftService.Client client;
  private TTransport transport;
  private URI policyStoreURI;
  private int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceClient.class);

  //TODO: Read connectionTimeout, policyStoreURI from conf
  public SentryServiceClient() {
    this.connectionTimeout = 20 * 1000;
  }

  private void init() throws TTransportException {
    if (policyStoreURI == null) {
      transport = new TSocket("localhost", 8038, connectionTimeout);
    }
    transport.open();
    LOGGER.info("Successfully opened transport");
    client = new SentryThriftService.Client(new TBinaryProtocol(transport));
    LOGGER.info("Successfully created client");
  }

  public static SentryServiceClient createClient() throws TTransportException {
    SentryServiceClient c = new SentryServiceClient();
    c.init();
    return c;
  }

  public static void destroyClient(SentryServiceClient c) {
    c.close();
  }

  public TCreateSentryRoleResponse createRole(TCreateSentryRoleRequest req) throws TException {
    return client.create_sentry_role(req);
  }

  private void close() {
    if (transport != null) {
      transport.close();
    }
  }
}