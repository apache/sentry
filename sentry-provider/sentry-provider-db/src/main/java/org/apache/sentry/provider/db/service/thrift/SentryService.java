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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.sentry.service.api.SentryThriftService;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryService implements Runnable {

  private static final Logger LOGGER = LoggerFactory
                                       .getLogger(SentryService.class);
  //TODO:make port configurable
  private final int SENTRY_POLICY_SERVICE_DEFAULT_PORT = 8038;
  private TServer tServer;
  private String hostname;
  private int port;
  private boolean isReady;

  public SentryService() {
    isReady = false;
  }

  //TODO: create the correct transport factory based on server config
  // return SASL transport appropriately
  private TTransportFactory getTransportFactory() {
    return new TTransportFactory();
  }

  //TODO: look at server config and create socket based on it
  private TServerSocket createTServerSocket(int port) throws TTransportException {
    return new TServerSocket(port);
  }

  private void createTThreadPoolServer() {
    //TODO:make these part of service configuration
    int minWorkerThreads = 5;
    int maxWorkerThreads = 20;
    //boolean tcpKeepAlive = true;
    //TODO:look at server config before deciding to use default
    port = SENTRY_POLICY_SERVICE_DEFAULT_PORT;

    //TODO: Add SASL support, SPS can be run only with SASL enabled
    boolean useSasl = false;
    SentryServiceHandler SentryServiceHandler = new SentryServiceHandler("sentry-policy-service", null);
    try
    {
      TProcessor processor =
        new SentryThriftService.Processor<SentryThriftService.Iface>(SentryServiceHandler);
      TServerTransport serverTransport = createTServerSocket(port);

      TTransportFactory transportFactory = getTransportFactory();

      TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport)
      .processor(processor)
      .transportFactory(transportFactory)
      .protocolFactory(new TBinaryProtocol.Factory())
      .minWorkerThreads(minWorkerThreads)
      .maxWorkerThreads(maxWorkerThreads);

      tServer = new TThreadPoolServer(args);
      LOGGER.info("Starting SentryService on port " + port + "...");
      tServer.serve();
      isReady = true;

    } catch(TTransportException e) {
      e.printStackTrace();
      LOGGER.error(e.toString());
    }
  }

  public void run() {
    createTThreadPoolServer();
  }

  public synchronized void startSentryService() throws TException
  {
    new Thread(this).start();
  }

  public synchronized void stopSentryService() throws TException {
    if (isReady == true) {
      tServer.stop();
      isReady = false;
    }
    LOGGER.info("Stopped SentryService...");
  }

  //TODO: Add JVM shutdown hook
  public static void main(String[] args) throws TException
  {
    SentryService server = new SentryService();
    server.startSentryService();
  }
}