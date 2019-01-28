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
package org.apache.sentry.tests.e2e.minisentry;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.common.ApiConstants.ClientConfig;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class InternalSentrySrv implements SentrySrv {

  public static class SentryServerContext implements ServerContext {
    private long contextId;

    public SentryServerContext(long contextId) {
      this.contextId = contextId;
    }

    public long getContextId() {
      return contextId;
    }
  }

  /**
   * Thrift even handler class to track client connections to Sentry service
   */
  public static class SentryThriftEvenHandler implements TServerEventHandler {
    // unique id for each client connection. We could see multiple simultaneous
    // client connections, some make it thread safe.
    private AtomicLong clientId = new AtomicLong();
    // Lists of clientId currently connected
    private List<Long> clientList = Lists.newArrayList();

    /**
     * Thrift callback when a new client is connecting
     */
    @Override
    public ServerContext createContext(TProtocol inputProto,
        TProtocol outputProto) {
      clientList.add(clientId.incrementAndGet());
      LOGGER.info("Client Connected: " + clientId.get());
      return new SentryServerContext(clientId.get());
    }

    /**
     * Thrift callback when a client is disconnecting
     */
    @Override
    public void deleteContext(ServerContext arg0, TProtocol arg1, TProtocol arg2) {
      clientList.remove(((SentryServerContext) arg0).getContextId());
      LOGGER.info("Client Disonnected: "
          + ((SentryServerContext) arg0).getContextId());
    }

    @Override
    public void preServe() {
    }

    @Override
    public void processContext(ServerContext arg0, TTransport arg1,
        TTransport arg2) {
    }

    public long getClientCount() {
      return clientList.size();
    }

    public List<Long> getClienList() {
      return clientList;
    }

    public long getClientId() {
      return clientId.get();
    }
  }

  private List<SentryService> sentryServers = Lists.newArrayList();
  private static final Logger LOGGER = LoggerFactory
      .getLogger(InternalSentrySrv.class);
  private boolean isActive = false;

  public InternalSentrySrv(Configuration sentryConf, int numServers)
      throws Exception {

    for (int count = 0; count < numServers; count++) {
      Configuration servConf = new Configuration(sentryConf);
      SentryService sentryServer = SentryServiceFactory.create(servConf);
      servConf.set(ClientConfig.SERVER_RPC_ADDRESS, sentryServer.getAddress()
          .getHostName());
      servConf.setInt(ClientConfig.SERVER_RPC_PORT, sentryServer.getAddress()
          .getPort());
      sentryServers.add(sentryServer);
    }
    isActive = true;
  }

  @Override
  public void startAll() throws Exception {
    if (!isActive) {
      throw new IllegalStateException("SentrySrv is no longer active");
    }
    for (int sentryServerNum = 0; sentryServerNum < sentryServers.size(); sentryServerNum++) {
      start(sentryServerNum);

      /*
       * In test, we don't create schema before running the test. Instead, we use dataNucleus to
       * create sentry tables when they are accessed. This creates potential deadlock when running
       * test. For example, the following shows the event sequence that causes deadlock
       * 1) thread_1 gets shared lock of SYSTABLES in order to read table SENTRY_HMS_NOTIFICATION_ID
       * 2) thread_2 gets shared lock of SYSTABLES in order to read table SENTRY_HMS_NOTIFICATION_ID
       * 3) thread_1 tries to get execution lock to create table SENTRY_HMS_NOTIFICATION_ID,
       *    and wait for execution lock because thread_2 got shared lock already.
       * 4) thread_2 tries to get execution lock to create table SENTRY_HMS_NOTIFICATION_ID,
       *    and wait for execution lock because thread_1 got shared lock already.
       * The solution is to let the instances of sentry service start with delay after the first one.
       * Specifically, let HMSfollower threads separates as far as possible by
       * (the interval / sentryServers.size()).
       *
       * This deadlock does not exist in production because schema is created before starting servioces.
       * Therefore, there is no table creation after service starts.
       */
      if (sentryServerNum > 0) {
        Thread.sleep(ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT / sentryServers.size());
      }
    }
  }

  @Override
  public void start(int serverNum) throws Exception {
    if (!isActive) {
      throw new IllegalStateException("SentrySrv is no longer active");
    }
    SentryService sentryServer = sentryServers.get(serverNum);
    sentryServer.start();

    // wait for startup
    final long start = System.currentTimeMillis();
    while (!sentryServer.isRunning()) {
      Thread.sleep(1000);
      if (System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
    sentryServer.setThriftEventHandler(new SentryThriftEvenHandler());
  }

  @Override
  public void restartHMSFollower(Configuration newConf, int serverNum,
      long sleepTime) throws Exception {
    if (!isActive) {
      throw new IllegalStateException("SentrySrv is no longer active");
    }
    SentryService sentryServer = sentryServers.get(serverNum);
    sentryServer.restartHMSFollower(newConf);
    Thread.sleep(sleepTime);
  }

  @Override
  public void stopAll() throws Exception {
    boolean cleanStop = true;
    if (!isActive) {
      throw new IllegalStateException("SentrySrv is no longer active");
    }
    for (int sentryServerNum = 0; sentryServerNum < sentryServers.size(); sentryServerNum++) {
      try {
        stop(sentryServerNum);
      } catch (Exception e) {
        LOGGER.error("Sentry Server " + sentryServerNum + " failed to stop");
        cleanStop = false;
      }
    }
    if (!cleanStop) {
      throw new IllegalStateException(
          "At least one of the servers failed to stop cleanly");
    }
  }

  @Override
  public void stop(int serverNum) throws Exception {
    if (!isActive) {
      throw new IllegalStateException("SentrySrv is no longer active");
    }
    SentryService sentryServer = sentryServers.get(serverNum);
    sentryServer.stop();
  }

  @Override
  public void close() {
    for (SentryService sentryServer : sentryServers) {
      try {
        sentryServer.stop();
      } catch (Exception e) {
        LOGGER.error("Error stoping Sentry service ", e);
      }
    }
    sentryServers.clear();
    isActive = false;
  }

  @Override
  public SentryService get(int serverNum) {
    return sentryServers.get(serverNum);
  }

  @Override
  public long getNumActiveClients(int serverNum) {
    SentryThriftEvenHandler thriftHandler = (SentryThriftEvenHandler) get(
        serverNum).getThriftEventHandler();
    LOGGER.warn("Total clients: " + thriftHandler.getClientId());
    for (Long clientId: thriftHandler.getClienList()) {
      LOGGER.warn("Got clients: " + clientId);
    }
    return thriftHandler.getClientCount();
  }

  @Override
  public long getNumActiveClients() {
    long numClients = 0;
    for (int sentryServerNum = 0; sentryServerNum < sentryServers.size(); sentryServerNum++) {
      numClients += getNumActiveClients(sentryServerNum);
    }
    return numClients;

  }

  @Override
  public long getTotalClients() {
    long totalClients = 0;
    for (int sentryServerNum = 0; sentryServerNum < sentryServers.size(); sentryServerNum++) {
      totalClients += getTotalClients(sentryServerNum);
    }
    return totalClients;
  }

  @Override
  public long getTotalClients(int serverNum) {
    SentryThriftEvenHandler thriftHandler = (SentryThriftEvenHandler) get(
        serverNum).getThriftEventHandler();
    return thriftHandler.getClientId();
  }
}
