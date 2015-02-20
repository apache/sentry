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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class InternalSentrySrv implements SentrySrv {

  private List<SentryService> sentryServers = Lists.newArrayList();
  private static TestingServer zkServer; // created only if in case of HA
  private static final Logger LOGGER = LoggerFactory
      .getLogger(InternalSentrySrv.class);
  private boolean isActive = false;

  public InternalSentrySrv(Configuration sentryConf, int numServers)
      throws Exception {
    // Enable HA when numServers is more that 1, start Curator TestingServer
    if (numServers > 1) {
      zkServer = new TestingServer();
      zkServer.start();
      sentryConf.setBoolean(ServerConfig.SENTRY_HA_ENABLED, true);
      sentryConf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM,
          zkServer.getConnectString());
    } else if (numServers <= 0) {
      throw new IllegalArgumentException("Invalid number of Servers: "
          + numServers + " ,must be > 0");
    }
    for (int count = 0; count < numServers; count++) {
      Configuration servConf = new Configuration(sentryConf);
      SentryService sentryServer = new SentryServiceFactory().create(servConf);
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
    sentryServer.waitForShutDown();
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
    if (zkServer != null) {
      try {
        zkServer.stop();
      } catch (IOException e) {
        LOGGER.warn("Error stoping ZK service ", e);
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
  public String getZKQuorum() throws Exception {
    if (zkServer == null) {
      throw new IOException("Sentry HA is not enabled");
    }
    return zkServer.getConnectString();
  }

  @Override
  public boolean isHaEnabled() {
    return zkServer != null;
  }

}
