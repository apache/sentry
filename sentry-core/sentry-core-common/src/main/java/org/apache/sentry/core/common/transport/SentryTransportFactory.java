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

package org.apache.sentry.core.common.transport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.exception.MissingConfigurationException;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Create Thrift transports suitable for talking to Sentry
 */

public class SentryTransportFactory {
  protected final Configuration conf;
  private String[] serverPrincipalParts;
  protected TTransport thriftTransport;
  private final int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryTransportFactory.class);
  // configs for connection retry
  private final int connectionFullRetryTotal;
  private final ArrayList<InetSocketAddress> endpoints;
  private final SentryClientTransportConfigInterface transportConfig;
  private static final ImmutableMap<String, String> SASL_PROPERTIES =
    ImmutableMap.of(Sasl.SERVER_AUTH, "true", Sasl.QOP, "auth-conf");

  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  public static class UgiSaslClientTransport extends TSaslClientTransport {
    UserGroupInformation ugi = null;

    public UgiSaslClientTransport(String mechanism, String protocol,
                                  String serverName, TTransport transport,
                                  boolean wrapUgi, Configuration conf)
      throws IOException, SaslException {
      super(mechanism, null, protocol, serverName, SASL_PROPERTIES, null,
        transport);
      if (wrapUgi) {
        ugi = UserGroupInformation.getLoginUser();
      }
    }

    // open the SASL transport with using the current UserGroupInformation
    // This is needed to get the current login context stored
    @Override
    public void open() throws TTransportException {
      if (ugi == null) {
        baseOpen();
      } else {
        try {
          if (ugi.isFromKeytab()) {
            ugi.checkTGTAndReloginFromKeytab();
          }
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws TTransportException {
              baseOpen();
              return null;
            }
          });
        } catch (IOException e) {
          throw new TTransportException("Failed to open SASL transport: " + e.getMessage(), e);
        } catch (InterruptedException e) {
          throw new TTransportException(
            "Interrupted while opening underlying transport: " + e.getMessage(), e);
        }
      }
    }

    private void baseOpen() throws TTransportException {
      super.open();
    }
  }

  /**
   * Initialize the object based on the sentry configuration provided.
   * List of configured servers are reordered randomly preventing all
   * clients connecting to the same server.
   *
   * @param conf            Sentry configuration
   * @param transportConfig transport configuration to use
   */
  public SentryTransportFactory(Configuration conf,
                                SentryClientTransportConfigInterface transportConfig) throws IOException {

    this.conf = conf;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    serverPrincipalParts = null;
    this.transportConfig = transportConfig;

    try {
      this.connectionTimeout = transportConfig.getServerRpcConnTimeoutInMs(conf);
      this.connectionFullRetryTotal = transportConfig.getSentryFullRetryTotal(conf);
      if(transportConfig.isKerberosEnabled(conf) &&
        transportConfig.useUserGroupInformation(conf)) {
          // Re-initializing UserGroupInformation, if needed
          UserGroupInformationInitializer.initialize(conf);
      }
      String hostsAndPortsStr = transportConfig.getSentryServerRpcAddress(conf);

      int serverPort = transportConfig.getServerRpcPort(conf);

      String[] hostsAndPortsStrArr = hostsAndPortsStr.split(",");
      HostAndPort[] hostsAndPorts = ThriftUtil.parseHostPortStrings(hostsAndPortsStrArr, serverPort);

      this.endpoints = new ArrayList<>(hostsAndPortsStrArr.length);
      for (HostAndPort endpoint : hostsAndPorts) {
        this.endpoints.add(
          new InetSocketAddress(endpoint.getHostText(), endpoint.getPort()));
        LOGGER.debug("Added server endpoint: " + endpoint.toString());
      }

      if((endpoints.size() > 1) && (transportConfig.isLoadBalancingEnabled(conf))) {
        // Reorder endpoints randomly to prevent all clients connecting to the same endpoint
        // and load balance the connections towards sentry servers
        Collections.shuffle(endpoints);
      }
    } catch (MissingConfigurationException e) {
      throw new RuntimeException("Sentry Thrift Client Creation Failed: " + e.getMessage(), e);
    }
  }

  /**
   * Initialize object based on the parameters provided provided.
   *
   * @param addr            Host address which the client needs to connect
   * @param port            Host Port which the client needs to connect
   * @param conf            Sentry configuration
   * @param transportConfig transport configuration to use
   */
  public SentryTransportFactory(String addr, int port, Configuration conf,
                                SentryClientTransportConfigInterface transportConfig) throws IOException {
    // copy the configuration because we may make modifications to it.
    this.conf = new Configuration(conf);
    serverPrincipalParts = null;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    this.transportConfig = transportConfig;

    try {
      this.endpoints = new ArrayList<>(1);
      this.endpoints.add(NetUtils.createSocketAddr(addr, port));
      this.connectionTimeout = transportConfig.getServerRpcConnTimeoutInMs(conf);
      this.connectionFullRetryTotal = transportConfig.getSentryFullRetryTotal(conf);
    } catch (MissingConfigurationException e) {
      throw new RuntimeException("Sentry Thrift Client Creation Failed: " + e.getMessage(), e);
    }
  }


  /**
   * On connection error, Iterates through all the configured servers and tries to connect.
   * On successful connection, control returns
   * On connection failure, continues iterating through all the configured sentry servers,
   * and then retries the whole server list no more than connectionFullRetryTotal times.
   * In this case, it won't introduce more latency when some server fails.
   * <p>
   * TODO: Add metrics for the number of successful connects and errors per client, and total number of retries.
   */
  public TTransport getTransport() throws IOException {
    IOException currentException = null;
    for (int retryCount = 0; retryCount < connectionFullRetryTotal; retryCount++) {
      try {
        return connectToAvailableServer();
      } catch (IOException e) {
        currentException = e;
        LOGGER.error(
                "Failed to connect to all the configured sentry servers, Retrying again");
      }
    }
    // Throws exception on reaching the connectionFullRetryTotal.
    LOGGER.error(
      String.format("Reach the max connection retry num %d ", connectionFullRetryTotal),
      currentException);
    throw currentException;
  }

  /**
   * Iterates through all the configured servers and tries to connect.
   * On connection error, tries to connect to next server.
   * Control returns on successful connection OR it's done trying to all the
   * configured servers.
   *
   * @throws IOException
   */
  private TTransport connectToAvailableServer() throws IOException {
    IOException currentException = null;
    for (InetSocketAddress addr : endpoints) {
      try {
        return connectToServer(addr);
      } catch (IOException e) {
        LOGGER.error(String.format("Failed connection to %s: %s",
          addr.toString(), e.getMessage()), e);
        currentException = e;
      }
    }
    throw currentException;
  }

  /**
   * Connect to the specified socket address and throw IOException if failed.
   *
   * @param serverAddress Address client needs to connect
   * @throws Exception if there is failure in establishing the connection.
   */
  private TTransport connectToServer(InetSocketAddress serverAddress) throws IOException {
    try {
      thriftTransport = createTransport(serverAddress);
      thriftTransport.open();
    } catch (TTransportException e) {
      throw new IOException("Failed to open transport: " + e.getMessage(), e);
    } catch (MissingConfigurationException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    LOGGER.debug("Successfully opened transport: " + thriftTransport + " to " + serverAddress);
    return thriftTransport;
  }

  /**
   * New socket is is created
   *
   * @param serverAddress
   * @return
   * @throws TTransportException
   * @throws MissingConfigurationException
   * @throws IOException
   */
  private TTransport createTransport(InetSocketAddress serverAddress)
    throws TTransportException, MissingConfigurationException, IOException {
    TTransport socket = new TSocket(serverAddress.getHostName(),
      serverAddress.getPort(), connectionTimeout);

    if (!transportConfig.isKerberosEnabled(conf)) {
      return socket;
    } else {
      String serverPrincipal = transportConfig.getSentryPrincipal(conf);
      serverPrincipal = SecurityUtil.getServerPrincipal(serverPrincipal, serverAddress.getAddress());
      LOGGER.debug("Using server kerberos principal: " + serverPrincipal);
      if (serverPrincipalParts == null) {
        serverPrincipalParts = SaslRpcServer.splitKerberosName(serverPrincipal);
        Preconditions.checkArgument(serverPrincipalParts.length == 3,
          "Kerberos principal should have 3 parts: " + serverPrincipal);
      }

      boolean wrapUgi = transportConfig.useUserGroupInformation(conf);
      return new UgiSaslClientTransport(SaslRpcServer.AuthMethod.KERBEROS.getMechanismName(),
        serverPrincipalParts[0], serverPrincipalParts[1],
        socket, wrapUgi, conf);
    }
  }

  private boolean isConnected() {
    return thriftTransport != null && thriftTransport.isOpen();
  }

  /**
   * Method currently closes the transport
   * TODO (Kalyan) Plan is to hold the transport and resuse it accross multiple client's
   * That way, new connection need not be created for each new client
   */
  public void releaseTransport() {
    close();
  }

  /**
   * Method closes the transport
   */
  public void close() {
    if (isConnected()) {
      thriftTransport.close();
    }
  }
}