/*
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
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.sasl.Sasl;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

/**
 * Factory for producing connected Thrift transports.
 * It can produce regular transports as well as Kerberos-enabled transports.
 * <p>
 * This class is immutable and thus thread-safe.
 */
@ThreadSafe
public final class SentryTransportFactory implements TransportFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryTransportFactory.class);

  private final Configuration conf;
  private final boolean useUgi;
  private final String serverPrincipal;
  private final int connectionTimeout;
  private final boolean isKerberosEnabled;
  private static final ImmutableMap<String, String> SASL_PROPERTIES =
    ImmutableMap.of(Sasl.SERVER_AUTH, "true", Sasl.QOP, "auth-conf");

  /**
   * Initialize the object based on the sentry configuration provided.
   *
   * @param conf            Sentry configuration
   * @param transportConfig transport configuration to use
   */
  public SentryTransportFactory(Configuration conf,
                         SentryClientTransportConfigInterface transportConfig) {

    this.conf = conf;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    connectionTimeout = transportConfig.getServerRpcConnTimeoutInMs(conf);
    isKerberosEnabled = transportConfig.isKerberosEnabled(conf);
    if (isKerberosEnabled) {
      useUgi = transportConfig.useUserGroupInformation(conf);
      serverPrincipal = transportConfig.getSentryPrincipal(conf);
    } else {
      serverPrincipal = null;
      useUgi = false;
    }
  }

  /**
   * Connect to the endpoint and return a connected Thrift transport.
   * @return Connection to the endpoint
   * @throws IOException
   */
  @Override
  public TTransportWrapper getTransport(HostAndPort endpoint) throws IOException {
    return new TTransportWrapper(connectToServer(new InetSocketAddress(endpoint.getHostText(),
                                                 endpoint.getPort())),
                                 endpoint);
  }

  /**
   * Connect to the specified socket address and throw IOException if failed.
   *
   * @param serverAddress Address client needs to connect
   * @throws Exception if there is failure in establishing the connection.
   */
  private TTransport connectToServer(InetSocketAddress serverAddress) throws IOException {
    try {
      TTransport thriftTransport = createTransport(serverAddress);
      thriftTransport.open();
      return thriftTransport;
    } catch (TTransportException e) {
      throw new IOException("Failed to open transport: " + e.getMessage(), e);
    }
  }

  /**
   * Create transport given InetSocketAddress
   * @param serverAddress - endpoint address
   * @return unconnected transport
   * @throws TTransportException
   * @throws IOException
   */
  @SuppressWarnings("squid:S2095")
  private TTransport createTransport(InetSocketAddress serverAddress)
          throws IOException {
    String hostName = serverAddress.getHostName();
    int port = serverAddress.getPort();
    TTransport socket = new TSocket(hostName, port, connectionTimeout);

    if (!isKerberosEnabled) {
      LOGGER.debug("created unprotected connection to {}:{} ", hostName, port);
      return socket;
    }

    String principal = SecurityUtil.getServerPrincipal(serverPrincipal, serverAddress.getAddress());
    String[] serverPrincipalParts = SaslRpcServer.splitKerberosName(principal);
    if (serverPrincipalParts.length != 3) {
      throw new IOException("Kerberos principal should have 3 parts: " + principal);
    }

    UgiSaslClientTransport connection =
            new UgiSaslClientTransport(SaslRpcServer.AuthMethod.KERBEROS.getMechanismName(),
              serverPrincipalParts[0], serverPrincipalParts[1],
              socket, useUgi);

    LOGGER.debug("creating secured connection to {}:{} ", hostName, port);
    return connection;
  }

  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  private static class UgiSaslClientTransport extends TSaslClientTransport {
    private UserGroupInformation ugi = null;

    UgiSaslClientTransport(String mechanism, String protocol,
                           String serverName, TTransport transport,
                           boolean wrapUgi)
            throws IOException {
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
          Thread.currentThread().interrupt();
          throw new TTransportException(
                  "Interrupted while opening underlying transport: " + e.getMessage(), e);
        }
      }
    }

    private void baseOpen() throws TTransportException {
      super.open();
    }
  }
}