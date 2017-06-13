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

package org.apache.sentry.core.common.utils;

import com.google.common.net.HostAndPort;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ThriftUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftUtil.class);

  public static void setImpersonator(final TProtocol in) {
    try {
      TTransport transport = in.getTransport();
      if (transport instanceof TSaslServerTransport) {
        String impersonator = ((TSaslServerTransport) transport).getSaslServer()
            .getAuthorizationID();
        setImpersonator(impersonator);
      }
    } catch (Exception e) {
      // If there has exception when get impersonator info, log the error information.
      LOGGER.warn("There is an error when get the impersonator:" + e.getMessage());
    }
  }

  public static void setIpAddress(final TProtocol in) {
    try {
      TTransport transport = in.getTransport();
      TSocket tSocket = getUnderlyingSocketFromTransport(transport);
      if (tSocket != null) {
        setIpAddress(tSocket.getSocket().getInetAddress().toString());
      } else {
        LOGGER.warn("Unknown Transport, cannot determine ipAddress");
      }
    } catch (Exception e) {
      // If there has exception when get impersonator info, log the error information.
      LOGGER.warn("There is an error when get the client's ip address:" + e.getMessage());
    }
  }

  /**
   * Returns the underlying TSocket from the transport, or null of the transport type is unknown.
   */
  private static TSocket getUnderlyingSocketFromTransport(TTransport transport) {
    Preconditions.checkNotNull(transport);
    if (transport instanceof TSaslServerTransport) {
      return (TSocket) ((TSaslServerTransport) transport).getUnderlyingTransport();
    } else if (transport instanceof TSaslClientTransport) {
      return (TSocket) ((TSaslClientTransport) transport).getUnderlyingTransport();
    } else if (transport instanceof TSocket) {
      return (TSocket) transport;
    }
    return null;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return "";
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  private static ThreadLocal<String> threadLocalImpersonator = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return "";
    }
  };

  public static void setImpersonator(String impersonator) {
    threadLocalImpersonator.set(impersonator);
  }

  public static String getImpersonator() {
    return threadLocalImpersonator.get();
  }

  /**
   * Utility function for parsing host and port strings. Expected form should be
   * (host:port). The hostname could be in ipv6 style. If port is not specified,
   * defaultPort will be used.
   */
  public static HostAndPort parseAddress(String address, int defaultPort) {
    return HostAndPort.fromString(address).withDefaultPort(defaultPort);
  }

}