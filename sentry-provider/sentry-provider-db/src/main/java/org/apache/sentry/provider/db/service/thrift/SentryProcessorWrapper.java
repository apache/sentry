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

import java.net.Socket;

import org.apache.sentry.provider.db.log.util.CommandUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SentryProcessorWrapper<I extends SentryPolicyService.Iface> extends
    SentryPolicyService.Processor<SentryPolicyService.Iface> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryProcessorWrapper.class);

  public SentryProcessorWrapper(I iface) {
    super(iface);
  }

  @Override
  public boolean process(TProtocol in, TProtocol out) throws TException {
    setIpAddress(in);
    setImpersonator(in);
    return super.process(in, out);
  }

  private void setImpersonator(final TProtocol in) {
    TTransport transport = in.getTransport();
    if (transport instanceof TSaslServerTransport) {
      String impersonator = ((TSaslServerTransport) transport).getSaslServer().getAuthorizationID();
      CommandUtil.setImpersonator(impersonator);
    }
  }

  private void setIpAddress(final TProtocol in) {
    TTransport transport = in.getTransport();
    TSocket tSocket = getUnderlyingSocketFromTransport(transport);
    if (tSocket != null) {
      setIpAddress(tSocket.getSocket());
    } else {
      LOGGER.warn("Unknown Transport, cannot determine ipAddress");
    }
  }

  private void setIpAddress(Socket socket) {
    CommandUtil.setIpAddress(socket.getInetAddress().toString());
  }

  /**
   * Returns the underlying TSocket from the transport, or null of the transport type is
   * unknown.
   */
  private TSocket getUnderlyingSocketFromTransport(TTransport transport) {
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
}
