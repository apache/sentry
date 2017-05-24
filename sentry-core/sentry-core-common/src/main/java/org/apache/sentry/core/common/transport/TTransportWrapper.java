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

import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;

/**
 * Extension of Thrift Transport which also provides the endpoint address.
 * The address is represented as {@link HostAndPort} object.
 */
public final class TTransportWrapper implements Closeable {
  private final TTransport transport;
  private final HostAndPort address;

  /**
   * @param transport Thrift transport (may be in any state)
   * @param address The address associated with this transport.
   */
  TTransportWrapper(TTransport transport, HostAndPort address) {
    this.transport = transport;
    this.address = address;
  }

  /**
   * @return Thrift transport value
   */
  public TTransport getTTransport() {
    return transport;
  }

  /**
   * @return endpoint address for the transport
   */
  public HostAndPort getAddress() {
    return address;
  }

  /**
   * @return True if and only if the transport is open
   */
  public boolean isOpen() {
    return transport.isOpen();
  }

  /**
   * Flush the underlying transport
   * @throws TTransportException
   */
  public void flush() throws TTransportException {
    transport.flush();
  }

  /**
   * @return human-readable representation of a transport.
   * It includes the endpoint address and open/closed state.
   */
  @Override
  public String toString() {
    return address.toString();
  }

  /**
   * Close the underlying transport
   */
  @Override
  public void close() {
    transport.close();
  }
}
