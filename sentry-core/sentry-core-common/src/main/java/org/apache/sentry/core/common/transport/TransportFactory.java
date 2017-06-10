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

import java.io.IOException;

/**
 * Generic transport factory interface.
 * <p>
 * The intention is to implement transport pool in more abstract terms
 * and be able to test it without actually connecting to any servers by
 * implementing mock transport factories.
 */
public interface TransportFactory {
  /**
   * Connect to the endpoint and return a connected Thrift transport.
   * @return Connection to the endpoint
   * @throws IOException
   */
  TTransportWrapper getTransport(HostAndPort endpoint) throws IOException;
}
