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

/**
 * Client interface for Proxy Invocation handlers
 * <p>
 * Defines interface that Sentry client's should expose to the Invocation handlers like
 * <code>RetryClientInvocationHandler</code> used to proxy the method invocation on sentry
 * client instances .
 * <p>
 * All the sentry clients that need retrying and failover capabilities should implement
 * this interface.
 */
public interface SentryServiceClient {
  /**
   * Connect to Sentry server.
   * Either creates a new connection or reuses an existing one.
   * @throws Exception on failure to acquire a transport towards server.
   */
  void connect() throws Exception;

  /**
   * Disconnect from the server. May close connection or return it to a
   * pool for reuse.
   */
  void disconnect();
}