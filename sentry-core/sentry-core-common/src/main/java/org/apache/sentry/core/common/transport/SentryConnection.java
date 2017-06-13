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
 * Representation of a connection to a Sentry Server.
 * <ul>
 *   <li>Connection is initialized using the {@link #connect()} method.</li>
 *   <li>When the connection is no longer used, the {@link #done()} method should be called to
 * deallocate any resources.</li>
 * <li>If the user detected that connection is broken, they should call
 * {@link #invalidate()} method. The connection can not be used after that.</li>
 * </ul>
 */
public interface SentryConnection {
  /**
   * Connect to Sentry server.
   * Either creates a new connection or reuses an existing one.
   * @throws Exception on failure to connect.
   */
  void connect() throws Exception;

  /**
   * Disconnect from the server. May close connection or return it to a
   * pool for reuse.
   */
  void done();

  /**
   * The connection is assumed to be non-working, invalidate it.
   * Subsequent {@link #connect() call} should attempt to obtain
   * another connection.
   * <p>
   * The implementation may attempt to connect
   * to another server immediately or delay it till the call to
   * {@link #connect()}.
   */
  void invalidate();
}