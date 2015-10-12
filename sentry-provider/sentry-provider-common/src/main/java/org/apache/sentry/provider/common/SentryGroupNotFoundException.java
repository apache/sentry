/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.common;

public class SentryGroupNotFoundException extends RuntimeException {
  private static final long serialVersionUID = -116202866086371881L;

  /**
   * Creates a new SentryGroupNotFoundException.
   */
  public SentryGroupNotFoundException() {
    super();
  }

  /**
   * Constructs a new SentryGroupNotFoundException.
   *
   * @param message
   *        the reason for the exception
   */
  public SentryGroupNotFoundException(String message) {
    super(message);
  }

  /**
   * Constructs a new SentryGroupNotFoundException.
   *
   * @param cause
   *        the underlying Throwable that caused this exception to be thrown.
   */
  public SentryGroupNotFoundException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a new SentryGroupNotFoundException.
   *
   * @param message
   *        the reason for the exception
   * @param cause
   *        the underlying Throwable that caused this exception to be thrown.
   */
  public SentryGroupNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
