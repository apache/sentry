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
package org.apache.sentry.service.thrift;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * SentryClientInvocationHandler is the base interface for all the InvocationHandler in SENTRY
 */
public abstract class SentryClientInvocationHandler implements InvocationHandler {

  /**
   * Close the InvocationHandler: An InvocationHandler may create some contexts,
   * these contexts should be close when the method "close()" of client be called.
   */
  @Override
  public final Object invoke(Object proxy, Method method, Object[] args) throws Exception {
    // close() doesn't throw exception we supress that in case of connection
    // loss. Changing SentryPolicyServiceClient#close() to throw an
    // exception would be a backward incompatible change for Sentry clients.
    if ("close".equals(method.getName()) && null == args) {
      close();
      return null;
    }
    return invokeImpl(proxy, method, args);
  }

  /**
   * Subclass should implement this method for special function
   */
  public abstract Object invokeImpl(Object proxy, Method method, Object[] args) throws Exception;

  /**
   * An abstract method "close", an invocationHandler should close its contexts at here.
   */
  public abstract void close();

}
