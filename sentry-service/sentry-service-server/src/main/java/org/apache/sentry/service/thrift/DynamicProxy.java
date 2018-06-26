/*
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * This class can be used to create proxy instances for interface implementations that do not
 * explicitly implement the interface.
 * In practice, this is just a development convenience when writing pluggable implementations
 * and shouldn't be encouraged to use in production, as there can be use cases that may result
 * in run-time errors, for example, interface default methods are not supported.
 *
 * Other than that, this class does validate if all methods are implemented by the given
 * implementation.
 */
public class DynamicProxy<T> implements InvocationHandler {
  private final Object obj;
  private final Class<T> iface;

  public DynamicProxy(Object obj, Class<T> iface, String objectClassName) {
    this.obj = checkNotNull(obj);
    this.iface = checkNotNull(iface);
    validateObjectImplementsInterface(objectClassName);
  }

  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Method proxiedMethod = findMethodBySignature(method);
    return proxiedMethod.invoke(obj, args);
  }

  private Method findMethodBySignature(Method method) throws NoSuchMethodException {
    return obj.getClass().getMethod(method.getName(),
      method.getParameterTypes());
  }

  private void validateObjectImplementsInterface(String className) {
    for (Method method : iface.getMethods()) {
      try {
        findMethodBySignature(method);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(
          String.format("Class %s does not implement %s.", className, iface.getName()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public T createProxy() {
    return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] {iface}, this);
  }
}
