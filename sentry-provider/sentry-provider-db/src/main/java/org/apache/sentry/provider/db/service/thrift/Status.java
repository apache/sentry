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

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.annotation.Nullable;

import org.apache.sentry.policystore.api.TSentryResponseStatus;
import org.apache.sentry.policystore.api.TSentryStatus;

/**
 * Simple factory to make returning TSentryStatus objects easy
 */
public class Status {
  public static TSentryResponseStatus OK() {
    return Create(TSentryStatus.OK, "");
  }
  public static TSentryResponseStatus AlreadyExists(String message, Throwable t) {
    return Create(TSentryStatus.ALREADY_EXISTS, message, t);
  }
  public static TSentryResponseStatus NoSuchObject(String message, Throwable t) {
    return Create(TSentryStatus.NO_SUCH_OBJECT, message, t);
  }
  public static TSentryResponseStatus Create(TSentryStatus value, String message) {
    return Create(value, null, null);
  }
  public static TSentryResponseStatus Create(TSentryStatus value, String message, @Nullable Throwable t) {
    TSentryResponseStatus status = new TSentryResponseStatus();
    status.setValue(value);
    status.setMessage(message);
    if (t != null) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      t.printStackTrace(printWriter);
      printWriter.close();
      status.setStack(stringWriter.toString());
    }
    return status;
  }
}