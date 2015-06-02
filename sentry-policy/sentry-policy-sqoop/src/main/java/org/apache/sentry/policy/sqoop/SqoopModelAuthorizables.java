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
package org.apache.sentry.policy.sqoop;

import org.apache.sentry.core.model.sqoop.Connector;
import org.apache.sentry.core.model.sqoop.Job;
import org.apache.sentry.core.model.sqoop.Link;
import org.apache.sentry.core.model.sqoop.Server;
import org.apache.sentry.core.model.sqoop.SqoopAuthorizable;
import org.apache.sentry.core.model.sqoop.SqoopAuthorizable.AuthorizableType;
import org.apache.sentry.provider.common.KeyValue;

public class SqoopModelAuthorizables {
  public static SqoopAuthorizable from(KeyValue keyValue) {
    String prefix = keyValue.getKey().toLowerCase();
    String name = keyValue.getValue().toLowerCase();
    for (AuthorizableType type : AuthorizableType.values()) {
      if(prefix.equalsIgnoreCase(type.name())) {
        return from(type, name);
      }
    }
    return null;
  }

  public static SqoopAuthorizable from(String keyValue) {
    return from(new KeyValue(keyValue));
  }

  public static SqoopAuthorizable from(AuthorizableType type, String name) {
    switch(type) {
    case SERVER:
      return new Server(name);
    case JOB:
      return new Job(name);
    case CONNECTOR:
      return new Connector(name);
    case LINK:
      return new Link(name);
    default:
      return null;
    }
  }
}
