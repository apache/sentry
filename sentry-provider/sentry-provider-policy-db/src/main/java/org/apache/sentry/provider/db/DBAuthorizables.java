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
package org.apache.sentry.provider.db;

import org.apache.sentry.core.AccessURI;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.Database;
import org.apache.sentry.core.Server;
import org.apache.sentry.core.Table;
import org.apache.sentry.core.View;
import org.apache.sentry.core.Authorizable.AuthorizableType;
import org.apache.sentry.provider.file.KeyValue;

public class DBAuthorizables {

  public static Authorizable from(KeyValue keyValue) {
    String prefix = keyValue.getKey().toLowerCase();
    String name = keyValue.getValue().toLowerCase();
    for(AuthorizableType type : AuthorizableType.values()) {
      if(prefix.equalsIgnoreCase(type.name())) {
        return from(type, name);
      }
    }
    return null;
  }
  public static Authorizable from(String s) {
    return from(new KeyValue(s));
  }

  private static Authorizable from(AuthorizableType type, String name) {
    switch (type) {
    case Server:
      return new Server(name);
    case Db:
      return new Database(name);
    case Table:
      return new Table(name);
    case View:
      return new View(name);
    case URI:
      return new AccessURI(name);
    default:
      return null;
    }
  }
}
