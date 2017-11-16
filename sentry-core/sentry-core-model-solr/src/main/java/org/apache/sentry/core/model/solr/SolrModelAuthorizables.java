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
package org.apache.sentry.core.model.solr;

import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.model.solr.SolrModelAuthorizable.AuthorizableType;

public class SolrModelAuthorizables {

  private SolrModelAuthorizables() {
    // Make constructor private to avoid instantiation
  }

  public static SolrModelAuthorizable from(KeyValue keyValue) {
    String prefix = keyValue.getKey().toLowerCase();
    String name = keyValue.getValue().toLowerCase();
    SolrModelAuthorizable result = null;
    for(AuthorizableType type : AuthorizableType.values()) {
      if(prefix.equalsIgnoreCase(type.name())) {
        switch (type) {
          case Collection:
            result = new Collection(name);
            break;
          case Admin:
            result = new AdminOperation(name);
            break;
          case Config:
            result = new Config(name);
            break;
          case Schema:
            result = new Schema(name);
            break;
          default:
            break;
        }
      }
    }
    return result;
  }

  public static SolrModelAuthorizable from(String s) {
    return from(new KeyValue(s));
  }
}
