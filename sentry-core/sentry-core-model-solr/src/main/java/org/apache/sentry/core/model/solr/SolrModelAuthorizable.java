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

import org.apache.sentry.core.common.Authorizable;

import com.google.common.annotations.VisibleForTesting;

public abstract class SolrModelAuthorizable implements Authorizable {

  public enum AuthorizableType {
    Collection,
    Field,
    Admin,
    Config,
    Schema
  };

  private final AuthorizableType type;
  private final String name;

  protected SolrModelAuthorizable(AuthorizableType type, String name) {
    this.type = type;
    this.name = name;
  }

  @Override
  public String getTypeName() {
    return type.name();
  }

  @Override
  public String getName() {
    return name;
  }

  @VisibleForTesting
  public AuthorizableType getAuthzType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("%s[name=%s]", getTypeName(), name);
  }
}
