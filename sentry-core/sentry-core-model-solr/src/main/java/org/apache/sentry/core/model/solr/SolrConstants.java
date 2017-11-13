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

public final class SolrConstants {

  public static final String ALL = "*";
  public static final String QUERY = "query";
  public static final String UPDATE = "update";
  /**
   * The property of sentry.solr.service is used to distinguish itself from multiple solr services. For example, there are two
   * solr services: service1 and service2 implemented authorization via sentry, and it must set the value of
   * sentry.solr.service=service1 or service2 to communicate with sentry service for authorization
   */
  public static final String SENTRY_SOLR_SERVICE_KEY = "sentry.solr.service";
  public static final String SENTRY_SOLR_SERVICE_DEFAULT = "service1";

  public static final String CORE_ADMIN = "core";
  public static final String COLLECTION_ADMIN = "collection";
  public static final String SECURITY_ADMIN = "security";

  private SolrConstants() {
    // Make constructor private to avoid instantiation
  }
}
