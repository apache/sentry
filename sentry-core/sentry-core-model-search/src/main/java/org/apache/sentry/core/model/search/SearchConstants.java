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
package org.apache.sentry.core.model.search;

public class SearchConstants {

  public static final String ALL = "*";
  public static final String QUERY = "query";
  public static final String UPDATE = "update";
  /**
   * The property of sentry.search.cluster was used to distinguish itself from multiple search clusters. For example, there are two
   * search clusters: cluster1 and cluster2 implemented authorization via sentry, and it must set the value of
   * sentry.search.cluster=cluster1 or cluster2 to communicate with sentry service for authorization
   */
  public static final String SENTRY_SEARCH_CLUSTER_KEY = "sentry.search.cluster";
  public static final String SENTRY_SEARCH_CLUSTER_DEFAULT = "cluster1";
}
