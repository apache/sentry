/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.component;

import org.apache.solr.common.params.SolrParams;

/**
 * Params interface to hold named key-value pairs for configuring {@link UserAttributeSource} subclasses
 * To be initialized from a {@link SolrParams} object.
 */
public interface UserAttributeSourceParams {

  /**
   * @param solrParams from which to initialize this object
   */
  void init(SolrParams solrParams);
}
