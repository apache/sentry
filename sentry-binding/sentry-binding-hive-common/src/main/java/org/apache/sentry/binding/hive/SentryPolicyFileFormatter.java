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

package org.apache.sentry.binding.hive;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

/**
 * SentryPolicyFileFormatter is to parse file and write data to file for sentry mapping data.
 */
public interface SentryPolicyFileFormatter {

  // write the sentry mapping data to file
  void write(String resourcePath, Configuration conf, Map<String, Map<String, Set<String>>> sentryMappingData)
      throws Exception;

  // parse the sentry mapping data from file
  Map<String, Map<String, Set<String>>> parse(String resourcePath, Configuration conf)
      throws Exception;

}
